use std::{collections::BTreeMap, pin::Pin, sync::Arc};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    compute::concat_batches,
    datatypes::{DataType, Field, SchemaBuilder, SchemaRef},
    error::ArrowError,
    ipc::{
        reader::StreamReader,
        writer::{IpcWriteOptions, StreamWriter},
    },
};
use arrow_flight::{
    decode::{DecodedPayload, FlightDataDecoder},
    sql::{
        self,
        server::{FlightSqlService as ArrowFlightSqlService, PeekableFlightDataStream},
        ActionBeginSavepointRequest, ActionBeginSavepointResult, ActionBeginTransactionRequest,
        ActionBeginTransactionResult, ActionCancelQueryRequest, ActionCancelQueryResult,
        ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
        ActionCreatePreparedStatementResult, ActionCreatePreparedSubstraitPlanRequest,
        ActionEndSavepointRequest, ActionEndTransactionRequest, Any, CommandGetCatalogs,
        CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys,
        CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes,
        CommandGetTables, CommandGetXdbcTypeInfo, CommandPreparedStatementQuery,
        CommandPreparedStatementUpdate, CommandStatementQuery, CommandStatementSubstraitPlan,
        CommandStatementUpdate, DoPutPreparedStatementResult, ProstMessageExt as _, SqlInfo,
        TicketStatementQuery,
    },
};
use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    IpcMessage, SchemaAsIpc, Ticket,
};
use datafusion::{
    common::{arrow::datatypes::Schema, ParamValues},
    dataframe::DataFrame,
    datasource::TableType,
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::{SQLOptions, SessionContext, SessionState},
    logical_expr::LogicalPlan,
    physical_plan::SendableRecordBatchStream,
    scalar::ScalarValue,
};
use datafusion_substrait::{
    logical_plan::consumer::from_substrait_plan, serializer::deserialize_bytes,
};
use futures::{Stream, StreamExt, TryStreamExt};
use log::info;
use once_cell::sync::Lazy;
use prost::bytes::Bytes;
use prost::Message;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use super::session::{SessionStateProvider, StaticSessionStateProvider};
use super::state::{CommandTicket, QueryHandle};

type Result<T, E = Status> = std::result::Result<T, E>;

/// FlightSqlService is a basic stateless FlightSqlService implementation.
pub struct FlightSqlService {
    provider: Box<dyn SessionStateProvider>,
    sql_options: Option<SQLOptions>,
}

impl FlightSqlService {
    /// Creates a new FlightSqlService with a static SessionState.
    pub fn new(state: SessionState) -> Self {
        Self::new_with_provider(Box::new(StaticSessionStateProvider::new(state)))
    }

    /// Creates a new FlightSqlService with a SessionStateProvider.
    pub fn new_with_provider(provider: Box<dyn SessionStateProvider>) -> Self {
        Self {
            provider,
            sql_options: None,
        }
    }

    /// Replaces the sql_options with the provided options.
    /// These options are used to verify all SQL queries.
    /// When None the default [`SQLOptions`] are used.
    pub fn with_sql_options(self, sql_options: SQLOptions) -> Self {
        Self {
            sql_options: Some(sql_options),
            ..self
        }
    }

    // Federate substrait plans instead of SQL
    // pub fn with_substrait() -> Self {
    // TODO: Substrait federation
    // }

    // Serves straightforward on the specified address.
    pub async fn serve(self, addr: String) -> Result<(), Box<dyn std::error::Error>> {
        let addr = addr.parse()?;
        info!("Listening on {addr:?}");

        let svc = FlightServiceServer::new(self);

        Ok(Server::builder().add_service(svc).serve(addr).await?)
    }

    async fn new_context<T>(
        &self,
        request: Request<T>,
    ) -> Result<(Request<T>, FlightSqlSessionContext)> {
        let (metadata, extensions, msg) = request.into_parts();
        let inspect_request = Request::from_parts(metadata, extensions, ());

        let state = self.provider.new_context(&inspect_request).await?;
        let ctx = SessionContext::new_with_state(state);

        let (metadata, extensions, _) = inspect_request.into_parts();
        Ok((
            Request::from_parts(metadata, extensions, msg),
            FlightSqlSessionContext {
                inner: ctx,
                sql_options: self.sql_options,
            },
        ))
    }
}

/// The schema for GetTableTypes
static GET_TABLE_TYPES_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    //TODO: Move this into arrow-flight itself, similar to the builder pattern for CommandGetCatalogs and CommandGetDbSchemas
    Arc::new(Schema::new(vec![Field::new(
        "table_type",
        DataType::Utf8,
        false,
    )]))
});

struct FlightSqlSessionContext {
    inner: SessionContext,
    sql_options: Option<SQLOptions>,
}

impl FlightSqlSessionContext {
    async fn sql_to_logical_plan(&self, sql: &str) -> DataFusionResult<LogicalPlan> {
        let plan = self.inner.state().create_logical_plan(sql).await?;
        let verifier = self.sql_options.unwrap_or_default();
        verifier.verify_plan(&plan)?;
        Ok(plan)
    }

    async fn execute_sql(&self, sql: &str) -> DataFusionResult<SendableRecordBatchStream> {
        let plan = self.sql_to_logical_plan(sql).await?;
        self.execute_logical_plan(plan).await
    }

    async fn execute_logical_plan(
        &self,
        plan: LogicalPlan,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.inner
            .execute_logical_plan(plan)
            .await?
            .execute_stream()
            .await
    }
}

#[tonic::async_trait]
impl ArrowFlightSqlService for FlightSqlService {
    type FlightService = FlightSqlService;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse>> + Send>>>> {
        info!("do_handshake");
        // Favor middleware over handshake
        // https://github.com/apache/arrow/issues/23836
        // https://github.com/apache/arrow/issues/25848
        Err(Status::unimplemented("handshake is not supported"))
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        _message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        let (request, ctx) = self.new_context(request).await?;

        let ticket = CommandTicket::try_decode(request.into_inner().ticket)
            .map_err(flight_error_to_status)?;

        match ticket.command {
            sql::Command::CommandStatementQuery(CommandStatementQuery { query, .. }) => {
                // print!("Query: {query}\n");

                let stream = ctx.execute_sql(&query).await.map_err(df_error_to_status)?;
                let arrow_schema = stream.schema();
                let arrow_stream = stream.map(|i| {
                    let batch = i.map_err(|e| FlightError::ExternalError(e.into()))?;
                    Ok(batch)
                });

                let flight_data_stream = FlightDataEncoderBuilder::new()
                    .with_schema(arrow_schema)
                    .build(arrow_stream)
                    .map_err(flight_error_to_status)
                    .boxed();

                Ok(Response::new(flight_data_stream))
            }
            sql::Command::CommandPreparedStatementQuery(CommandPreparedStatementQuery {
                prepared_statement_handle,
            }) => {
                let handle = QueryHandle::try_decode(prepared_statement_handle)?;

                let mut plan = ctx
                    .sql_to_logical_plan(handle.query())
                    .await
                    .map_err(df_error_to_status)?;

                if let Some(param_values) =
                    decode_param_values(handle.parameters()).map_err(arrow_error_to_status)?
                {
                    plan = plan
                        .with_param_values(param_values)
                        .map_err(df_error_to_status)?;
                }

                let stream = ctx
                    .execute_logical_plan(plan)
                    .await
                    .map_err(df_error_to_status)?;
                let arrow_schema = stream.schema();
                let arrow_stream = stream.map(|i| {
                    let batch = i.map_err(|e| FlightError::ExternalError(e.into()))?;
                    Ok(batch)
                });

                let flight_data_stream = FlightDataEncoderBuilder::new()
                    .with_schema(arrow_schema)
                    .build(arrow_stream)
                    .map_err(flight_error_to_status)
                    .boxed();

                Ok(Response::new(flight_data_stream))
            }
            sql::Command::CommandStatementSubstraitPlan(CommandStatementSubstraitPlan {
                plan,
                ..
            }) => {
                let substrait_bytes = &plan
                    .ok_or(Status::invalid_argument(
                        "Expected substrait plan, found None",
                    ))?
                    .plan;

                let plan = parse_substrait_bytes(&ctx, substrait_bytes).await?;

                let state = ctx.inner.state();
                let df = DataFrame::new(state, plan);

                let stream = df.execute_stream().await.map_err(df_error_to_status)?;
                let arrow_schema = stream.schema();
                let arrow_stream = stream.map(|i| {
                    let batch = i.map_err(|e| FlightError::ExternalError(e.into()))?;
                    Ok(batch)
                });

                let flight_data_stream = FlightDataEncoderBuilder::new()
                    .with_schema(arrow_schema)
                    .build(arrow_stream)
                    .map_err(flight_error_to_status)
                    .boxed();

                Ok(Response::new(flight_data_stream))
            }
            _ => {
                return Err(Status::internal(format!(
                    "statement handle not found: {:?}",
                    ticket.command
                )));
            }
        }
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        let (request, ctx) = self.new_context(request).await?;

        let sql = &query.query;
        info!("get_flight_info_statement with query={sql}");

        let flight_descriptor = request.into_inner();

        let plan = ctx
            .sql_to_logical_plan(sql)
            .await
            .map_err(df_error_to_status)?;

        let dataset_schema = get_schema_for_plan(&plan);

        // Form the response ticket (that the client will pass back to DoGet)
        let ticket = CommandTicket::new(sql::Command::CommandStatementQuery(query))
            .try_encode()
            .map_err(flight_error_to_status)?;

        let endpoint = FlightEndpoint::new().with_ticket(Ticket { ticket });

        let flight_info = FlightInfo::new()
            .with_endpoint(endpoint)
            // return descriptor we were passed
            .with_descriptor(flight_descriptor)
            .try_with_schema(dataset_schema.as_ref())
            .map_err(arrow_error_to_status)?;

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_substrait_plan(
        &self,
        query: CommandStatementSubstraitPlan,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_substrait_plan");
        let (request, ctx) = self.new_context(request).await?;

        let substrait_bytes = &query
            .plan
            .as_ref()
            .ok_or(Status::invalid_argument(
                "Expected substrait plan, found None",
            ))?
            .plan;

        let plan = parse_substrait_bytes(&ctx, substrait_bytes).await?;

        let flight_descriptor = request.into_inner();

        let dataset_schema = get_schema_for_plan(&plan);

        // Form the response ticket (that the client will pass back to DoGet)
        let ticket = CommandTicket::new(sql::Command::CommandStatementSubstraitPlan(query))
            .try_encode()
            .map_err(flight_error_to_status)?;

        let endpoint = FlightEndpoint::new().with_ticket(Ticket { ticket });

        let flight_info = FlightInfo::new()
            .with_endpoint(endpoint)
            // return descriptor we were passed
            .with_descriptor(flight_descriptor)
            .try_with_schema(dataset_schema.as_ref())
            .map_err(arrow_error_to_status)?;

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        let (request, ctx) = self.new_context(request).await?;

        let handle = QueryHandle::try_decode(cmd.prepared_statement_handle.clone())
            .map_err(|e| Status::internal(format!("Error decoding handle: {e}")))?;

        info!("get_flight_info_prepared_statement with handle={handle}");

        let flight_descriptor = request.into_inner();

        let sql = handle.query();
        let plan = ctx
            .sql_to_logical_plan(sql)
            .await
            .map_err(df_error_to_status)?;

        let dataset_schema = get_schema_for_plan(&plan);

        // Form the response ticket (that the client will pass back to DoGet)
        let ticket = CommandTicket::new(sql::Command::CommandPreparedStatementQuery(cmd))
            .try_encode()
            .map_err(flight_error_to_status)?;

        let endpoint = FlightEndpoint::new().with_ticket(Ticket { ticket });

        let flight_info = FlightInfo::new()
            .with_endpoint(endpoint)
            // return descriptor we were passed
            .with_descriptor(flight_descriptor)
            .try_with_schema(dataset_schema.as_ref())
            .map_err(arrow_error_to_status)?;

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_catalogs");
        let (request, _ctx) = self.new_context(request).await?;

        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(arrow_error_to_status)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_schemas");
        let (request, _ctx) = self.new_context(request).await?;
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(arrow_error_to_status)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_tables");
        let (request, _ctx) = self.new_context(request).await?;

        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(arrow_error_to_status)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_table_types");
        let (request, _ctx) = self.new_context(request).await?;

        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&GET_TABLE_TYPES_SCHEMA)
            .map_err(arrow_error_to_status)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_sql_info");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented("Implement CommandGetSqlInfo"))
    }

    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_primary_keys");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented(
            "Implement get_flight_info_primary_keys",
        ))
    }

    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_exported_keys");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented(
            "Implement get_flight_info_exported_keys",
        ))
    }

    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_imported_keys");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented(
            "Implement get_flight_info_imported_keys",
        ))
    }

    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_cross_reference");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented(
            "Implement get_flight_info_cross_reference",
        ))
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_xdbc_type_info");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented(
            "Implement get_flight_info_xdbc_type_info",
        ))
    }

    async fn do_get_statement(
        &self,
        _ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_statement");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented("Implement do_get_statement"))
    }

    async fn do_get_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_prepared_statement");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented("Implement do_get_prepared_statement"))
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_catalogs");
        let (_request, ctx) = self.new_context(request).await?;
        let catalog_names = ctx.inner.catalog_names();

        let mut builder = query.into_builder();
        for catalog_name in &catalog_names {
            builder.append(catalog_name);
        }
        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_schemas");
        let (_request, ctx) = self.new_context(request).await?;
        let catalog_name = query.catalog.clone();
        // Append all schemas to builder, the builder handles applying the filters.
        let mut builder = query.into_builder();
        if let Some(catalog_name) = &catalog_name {
            if let Some(catalog) = ctx.inner.catalog(catalog_name) {
                for schema_name in &catalog.schema_names() {
                    builder.append(catalog_name, schema_name);
                }
            }
        };

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_tables");
        let (_request, ctx) = self.new_context(request).await?;
        let catalog_name = query.catalog.clone();
        let mut builder = query.into_builder();
        // Append all schemas/tables to builder, the builder handles applying the filters.
        if let Some(catalog_name) = &catalog_name {
            if let Some(catalog) = ctx.inner.catalog(catalog_name) {
                for schema_name in &catalog.schema_names() {
                    if let Some(schema) = catalog.schema(schema_name) {
                        for table_name in &schema.table_names() {
                            if let Some(table) =
                                schema.table(table_name).await.map_err(df_error_to_status)?
                            {
                                builder
                                    .append(
                                        catalog_name,
                                        schema_name,
                                        table_name,
                                        table.table_type().to_string(),
                                        &table.schema(),
                                    )
                                    .map_err(flight_error_to_status)?;
                            }
                        }
                    }
                }
            }
        };

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_table_types");
        let (_, _) = self.new_context(request).await?;

        // Report all variants of table types that datafusion uses.
        let table_types: ArrayRef = Arc::new(StringArray::from(
            vec![TableType::Base, TableType::View, TableType::Temporary]
                .into_iter()
                .map(|tt| tt.to_string())
                .collect::<Vec<String>>(),
        ));

        let batch = RecordBatch::try_from_iter(vec![("table_type", table_types)]).unwrap();

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(GET_TABLE_TYPES_SCHEMA.clone())
            .build(futures::stream::once(async { Ok(batch) }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_sql_info");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented("Implement do_get_sql_info"))
    }

    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_primary_keys");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented("Implement do_get_primary_keys"))
    }

    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_exported_keys");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented("Implement do_get_exported_keys"))
    }

    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_imported_keys");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented("Implement do_get_imported_keys"))
    }

    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_cross_reference");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented("Implement do_get_cross_reference"))
    }

    async fn do_get_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_xdbc_type_info");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented("Implement do_get_xdbc_type_info"))
    }

    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        info!("do_put_statement_update");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented("Implement do_put_statement_update"))
    }

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        info!("do_put_prepared_statement_query");
        let (request, _) = self.new_context(request).await?;

        let mut handle = QueryHandle::try_decode(query.prepared_statement_handle)?;

        info!(
            "do_action_create_prepared_statement query={:?}",
            handle.query()
        );
        // Collect request flight data as parameters
        // Decode and encode as a single ipc stream
        let mut decoder =
            FlightDataDecoder::new(request.into_inner().map_err(status_to_flight_error));
        let schema = decode_schema(&mut decoder).await?;
        let mut parameters = Vec::new();
        let mut encoder =
            StreamWriter::try_new(&mut parameters, &schema).map_err(arrow_error_to_status)?;
        let mut total_rows = 0;
        while let Some(msg) = decoder.try_next().await? {
            match msg.payload {
                DecodedPayload::None => {}
                DecodedPayload::Schema(_) => {
                    return Err(Status::invalid_argument(
                        "parameter flight data must contain a single schema",
                    ));
                }
                DecodedPayload::RecordBatch(record_batch) => {
                    total_rows += record_batch.num_rows();
                    encoder
                        .write(&record_batch)
                        .map_err(arrow_error_to_status)?;
                }
            }
        }
        if total_rows > 1 {
            return Err(Status::invalid_argument(
                "parameters should contain a single row",
            ));
        }

        handle.set_parameters(Some(parameters.into()));

        let res = DoPutPreparedStatementResult {
            prepared_statement_handle: Some(Bytes::from(handle)),
        };

        Ok(res)
    }

    async fn do_put_prepared_statement_update(
        &self,
        _handle: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        info!("do_put_prepared_statement_update");
        let (_, _) = self.new_context(request).await?;

        // statements like "CREATE TABLE.." or "SET datafusion.nnn.." call this function
        // and we are required to return some row count here
        Ok(-1)
    }

    async fn do_put_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        info!("do_put_prepared_statement_update");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented(
            "Implement do_put_prepared_statement_update",
        ))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let (_, ctx) = self.new_context(request).await?;

        let sql = query.query.clone();
        info!(
            "do_action_create_prepared_statement query={:?}",
            query.query
        );

        let plan = ctx
            .sql_to_logical_plan(sql.as_str())
            .await
            .map_err(df_error_to_status)?;

        let dataset_schema = get_schema_for_plan(&plan);
        let parameter_schema = parameter_schema_for_plan(&plan).map_err(|e| e.as_ref().clone())?;

        let dataset_schema =
            encode_schema(dataset_schema.as_ref()).map_err(arrow_error_to_status)?;
        let parameter_schema =
            encode_schema(parameter_schema.as_ref()).map_err(arrow_error_to_status)?;

        let handle = QueryHandle::new(sql, None);

        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: Bytes::from(handle),
            dataset_schema,
            parameter_schema,
        };

        Ok(res)
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        let (_, _) = self.new_context(request).await?;

        let handle = query.prepared_statement_handle.as_ref();
        if let Ok(handle) = std::str::from_utf8(handle) {
            info!("do_action_close_prepared_statement with handle {handle:?}",);

            // NOP since stateless
        }
        Ok(())
    }

    async fn do_action_create_prepared_substrait_plan(
        &self,
        _query: ActionCreatePreparedSubstraitPlanRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        info!("do_action_create_prepared_substrait_plan");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented(
            "Implement do_action_create_prepared_substrait_plan",
        ))
    }

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        let (_, _) = self.new_context(request).await?;

        info!("do_action_begin_transaction");
        Err(Status::unimplemented(
            "Implement do_action_begin_transaction",
        ))
    }

    async fn do_action_end_transaction(
        &self,
        _query: ActionEndTransactionRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        info!("do_action_end_transaction");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented("Implement do_action_end_transaction"))
    }

    async fn do_action_begin_savepoint(
        &self,
        _query: ActionBeginSavepointRequest,
        request: Request<Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        info!("do_action_begin_savepoint");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented("Implement do_action_begin_savepoint"))
    }

    async fn do_action_end_savepoint(
        &self,
        _query: ActionEndSavepointRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        info!("do_action_end_savepoint");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented("Implement do_action_end_savepoint"))
    }

    async fn do_action_cancel_query(
        &self,
        _query: ActionCancelQueryRequest,
        request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        info!("do_action_cancel_query");
        let (_, _) = self.new_context(request).await?;

        Err(Status::unimplemented("Implement do_action_cancel_query"))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Takes a substrait plan serialized as [Bytes] and deserializes this to
/// a Datafusion [LogicalPlan]
async fn parse_substrait_bytes(
    ctx: &FlightSqlSessionContext,
    substrait: &Bytes,
) -> Result<LogicalPlan> {
    let substrait_plan = deserialize_bytes(substrait.to_vec())
        .await
        .map_err(df_error_to_status)?;

    from_substrait_plan(&ctx.inner.state(), &substrait_plan)
        .await
        .map_err(df_error_to_status)
}

/// Encodes the schema IPC encoded (schema_bytes)
fn encode_schema(schema: &Schema) -> std::result::Result<Bytes, ArrowError> {
    let options = IpcWriteOptions::default();

    // encode the schema into the correct form
    let message: Result<IpcMessage, ArrowError> = SchemaAsIpc::new(schema, &options).try_into();

    let IpcMessage(schema) = message?;

    Ok(schema)
}

/// Return the schema for the specified logical plan
fn get_schema_for_plan(logical_plan: &LogicalPlan) -> SchemaRef {
    // gather real schema, but only
    let schema = Schema::from(logical_plan.schema().as_ref()).into();

    // Use an empty FlightDataEncoder to determine the schema of the encoded flight data.
    // This is necessary as the schema can change based on dictionary hydration behavior.
    let flight_data_stream = FlightDataEncoderBuilder::new()
        // Inform the builder of the input stream schema
        .with_schema(schema)
        .build(futures::stream::iter([]));

    // Retrieve the schema of the encoded data
    flight_data_stream
        .known_schema()
        .expect("flight data schema should be known when explicitly provided via `with_schema`")
}

fn parameter_schema_for_plan(plan: &LogicalPlan) -> Result<SchemaRef, Box<Status>> {
    let parameters = plan
        .get_parameter_types()
        .map_err(df_error_to_status)?
        .into_iter()
        .map(|(name, dt)| {
            dt.map(|dt| (name.clone(), dt)).ok_or_else(|| {
                Status::internal(format!(
                    "unable to determine type of query parameter {name}"
                ))
            })
        })
        // Collect into BTreeMap so we get a consistent order of the parameters
        .collect::<Result<BTreeMap<_, _>, Status>>()?;

    let mut builder = SchemaBuilder::new();
    parameters
        .into_iter()
        .for_each(|(name, typ)| builder.push(Field::new(name, typ, false)));
    Ok(builder.finish().into())
}

fn arrow_error_to_status(err: ArrowError) -> Status {
    Status::internal(format!("{err:?}"))
}

fn flight_error_to_status(err: FlightError) -> Status {
    Status::internal(format!("{err:?}"))
}

fn df_error_to_status(err: DataFusionError) -> Status {
    Status::internal(format!("{err:?}"))
}

fn status_to_flight_error(status: Status) -> FlightError {
    FlightError::Tonic(Box::new(status))
}

async fn decode_schema(decoder: &mut FlightDataDecoder) -> Result<SchemaRef, Status> {
    while let Some(msg) = decoder.try_next().await? {
        match msg.payload {
            DecodedPayload::None => {}
            DecodedPayload::Schema(schema) => {
                return Ok(schema);
            }
            DecodedPayload::RecordBatch(_) => {
                return Err(Status::invalid_argument(
                    "parameter flight data must have a known schema",
                ));
            }
        }
    }

    Err(Status::invalid_argument(
        "parameter flight data must have a schema",
    ))
}

// Decode parameter ipc stream as ParamValues
fn decode_param_values(
    parameters: Option<&[u8]>,
) -> Result<Option<ParamValues>, arrow::error::ArrowError> {
    parameters
        .map(|parameters| {
            let decoder = StreamReader::try_new(parameters, None)?;
            let schema = decoder.schema();
            let batches = decoder.into_iter().collect::<Result<Vec<_>, _>>()?;
            let batch = concat_batches(&schema, batches.iter())?;
            Ok(record_to_param_values(&batch)?)
        })
        .transpose()
}

// Converts a record batch with a single row into ParamValues
fn record_to_param_values(batch: &RecordBatch) -> Result<ParamValues, DataFusionError> {
    let mut param_values: Vec<(String, Option<usize>, ScalarValue)> = Vec::new();

    let mut is_list = true;
    for col_index in 0..batch.num_columns() {
        let array = batch.column(col_index);
        let scalar = ScalarValue::try_from_array(array, 0)?;
        let name = batch
            .schema_ref()
            .field(col_index)
            .name()
            .trim_start_matches('$')
            .to_string();
        let index = name.parse().ok();
        is_list &= index.is_some();
        param_values.push((name, index, scalar));
    }
    if is_list {
        let mut values: Vec<(Option<usize>, ScalarValue)> = param_values
            .into_iter()
            .map(|(_name, index, value)| (index, value))
            .collect();
        values.sort_by_key(|(index, _value)| *index);
        Ok(values
            .into_iter()
            .map(|(_index, value)| value)
            .collect::<Vec<ScalarValue>>()
            .into())
    } else {
        Ok(param_values
            .into_iter()
            .map(|(name, _index, value)| (name, value))
            .collect::<Vec<(String, ScalarValue)>>()
            .into())
    }
}
