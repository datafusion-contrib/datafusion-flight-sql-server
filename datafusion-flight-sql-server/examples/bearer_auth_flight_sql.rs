//! # DataFusion Flight SQL Server with Bearer Token Authentication Example
//!
//! This example demonstrates how to integrate Bearer Token authentication into a
//! DataFusion Flight SQL server.
//!
//! Key components:
//! - `bearer_auth_interceptor`: A tonic interceptor that validates "Authorization: Bearer <token>"
//!   headers. For simplicity, it uses a hardcoded list of valid tokens ("token1", "token2").
//! - `UserData`: A simple struct holding user information (e.g., user_id) extracted from
//!   a valid token and inserted into request extensions.
//! - `MySessionStateProvider`: A custom `SessionStateProvider` that retrieves `UserData`
//!   from request extensions. This allows tailoring the `SessionState` for the request,
//!   although in this example, it primarily clones a base context after successful authentication.
//!   It also registers a "test" CSV table for querying.
//!
//! ## Running the Example
//!
//! The server will start on `0.0.0.0:50051`. The `main` function includes client code
//! that attempts to connect and perform a `GetTables` Flight SQL action using:
//! 1. A valid token ("token1") - Expected to succeed.
//! 2. An invalid token ("invalidtoken") - Expected to fail authentication.
//! 3. No token - Expected to fail authentication.
//!
//! Observe the server console output for messages from the interceptor and session provider,
//! and the client output for the success/failure of each attempt.

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::CommandGetTables;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*; // Covers SessionContext, CsvReadOptions, etc.
use datafusion::execution::context::{SessionConfig, SessionState}; // SessionState is not in prelude
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_flight_sql_server::service::FlightSqlService;
use datafusion_flight_sql_server::session::SessionStateProvider;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{metadata::MetadataValue, Request, Status};

// UserData struct remains the same
#[derive(Clone, Debug)]
pub struct UserData {
    pub user_id: u32,
}

// bearer_auth_interceptor remains the same
async fn bearer_auth_interceptor(mut req: Request<()>) -> Result<Request<()>, Status> {
    let auth_header = req
        .metadata()
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("no authorization provided"))?;

    let auth_str = auth_header
        .to_str()
        .map_err(|_| Status::unauthenticated("invalid authorization header encoding"))?;

    if !auth_str.starts_with("Bearer ") {
        return Err(Status::unauthenticated(
            "invalid authorization header format",
        ));
    }

    let token = &auth_str["Bearer ".len()..];

    let user_data = match token {
        "token1" => UserData { user_id: 1 },
        "token2" => UserData { user_id: 2 },
        _ => return Err(Status::unauthenticated("invalid token")),
    };

    req.extensions_mut().insert(user_data);
    Ok(req)
}

// Updated MySessionStateProvider
#[derive(Debug, Clone)] // Removed Default
pub struct MySessionStateProvider {
    base_context: SessionContext,
}

impl MySessionStateProvider {
    async fn try_new() -> Result<Self> { // datafusion::error::Result
        let ctx = SessionContext::new();
        // Construct path to test.csv relative to CARGO_MANIFEST_DIR of the datafusion-flight-sql-server crate
        let csv_path = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/test.csv");
        ctx.register_csv("test", csv_path, CsvReadOptions::new()).await?;
        Ok(Self { base_context: ctx })
    }
}

#[async_trait]
impl SessionStateProvider for MySessionStateProvider {
    async fn new_context(&self, request: &Request<()>) -> Result<SessionState, Status> { // tonic::Result for Status
        if let Some(user_data) = request.extensions().get::<UserData>() {
            println!(
                "Session context for user_id: {}. Cloning base context.",
                user_data.user_id
            );
            let state = self.base_context.state().clone();
            // Optional: Customize state based on user_data
            // state.set_config_option("datafusion.user_id", &user_data.user_id.to_string()).map_err(|e| Status::internal(format!("Failed to set config: {}",e)))?;
            Ok(state)
        } else {
            Err(Status::unauthenticated(
                "User data not found in request extensions (MySessionStateProvider)",
            ))
        }
    }
}

// Updated new_client_with_auth function
async fn new_client_with_auth(
    dsn: String,
    token: Option<String>,
) -> Result<FlightSqlServiceClient<Channel>> { // datafusion::error::Result
    let endpoint = Endpoint::from_shared(dsn.clone())
        .map_err(|e| DataFusionError::External(format!("Invalid DSN {}: {}", dsn, e).into()))?
        .connect_timeout(std::time::Duration::from_secs(10));

    let channel = endpoint.connect().await
        .map_err(|e| DataFusionError::External(format!("Failed to connect to {}: {}", dsn, e).into()))?;

    let service_client = FlightSqlServiceClient::with_interceptor(
        channel,
        move |mut req: Request<()>| {
            if let Some(token_str) = token.clone() {
                let bearer_token = format!("Bearer {}", token_str);
                match MetadataValue::try_from(&bearer_token) {
                    Ok(metadata_val) => req.metadata_mut().insert("authorization", metadata_val),
                    Err(_) => return Err(Status::invalid_argument("Invalid token format for metadata")),
                };
            }
            Ok(req)
        },
    );
    Ok(service_client)
}

fn status_to_df_error(err: tonic::Status) -> DataFusionError {
    DataFusionError::External(format!("Tonic status error: {}", err).into())
}

#[tokio::main]
async fn main() -> Result<()> { // datafusion::error::Result<()>
    // Server Setup
    let dsn: String = "0.0.0.0:50051".to_string();
    let state_provider = Arc::new(MySessionStateProvider::try_new().await?);
    let base_service = FlightSqlService::new_with_state_provider(state_provider).into_service();
    let wrapped_service = tonic::service::interceptor_fn(base_service, bearer_auth_interceptor);
    let addr: std::net::SocketAddr = dsn.parse().map_err(|e| DataFusionError::External(format!("Invalid address format {}: {}", dsn, e).into()))?;

    tokio::spawn(async move {
        println!("Bearer Authentication Flight SQL server listening on {}", addr);
        if let Err(e) = Server::builder().add_service(wrapped_service).serve(addr).await {
            eprintln!("Server error: {}", e);
        }
    });

    // Wait for server to run
    sleep(Duration::from_secs(3)).await;

    // Client Setup and Testing
    let client_dsn = "http://localhost:50051".to_string();

    // Test Case 1: Valid Token
    println!("\nAttempting GetTables with valid token (token1)...");
    match new_client_with_auth(client_dsn.clone(), Some("token1".to_string())).await {
        Ok(mut client) => {
            let request = CommandGetTables { catalog: None, db_schema_filter_pattern: None, table_name_filter_pattern: None, table_types: vec![], include_schema: false };
            match client.get_tables(request).await {
                Ok(response) => println!("GetTables with token1 SUCCEEDED. Response: {:?}", response.into_inner()),
                Err(e) => eprintln!("GetTables with token1 FAILED: {}", status_to_df_error(e)),
            }
        }
        Err(e) => eprintln!("Failed to create client with token1: {}", e),
    }

    // Test Case 2: Invalid Token
    println!("\nAttempting GetTables with invalid token (invalidtoken)...");
    match new_client_with_auth(client_dsn.clone(), Some("invalidtoken".to_string())).await {
        Ok(mut client) => {
            let request = CommandGetTables { catalog: None, db_schema_filter_pattern: None, table_name_filter_pattern: None, table_types: vec![], include_schema: false };
            match client.get_tables(request).await {
                Ok(response) => println!("GetTables with invalidtoken SUCCEEDED (unexpected). Response: {:?}", response.into_inner()),
                Err(e) => eprintln!("GetTables with invalidtoken FAILED (as expected): {}", status_to_df_error(e)),
            }
        }
        Err(e) => eprintln!("Failed to create client with invalidtoken: {}", e),
    }

    // Test Case 3: No Token
    println!("\nAttempting GetTables with no token...");
    match new_client_with_auth(client_dsn.clone(), None).await {
        Ok(mut client) => {
            let request = CommandGetTables { catalog: None, db_schema_filter_pattern: None, table_name_filter_pattern: None, table_types: vec![], include_schema: false };
            match client.get_tables(request).await {
                Ok(response) => println!("GetTables with no token SUCCEEDED (unexpected). Response: {:?}", response.into_inner()),
                Err(e) => eprintln!("GetTables with no token FAILED (as expected): {}", status_to_df_error(e)),
            }
        }
        Err(e) => eprintln!("Failed to create client with no token: {}", e),
    }

    Ok(())
}
