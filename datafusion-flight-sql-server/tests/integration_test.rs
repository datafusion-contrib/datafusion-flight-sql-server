use std::sync::Arc;

use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion::arrow::{
    array::{Int32Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use datafusion::{
    datasource::MemTable,
    execution::context::{SessionContext, SessionState},
};
use datafusion_flight_sql_server::service::FlightSqlService;
use futures::TryStreamExt;
use tokio::time::{sleep, Duration};
use tonic::transport::{Channel, Endpoint};

fn create_test_session() -> SessionState {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();

    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("users", Arc::new(table)).unwrap();

    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int32, false),
        Field::new("user_id", DataType::Int32, false),
        Field::new("amount", DataType::Int32, false),
    ]));

    let orders_batch = RecordBatch::try_new(
        orders_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![100, 101, 102, 103])),
            Arc::new(Int32Array::from(vec![1, 2, 1, 3])),
            Arc::new(Int32Array::from(vec![50, 75, 100, 25])),
        ],
    )
    .unwrap();

    let orders_table = MemTable::try_new(orders_schema, vec![vec![orders_batch]]).unwrap();
    ctx.register_table("orders", Arc::new(orders_table))
        .unwrap();

    ctx.state()
}

async fn start_test_server(addr: String, state: SessionState) {
    tokio::spawn(async move {
        FlightSqlService::new(state)
            .serve(addr)
            .await
            .expect("Server should start successfully");
    });

    sleep(Duration::from_millis(500)).await;
}

async fn create_test_client(addr: &str) -> FlightSqlServiceClient<Channel> {
    let endpoint = Endpoint::new(addr.to_string()).expect("Valid endpoint");
    let channel = endpoint.connect().await.expect("Connection successful");
    FlightSqlServiceClient::new(channel)
}

#[tokio::test]
async fn test_basic_query_execution() {
    let addr = "0.0.0.0:50061";
    let state = create_test_session();
    start_test_server(addr.to_string(), state).await;

    let mut client = create_test_client(&format!("http://{}", addr)).await;

    let flight_info = client
        .execute("SELECT * FROM users".to_string(), None)
        .await
        .expect("Query should succeed");

    let ticket = flight_info
        .endpoint
        .first()
        .expect("Should have endpoint")
        .ticket
        .clone()
        .expect("Should have ticket");

    let mut stream = client.do_get(ticket).await.expect("do_get should succeed");

    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await.expect("Stream should work") {
        batches.push(batch);
    }

    assert!(!batches.is_empty(), "Should have result batches");

    let first_batch = &batches[0];
    assert_eq!(first_batch.num_columns(), 2);
    assert_eq!(first_batch.schema().field(0).name(), "id");
    assert_eq!(first_batch.schema().field(1).name(), "name");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_query_with_filter() {
    let addr = "0.0.0.0:50062";
    let state = create_test_session();
    start_test_server(addr.to_string(), state).await;

    let mut client = create_test_client(&format!("http://{}", addr)).await;

    let flight_info = client
        .execute("SELECT name FROM users WHERE id > 1".to_string(), None)
        .await
        .expect("Query should succeed");

    let ticket = flight_info
        .endpoint
        .first()
        .expect("Should have endpoint")
        .ticket
        .clone()
        .expect("Should have ticket");

    let mut stream = client.do_get(ticket).await.expect("do_get should succeed");

    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await.expect("Stream should work") {
        batches.push(batch);
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Should have 2 rows after filter");
}

#[tokio::test]
async fn test_prepared_statement_creation() {
    let addr = "0.0.0.0:50063";
    let state = create_test_session();
    start_test_server(addr.to_string(), state).await;

    let mut client = create_test_client(&format!("http://{}", addr)).await;

    let query = "SELECT * FROM users WHERE id = $1";
    let prepared = client
        .prepare(query.to_string(), None)
        .await
        .expect("Prepare should succeed");

    let dataset_schema = prepared
        .dataset_schema()
        .expect("Should have dataset schema");
    assert_eq!(dataset_schema.fields().len(), 2);

    let parameter_schema = prepared
        .parameter_schema()
        .expect("Should have parameter schema");
    assert_eq!(parameter_schema.fields().len(), 1);
}

#[tokio::test]
async fn test_get_schemas() {
    let addr = "0.0.0.0:50064";
    let state = create_test_session();
    start_test_server(addr.to_string(), state).await;

    let mut client = create_test_client(&format!("http://{}", addr)).await;

    let flight_info = client
        .get_db_schemas(arrow_flight::sql::CommandGetDbSchemas {
            catalog: Some("datafusion".to_string()),
            db_schema_filter_pattern: None,
        })
        .await
        .expect("GetDbSchemas should succeed");

    let ticket = flight_info
        .endpoint
        .first()
        .expect("Should have endpoint")
        .ticket
        .clone()
        .expect("Should have ticket");

    let mut stream = client.do_get(ticket).await.expect("do_get should succeed");

    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await.expect("Stream should work") {
        batches.push(batch);
    }

    assert!(!batches.is_empty(), "Should have schema results");
}

#[tokio::test]
async fn test_get_tables() {
    let addr = "0.0.0.0:50065";
    let state = create_test_session();
    start_test_server(addr.to_string(), state).await;

    let mut client = create_test_client(&format!("http://{}", addr)).await;

    let flight_info = client
        .get_tables(arrow_flight::sql::CommandGetTables {
            catalog: Some("datafusion".to_string()),
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: true,
        })
        .await
        .expect("GetTables should succeed");

    let ticket = flight_info
        .endpoint
        .first()
        .expect("Should have endpoint")
        .ticket
        .clone()
        .expect("Should have ticket");

    let mut stream = client.do_get(ticket).await.expect("do_get should succeed");

    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await.expect("Stream should work") {
        batches.push(batch);
    }

    assert!(!batches.is_empty(), "Should have table results");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0, "Should have at least one table");
}

#[tokio::test]
async fn test_invalid_query() {
    let addr = "0.0.0.0:50066";
    let state = create_test_session();
    start_test_server(addr.to_string(), state).await;

    let mut client = create_test_client(&format!("http://{}", addr)).await;

    let result = client
        .execute("SELECT * FROM nonexistent_table".to_string(), None)
        .await;

    assert!(result.is_err(), "Query should fail for nonexistent table");
}

#[tokio::test]
async fn test_query_with_aggregation() {
    let addr = "0.0.0.0:50067";
    let state = create_test_session();
    start_test_server(addr.to_string(), state).await;

    let mut client = create_test_client(&format!("http://{}", addr)).await;

    let flight_info = client
        .execute("SELECT COUNT(*) as count FROM users".to_string(), None)
        .await
        .expect("Query should succeed");

    let ticket = flight_info
        .endpoint
        .first()
        .expect("Should have endpoint")
        .ticket
        .clone()
        .expect("Should have ticket");

    let mut stream = client.do_get(ticket).await.expect("do_get should succeed");

    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await.expect("Stream should work") {
        batches.push(batch);
    }

    assert!(!batches.is_empty(), "Should have result batches");

    let first_batch = &batches[0];
    assert_eq!(first_batch.num_columns(), 1);
    assert_eq!(first_batch.schema().field(0).name(), "count");
}

#[tokio::test]
async fn test_query_with_join() {
    let addr = "0.0.0.0:50068";
    let state = create_test_session();
    start_test_server(addr.to_string(), state).await;

    let mut client = create_test_client(&format!("http://{}", addr)).await;

    let flight_info = client
        .execute(
            r#"
            SELECT u.id, u.name, o.order_id 
                FROM users u 
                JOIN orders o 
                    ON u.id = o.user_id "#
                .to_string(),
            None,
        )
        .await
        .expect("Join query should succeed");

    let ticket = flight_info.endpoint[0].ticket.clone().unwrap();
    let mut stream = client.do_get(ticket).await.expect("do_get should succeed");

    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await.expect("Stream should work") {
        batches.push(batch);
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4, "Should have 4 rows from join");
}
