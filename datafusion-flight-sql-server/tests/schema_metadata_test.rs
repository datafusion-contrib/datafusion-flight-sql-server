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
use datafusion_flight_sql_server::{config::FlightSqlServiceConfig, service::FlightSqlService};
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
    let config = FlightSqlServiceConfig {
        schema_with_metadata: true,
    };

    let service = FlightSqlService::new(state).with_config(config);

    tokio::spawn(async move {
        service
            .serve(addr)
            .await
            .expect("Server should start successfully")
    });
    sleep(Duration::from_millis(500)).await;
}

async fn create_test_client(addr: &str) -> FlightSqlServiceClient<Channel> {
    let endpoint = Endpoint::new(addr.to_string()).expect("Valid endpoint");
    let channel = endpoint.connect().await.expect("Connection successful");
    FlightSqlServiceClient::new(channel)
}

#[tokio::test]
async fn test_schema_contains_table_name_metadata() {
    let addr = "0.0.0.0:50071";
    let state = create_test_session();
    start_test_server(addr.to_string(), state).await;

    let mut client = create_test_client(&format!("http://{}", addr)).await;

    let flight_info = client
        .execute("SELECT id, name FROM users".to_string(), None)
        .await
        .expect("Query should succeed");

    let schema = flight_info
        .try_decode_schema()
        .expect("Should decode schema");

    for field in schema.fields() {
        assert!(
            field.metadata().contains_key("table_name"),
            "Field {} should have table_name metadata",
            field.name()
        );

        assert_eq!(
            field.metadata().get("table_name").unwrap(),
            "users",
            "Field {} should have table_name='users'",
            field.name()
        );
    }
}

#[tokio::test]
async fn test_schema_metadata_with_subquery_and_join() {
    let addr = "0.0.0.0:50072";
    let state = create_test_session();
    start_test_server(addr.to_string(), state).await;

    let mut client = create_test_client(&format!("http://{}", addr)).await;

    let query = r#"
        SELECT u.id, u.name, o.amount
        FROM users u
            JOIN (SELECT * FROM orders WHERE AMOUNT > 25 ) o 
                ON u.id = o.user_id
    "#;

    let flight_info = client
        .execute(query.to_string(), None)
        .await
        .expect("Query with subquery and join should succeed");

    let schema = flight_info
        .try_decode_schema()
        .expect("Should decode schema");

    assert_eq!(schema.fields().len(), 3, "Should have 3 fields");

    // Both fields should have table_name metadata pointing to 'u'
    let id_field = schema.field(0);
    let name_field = schema.field(1);

    // This field should have table_name metadata pointing to 'o'
    let amount_field = schema.field(2);

    assert_eq!(id_field.name(), "id");
    assert_eq!(name_field.name(), "name");
    assert_eq!(amount_field.name(), "amount");

    assert!(
        id_field.metadata().contains_key("table_name"),
        "id field should have table_name metadata"
    );
    assert_eq!(
        id_field.metadata().get("table_name").unwrap(),
        "u",
        "id field should have table_name='u'"
    );

    assert!(
        name_field.metadata().contains_key("table_name"),
        "name field should have table_name metadata"
    );
    assert_eq!(
        name_field.metadata().get("table_name").unwrap(),
        "u",
        "name field should have table_name='u'"
    );

    assert!(
        amount_field.metadata().contains_key("table_name"),
        "amount field should have table_name metadata"
    );
    assert_eq!(
        amount_field.metadata().get("table_name").unwrap(),
        "o",
        "amount field should have table_name='o'"
    );
}
