# DataFusion Flight SQL Server

The `datafusion-flight-sql-server` is a Flight SQL server that implements the
necessary endpoints to use DataFusion as the query engine.

## Getting Started

To use `datafusion-flight-sql-server` in your Rust project, run:

```sh
$ cargo add datafusion-flight-sql-server
```

## Example

Here's a basic example of setting up a Flight SQL server:

```rust
use datafusion_flight_sql_server::service::FlightSqlService;
use datafusion::{
    execution::{
        context::SessionContext,
        options::CsvReadOptions,
    },
};

async {
    let dsn: String = "0.0.0.0:50051".to_string();
    let remote_ctx = SessionContext::new();
    remote_ctx
        .register_csv("test", "./examples/test.csv", CsvReadOptions::new())
        .await.expect("Register csv");

    FlightSqlService::new(remote_ctx.state()).serve(dsn.clone())
        .await
        .expect("Run flight sql service");

};
```

This example sets up a Flight SQL server listening on `127.0.0.1:50051`.


## Docs

[Docs link](https://datafusion-contrib.github.io/datafusion-flight-sql-server/datafusion_flight_sql_server/).


## Acknowledgments

This repository was a Rust crate that was first built as a part of
[datafusion-federation](https://github.com/datafusion-contrib/datafusion-federation/)
repository. 
