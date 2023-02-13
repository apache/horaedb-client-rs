// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use ceresdb_client_rs::{
    db_client::{Builder, DbClient, Mode},
    model::{
        sql_query::{display::CsvFormatter, Request as SqlQueryRequest},
        value::Value,
        write::{point::PointBuilder, Request as WriteRequest},
    },
    RpcContext,
};
use chrono::Local;

async fn create_table(client: &Arc<dyn DbClient>, rpc_ctx: &RpcContext) {
    let create_table_sql = r#"CREATE TABLE IF NOT EXISTS ceresdb (
                str_tag string TAG,
                int_tag int32 TAG,
                var_tag varbinary TAG,
                str_field string,
                int_field int32,
                bin_field varbinary,
                t timestamp NOT NULL,
                TIMESTAMP KEY(t)) ENGINE=Analytic with
(enable_ttl='false')"#;
    let req = SqlQueryRequest {
        tables: vec!["ceresdb".to_string()],
        sql: create_table_sql.to_string(),
    };
    let resp = client
        .sql_query(rpc_ctx, &req)
        .await
        .expect("Should succeed to create table");
    println!("Create table result:{:?}", resp);
}

async fn drop_table(client: &Arc<dyn DbClient>, rpc_ctx: &RpcContext) {
    let drop_table_sql = "DROP TABLE ceresdb";
    let req = SqlQueryRequest {
        tables: vec!["ceresdb".to_string()],
        sql: drop_table_sql.to_string(),
    };
    let _resp = client
        .sql_query(rpc_ctx, &req)
        .await
        .expect("Should succeed to drop table");
    println!("Drop table success!");
}

async fn write(client: &Arc<dyn DbClient>, rpc_ctx: &RpcContext) {
    let ts1 = Local::now().timestamp_millis();
    let mut write_req = WriteRequest::default();
    let test_table = "ceresdb";

    let points = vec![
        PointBuilder::new(test_table.to_string())
            .timestamp(ts1)
            .tag("str_tag".to_string(), Value::String("tag_val1".to_string()))
            .tag("int_tag".to_string(), Value::Int32(42))
            .tag(
                "var_tag".to_string(),
                Value::Varbinary(b"tag_bin_val1".to_vec()),
            )
            .field(
                "str_field".to_string(),
                Value::String("field_val1".to_string()),
            )
            .field("int_field".to_string(), Value::Int32(42))
            .field(
                "bin_field".to_string(),
                Value::Varbinary(b"field_bin_val1".to_vec()),
            )
            .build()
            .unwrap(),
        PointBuilder::new(test_table.to_string())
            .timestamp(ts1 + 40)
            .tag("str_tag".to_string(), Value::String("tag_val2".to_string()))
            .tag("int_tag".to_string(), Value::Int32(43))
            .tag(
                "var_tag".to_string(),
                Value::Varbinary(b"tag_bin_val2".to_vec()),
            )
            .field(
                "str_field".to_string(),
                Value::String("field_val2".to_string()),
            )
            .field(
                "bin_field".to_string(),
                Value::Varbinary(b"field_bin_val2".to_vec()),
            )
            .build()
            .unwrap(),
    ];

    write_req.add_points(points);

    let res = client
        .write(rpc_ctx, &write_req)
        .await
        .expect("Should success to write");
    println!("{:?}", res);
}

async fn sql_query(client: &Arc<dyn DbClient>, rpc_ctx: &RpcContext) {
    let req = SqlQueryRequest {
        tables: vec!["ceresdb".to_string()],
        sql: "select * from ceresdb;".to_string(),
    };
    let resp = client
        .sql_query(rpc_ctx, &req)
        .await
        .expect("Should succeed to query");
    let csv_formatter = CsvFormatter { resp };
    println!("Rows in the resp:\n{}", csv_formatter);
}

#[tokio::main]
async fn main() {
    // you should ensure ceresdb is running, and grpc port is set to 8831
    let client = Builder::new("127.0.0.1:8831".to_string(), Mode::Direct).build();
    let rpc_ctx = RpcContext::default().database("public".to_string());

    println!("------------------------------------------------------------------");
    println!("### create table:");
    create_table(&client, &rpc_ctx).await;
    println!("------------------------------------------------------------------");

    println!("### write:");
    write(&client, &rpc_ctx).await;
    println!("------------------------------------------------------------------");

    println!("### read:");
    sql_query(&client, &rpc_ctx).await;
    println!("------------------------------------------------------------------");

    println!("### drop table:");
    drop_table(&client, &rpc_ctx).await;
    println!("------------------------------------------------------------------");
}
