// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use ceresdb_client_rs::{
    model::{request::QueryRequestPb, value::Value, write::WriteRequestBuilder},
    rpc_client::RpcContext,
    DbClient, GrpcConfig, StandaloneImplBuilder,
};
use chrono::Local;

async fn create_table(client: &impl DbClient, rpc_ctx: &RpcContext) {
    let create_table_sql = r#"CREATE TABLE ceresdb (
                str_tag string TAG,
                int_tag int32 TAG,
                var_tag varbinary TAG,
                str_field string,
                int_field int32,
                bin_field varbinary,
                t timestamp NOT NULL,
                TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl='false')"#;
    let req = QueryRequestPb {
        metrics: vec!["ceresdb".to_string()],
        ql: create_table_sql.to_string(),
    };
    let _resp = client.query(rpc_ctx, &req).await.unwrap();
    println!("Create table success!");
}

async fn drop_table(client: &impl DbClient, rpc_ctx: &RpcContext) {
    let drop_table_sql = "DROP TABLE ceresdb";
    let req = QueryRequestPb {
        metrics: vec!["ceresdb".to_string()],
        ql: drop_table_sql.to_string(),
    };
    let _resp = client.query(rpc_ctx, &req).await.unwrap();
    println!("Drop table success!");
}

async fn write(client: &impl DbClient, rpc_ctx: &RpcContext) {
    let ts1 = Local::now().timestamp_millis();
    let mut write_req_builder = WriteRequestBuilder::default();
    // first row
    write_req_builder
        .row_builder()
        .metric("ceresdb".to_string())
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
        .finish()
        .unwrap();

    // second row
    write_req_builder
        .row_builder()
        .metric("ceresdb".to_string())
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
        .finish()
        .unwrap();

    let write_req = write_req_builder.build();
    let res = client.write(rpc_ctx, &write_req).await;
    println!("{:?}", res);
}

async fn query(client: &impl DbClient, rpc_ctx: &RpcContext) {
    let req = QueryRequestPb {
        metrics: vec!["ceresdb".to_string()],
        ql: "select * from ceresdb;".to_string(),
    };
    let resp = client.query(rpc_ctx, &req).await.unwrap();
    println!("Rows in the resp:{:?}", resp);

    for (row_id, row) in resp.rows.iter().enumerate() {
        let format_row: Vec<_> = row
            .datums
            .iter()
            .zip(resp.schema.column_schemas.iter())
            .map(|(datum, col_schema)| format!("{:?}:{:?}", col_schema.name, datum))
            .collect();
        println!("row{}:{:?}", row_id, format_row);
    }
}

#[tokio::main]
async fn main() {
    // you should ensure ceresdb is running, and grpc port is set to 8831
    let client = StandaloneImplBuilder::new(1)
        .grpc_config(GrpcConfig::default())
        .build("127.0.0.1:8831".to_string());
    let rpc_ctx = RpcContext::new("public".to_string(), "".to_string());

    println!("------------------------------------------------------------------");
    println!("### create table:");
    create_table(&client, &rpc_ctx).await;
    println!("------------------------------------------------------------------");

    println!("### write:");
    write(&client, &rpc_ctx).await;
    println!("------------------------------------------------------------------");

    println!("### read:");
    query(&client, &rpc_ctx).await;
    println!("------------------------------------------------------------------");

    println!("### drop table:");
    drop_table(&client, &rpc_ctx).await;
    println!("------------------------------------------------------------------");
}
