// Copyright 2023 The HoraeDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use chrono::Local;
use horaedb_client::{
    db_client::{Builder, DbClient, Mode},
    model::{
        sql_query::{display::CsvFormatter, Request as SqlQueryRequest},
        value::Value,
        write::{point::PointBuilder, Request as WriteRequest},
    },
    Authorization, RpcConfig, RpcContext,
};

async fn create_table(client: &Arc<dyn DbClient>, rpc_ctx: &RpcContext) {
    let create_table_sql = r#"CREATE TABLE IF NOT EXISTS horaedb (
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
        tables: vec!["horaedb".to_string()],
        sql: create_table_sql.to_string(),
    };
    let resp = client
        .sql_query(rpc_ctx, &req)
        .await
        .expect("Should succeed to create table");
    println!("Create table result:{resp:?}");
}

async fn drop_table(client: &Arc<dyn DbClient>, rpc_ctx: &RpcContext) {
    let drop_table_sql = "DROP TABLE horaedb";
    let req = SqlQueryRequest {
        tables: vec!["horaedb".to_string()],
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
    let test_table = "horaedb";

    let points = vec![
        PointBuilder::new(test_table)
            .timestamp(ts1)
            .tag("str_tag", Value::String("tag_val1".to_string()))
            .tag("int_tag", Value::Int32(42))
            .tag("var_tag", Value::Varbinary(b"tag_bin_val1".to_vec()))
            .field("str_field", Value::String("field_val1".to_string()))
            .field("int_field".to_string(), Value::Int32(42))
            .field("bin_field", Value::Varbinary(b"field_bin_val1".to_vec()))
            .build()
            .unwrap(),
        PointBuilder::new(test_table)
            .timestamp(ts1 + 40)
            .tag("str_tag", Value::String("tag_val2".to_string()))
            .tag("int_tag", Value::Int32(43))
            .tag("var_tag", Value::Varbinary(b"tag_bin_val2".to_vec()))
            .field("str_field", Value::String("field_val2".to_string()))
            .field("bin_field", Value::Varbinary(b"field_bin_val2".to_vec()))
            .build()
            .unwrap(),
    ];

    write_req.add_points(points);

    let res = client
        .write(rpc_ctx, &write_req)
        .await
        .expect("Should success to write");
    println!("{res:?}");
}

async fn sql_query(client: &Arc<dyn DbClient>, rpc_ctx: &RpcContext) {
    let req = SqlQueryRequest {
        tables: vec!["horaedb".to_string()],
        sql: "select * from horaedb;".to_string(),
    };
    let resp = client
        .sql_query(rpc_ctx, &req)
        .await
        .expect("Should succeed to query");
    let csv_formatter = CsvFormatter { resp };
    println!("Rows in the resp:\n{csv_formatter}");
}

#[tokio::main]
async fn main() {
    // you should ensure horaedb is running, and grpc port is set to 8831
    let mut rpc_config = RpcConfig::default();
    // Set authorization if needed
    rpc_config.authorization = Some(Authorization {
        username: "user".to_string(),
        password: "pass".to_string(),
    });
    let client = Builder::new("127.0.0.1:8831".to_string(), Mode::Direct)
        .rpc_config(rpc_config)
        .build();
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
