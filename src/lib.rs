// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! This crate provides an user-friendly client for [HoraeDB ](https://github.com/HoraeDB /horaedb).
//!
//! With this crate, you can access a standalone HoraeDB or a HoraeDB cluster
//! and manipulates the data in it. And the underlying communication between the
//! client the HoraeDB servers is based on the gRPC, and the protocol is defined in the [ceresdbproto](https://github.com/HoraeDB /ceresdbproto).
//!
//! ## Choose Mode
//!
//! Two access [`Mode`](Mode)s are provided by the client, `Proxy` and `Direct`:
//! - When accessing HoraeDB cluster by `Direct` mode, the requests will be sent
//!   directly to the right HoraeDB instance determined by routing information.
//! - When accessing HoraeDB by `Proxy` mode, the requests are just sent to any
//!   one HoraeDB instance, which takes the responsibilities for forwarding the
//!   requests.
//!
//! If the client can't access the HoraeDB server directly because of the
//! network partition, `Proxy` mode is the only choice. Otherwise, `Direct` mode
//! is suggested for better performance.
//!
//! ## Usage
//!
//! Build the client, and then manipulate the HoraeDB by writing and querying.
//!
//! ### Example
//! Here is an example to create a table in HoraeDB by the client.
//!
//! ```rust,no_run
//! # use futures::prelude::*;
//!
//! # use horaedb_client::{Builder, Mode, RpcContext, SqlQueryRequest};
//! # fn main() {
//! # futures::executor::block_on(async {
//! let client = Builder::new("127.0.0.1:8831".to_string(), Mode::Direct).build();
//! let rpc_ctx = RpcContext::default().database("public".to_string());
//!
//! let create_table_sql = r#"CREATE TABLE IF NOT EXISTS horaedb (
//!     str_tag string TAG,
//!     int_tag int32 TAG,
//!     var_tag varbinary TAG,
//!     str_field string,
//!     int_field int32,
//!     bin_field varbinary,
//!     t timestamp NOT NULL,
//!     TIMESTAMP KEY(t)) ENGINE=Analytic with
//!     (enable_ttl='false')"#;
//!
//! let req = SqlQueryRequest {
//!     tables: vec!["horaedb".to_string()],
//!     sql: create_table_sql.to_string(),
//! };
//! let resp = client
//!     .sql_query(&rpc_ctx, &req)
//!     .await
//!     .expect("Should succeed to create table");
//!
//! println!("Create table result:{:?}", resp);
//! # });
//! # }
//! ```

mod config;
#[doc(hidden)]
pub mod db_client;
mod errors;
#[doc(hidden)]
pub mod model;
mod router;
mod rpc_client;
mod util;

#[doc(inline)]
pub use crate::{
    config::RpcConfig,
    db_client::{Builder, DbClient, Mode},
    errors::{Error, Result},
    model::{
        sql_query::{Request as SqlQueryRequest, Response as SqlQueryResponse},
        write::{Request as WriteRequest, Response as WriteResponse},
    },
    rpc_client::RpcContext,
};
