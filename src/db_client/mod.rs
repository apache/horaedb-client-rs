// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Client interface

mod builder;
mod cluster;
mod inner;
mod standalone;

use async_trait::async_trait;
pub use builder::Builder;

use crate::{
    model::{
        sql_query::{Request as SqlQueryRequest, Response as SqlQueryResponse},
        write::{Request as WriteRequest, Response as WriteResponse},
    },
    rpc_client::RpcContext,
    Result,
};

#[async_trait]
pub trait DbClient: Send + Sync {
    async fn sql_query(&self, ctx: &RpcContext, req: &SqlQueryRequest) -> Result<SqlQueryResponse>;
    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> Result<WriteResponse>;
}
