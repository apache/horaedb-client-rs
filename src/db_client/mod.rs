// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

mod builder;
mod cluster;
mod result;
mod standalone;

use async_trait::async_trait;
pub use builder::{Builder, Mode};
pub use result::{QueryResult, QueryResultVec, WriteResult, WriteResultVec};

use crate::{
    model::{request::QueryRequest, write::WriteRequest},
    rpc_client::RpcContext,
};

#[async_trait]
pub trait DbClient {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> QueryResultVec;
    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> WriteResultVec;
}
