// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

mod builder;
mod cluster;
mod standalone;

use async_trait::async_trait;
pub use builder::{Builder, Mode};

use crate::{
    model::{
        request::QueryRequest,
        write::{WriteRequest, WriteResponse},
        QueryResponse,
    },
    rpc_client::RpcContext,
    Result,
};

#[async_trait]
pub trait DbClient: Send + Sync {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> Result<QueryResponse>;
    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> Result<WriteResponse>;
}
