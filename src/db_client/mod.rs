pub mod cluster;
pub mod standalone;

use async_trait::async_trait;

use crate::{
    errors::Result,
    model::{
        request::QueryRequest,
        write::{WriteRequest, WriteResult},
        QueryResponse,
    },
    rpc_client::RpcContext,
};

/// Route metric to endpoint
#[async_trait]
pub trait DbClient {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> Result<QueryResponse>;
    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> Result<WriteResult>;
}
