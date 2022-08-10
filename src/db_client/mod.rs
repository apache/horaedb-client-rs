pub mod cluster;
pub mod standalone;

use crate::errors::Result;
use async_trait::async_trait;
use crate::model::QueryResponse;
use crate::model::request::QueryRequest;
use crate::model::write::{WriteRequest, WriteResult};
use crate::rpc_client::RpcContext;

/// Route metric to endpoint
#[async_trait]
pub trait DbClient {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> Result<QueryResponse>;
    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> Result<WriteResult>;
}
