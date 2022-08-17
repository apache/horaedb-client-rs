pub mod cluster;
pub mod result;
pub mod standalone;

use async_trait::async_trait;
pub use result::{QueryResult, QueryResultVec, WriteResult, WriteResultVec};

use crate::{
    model::{request::QueryRequest, write::WriteRequest},
    rpc_client::RpcContext,
};

/// Route metric to endpoint
#[async_trait]
pub trait DbClient {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> QueryResultVec;
    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> WriteResultVec;
}
