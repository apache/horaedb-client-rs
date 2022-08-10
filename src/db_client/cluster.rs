use crate::rpc_client::{RpcClient, RpcContext, RpcClientBuilder};
use crate::{GrpcConfig, RpcOptions};
use crate::db_client::DbClient;
use crate::model::QueryResponse;
use crate::model::request::QueryRequest;
use crate::model::write::{WriteRequest, WriteResult};
use async_trait::async_trait;
use crate::errors::Result;

/// Now, `StandaloneImpl` just wrap `RpcClient` simply.
struct ClusterImpl {}

#[async_trait]
impl DbClient for ClusterImpl {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> Result<QueryResponse> {
        todo!()
    }

    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> Result<WriteResult> {
        todo!()
    }
}