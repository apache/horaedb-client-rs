use crate::rpc_client::{RpcClient, RpcContext, RpcClientBuilder};
use crate::{GrpcConfig, RpcOptions};
use crate::db_client::DbClient;
use crate::model::QueryResponse;
use crate::model::request::QueryRequest;
use crate::model::write::{WriteRequest, WriteResult};
use async_trait::async_trait;
use crate::errors::Result;

/// Now, `StandaloneImpl` just wrap `RpcClient` simply.
pub struct StandaloneImpl {
    rpc_client: RpcClient,
}

#[async_trait]
impl DbClient for StandaloneImpl {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> Result<QueryResponse> {
        self.rpc_client.query(ctx, req).await
    }

    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> Result<WriteResult> {
        self.rpc_client.write(ctx, req).await
    }
}

/// Builder for StandaloneImpl
pub struct StandaloneImplBuilder {
    rpc_builder: RpcClientBuilder,
}

impl StandaloneImplBuilder {
    pub fn new(endpoint: String) -> Self {
        Self {
            rpc_builder: RpcClientBuilder::new(endpoint),
        }
    }

    #[inline]
    pub fn grpc_config(mut self, grpc_config: GrpcConfig) -> Self {
        self.rpc_builder = self.rpc_builder.grpc_config(grpc_config);
        self
    }

    #[inline]
    pub fn rpc_opts(mut self, rpc_opts: RpcOptions) -> Self {
        self.rpc_builder = self.rpc_builder.rpc_opts(rpc_opts);
        self
    }

    pub fn build(self) -> StandaloneImpl {
        StandaloneImpl {
            rpc_client: self.rpc_builder.build(),
        }
    }
}
