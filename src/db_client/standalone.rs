use async_trait::async_trait;

use crate::{
    db_client::DbClient,
    errors::Result,
    model::{
        request::QueryRequest,
        write::{WriteRequest, WriteResult},
        QueryResponse,
    },
    rpc_client::{GrpcClient, GrpcClientBuilder, RpcContext},
    GrpcConfig, RpcOptions,
};

/// Now, `StandaloneImpl` just wrap `RpcClient` simply.
pub struct StandaloneImpl {
    rpc_client: GrpcClient,
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
    rpc_builder: GrpcClientBuilder,
}

impl StandaloneImplBuilder {
    pub fn new(endpoint: String) -> Self {
        Self {
            rpc_builder: GrpcClientBuilder::new(endpoint),
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
