use async_trait::async_trait;

use super::{QueryResult, QueryResultVec, WriteResult, WriteResultVec};
use crate::{
    db_client::DbClient,
    model::{convert, request::QueryRequest, write::WriteRequest},
    rpc_client::{GrpcClient, GrpcClientBuilder, RpcContext},
    Error, GrpcConfig, RpcOptions,
};

/// Now, `StandaloneImpl` just wrap `RpcClient` simply.
pub struct StandaloneImpl {
    pub(crate) rpc_client: GrpcClient,
}

#[async_trait]
impl DbClient for StandaloneImpl {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> QueryResultVec {
        let result_pb = self.rpc_client.query(ctx, &req.clone().into()).await;
        let result = result_pb.and_then(|resp_pb| {
            convert::parse_queried_rows(&resp_pb.schema_content, &resp_pb.rows)
                .map_err(Error::Client)
        });
        vec![QueryResult::new(result)]
    }

    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> WriteResultVec {
        let req_pb = req.clone().into();
        let result = self
            .rpc_client
            .write(ctx, &req_pb)
            .await
            .map(|resp_pb| resp_pb.into());
        let metrics: Vec<_> = req_pb.metrics.into_iter().map(|e| e.metric).collect();
        vec![WriteResult::new(metrics, result)]
    }
}

/// Builder for StandaloneImpl
pub struct StandaloneImplBuilder {
    rpc_builder: GrpcClientBuilder,
}

impl StandaloneImplBuilder {
    pub fn new(thread_num: usize) -> Self {
        Self {
            rpc_builder: GrpcClientBuilder::new(thread_num),
        }
    }

    #[inline]
    pub fn grpc_config(&mut self, grpc_config: GrpcConfig) -> &mut Self {
        self.rpc_builder.grpc_config(grpc_config);
        self
    }

    #[inline]
    pub fn rpc_opts(&mut self, rpc_opts: RpcOptions) -> &mut Self {
        self.rpc_builder.rpc_opts(rpc_opts);
        self
    }

    pub fn build(&self, endpoint: String) -> StandaloneImpl {
        StandaloneImpl {
            rpc_client: self.rpc_builder.build(endpoint),
        }
    }
}
