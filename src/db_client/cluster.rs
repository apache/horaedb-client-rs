use async_trait::async_trait;

use crate::{
    db_client::DbClient,
    errors::Result,
    model::{
        request::QueryRequest,
        write::{WriteRequest, WriteResult},
        QueryResponse,
    },
    route_client::RouteClient,
    rpc_client::RpcContext,
    GrpcConfig, RpcOptions,
};

/// Now, `StandaloneImpl` just wrap `RpcClient` simply.

struct ClusterImpl<R: RouteClient, D: DbClient> {
    route_client: R,
    db_client: D,
}

impl<R: RouteClient, D: DbClient> ClusterImpl<R, D> {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> Result<QueryResponse> {
        todo!()
    }

    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> Result<WriteResult> {
        todo!()
    }
}
