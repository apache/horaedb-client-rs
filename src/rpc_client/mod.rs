// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

mod grpc_client;
mod mock_rpc_client;

use async_trait::async_trait;
pub use grpc_client::{GrpcClient, GrpcClientBuilder};
pub use mock_rpc_client::MockRpcClient;

use crate::{
    errors::Result,
    model::{
        request::QueryRequest,
        route::{RouteRequest, RouteResponse},
        write::{WriteRequest, WriteResult},
        QueryResponse,
    },
};

/// Context for rpc request.
#[derive(Clone, Debug)]
pub struct RpcContext {
    pub tenant: String,
    pub token: String,
}

impl RpcContext {
    pub fn new(tenant: String, token: String) -> Self {
        Self { tenant, token }
    }
}

#[async_trait]
pub trait RpcClient: Send + Sync {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> Result<QueryResponse>;
    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> Result<WriteResult>;
    async fn route(&self, ctx: &RpcContext, req: &RouteRequest) -> Result<RouteResponse>;
}
