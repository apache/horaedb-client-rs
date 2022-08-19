// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

mod mock_rpc_client;
mod rpc_client_impl;

use async_trait::async_trait;
use ceresdbproto::storage::{
    QueryRequest as QueryRequestPb, QueryResponse as QueryResponsePb,
    RouteRequest as RouteRequestPb, RouteResponse as RouteResponsePb,
    WriteRequest as WriteRequestPb, WriteResponse as WriteResponsePb,
};
pub use mock_rpc_client::MockRpcClient;
pub use rpc_client_impl::{RpcClientImpl, RpcClientImplBuilder};

use crate::errors::Result;

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
    async fn query(&self, ctx: &RpcContext, req: &QueryRequestPb) -> Result<QueryResponsePb>;
    async fn write(&self, ctx: &RpcContext, req: &WriteRequestPb) -> Result<WriteResponsePb>;
    async fn route(&self, ctx: &RpcContext, req: &RouteRequestPb) -> Result<RouteResponsePb>;
}
