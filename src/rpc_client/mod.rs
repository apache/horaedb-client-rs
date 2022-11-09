// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

mod mock_rpc_client;
mod rpc_client_impl;

use std::sync::Arc;

use async_trait::async_trait;
use ceresdbproto::storage::{
    QueryRequest as QueryRequestPb, QueryResponse as QueryResponsePb,
    RouteRequest as RouteRequestPb, RouteResponse as RouteResponsePb,
    WriteRequest as WriteRequestPb, WriteResponse as WriteResponsePb,
};
pub use mock_rpc_client::MockRpcClient;
pub use rpc_client_impl::{RpcClientImpl, RpcClientImplFactory};

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
    async fn query(&self, ctx: &RpcContext, req: QueryRequestPb) -> Result<QueryResponsePb>;
    async fn write(&self, ctx: &RpcContext, req: WriteRequestPb) -> Result<WriteResponsePb>;
    async fn route(&self, ctx: &RpcContext, req: RouteRequestPb) -> Result<RouteResponsePb>;
}

#[async_trait]
pub trait RpcClientFactory: Send + Sync{
    // The Build method may fail because of invalid endpoint, so it returns a Result. 
    // Any caller calls this method should handle the potencial error
    async fn build(&self, endpoint: String) -> Result<Arc<dyn RpcClient>>;
}