// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

mod mock_rpc_client;
mod rpc_client_impl;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use ceresdbproto::storage::{
    QueryRequest as QueryRequestPb, QueryResponse as QueryResponsePb,
    RouteRequest as RouteRequestPb, RouteResponse as RouteResponsePb,
    WriteRequest as WriteRequestPb, WriteResponse as WriteResponsePb,
};
pub use mock_rpc_client::MockRpcClient;
pub use rpc_client_impl::RpcClientImplFactory;

use crate::errors::Result;

/// Context for rpc request.
#[derive(Clone, Debug)]
pub struct RpcContext {
    pub tenant: String,
    pub token: String,
    pub timeout: Option<Duration>,
}

impl RpcContext {
    pub fn new(tenant: String, token: String) -> Self {
        Self {
            tenant,
            token,
            timeout: None,
        }
    }

    /// Build [RpcContext] with timeout.
    pub fn with_timeout(tenant: String, token: String, timeout: Duration) -> Self {
        Self {
            tenant,
            token,
            timeout: Some(timeout),
        }
    }
}

#[async_trait]
pub trait RpcClient: Send + Sync {
    async fn query(&self, ctx: &RpcContext, req: QueryRequestPb) -> Result<QueryResponsePb>;
    async fn write(&self, ctx: &RpcContext, req: WriteRequestPb) -> Result<WriteResponsePb>;
    async fn route(&self, ctx: &RpcContext, req: RouteRequestPb) -> Result<RouteResponsePb>;
}

#[async_trait]
pub trait RpcClientFactory: Send + Sync {
    /// Build `RpcClient`.
    ///
    /// It may fail because of invalid endpoint. Any caller calls this method
    /// should handle the potential error.
    async fn build(&self, endpoint: String) -> Result<Arc<dyn RpcClient>>;
}
