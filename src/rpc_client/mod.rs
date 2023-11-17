// Copyright 2022 HoraeDB Project Authors. Licensed under Apache-2.0.

//! Rpc client

mod mock_rpc_client;
mod rpc_client_impl;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use ceresdbproto::storage::{
    RouteRequest as RouteRequestPb, RouteResponse as RouteResponsePb,
    SqlQueryRequest as QueryRequestPb, SqlQueryResponse as QueryResponsePb,
    WriteRequest as WriteRequestPb, WriteResponse as WriteResponsePb,
};
pub use mock_rpc_client::MockRpcClient;
pub use rpc_client_impl::RpcClientImplFactory;

use crate::errors::Result;

/// Context for rpc request.
#[derive(Clone, Debug, Default)]
pub struct RpcContext {
    pub database: Option<String>,
    pub timeout: Option<Duration>,
}

impl RpcContext {
    pub fn database(mut self, database: String) -> Self {
        self.database = Some(database);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}
#[async_trait]
pub trait RpcClient: Send + Sync {
    async fn sql_query(&self, ctx: &RpcContext, req: QueryRequestPb) -> Result<QueryResponsePb>;
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
