// Copyright 2023 The HoraeDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod mock_rpc_client;
mod rpc_client_impl;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use horaedbproto::storage::{
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
