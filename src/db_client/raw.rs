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

use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    db_client::{inner::InnerClient, DbClient},
    model::{
        sql_query::{Request as SqlQueryRequest, Response as SqlQueryResponse},
        write::{Request as WriteRequest, Response as WriteResponse},
    },
    rpc_client::{RpcClientFactory, RpcContext},
    Result,
};

/// Client for horaedb of standalone mode.
///
/// Now, [`RawImpl`] just wraps [`InnerClient`] simply.
pub struct RawImpl<F: RpcClientFactory> {
    inner_client: InnerClient<F>,
    default_database: Option<String>,
}

impl<F: RpcClientFactory> RawImpl<F> {
    pub fn new(factory: Arc<F>, endpoint: String, default_database: Option<String>) -> Self {
        Self {
            inner_client: InnerClient::new(factory, endpoint),
            default_database,
        }
    }
}

#[async_trait]
impl<F: RpcClientFactory> DbClient for RawImpl<F> {
    async fn sql_query(&self, ctx: &RpcContext, req: &SqlQueryRequest) -> Result<SqlQueryResponse> {
        let ctx = crate::db_client::resolve_database(ctx, &self.default_database)?;
        self.inner_client.sql_query_internal(&ctx, req).await
    }

    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> Result<WriteResponse> {
        let ctx = crate::db_client::resolve_database(ctx, &self.default_database)?;
        self.inner_client.write_internal(&ctx, req).await
    }
}
