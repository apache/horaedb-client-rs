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

use horaedbproto::storage;
use tokio::sync::OnceCell;

use crate::{
    model::{
        sql_query::{Request as SqlQueryRequest, Response as SqlQueryResponse},
        write::{Request as WriteRequest, Response as WriteResponse, WriteTableRequestPbsBuilder},
    },
    rpc_client::{RpcClient, RpcClientFactory, RpcContext},
    Result,
};

/// Inner client for both standalone and route based modes.
///
/// Now, [`InnerClient`] just wraps [`RpcClient`] simply.
pub(crate) struct InnerClient<F: RpcClientFactory> {
    factory: Arc<F>,
    endpoint: String,
    inner_client: OnceCell<Arc<dyn RpcClient>>,
}

impl<F: RpcClientFactory> InnerClient<F> {
    pub fn new(factory: Arc<F>, endpoint: String) -> Self {
        InnerClient {
            factory,
            endpoint,
            inner_client: OnceCell::new(),
        }
    }

    #[inline]
    async fn init(&self) -> Result<Arc<dyn RpcClient>> {
        self.factory.build(self.endpoint.clone()).await
    }

    pub async fn sql_query_internal(
        &self,
        ctx: &RpcContext,
        req: &SqlQueryRequest,
    ) -> Result<SqlQueryResponse> {
        assert!(ctx.database.is_some());

        let client_handle = self.inner_client.get_or_try_init(|| self.init()).await?;
        let req_ctx = storage::RequestContext {
            database: ctx.database.clone().unwrap(),
        };
        let req_pb = storage::SqlQueryRequest {
            context: Some(req_ctx),
            tables: req.tables.clone(),
            sql: req.sql.clone(),
        };

        client_handle
            .as_ref()
            .sql_query(ctx, req_pb)
            .await
            .and_then(SqlQueryResponse::try_from)
    }

    pub async fn write_internal(
        &self,
        ctx: &RpcContext,
        req: &WriteRequest,
    ) -> Result<WriteResponse> {
        assert!(ctx.database.is_some());

        let client_handle = self.inner_client.get_or_try_init(|| self.init()).await?;
        let req_ctx = storage::RequestContext {
            database: ctx.database.clone().unwrap(),
        };
        let write_table_request_pbs = WriteTableRequestPbsBuilder(req.clone()).build();
        let req_pb = storage::WriteRequest {
            context: Some(req_ctx),
            table_requests: write_table_request_pbs,
        };

        client_handle
            .write(ctx, req_pb)
            .await
            .map(|resp_pb| resp_pb.into())
    }
}
