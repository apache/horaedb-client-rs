// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Inner client

use std::sync::Arc;

use ceresdbproto::storage;
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
