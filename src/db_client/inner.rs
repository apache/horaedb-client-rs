// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Inner client

use std::sync::Arc;

use tokio::sync::OnceCell;

use crate::{
    model::{
        sql_query::{Request as SqlQueryRequest, Response as SqlQueryResponse},
        write::{Request as WriteRequest, Response as WriteResponse},
    },
    rpc_client::{RpcClient, RpcClientFactory, RpcContext},
    Result,
};

/// Inner client for both standalone and cluster modes.
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
        let client_handle = self.inner_client.get_or_try_init(|| self.init()).await?;

        client_handle
            .as_ref()
            .sql_query(ctx, req.clone().into())
            .await
            .and_then(SqlQueryResponse::try_from)
    }

    pub async fn write_internal(
        &self,
        ctx: &RpcContext,
        req: &WriteRequest,
    ) -> Result<WriteResponse> {
        let client_handle = self.inner_client.get_or_try_init(|| self.init()).await?;
        client_handle
            .write(ctx, req.clone().into())
            .await
            .map(|resp_pb| resp_pb.into())
    }
}
