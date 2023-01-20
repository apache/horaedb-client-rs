// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;

use super::inner::InnerClient;
use crate::{
    db_client::DbClient,
    model::{
        sql_query::{Request as SqlQueryRequest, Response as SqlQueryResponse},
        write::{Request as WriteRequest, Response as WriteResponse},
    },
    rpc_client::{RpcClientFactory, RpcContext},
    Result,
};

/// Client for ceresdb of standalone mode.
///
/// Now, [`StandaloneImpl`] just wraps [`InnerClient`] simply.
pub struct StandaloneImpl<F: RpcClientFactory> {
    inner_client: InnerClient<F>,
}

impl<F: RpcClientFactory> StandaloneImpl<F> {
    pub fn new(factory: Arc<F>, endpoint: String) -> Self {
        Self {
            inner_client: InnerClient::new(factory, endpoint),
        }
    }
}

#[async_trait]
impl<F: RpcClientFactory> DbClient for StandaloneImpl<F> {
    async fn query(&self, ctx: &RpcContext, req: &SqlQueryRequest) -> Result<SqlQueryResponse> {
        self.inner_client.query_internal(ctx, req).await
    }

    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> Result<WriteResponse> {
        self.inner_client.write_internal(ctx, req).await
    }
}
