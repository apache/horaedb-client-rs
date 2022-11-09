// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;

use super::direct::DirectInnerClient;
use crate::{
    db_client::DbClient,
    model::{
        request::QueryRequest,
        write::{WriteRequest, WriteResponse},
        QueryResponse,
    },
    rpc_client::{RpcClientFactory, RpcContext},
    Result,
};

/// Client for ceresdb of standalone mode.
///
/// Now, [`StandaloneImpl`] just wraps [`RpcClient`] simply.
pub struct StandaloneImpl<F: RpcClientFactory> {
    inner_client: DirectInnerClient<F>,
}

impl<F: RpcClientFactory> StandaloneImpl<F> {
    pub fn new(factory: Arc<F>, endpoint: String) -> Self {
        Self {
            inner_client: DirectInnerClient::new(factory, endpoint),
        }
    }
}

#[async_trait]
impl<F: RpcClientFactory> DbClient for StandaloneImpl<F> {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> Result<QueryResponse> {
        self.inner_client.query_internal(ctx, req).await
    }

    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> Result<WriteResponse> {
        self.inner_client.write_internal(ctx, req).await
    }
}
