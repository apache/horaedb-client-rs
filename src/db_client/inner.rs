// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tokio::sync::OnceCell;

use crate::{
    model::{
        convert,
        request::QueryRequest,
        write::{WriteRequest, WriteResponse},
        QueryResponse, Schema,
    },
    rpc_client::{RpcClient, RpcClientFactory, RpcContext},
    Error, Result,
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

    pub async fn query_internal(
        &self,
        ctx: &RpcContext,
        req: &QueryRequest,
    ) -> Result<QueryResponse> {
        let client_handle = self.inner_client.get_or_try_init(|| self.init()).await?;
        let result_pb = client_handle.as_ref().query(ctx, req.clone().into()).await;

        result_pb.and_then(|resp_pb| {
            if !resp_pb.schema_content.is_empty() {
                convert::parse_queried_rows(&resp_pb.schema_content, &resp_pb.rows)
                    .map_err(Error::Client)
            } else {
                Ok(QueryResponse {
                    schema: Schema::default(),
                    rows: Vec::new(),
                    affected_rows: resp_pb.affected_rows,
                })
            }
        })
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
