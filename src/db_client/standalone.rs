// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;

use crate::{
    db_client::DbClient,
    model::{
        convert,
        request::QueryRequest,
        write::{WriteRequest, WriteResponse},
        QueryResponse, Schema,
    },
    rpc_client::{RpcClient, RpcContext},
    Error, Result,
};

/// Client for ceresdb of standalone mode.
///
/// Now, [`StandaloneImpl`] just wraps [`RpcClient`] simply.
pub struct StandaloneImpl<R: RpcClient> {
    pub rpc_client: R,
}

#[async_trait]
impl<R: RpcClient> DbClient for StandaloneImpl<R> {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> Result<QueryResponse> {
        self.query_internal(ctx, req.clone()).await
    }

    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> Result<WriteResponse> {
        self.write_internal(ctx, req.clone()).await
    }
}

impl<R: RpcClient> StandaloneImpl<R> {
    pub fn new(rpc_client: R) -> Self {
        Self { rpc_client }
    }

    pub async fn query_internal(
        &self,
        ctx: &RpcContext,
        req: QueryRequest,
    ) -> Result<QueryResponse> {
        let result_pb = self.rpc_client.query(ctx, &req.into()).await;
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
        req: WriteRequest,
    ) -> Result<WriteResponse> {
        self.rpc_client
            .write(ctx, &req.into())
            .await
            .map(|resp_pb| resp_pb.into())
    }
}
