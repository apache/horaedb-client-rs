// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;

use super::{DbClient, QueryResult, QueryResultVec, WriteResult, WriteResultVec};
use crate::{
    model::{convert, request::QueryRequest, write::WriteRequest},
    rpc_client::{RpcClient, RpcContext},
    Error,
};

/// Client for ceresdb of standalone mode.
/// 
/// Now, [`StandaloneImpl`] just wraps [`RpcClient`] simply.
pub struct StandaloneImpl<R: RpcClient> {
    pub rpc_client: R,
}

#[async_trait]
impl<R: RpcClient> DbClient for StandaloneImpl<R> {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> QueryResultVec {
        vec![self.query_internal(ctx, req.clone()).await]
    }

    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> WriteResultVec {
        vec![self.write_internal(ctx, req.clone()).await]
    }
}

impl<R: RpcClient> StandaloneImpl<R> {
    pub fn new(rpc_client: R) -> Self {
        Self { rpc_client }
    }

    pub async fn query_internal(&self, ctx: &RpcContext, req: QueryRequest) -> QueryResult {
        let result_pb = self.rpc_client.query(ctx, &req.into()).await;
        QueryResult::new(result_pb.and_then(|resp_pb| {
            convert::parse_queried_rows(&resp_pb.schema_content, &resp_pb.rows)
                .map_err(Error::Client)
        }))
    }

    pub async fn write_internal(&self, ctx: &RpcContext, req: WriteRequest) -> WriteResult {
        let metrics: Vec<_> = req.write_entries.iter().map(|(m, _)| m.clone()).collect();
        WriteResult::new(
            metrics,
            self.rpc_client
                .write(ctx, &req.into())
                .await
                .map(|resp_pb| resp_pb.into()),
        )
    }
}
