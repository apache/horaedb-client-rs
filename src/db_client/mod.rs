// Copyright 2022 HoraeDB Project Authors. Licensed under Apache-2.0.

//! This module provides the definition and implementations of the `DbClient`.

mod builder;
mod inner;
mod raw;
mod route_based;

use async_trait::async_trait;
pub use builder::{Builder, Mode};

use crate::{
    model::{
        sql_query::{Request as SqlQueryRequest, Response as SqlQueryResponse},
        write::{Request as WriteRequest, Response as WriteResponse},
    },
    rpc_client::RpcContext,
    Result,
};

#[async_trait]
pub trait DbClient: Send + Sync {
    async fn sql_query(&self, ctx: &RpcContext, req: &SqlQueryRequest) -> Result<SqlQueryResponse>;
    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> Result<WriteResponse>;
}

pub(crate) fn resolve_database(
    ctx: &RpcContext,
    default_database: &Option<String>,
) -> Result<RpcContext> {
    match (&ctx.database, default_database) {
        (Some(_), _) => Ok(ctx.clone()),
        (None, Some(default_database)) => Ok(RpcContext {
            database: Some(default_database.clone()),
            ..ctx.clone()
        }),
        (None, None) => Err(crate::Error::NoDatabase),
    }
}
