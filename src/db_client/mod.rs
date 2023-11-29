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
