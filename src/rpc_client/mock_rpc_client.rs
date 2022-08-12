use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use dashmap::DashMap;

use crate::{
    model::{
        request::QueryRequest,
        route::{Route, RouteRequest, RouteResponse},
        write::{WriteRequest, WriteResult},
        QueryResponse,
    },
    rpc_client::{RpcClient, RpcContext},
};

/// Rpc client used for testing.
pub struct MockRpcClient {
    pub route_table: Arc<DashMap<String, Route>>,
}

#[async_trait]
impl RpcClient for MockRpcClient {
    async fn query(&self, _ctx: &RpcContext, _req: &QueryRequest) -> crate::Result<QueryResponse> {
        todo!()
    }

    async fn write(&self, _ctx: &RpcContext, _req: &WriteRequest) -> crate::Result<WriteResult> {
        todo!()
    }

    async fn route(&self, _ctx: &RpcContext, req: &RouteRequest) -> crate::Result<RouteResponse> {
        let route_tables = self.route_table.clone();
        let routes: Vec<_> = req
            .metrics
            .iter()
            .map(|m| route_tables.get(m).unwrap().clone())
            .collect();
        Ok(RouteResponse { routes: routes })
    }
}
