// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use ceresdbproto::storage::{
    Endpoint as EndpointPb, QueryRequest as QueryRequestPb, QueryResponse as QueryResponsePb,
    Route as RoutePb, RouteRequest as RouteRequestPb, RouteResponse as RouteResponsePb,
    WriteRequest as WriteRequestPb, WriteResponse as WriteResponsePb,
};
use dashmap::DashMap;

use crate::{
    model::route::Endpoint,
    rpc_client::{RpcClient, RpcContext},
    Result,
};

/// Rpc client used for testing.
pub struct MockRpcClient {
    pub route_table: Arc<DashMap<String, Endpoint>>,
}

#[async_trait]
impl RpcClient for MockRpcClient {
    async fn query(&self, _ctx: &RpcContext, _req: QueryRequestPb) -> Result<QueryResponsePb> {
        todo!()
    }

    async fn write(&self, _ctx: &RpcContext, _req: WriteRequestPb) -> Result<WriteResponsePb> {
        todo!()
    }

    async fn route(&self, _ctx: &RpcContext, req: RouteRequestPb) -> Result<RouteResponsePb> {
        let route_tables = self.route_table.clone();
        let routes: Vec<_> = req
            .metrics
            .iter()
            .map(|m| {
                let endpoint = route_tables.get(m.as_str()).unwrap().value().clone();
                let mut route_pb = RoutePb::default();
                let endpoint_pb = EndpointPb {
                    ip: endpoint.addr,
                    port: endpoint.port,
                };
                route_pb.metric = m.clone();
                route_pb.endpoint = Some(endpoint_pb);
                route_pb
            })
            .collect();
        let route_resp = RouteResponsePb {
            header: None,
            routes,
        };
        Ok(route_resp)
    }
}
