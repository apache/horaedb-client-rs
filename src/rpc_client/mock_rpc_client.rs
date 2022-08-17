use std::{collections::HashMap, sync::Arc};

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
};

/// Rpc client used for testing.
pub struct MockRpcClient {
    pub route_table: Arc<DashMap<String, Endpoint>>,
}

#[async_trait]
impl RpcClient for MockRpcClient {
    async fn query(
        &self,
        _ctx: &RpcContext,
        _req: &QueryRequestPb,
    ) -> crate::Result<QueryResponsePb> {
        todo!()
    }

    async fn write(
        &self,
        _ctx: &RpcContext,
        _req: &WriteRequestPb,
    ) -> crate::Result<WriteResponsePb> {
        todo!()
    }

    async fn route(
        &self,
        _ctx: &RpcContext,
        req: &RouteRequestPb,
    ) -> crate::Result<RouteResponsePb> {
        let route_tables = self.route_table.clone();
        let routes: Vec<_> = req
            .metrics
            .iter()
            .map(|m| {
                let endpoint = route_tables.get(m.as_str()).unwrap().value().clone();
                let mut route_pb = RoutePb::default();
                let mut endpoint_pb = EndpointPb::default();
                endpoint_pb.set_ip(endpoint.ip);
                endpoint_pb.set_port(endpoint.port);
                route_pb.set_metric(m.clone());
                route_pb.set_endpoint(endpoint_pb);
                route_pb
            })
            .collect();
        let mut route_resp = RouteResponsePb::default();
        route_resp.set_routes(routes.into());
        Ok(route_resp)
    }
}
