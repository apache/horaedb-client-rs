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

use std::sync::Arc;

use async_trait::async_trait;
use horaedbproto::storage::{
    Endpoint as EndpointPb, Route as RoutePb, RouteRequest as RouteRequestPb,
    RouteResponse as RouteResponsePb, SqlQueryRequest as QueryRequestPb,
    SqlQueryResponse as QueryResponsePb, WriteRequest as WriteRequestPb,
    WriteResponse as WriteResponsePb,
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
    async fn sql_query(&self, _ctx: &RpcContext, _req: QueryRequestPb) -> Result<QueryResponsePb> {
        todo!()
    }

    async fn write(&self, _ctx: &RpcContext, _req: WriteRequestPb) -> Result<WriteResponsePb> {
        todo!()
    }

    async fn route(&self, _ctx: &RpcContext, req: RouteRequestPb) -> Result<RouteResponsePb> {
        let route_tables = self.route_table.clone();
        let routes: Vec<_> = req
            .tables
            .iter()
            .filter_map(|m| {
                let endpoint = match route_tables.get(m.as_str()) {
                    Some(v) => v.value().clone(),
                    None => return None,
                };
                let mut route_pb = RoutePb::default();
                let endpoint_pb = EndpointPb {
                    ip: endpoint.addr,
                    port: endpoint.port,
                };
                route_pb.table = m.clone();
                route_pb.endpoint = Some(endpoint_pb);
                Some(route_pb)
            })
            .collect();
        let route_resp = RouteResponsePb {
            header: None,
            routes,
        };
        Ok(route_resp)
    }
}
