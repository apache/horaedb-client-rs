use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use dashmap::DashMap;

use super::{WriteResult, WriteResultVec, QueryResultVec};
use crate::{
    errors::Result,
    model::{
        request::QueryRequest,
        route::Endpoint,
        write::{WriteRequest, WriteResponse},
        QueryResponse,
    },
    router::Router,
    rpc_client::RpcContext,
    StandaloneImpl, StandaloneImplBuilder,
};

/// Now, `StandaloneImpl` just wrap `RpcClient` simply.
struct ClusterImpl<R: Router> {
    route_client: R,
    standalone_pool: StandalonePool,
}

impl<R: Router> ClusterImpl<R> {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> QueryResultVec {
        todo!()
    }

    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> WriteResultVec {
        // Get metric's related endpoint(maybe not exists, filtered them)).
        // Partition entires in request according to related endpoint.
        let route_metrics: HashSet<_> = req
            .write_entries
            .iter()
            .map(|w| w.series.metric.clone())
            .collect();
        let route_metrics: Vec<_> = route_metrics.into_iter().collect();
        let endpoints = match self.route_client.route(&route_metrics, ctx, false).await {
            Ok(ep) => ep,
            Err(e) => {
                return vec![WriteResult::new(route_metrics, Err(e))];
            }
        };

        // Get
        todo!()
    }



}

struct StandalonePool {
    pool: DashMap<Endpoint, Arc<StandaloneImpl>>,
    standalone_buidler: StandaloneImplBuilder,
}

impl StandalonePool {
    fn new(standalone_buidler: StandaloneImplBuilder) -> Self {
        Self {
            pool: DashMap::new(),
            standalone_buidler: standalone_buidler,
        }
    }

    fn get_standalone(&self, endpoint: &Endpoint) -> Arc<StandaloneImpl> {
        if let Some(c) = self.pool.get(endpoint) {
            // If exist in cache, return.
            c.value().clone()
        } else {
            // If not exist, build --> insert --> return.
            self.pool
                .entry(endpoint.clone())
                .or_insert(Arc::new(
                    self.standalone_buidler.build(endpoint.to_string()),
                ))
                .clone()
        }
    }
}
