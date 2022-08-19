// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::join_all;

use super::{
    standalone::StandaloneImpl, DbClient, QueryResult, QueryResultVec, WriteResult, WriteResultVec,
};
use crate::{
    errors::should_refresh,
    model::{request::QueryRequest, route::Endpoint, write::WriteRequest},
    router::Router,
    rpc_client::{GrpcClientBuilder, RpcClientImpl, RpcContext},
    Error,
};

/// Client for ceresdb of cluster mode.
pub struct ClusterImpl<R: Router> {
    router: R,
    standalone_pool: StandalonePool,
}

#[async_trait]
impl<R: Router> DbClient for ClusterImpl<R> {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> QueryResultVec {
        if req.metrics.is_empty() {
            return vec![QueryResult::new(Err(Error::Unknown(
                "Metrics in query request can't be empty in cluster mode".to_string(),
            )))];
        }

        let endpoint = match self.router.route(&req.metrics, ctx).await {
            Ok(mut eps) => {
                if let Some(ep) = eps[0].take() {
                    ep
                } else {
                    return vec![QueryResult::new(Err(Error::Unknown(
                        "Metric doesn't have corresponding endpoint".to_string(),
                    )))];
                }
            }
            Err(e) => {
                return vec![QueryResult::new(Err(e))];
            }
        };

        let clnt = self.standalone_pool.get_or_create(&endpoint).clone();
        vec![QueryResult::new(
            clnt.query_internal(ctx, req.clone())
                .await
                .result
                .map_err(|e| {
                    self.router.evict(&req.metrics);
                    e
                }),
        )]
    }

    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> WriteResultVec {
        // Get metrics' related endpoints(some may not exist).
        let should_routes: Vec<_> = req.write_entries.iter().map(|(m, _)| m.clone()).collect();
        let endpoints = match self.router.route(&should_routes, ctx).await {
            Ok(ep) => ep,
            Err(e) => {
                return vec![WriteResult::new(should_routes, Err(e))];
            }
        };

        // Partition write entries in request according to related endpoints.
        let mut write_results = Vec::new();
        let mut no_corresponding_endpoints = Vec::new();
        let mut partition_by_endpoint = HashMap::new();
        endpoints
            .into_iter()
            .zip(should_routes.into_iter())
            .for_each(|(ep, m)| match ep {
                Some(ep) => {
                    let write_req = partition_by_endpoint
                        .entry(ep)
                        .or_insert_with(WriteRequest::default);
                    write_req.write_entries.insert(
                        m.clone(),
                        req.write_entries.get(m.as_str()).cloned().unwrap(),
                    );
                }
                None => {
                    no_corresponding_endpoints.push(m);
                }
            });

        if !no_corresponding_endpoints.is_empty() {
            write_results.push(WriteResult::new(
                no_corresponding_endpoints,
                Err(Error::Unknown(
                    "Metrics don't have corresponding endpoints".to_string(),
                )),
            ));
        }

        // Get client and send.
        if !partition_by_endpoint.is_empty() {
            let clnt_req_paris: Vec<_> = partition_by_endpoint
                .into_iter()
                .map(|(ep, req)| (self.standalone_pool.get_or_create(&ep), req))
                .collect();
            let mut futures = Vec::with_capacity(clnt_req_paris.len());
            for (clnt, req) in clnt_req_paris {
                futures.push(async move { clnt.write_internal(ctx, req).await })
            }

            // Await rpc results and evict invalid route entries.
            let rpc_write_results = join_all(futures).await;
            let evicts: Vec<_> = rpc_write_results
                .iter()
                .filter_map(|res| {
                    if let Err(Error::Server(serv_err)) = &res.result &&
                    should_refresh(serv_err.code, &serv_err.msg) {
                    Some(res.metrics.clone().into_iter())
                } else {
                    None
                }
                })
                .flatten()
                .collect();
            self.router.evict(&evicts);
        }

        write_results
    }
}

impl<R: Router> ClusterImpl<R> {
    pub fn new(route_client: R, standalone_buidler: GrpcClientBuilder) -> Self {
        Self {
            router: route_client,
            standalone_pool: StandalonePool::new(standalone_buidler),
        }
    }
}

struct StandalonePool {
    pool: DashMap<Endpoint, Arc<StandaloneImpl<RpcClientImpl>>>,
    standalone_buidler: GrpcClientBuilder,
}

// TODO better to add gc.
impl StandalonePool {
    fn new(standalone_buidler: GrpcClientBuilder) -> Self {
        Self {
            pool: DashMap::new(),
            standalone_buidler,
        }
    }

    fn get_or_create(&self, endpoint: &Endpoint) -> Arc<StandaloneImpl<RpcClientImpl>> {
        if let Some(c) = self.pool.get(endpoint) {
            // If exist in cache, return.
            c.value().clone()
        } else {
            // If not exist, build --> insert --> return.
            self.pool
                .entry(endpoint.clone())
                .or_insert(Arc::new(StandaloneImpl::new(
                    self.standalone_buidler.build(endpoint.to_string()),
                )))
                .clone()
        }
    }
}
