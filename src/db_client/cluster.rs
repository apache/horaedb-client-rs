// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::join_all;

use super::{standalone::StandaloneImpl, DbClient};
use crate::{
    errors::ClusterWriteError,
    model::{
        request::QueryRequest,
        route::Endpoint,
        write::{WriteRequest, WriteResponse},
        QueryResponse,
    },
    router::Router,
    rpc_client::{RpcClientImpl, RpcClientImplBuilder, RpcContext},
    util::should_refresh,
    Error, Result,
};

/// Client for ceresdb of cluster mode.
pub struct ClusterImpl<R: Router> {
    router: R,
    standalone_pool: StandalonePool,
}

#[async_trait]
impl<R: Router> DbClient for ClusterImpl<R> {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> Result<QueryResponse> {
        if req.metrics.is_empty() {
            return Err(Error::Unknown(
                "Metrics in query request can't be empty in cluster mode".to_string(),
            ));
        }

        let endpoint = match self.router.route(&req.metrics, ctx).await {
            Ok(mut eps) => {
                if let Some(ep) = eps[0].take() {
                    ep
                } else {
                    return Err(Error::Unknown(
                        "Metric doesn't have corresponding endpoint".to_string(),
                    ));
                }
            }
            Err(e) => {
                return Err(e);
            }
        };

        let client = self.standalone_pool.get_or_create(&endpoint).clone();

        client.query_internal(ctx, req.clone()).await.map_err(|e| {
            self.router.evict(&req.metrics);
            e
        })
    }

    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> Result<WriteResponse> {
        // Get metrics' related endpoints(some may not exist).
        let should_routes: Vec<_> = req.write_entries.iter().map(|(m, _)| m.clone()).collect();
        let endpoints = self.router.route(&should_routes, ctx).await?;

        // Partition write entries in request according to related endpoints.
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

        // Get client and send.
        let mut write_metrics = vec![Vec::new(); partition_by_endpoint.len()];
        let client_req_paris: Vec<_> = partition_by_endpoint
            .into_iter()
            .enumerate()
            .map(|(idx, (ep, req))| {
                assert!(idx < write_metrics.len());
                write_metrics[idx].extend(req.write_entries.iter().map(|(m, _)| m.clone()));
                (self.standalone_pool.get_or_create(&ep), req)
            })
            .collect();
        let mut futures = Vec::with_capacity(client_req_paris.len());
        for (client, req) in client_req_paris {
            futures.push(async move { client.write_internal(ctx, req).await })
        }

        // Await rpc results and collect results.
        let mut metrics_result_pairs: Vec<_> = join_all(futures)
            .await
            .into_iter()
            .zip(write_metrics.into_iter())
            .map(|(results, metrics)| (metrics, results))
            .collect();
        metrics_result_pairs.push((
            no_corresponding_endpoints,
            Err(Error::Unknown(
                "Metrics don't have corresponding endpoints".to_string(),
            )),
        ));

        // Process results:
        //  + Evict outdated endpoints.
        //  + Merge results and return.
        let evicts: Vec<_> = metrics_result_pairs
            .iter()
            .filter_map(|(metrics, result)| {
                if let Err(Error::Server(server_error)) = &result &&
                should_refresh(server_error.code, &server_error.msg) {
                Some(metrics.clone())
            } else {
                None
            }
            })
            .flatten()
            .collect();
        self.router.evict(&evicts);

        let cluster_error: ClusterWriteError = metrics_result_pairs.into();
        if cluster_error.all_ok() {
            Ok(cluster_error.ok.1)
        } else {
            Err(Error::ClusterWriteError(cluster_error))
        }
    }
}

impl<R: Router> ClusterImpl<R> {
    pub fn new(route_client: R, standalone_builder: RpcClientImplBuilder) -> Self {
        Self {
            router: route_client,
            standalone_pool: StandalonePool::new(standalone_builder),
        }
    }
}

struct StandalonePool {
    pool: DashMap<Endpoint, Arc<StandaloneImpl<RpcClientImpl>>>,
    standalone_builder: RpcClientImplBuilder,
}

// TODO better to add gc.
impl StandalonePool {
    fn new(standalone_builder: RpcClientImplBuilder) -> Self {
        Self {
            pool: DashMap::new(),
            standalone_builder,
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
                    self.standalone_builder.build(endpoint.to_string()),
                )))
                .clone()
        }
    }
}
