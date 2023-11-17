// Copyright 2022 HoraeDB Project Authors. Licensed under Apache-2.0.

//! Client for route based mode

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::join_all;
use tokio::sync::OnceCell;

use crate::{
    db_client::{inner::InnerClient, DbClient},
    errors::RouteBasedWriteError,
    model::{
        route::Endpoint,
        sql_query::{Request as SqlQueryRequest, Response as SqlQueryResponse},
        write::{Request as WriteRequest, Response as WriteResponse},
    },
    router::{Router, RouterImpl},
    rpc_client::{RpcClientFactory, RpcContext},
    util::should_refresh,
    Error, Result,
};

/// Client implementation for horaedb while using route based mode.
pub struct RouteBasedImpl<F: RpcClientFactory> {
    factory: Arc<F>,
    router_endpoint: String,
    router: OnceCell<Box<dyn Router>>,
    standalone_pool: DirectClientPool<F>,
    default_database: Option<String>,
}

impl<F: RpcClientFactory> RouteBasedImpl<F> {
    pub fn new(factory: Arc<F>, router_endpoint: String, default_database: Option<String>) -> Self {
        Self {
            factory: factory.clone(),
            router_endpoint,
            router: OnceCell::new(),
            standalone_pool: DirectClientPool::new(factory),
            default_database,
        }
    }

    async fn init_router(&self) -> Result<Box<dyn Router>> {
        let router_client = self.factory.build(self.router_endpoint.clone()).await?;
        let default_endpoint: Endpoint = self.router_endpoint.parse().map_err(|e| {
            Error::Client(format!(
                "Failed to parse default endpoint:{}, err:{}",
                self.router_endpoint, e
            ))
        })?;
        Ok(Box::new(RouterImpl::new(default_endpoint, router_client)))
    }
}

#[async_trait]
impl<F: RpcClientFactory> DbClient for RouteBasedImpl<F> {
    async fn sql_query(&self, ctx: &RpcContext, req: &SqlQueryRequest) -> Result<SqlQueryResponse> {
        if req.tables.is_empty() {
            return Err(Error::Unknown(
                "tables in query request can't be empty in route based mode".to_string(),
            ));
        }
        let ctx = crate::db_client::resolve_database(ctx, &self.default_database)?;

        let router_handle = self.router.get_or_try_init(|| self.init_router()).await?;

        let endpoint = match router_handle.route(&req.tables, &ctx).await {
            Ok(mut eps) => {
                if let Some(ep) = eps[0].take() {
                    ep
                } else {
                    return Err(Error::Unknown(
                        "table doesn't have corresponding endpoint".to_string(),
                    ));
                }
            }
            Err(e) => {
                return Err(e);
            }
        };

        let client = self.standalone_pool.get_or_create(&endpoint).clone();

        client.sql_query_internal(&ctx, req).await.map_err(|e| {
            router_handle.evict(&req.tables);
            e
        })
    }

    async fn write(&self, ctx: &RpcContext, req: &WriteRequest) -> Result<WriteResponse> {
        let ctx = crate::db_client::resolve_database(ctx, &self.default_database)?;

        // Get tables' related endpoints(some may not exist).
        let should_routes: Vec<_> = req.point_groups.keys().cloned().collect();
        let router_handle = self.router.get_or_try_init(|| self.init_router()).await?;
        let endpoints = router_handle.route(&should_routes, &ctx).await?;

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
                    write_req.point_groups.insert(
                        m.clone(),
                        req.point_groups.get(m.as_str()).cloned().unwrap(),
                    );
                }
                None => {
                    no_corresponding_endpoints.push(m);
                }
            });

        // Get client and send.
        let mut write_tables = vec![Vec::new(); partition_by_endpoint.len()];
        let client_req_paris: Vec<_> = partition_by_endpoint
            .into_iter()
            .enumerate()
            .map(|(idx, (ep, req))| {
                assert!(idx < write_tables.len());
                write_tables[idx].extend(req.point_groups.keys().cloned());
                (self.standalone_pool.get_or_create(&ep), req)
            })
            .collect();
        let mut futures = Vec::with_capacity(client_req_paris.len());
        for (client, req) in client_req_paris {
            let ctx_clone = ctx.clone();
            futures.push(async move { client.write_internal(&ctx_clone, &req).await })
        }

        // Await rpc results and collect results.
        let mut tables_result_pairs: Vec<_> = join_all(futures)
            .await
            .into_iter()
            .zip(write_tables.into_iter())
            .map(|(results, tables)| (tables, results))
            .collect();

        if !no_corresponding_endpoints.is_empty() {
            tables_result_pairs.push((
                no_corresponding_endpoints,
                Err(Error::Unknown(
                    "tables don't have corresponding endpoints".to_string(),
                )),
            ));
        }

        // Process results:
        //  + Evict outdated endpoints.
        //  + Merge results and return.
        let evicts: Vec<_> = tables_result_pairs
            .iter()
            .filter_map(|(tables, result)| {
                if let Err(Error::Server(server_error)) = &result {
                    if should_refresh(server_error.code, &server_error.msg) {
                        Some(tables.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .flatten()
            .collect();
        router_handle.evict(&evicts);

        let route_based_error: RouteBasedWriteError = tables_result_pairs.into();
        if route_based_error.all_ok() {
            Ok(route_based_error.ok.1)
        } else {
            Err(Error::RouteBasedWriteError(route_based_error))
        }
    }
}

/// DirectClientPool is the pool actually holding connections to data nodes.
struct DirectClientPool<F: RpcClientFactory> {
    pool: DashMap<Endpoint, Arc<InnerClient<F>>>,
    factory: Arc<F>,
}

impl<F: RpcClientFactory> DirectClientPool<F> {
    fn new(factory: Arc<F>) -> Self {
        Self {
            pool: DashMap::new(),
            factory,
        }
    }

    fn get_or_create(&self, endpoint: &Endpoint) -> Arc<InnerClient<F>> {
        if let Some(c) = self.pool.get(endpoint) {
            // If exist in cache, return.
            c.value().clone()
        } else {
            // If not exist, build --> insert --> return.
            self.pool
                .entry(endpoint.clone())
                .or_insert(Arc::new(InnerClient::new(
                    self.factory.clone(),
                    endpoint.to_string(),
                )))
                .clone()
        }
    }
}
