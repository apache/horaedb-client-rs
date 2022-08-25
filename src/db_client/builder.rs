// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::{
    db_client::{cluster::ClusterImpl, standalone::StandaloneImpl, DbClient},
    router::RouterImpl,
    rpc_client::RpcClientImplBuilder,
    RpcConfig, RpcOptions,
};

#[derive(Debug, Clone)]
pub enum Mode {
    Standalone,
    Cluster,
}

/// Client builder, has standalone mode and cluster mode.
///
/// You should define the mode in [`new`],
/// and it cannot be changed after.
///
/// [`new`]: Builder::new
#[derive(Debug, Clone)]
pub struct Builder {
    mode: Mode,
    endpoint: String,
    rpc_opts: RpcOptions,
    grpc_config: RpcConfig,
}

impl Builder {
    pub fn new(endpoint: String, mode: Mode) -> Self {
        Self {
            mode,
            endpoint,
            rpc_opts: RpcOptions::default(),
            grpc_config: RpcConfig::default(),
        }
    }

    #[inline]
    pub fn grpc_config(mut self, grpc_config: RpcConfig) -> Self {
        self.grpc_config = grpc_config;
        self
    }

    #[inline]
    pub fn rpc_opts(mut self, rpc_opts: RpcOptions) -> Self {
        self.rpc_opts = rpc_opts;
        self
    }

    pub fn build(self) -> Arc<dyn DbClient> {
        let rpc_client_builder = RpcClientImplBuilder::new(self.grpc_config, self.rpc_opts);

        match self.mode {
            Mode::Standalone => {
                Arc::new(StandaloneImpl::new(rpc_client_builder.build(self.endpoint)))
            }

            Mode::Cluster => {
                let router = RouterImpl::new(rpc_client_builder.build(self.endpoint));
                Arc::new(ClusterImpl::new(router, rpc_client_builder))
            }
        }
    }
}
