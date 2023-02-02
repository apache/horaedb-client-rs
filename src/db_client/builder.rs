// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Client builder

use std::sync::Arc;

use crate::{
    db_client::{raw::RawImpl, route_based::RouteBasedImpl, DbClient},
    rpc_client::RpcClientImplFactory,
    RpcConfig, RpcOptions,
};

/// Client mode
///
/// + In `Direct` mode, request will be sent to corresponding endpoint
/// directly(maybe need to get the target endpoint by route request first).
/// + In `Proxy` mode, request will be sent to proxy server responsible for
/// forwarding the request.
#[derive(Debug, Clone)]
pub enum Mode {
    Direct,
    Proxy,
}

/// Client builder, has standalone mode and route based mode.
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
    // We hide this detail new method for the convenience of users.
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
        let rpc_client_factory =
            Arc::new(RpcClientImplFactory::new(self.grpc_config, self.rpc_opts));

        match self.mode {
            Mode::Direct => Arc::new(RouteBasedImpl::new(rpc_client_factory, self.endpoint)),
            Mode::Proxy => Arc::new(RawImpl::new(rpc_client_factory, self.endpoint)),
        }
    }
}
