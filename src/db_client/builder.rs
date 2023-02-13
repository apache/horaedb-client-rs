// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Client builder

use std::sync::Arc;

use crate::{
    db_client::{raw::RawImpl, route_based::RouteBasedImpl, DbClient},
    rpc_client::RpcClientImplFactory,
    RpcConfig,
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
    default_database: Option<String>,
    rpc_config: RpcConfig,
}

impl Builder {
    // We hide this detail new method for the convenience of users.
    pub fn new(endpoint: String, mode: Mode) -> Self {
        Self {
            mode,
            endpoint,
            rpc_config: RpcConfig::default(),
            default_database: None,
        }
    }

    #[inline]
    pub fn default_database(mut self, default_database: String) -> Self {
        self.default_database = Some(default_database);
        self
    }

    #[inline]
    pub fn rpc_config(mut self, rpc_config: RpcConfig) -> Self {
        self.rpc_config = rpc_config;
        self
    }

    pub fn build(self) -> Arc<dyn DbClient> {
        let rpc_client_factory = Arc::new(RpcClientImplFactory::new(self.rpc_config));

        match self.mode {
            Mode::Direct => Arc::new(RouteBasedImpl::new(
                rpc_client_factory,
                self.endpoint,
                self.default_database,
            )),
            Mode::Proxy => Arc::new(RawImpl::new(
                rpc_client_factory,
                self.endpoint,
                self.default_database,
            )),
        }
    }
}
