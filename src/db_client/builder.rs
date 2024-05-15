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

use crate::{
    db_client::{raw::RawImpl, route_based::RouteBasedImpl, DbClient},
    rpc_client::RpcClientImplFactory,
    Authorization, RpcConfig,
};

/// Access mode to HoraeDB server(s).
#[derive(Debug, Clone)]
pub enum Mode {
    /// When accessing HoraeDB cluster by `Direct` mode, the requests will be
    /// sent directly to the right HoraeDB instance determined by routing
    /// information.
    Direct,
    /// When accessing HoraeDB by `Proxy` mode, the requests are just sent to
    /// any one HoraeDB instance, which takes the responsibilities for
    /// forwarding the requests.
    Proxy,
}

/// The builder for building [`DbClient`](DbClient).
#[derive(Debug, Clone)]
pub struct Builder {
    mode: Mode,
    endpoint: String,
    default_database: Option<String>,
    rpc_config: RpcConfig,
    authorization: Option<Authorization>,
}

impl Builder {
    // We hide this detail new method for the convenience of users.
    pub fn new(endpoint: String, mode: Mode) -> Self {
        Self {
            mode,
            endpoint,
            rpc_config: RpcConfig::default(),
            default_database: None,
            authorization: None,
        }
    }

    #[inline]
    pub fn default_database(mut self, default_database: impl Into<String>) -> Self {
        self.default_database = Some(default_database.into());
        self
    }

    #[inline]
    pub fn rpc_config(mut self, rpc_config: RpcConfig) -> Self {
        self.rpc_config = rpc_config;
        self
    }

    #[inline]
    pub fn authorization(mut self, authorization: Authorization) -> Self {
        self.authorization = Some(authorization);
        self
    }

    pub fn build(self) -> Arc<dyn DbClient> {
        let rpc_client_factory = Arc::new(RpcClientImplFactory::new(
            self.rpc_config,
            self.authorization,
        ));

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
