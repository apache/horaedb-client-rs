// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use ceresdbproto::storage_grpc::StorageServiceClient;
use grpcio::{CallOption, ChannelBuilder, EnvBuilder, MetadataBuilder};

use crate::{
    errors::{self, Error, Result, ServerError},
    model::{
        convert,
        request::QueryRequest,
        row::{QueriedRows, QueryResponse},
    },
    options::{GrpcConfig, RpcOptions},
};

const RPC_HEADER_TENANT_KEY: &str = "x-ceresdb-access-tenant";

/// Context for rpc request.
#[derive(Clone, Debug)]
pub struct RpcContext {
    pub tenant: String,
    pub token: String,
}

/// The abstraction for client of ceresdb server.
#[async_trait]
pub trait DbClient {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> Result<QueryResponse>;
}

/// The implementation for DbClient is based on grpc protocol.
#[derive(Clone)]
pub struct Client {
    raw_client: Arc<StorageServiceClient>,
    rpc_opts: RpcOptions,
}

impl Client {
    /// Make the `CallOption` for grpc request.
    fn make_call_option(&self, ctx: &RpcContext) -> Result<CallOption> {
        let mut builder = MetadataBuilder::with_capacity(1);
        builder
            .add_str(RPC_HEADER_TENANT_KEY, &ctx.tenant)
            .map_err(|e| Error::Client(format!("invalid tenant:{}, err:{}", ctx.tenant, e)))?;
        let headers = builder.build();

        Ok(CallOption::default()
            .timeout(self.rpc_opts.read_timeout)
            .headers(headers))
    }
}

// TODO(kamille) may be better to rename `DbClient` and `Client`
#[async_trait]
impl DbClient for Client {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequest) -> Result<QueryResponse> {
        let call_opt = self.make_call_option(ctx)?;
        let mut resp = self
            .raw_client
            .query_async_opt(&req.clone().into(), call_opt)?
            .await?;

        if !errors::is_ok(resp.get_header().code) {
            let header = resp.take_header();
            return Err(Error::Server(ServerError {
                code: header.code,
                msg: header.error,
            }));
        }

        // TODO: maybe add a flag in protos to indicate if schema exists
        if resp.schema_content.is_empty() {
            return Ok(QueryResponse::AffectedRows(resp.affected_rows));
        }

        if resp.rows.is_empty() {
            return Ok(QueryResponse::Rows(QueriedRows::default()));
        }

        convert::parse_queried_rows(&resp.schema_content, &resp.rows)
            .map_err(|err_msg| Error::Client(err_msg))
            .map(QueryResponse::Rows)
    }
}

/// Builder for building an [`Client`].
#[derive(Debug, Clone)]
pub struct Builder {
    endpoint: String,
    rpc_opts: RpcOptions,
    grpc_config: GrpcConfig,
}

impl Builder {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            rpc_opts: RpcOptions::default(),
            grpc_config: GrpcConfig::default(),
        }
    }

    #[inline]
    pub fn grpc_config(mut self, grpc_config: GrpcConfig) -> Self {
        self.grpc_config = grpc_config;
        self
    }

    #[inline]
    pub fn rpc_opts(mut self, rpc_opts: RpcOptions) -> Self {
        self.rpc_opts = rpc_opts;
        self
    }

    pub fn build(self) -> Client {
        let env = {
            let mut env_builder = EnvBuilder::new();
            if let Some(thread_num) = self.grpc_config.thread_num {
                env_builder = env_builder.cq_count(thread_num);
            }

            Arc::new(env_builder.build())
        };

        let channel = ChannelBuilder::new(env)
            .max_send_message_len(self.grpc_config.max_send_msg_len)
            .max_receive_message_len(self.grpc_config.max_recv_msg_len)
            .keepalive_time(self.grpc_config.keepalive_time)
            .keepalive_timeout(self.grpc_config.keepalive_timeout)
            .connect(&self.endpoint);
        let raw_client = Arc::new(StorageServiceClient::new(channel));
        Client {
            raw_client,
            rpc_opts: self.rpc_opts,
        }
    }
}
