// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use ceresdbproto::{
    storage::{
        QueryRequest as QueryRequestPb, QueryResponse as QueryResponsePb,
        RouteRequest as RouteRequestPb, RouteResponse as RouteResponsePb,
        WriteRequest as WriteRequestPb, WriteResponse as WriteResponsePb,
    },
    storage_grpc::StorageServiceClient,
};
use grpcio::{CallOption, Channel, ChannelBuilder, EnvBuilder, Environment, MetadataBuilder};

use crate::{
    errors::{Error, Result, ServerError},
    options::{RpcConfig, RpcOptions},
    rpc_client::{RpcClient, RpcContext},
    util::is_ok,
};

const RPC_HEADER_TENANT_KEY: &str = "x-ceresdb-access-tenant";

/// The implementation for DbClient is based on grpc protocol.
#[derive(Clone)]
pub struct RpcClientImpl {
    raw_client: Arc<StorageServiceClient>,
    rpc_opts: RpcOptions,
    channel: Channel,
}

#[async_trait]
impl RpcClient for RpcClientImpl {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequestPb) -> Result<QueryResponsePb> {
        self.check_connectivity().await?;

        let call_opt = self.make_call_option(ctx)?;
        let mut resp = self.raw_client.query_async_opt(req, call_opt)?.await?;

        if !is_ok(resp.get_header().code) {
            let header = resp.take_header();
            return Err(Error::Server(ServerError {
                code: header.code,
                msg: header.error,
            }));
        }

        if resp.schema_content.is_empty() {
            let mut r = QueryResponsePb::default();
            r.set_affected_rows(resp.affected_rows);
            return Ok(r);
        }

        Ok(resp)
    }

    async fn write(&self, ctx: &RpcContext, req: &WriteRequestPb) -> Result<WriteResponsePb> {
        self.check_connectivity().await?;

        let call_opt = self.make_call_option(ctx)?;
        let mut resp = self.raw_client.write_async_opt(req, call_opt)?.await?;
        if !is_ok(resp.get_header().code) {
            let header = resp.take_header();
            return Err(Error::Server(ServerError {
                code: header.code,
                msg: header.error,
            }));
        }

        Ok(resp)
    }

    async fn route(&self, ctx: &RpcContext, req: &RouteRequestPb) -> Result<RouteResponsePb> {
        self.check_connectivity().await?;

        let call_opt = self.make_call_option(ctx)?;
        let mut resp = self.raw_client.route_async_opt(req, call_opt)?.await?;
        if !is_ok(resp.get_header().code) {
            let header = resp.take_header();
            return Err(Error::Server(ServerError {
                code: header.code,
                msg: header.error,
            }));
        }

        Ok(resp)
    }
}

impl RpcClientImpl {
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

    async fn check_connectivity(&self) -> Result<()> {
        if !self
            .channel
            .wait_for_connected(self.rpc_opts.connect_timeout)
            .await
        {
            return Err(Error::Connect(
                "Connection broken and try for reconnecting failed".to_string(),
            ));
        }

        Ok(())
    }
}

/// Builder for building an [`Client`].
#[derive(Clone)]
pub struct RpcClientImplBuilder {
    rpc_opts: RpcOptions,
    grpc_config: RpcConfig,
    env: Arc<Environment>,
}

#[allow(clippy::return_self_not_must_use)]
impl RpcClientImplBuilder {
    pub fn new(grpc_config: RpcConfig, rpc_opts: RpcOptions) -> Self {
        let env = {
            let mut env_builder = EnvBuilder::new();
            if let Some(thread_num) = grpc_config.thread_num {
                env_builder = env_builder.cq_count(thread_num);
            }

            Arc::new(env_builder.build())
        };

        Self {
            rpc_opts,
            grpc_config,
            env,
        }
    }

    pub fn build(&self, endpoint: String) -> RpcClientImpl {
        let channel = ChannelBuilder::new(self.env.clone())
            .max_send_message_len(self.grpc_config.max_send_msg_len)
            .max_receive_message_len(self.grpc_config.max_recv_msg_len)
            .keepalive_time(self.grpc_config.keepalive_time)
            .keepalive_timeout(self.grpc_config.keepalive_timeout)
            .connect(&endpoint);
        let channel_clone = channel.clone();
        let raw_client = Arc::new(StorageServiceClient::new(channel));
        RpcClientImpl {
            raw_client,
            rpc_opts: self.rpc_opts.clone(),
            channel: channel_clone,
        }
    }
}
