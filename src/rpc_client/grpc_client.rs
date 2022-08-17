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
use grpcio::{CallOption, ChannelBuilder, EnvBuilder, Environment, MetadataBuilder};

use crate::{
    errors::{self, Error, Result, ServerError},
    options::{GrpcConfig, RpcOptions},
    rpc_client::{RpcClient, RpcContext},
};

const RPC_HEADER_TENANT_KEY: &str = "x-ceresdb-access-tenant";

/// The implementation for DbClient is based on grpc protocol.
#[derive(Clone)]
pub struct GrpcClient {
    raw_client: Arc<StorageServiceClient>,
    rpc_opts: RpcOptions,
}

impl GrpcClient {
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

    pub async fn query(&self, ctx: &RpcContext, req: &QueryRequestPb) -> Result<QueryResponsePb> {
        let call_opt = self.make_call_option(ctx)?;
        let mut resp = self.raw_client.query_async_opt(req, call_opt)?.await?;

        if !errors::is_ok(resp.get_header().code) {
            let header = resp.take_header();
            return Err(Error::Server(ServerError {
                code: header.code,
                msg: header.error,
            }));
        }

        if resp.schema_content.is_empty() {
            let mut r = QueryResponsePb::default();
            r.affected_rows = resp.affected_rows;
            return Ok(r);
        }

        Ok(resp)
    }

    pub async fn write(&self, ctx: &RpcContext, req: &WriteRequestPb) -> Result<WriteResponsePb> {
        let call_opt = self.make_call_option(ctx)?;
        let mut resp = self.raw_client.write_async_opt(req, call_opt)?.await?;
        if !errors::is_ok(resp.get_header().code) {
            let header = resp.take_header();
            return Err(Error::Server(ServerError {
                code: header.code,
                msg: header.error,
            }));
        }

        Ok(resp)
    }
}

#[async_trait]
impl RpcClient for GrpcClient {
    async fn query(&self, ctx: &RpcContext, req: &QueryRequestPb) -> Result<QueryResponsePb> {
        self.query(ctx, req).await
    }

    async fn write(&self, ctx: &RpcContext, req: &WriteRequestPb) -> Result<WriteResponsePb> {
        self.write(ctx, req).await
    }

    async fn route(&self, ctx: &RpcContext, req: &RouteRequestPb) -> Result<RouteResponsePb> {
        todo!()
    }
}

/// Builder for building an [`Client`].
#[derive(Clone)]
pub struct GrpcClientBuilder {
    rpc_opts: RpcOptions,
    grpc_config: GrpcConfig,
    env: Arc<Environment>,
}

#[allow(clippy::return_self_not_must_use)]
impl GrpcClientBuilder {
    pub fn new(thread_num: usize) -> Self {
        let env = {
            let mut env_builder = EnvBuilder::new();
            env_builder = env_builder.cq_count(thread_num);
            Arc::new(env_builder.build())
        };

        Self {
            rpc_opts: RpcOptions::default(),
            grpc_config: GrpcConfig::default(),
            env,
        }
    }

    #[inline]
    pub fn grpc_config(&mut self, grpc_config: GrpcConfig) -> &mut Self {
        self.grpc_config = grpc_config;
        self
    }

    #[inline]
    pub fn rpc_opts(&mut self, rpc_opts: RpcOptions) -> &mut Self {
        self.rpc_opts = rpc_opts;
        self
    }

    pub fn build(&self, endpoint: String) -> GrpcClient {
        let channel = ChannelBuilder::new(self.env.clone())
            .max_send_message_len(self.grpc_config.max_send_msg_len)
            .max_receive_message_len(self.grpc_config.max_recv_msg_len)
            .keepalive_time(self.grpc_config.keepalive_time)
            .keepalive_timeout(self.grpc_config.keepalive_timeout)
            .connect(&endpoint);
        let raw_client = Arc::new(StorageServiceClient::new(channel));
        GrpcClient {
            raw_client,
            rpc_opts: self.rpc_opts.clone(),
        }
    }
}
