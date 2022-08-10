// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

pub mod rpc_client;
pub mod errors;
pub mod model;
pub mod options;
mod route_client;
pub mod db_client;

pub use crate::{
    db_client::DbClient,
    db_client::standalone::{StandaloneImpl, StandaloneImplBuilder},
    errors::{Error, Result},
    model::write::is_reserved_column_name,
    options::{GrpcConfig, RpcOptions},
};
