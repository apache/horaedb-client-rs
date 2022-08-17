// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

pub mod db_client;
pub mod errors;
pub mod model;
pub mod options;
mod router;
pub mod rpc_client;

pub use crate::{
    db_client::{
        standalone::{StandaloneImpl, StandaloneImplBuilder},
        DbClient,
    },
    errors::{Error, Result},
    model::write::is_reserved_column_name,
    options::{GrpcConfig, RpcOptions},
};
