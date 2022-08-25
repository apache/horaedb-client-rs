// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

pub mod db_client;
mod errors;
pub mod model;
mod options;
mod router;
mod rpc_client;
mod util;

pub use crate::{
    errors::{Error, Result},
    options::{RpcConfig, RpcOptions},
    rpc_client::RpcContext,
};
