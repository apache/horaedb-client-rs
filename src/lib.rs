// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

pub mod client;
pub mod errors;
pub mod model;
pub mod options;

pub use crate::{
    client::{Builder, DbClient},
    errors::{Error, Result},
    options::{GrpcConfig, RpcOptions},
};
