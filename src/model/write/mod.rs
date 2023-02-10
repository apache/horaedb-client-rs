// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Model for write

pub mod point;
mod request;
mod response;

pub use request::{pb_builder::WriteTableRequestPbsBuilder, Request};
pub use response::Response;
