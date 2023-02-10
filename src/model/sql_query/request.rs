// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sql query request

/// Query request
/// Avoid exposed interfaces explicitly depending on ceresproto
#[derive(Debug, Clone)]
pub struct Request {
    pub tables: Vec<String>,
    pub sql: String,
}
