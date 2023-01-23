// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Sql query request

use ceresdbproto::storage::SqlQueryRequest;

/// Query request
/// Avoid exposed interfaces explicitly depending on ceresproto
#[derive(Debug, Clone)]
pub struct Request {
    pub metrics: Vec<String>,
    pub sql: String,
}

impl From<Request> for SqlQueryRequest {
    fn from(req: Request) -> Self {
        SqlQueryRequest {
            tables: req.metrics,
            sql: req.sql,
        }
    }
}
