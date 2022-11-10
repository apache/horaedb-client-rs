// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use ceresdbproto::storage::QueryRequest as QueryRequestPb;

/// Query request
/// Avoid exposed interfaces explicitly depending on ceresproto
#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub metrics: Vec<String>,
    pub ql: String,
}

impl From<QueryRequest> for QueryRequestPb {
    fn from(req: QueryRequest) -> Self {
        QueryRequestPb {
            metrics: req.metrics,
            ql: req.ql,
        }
    }
}
