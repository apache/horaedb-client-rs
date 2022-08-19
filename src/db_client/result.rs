// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use crate::{
    errors::Result,
    model::{write::WriteResponse, QueryResponse},
};

/// Query result of [`DbClient`].
///
/// It just wraps [`Result<QueryResponse>`] simply now,
/// and may contains more contents in future.
pub struct QueryResult {
    pub result: Result<QueryResponse>,
}

impl QueryResult {
    pub fn new(result: Result<QueryResponse>) -> Self {
        Self { result }
    }
}

pub type QueryResultVec = Vec<QueryResult>;

/// Write result of [`Dbclient`].
///
/// It contains metrics(tables) and their related write results.
pub struct WriteResult {
    pub metrics: Vec<String>,
    pub result: Result<WriteResponse>,
}

pub type WriteResultVec = Vec<WriteResult>;

impl WriteResult {
    pub fn new(metrics: Vec<String>, result: Result<WriteResponse>) -> Self {
        Self { metrics, result }
    }
}
