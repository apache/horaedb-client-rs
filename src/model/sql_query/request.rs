// Copyright 2022 HoraeDB Project Authors. Licensed under Apache-2.0.

/// Sql query request.
#[derive(Debug, Clone)]
pub struct Request {
    /// The tables involved in the sql.
    ///
    /// This is a hint, by which the client can find the right server to handle
    /// the query, can accelerate query.
    pub tables: Vec<String>,
    /// The sql for query.
    pub sql: String,
}
