// Copyright 2023 The HoraeDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
