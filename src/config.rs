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

use std::time::Duration;

/// Config for the underlying grpc client
#[derive(Debug, Clone)]
pub struct RpcConfig {
    /// Thread num used by the grpc client.
    ///
    /// The number of cpu cores will be used if not set.
    pub thread_num: Option<usize>,
    /// The max length of the message sent to server.
    ///
    /// -1 means unlimited, and the default value is 20MB.
    pub max_send_msg_len: i32,
    /// The max length of the message received from server.
    ///
    /// -1 means unlimited, and the default value is 1GB.
    pub max_recv_msg_len: i32,
    /// The interval for htt2 ping frames.
    ///
    /// Default value is 600s.
    pub keep_alive_interval: Duration,
    /// Timeout for http2 ping frame acknowledgement.
    ///
    /// If the ping is not acknowledged within the timeout, the connection will
    /// be closed, and default value is 3s.
    pub keep_alive_timeout: Duration,
    /// Enables http2_keep_alive or not.
    ///
    /// It is enabled by default.
    pub keep_alive_while_idle: bool,
    /// Timeout for write operation.
    ///
    /// Default value is 5s.
    pub default_write_timeout: Duration,
    /// Timeout for sql_query operation.
    ///
    /// Default value is 60s.
    pub default_sql_query_timeout: Duration,
    /// Timeout for connection.
    ///
    /// Default value is 3s.
    pub connect_timeout: Duration,

    /// Authorization for rpc.
    pub authorization: Option<Authorization>,
}

#[derive(Debug, Clone)]
pub struct Authorization {
    pub username: String,
    pub password: String,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            thread_num: None,
            // 20MB
            max_send_msg_len: 20 * (1 << 20),
            // 1GB
            max_recv_msg_len: 1 << 30,
            keep_alive_interval: Duration::from_secs(60 * 10),
            keep_alive_timeout: Duration::from_secs(3),
            keep_alive_while_idle: true,
            default_write_timeout: Duration::from_secs(5),
            default_sql_query_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(3),
            authorization: None,
        }
    }
}
