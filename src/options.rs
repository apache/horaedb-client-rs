// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Options in client

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RpcConfig {
    /// Set the thread num as the cpu cores number if not set.
    pub thread_num: Option<usize>,
    /// -1 means unlimited.
    pub max_send_msg_len: i32,
    /// -1 means unlimited.
    pub max_recv_msg_len: i32,
    /// An interval for htt2 ping frames.
    pub keep_alive_interval: Duration,
    /// Timeout for http2 ping frame acknowledgement.
    pub keep_alive_timeout: Duration,
    /// Enables http2_keep_alive or not.
    pub keep_alive_while_idle: bool,
    /// Timeout for write operation.
    pub default_write_timeout: Duration,
    /// Timeout for sql_query operation.
    pub default_sql_query_timeout: Duration,
    /// Timeout for connection.
    pub connect_timeout: Duration,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            thread_num: None,
            // 20MB
            max_send_msg_len: 20 * (1 << 20),
            // 1GB
            max_recv_msg_len: 1 << 30,
            // Sets an interval for HTTP2 Ping frames should be sent to keep a connection alive
            keep_alive_interval: Duration::from_secs(60 * 10),
            // A timeout for receiving an acknowledgement of the keep-alive ping
            // If the ping is not acknowledged within the timeout, the connection will be closed
            keep_alive_timeout: Duration::from_secs(3),
            // default keep http2 connections alive while idle
            keep_alive_while_idle: true,
            default_write_timeout: Duration::from_secs(5),
            default_sql_query_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(3),
        }
    }
}
