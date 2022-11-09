// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RpcConfig {
    /// Set the thread num as the cpu cores number if not set.
    pub thread_num: Option<usize>,
    /// -1 means unlimited
    pub max_send_msg_len: i32,
    /// -1 means unlimited
    pub max_recv_msg_len: i32,
    // an interval for htt2 ping frames
    pub keepalive_interval: Duration,
    // timeout for http2 ping frame acknowledement
    pub keepalive_timeout: Duration,
    // enables http2_keep_alive or not
    pub keep_alive_while_idle: bool,
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
            keepalive_interval: Duration::from_secs(60 * 10),
            // A timeout for receiving an acknowledgement of the keep-alive ping
            // If the ping is not acknowledged within the timeout, the connection will be closed
            keepalive_timeout: Duration::from_secs(3),
            // default keep http2 connections alive while idle
            keep_alive_while_idle: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RpcOptions {
    pub write_timeout: Duration,
    pub read_timeout: Duration,
    pub connect_timeout: Duration,
}

impl Default for RpcOptions {
    fn default() -> Self {
        Self {
            write_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(3),
        }
    }
}
