// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct GrpcConfig {
    /// Set the thread num as the cpu cores number if not set.
    pub thread_num: Option<usize>,
    /// -1 means unlimited
    pub max_send_msg_len: i32,
    /// -1 means unlimited
    pub max_recv_msg_len: i32,
    pub keepalive_time: Duration,
    pub keepalive_timeout: Duration,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            thread_num: None,
            // 20MB
            max_send_msg_len: 20 * (1 << 20),
            // 1GB
            max_recv_msg_len: 1 << 30,
            // 1day
            keepalive_time: Duration::from_secs(3600 * 30),
            keepalive_timeout: Duration::from_secs(3),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RpcOptions {
    pub write_timeout: Duration,
    pub read_timeout: Duration,
}

impl Default for RpcOptions {
    fn default() -> Self {
        Self {
            write_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(60),
        }
    }
}
