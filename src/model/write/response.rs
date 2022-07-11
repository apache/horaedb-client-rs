// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

/// Write request
/// You can write into multiple metrics once
#[derive(Debug)]
pub struct WriteOk {
    pub metrics: Vec<String>,
    pub success: u32,
    pub failed: u32,
}
