// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Write response

#[derive(Debug)]
pub struct WriteOk {
    pub metrics: Vec<String>,
    pub success: u32,
    pub failed: u32,
}
