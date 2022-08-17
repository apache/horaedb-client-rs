// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Write response.

use ceresdbproto::storage::WriteResponse as WriteResponsePb;

#[derive(Debug)]
pub struct WriteResponse {
    pub success: u32,
    pub failed: u32,
}

impl From<WriteResponsePb> for WriteResponse {
    fn from(resp_pb: WriteResponsePb) -> Self {
        WriteResponse {
            success: resp_pb.success,
            failed: resp_pb.failed,
        }
    }
}
