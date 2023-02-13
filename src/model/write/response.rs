// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Write response.

use ceresdbproto::storage::WriteResponse as WriteResponsePb;

#[derive(Clone, Debug)]
pub struct Response {
    pub success: u32,
    pub failed: u32,
}

impl Response {
    pub fn new(success: u32, failed: u32) -> Self {
        Self { success, failed }
    }
}

impl From<WriteResponsePb> for Response {
    fn from(resp_pb: WriteResponsePb) -> Self {
        Response {
            success: resp_pb.success,
            failed: resp_pb.failed,
        }
    }
}
