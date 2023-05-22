// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use ceresdbproto::storage::WriteResponse as WriteResponsePb;

/// The write response for the [`WriteRequest`](crate::model::write::Request).
#[derive(Clone, Debug)]
pub struct Response {
    /// The number of the rows written successfully.
    pub success: u32,
    /// The number of the rows which fail to write.
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
