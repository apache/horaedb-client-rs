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

use horaedbproto::storage::WriteResponse as WriteResponsePb;

/// The response for the [`WriteRequest`](crate::model::write::Request).
#[derive(Clone, Debug)]
pub struct Response {
    /// The number of the rows written successfully
    pub success: u32,
    /// The number of the rows which fail to write
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
