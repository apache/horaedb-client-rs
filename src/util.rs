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

/// Server status code
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum StatusCode {
    Ok = 200,
    InvalidArgument = 400,
    NotFound = 404,
    TooManyRequests = 429,
    InternalError = 500,
}

impl StatusCode {
    pub fn as_u32(&self) -> u32 {
        *self as u32
    }
}

#[inline]
pub fn is_ok(code: u32) -> bool {
    code == StatusCode::Ok.as_u32()
}

// TODO may change in future.
#[inline]
pub fn should_refresh(code: u32, msg: &str) -> bool {
    code == StatusCode::InvalidArgument.as_u32()
        && msg.contains("Table")
        && msg.contains("not found")
}
