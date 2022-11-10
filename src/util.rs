/// Server status code.
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
#[allow(dead_code)]
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
