use std::fmt::Display;

use ceresdbproto::storage::Endpoint as EndPointPb;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Endpoint {
    pub ip: String,
    pub port: u32,
}

impl Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}:{}", self.ip, self.port))
    }
}

impl Endpoint {
    pub fn new(ip: String, port: u32) -> Self {
        Self { ip, port }
    }
}

impl From<EndPointPb> for Endpoint {
    fn from(endpoint_pb: EndPointPb) -> Self {
        Self {
            ip: endpoint_pb.ip,
            port: endpoint_pb.port,
        }
    }
}
