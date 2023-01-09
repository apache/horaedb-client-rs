// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{fmt::Display, str::FromStr};

use ceresdbproto::storage::Endpoint as EndPointPb;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Endpoint {
    pub addr: String,
    pub port: u32,
}

impl Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}:{}", self.addr, self.port))
    }
}

impl Endpoint {
    pub fn new(ip: String, port: u32) -> Self {
        Self { addr: ip, port }
    }
}

impl FromStr for Endpoint {
    type Err = Box<dyn std::error::Error + Send + Sync>;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (addr, raw_port) = match s.rsplit_once(':') {
            Some(v) => v,
            None => {
                let err_msg = "Can't find ':' in the source string".to_string();
                return Err(Self::Err::from(err_msg));
            }
        };

        if addr.is_empty() {
            let err_msg = "Empty addr in the source string".to_string();
            return Err(Self::Err::from(err_msg));
        }

        let port = raw_port.parse().map_err(|e| {
            let err_msg = format!("Fail to parse port:{}, err:{}", raw_port, e);
            Self::Err::from(err_msg)
        })?;
        if port > u16::MAX as u32 {
            let err_msg = "Too large port (<=65536)".to_string();
            return Err(Self::Err::from(err_msg));
        }

        Ok(Endpoint {
            addr: addr.to_string(),
            port,
        })
    }
}

impl From<EndPointPb> for Endpoint {
    fn from(endpoint_pb: EndPointPb) -> Self {
        Self {
            addr: endpoint_pb.ip,
            port: endpoint_pb.port,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_endpoint() {
        let normal_cases = vec![
            ("127.0.0.1:80", "127.0.0.1", 80),
            ("hello.world.com:1080", "hello.world.com", 1080),
            ("ceresdb.io:8831", "ceresdb.io", 8831),
        ];

        for (raw_endpoint, addr, port) in normal_cases {
            let endpoint: Endpoint = raw_endpoint.parse().unwrap();
            assert_eq!(addr, endpoint.addr);
            assert_eq!(port, endpoint.port);
        }

        let abnormal_cases = vec!["127.0.0.1", ":1080", "", "0:99999999"];
        for raw_endpoint in abnormal_cases {
            let parse_res = raw_endpoint.parse::<Endpoint>();
            assert!(parse_res.is_err());
        }
    }
}
