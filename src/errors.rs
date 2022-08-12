// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

#[derive(Debug)]
pub enum Error {
    /// Error from the running server.
    Server(ServerError),
    /// Error from the rpc.
    /// Note that any error caused by a running server wont be wrapped in the
    /// grpc errors.
    Rpc(grpcio::Error),
    /// Error from the client and basically the rpc request has not been called
    /// yet or the rpc request has already been finished successfully.
    Client(String),
    /// Error unknown
    Unknown(String),
}

#[derive(Debug, Clone)]
pub struct ServerError {
    pub code: u32,
    pub msg: String,
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<grpcio::Error> for Error {
    fn from(grpc_err: grpcio::Error) -> Self {
        Error::Rpc(grpc_err)
    }
}

#[inline]
pub fn is_ok(code: u32) -> bool {
    code == 200
}
