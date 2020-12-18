use std::error::Error;
use strum_macros::Display;
use crate::net::NetOpError;
use crate::stream::errors::TcpSockError::TlsError;

#[derive(Clone, Debug, Display)]
pub enum TcpSockError {
    InvalidUrlScheme(String) ,
    UnableToResolveHostName(String),
    IoError(String),
    TlsError(String)
}

impl Error for TcpSockError {}

impl From<NetOpError> for TcpSockError {
    fn from(f: NetOpError) -> Self {
        match f {
            NetOpError::InvalidUrlScheme(s) => TcpSockError::InvalidUrlScheme(s),
            NetOpError::UnableToResolveHostName(s) => TcpSockError::UnableToResolveHostName(s),
            NetOpError::UnableToReadCert(s) => TlsError(s)
        }
    }
}