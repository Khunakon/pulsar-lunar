use std::error::Error;
use strum_macros::Display;

#[derive(Debug, Display)]
pub enum TcpSockError {
    InvalidUrlScheme(String) ,
    CannotResolveHostName(String),
    IoError(String),
    TlsError(String)
}

impl Error for TcpSockError {}