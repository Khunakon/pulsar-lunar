use std::error::Error;
use strum_macros::Display;
use crate::net::NetOpError;
use crate::net::errors::TcpSockError::TlsError;
use crate::message::proto::ServerError;
use crate::net::models::outbound::RequestKey;

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

#[derive(Display, Debug)]
pub enum ConnectionError {
    Disconnected,
    PulsarError {
        server_error: Option<ServerError>,
        message: Option<String>
    },
    Unexpected(String),
    SocketError(TcpSockError),
    NetOpError(NetOpError)
}

impl Error for ConnectionError {}

impl From<TcpSockError> for ConnectionError {
    fn from(e: TcpSockError) -> Self { ConnectionError::SocketError(e) }
}

impl From<NetOpError> for ConnectionError {
    fn from(e: NetOpError) -> Self {
        ConnectionError::NetOpError(e)
    }
}

#[derive(Debug, Display)]
pub enum SendError {
    SendTimeout(RequestKey),
    MessageNotSent(String),
    Unexpected(String)
}

impl Error for SendError {}
