use std::error::Error;
use strum_macros::Display;
use crate::message::proto::ServerError;
use crate::stream::errors::TcpSockError;

#[derive(Display, Debug)]
pub enum ConnectionError {
    Disconnected,
    PulsarError {
        server_error: Option<ServerError>,
        message: Option<String>
    },
    Unexpected(String),
    SocketError(TcpSockError)
}

impl Error for ConnectionError {}

impl Into<ConnectionError> for TcpSockError {
    fn into(self) -> ConnectionError {
        ConnectionError::SocketError(self)
    }
}