use std::error::Error;
use strum_macros::Display;
use crate::message::proto::ServerError;
use crate::stream::errors::TcpSockError;
use crate::net;
use std::sync::Arc;
use crate::client::lookup::errors::LookupError;
use crate::client::models::outbound;
use crate::client::models::outbound::SendError;
use crate::message::errors::SerDeError;

#[derive(Display, Debug)]
pub enum ConnectionError {
    Disconnected,
    PulsarError {
        server_error: Option<ServerError>,
        message: Option<String>
    },
    Unexpected(String),
    SocketError(TcpSockError),
    NetOpError(net::NetOpError)
}

impl Error for ConnectionError {}

impl From<TcpSockError> for ConnectionError {
    fn from(e: TcpSockError) -> Self { ConnectionError::SocketError(e) }
}

impl From<net::NetOpError> for ConnectionError {
    fn from(e: net::NetOpError) -> Self {
        ConnectionError::NetOpError(e)
    }
}

#[derive(Display, Debug)]
pub enum ProducerError {
    Connection(ConnectionError),
    Custom(String),
    Io(std::io::Error),
    Lookup(LookupError),
    CreationFail(String),
    ErrorSendRequest(outbound::SendError),
    ErrorSendMessage(String),
    CloseFail(String),
    SerDe(SerDeError)
    //PartialSend(Vec<Result<SendFuture, Error>>),
    // Indiciates the error was part of sending a batch, and thus shared across the batch
    //Batch(Arc<Error>)
}

impl From<ConnectionError> for ProducerError {
    fn from(e: ConnectionError) -> Self {
        ProducerError::Connection(e)
    }
}

impl From<LookupError> for ProducerError {
    fn from(e: LookupError) -> Self {
        ProducerError::Lookup(e)
    }
}

impl From<outbound::SendError> for ProducerError {
    fn from(e: SendError) -> Self { ProducerError::ErrorSendRequest(e) }
}

impl From<SerDeError> for ProducerError {
    fn from(e: SerDeError) -> Self { ProducerError::SerDe(e) }
}

impl From<std::io::Error> for ProducerError {
    fn from(e: std::io::Error) -> Self { ProducerError::Io(e) }
}

impl Error for ProducerError {}

#[derive(Display, Debug)]
pub enum ConsumerError {
    Connection(ConnectionError),
    Custom(String),
    Io(std::io::Error),
    Lookup(LookupError),
    SubscribeFail(String),
    ErrorSendRequest(outbound::SendError),
    ErrorSendMessage(String),
    CloseFail(String),
    SerDe(SerDeError)
}

impl From<ConnectionError> for ConsumerError {
    fn from(e: ConnectionError) -> Self {
        ConsumerError::Connection(e)
    }
}

impl Error for ConsumerError {}