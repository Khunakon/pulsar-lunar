use std::error::Error;

use strum_macros::Display;

use crate::netflow::errors::ConnectionError;
use crate::netflow::errors::SendError;
use crate::discovery::errors::LookupError;
use crate::message::errors::SerDeError;

#[derive(Display, Debug)]
pub enum ProducerError {
    Connection(ConnectionError),
    Custom(String),
    Io(std::io::Error),
    Lookup(LookupError),
    CreationFail(String),
    ErrorSendRequest(SendError),
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

impl From<SendError> for ProducerError {
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
    ErrorSendRequest(SendError),
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