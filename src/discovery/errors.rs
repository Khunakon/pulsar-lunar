use strum_macros::Display;
use crate::net::errors::{SendError, ConnectionError};
use crate::message::proto;
use crate::message::validation::ValidationError;
use url::ParseError;

#[derive(Debug, Display)]
pub enum LookupError {
    ServiceNotReady,
    RequestNotSent(SendError),
    Query(Option<proto::ServerError>),
    ResponseValidationError(ValidationError),
    NotFound(String),
    Unexpected(String),
    ConnectionError(ConnectionError)
}

impl LookupError {
    pub fn can_be_retry(error: &LookupError) -> bool {
        match error {
            LookupError::RequestNotSent(_) | LookupError::ServiceNotReady => true,
            _ => false
        }
    }
}

impl From<SendError> for LookupError {
    fn from(e: SendError) -> Self {
        LookupError::RequestNotSent(e)
    }
}

impl From<ValidationError> for LookupError {
    fn from(e: ValidationError) -> Self { LookupError::ResponseValidationError(e) }
}

impl From<ParseError> for LookupError {
    fn from(e: ParseError) -> Self { LookupError::Unexpected(e.to_string())}
}

impl From<ConnectionError> for LookupError {
    fn from(e: ConnectionError) -> Self { LookupError::ConnectionError(e) }
}