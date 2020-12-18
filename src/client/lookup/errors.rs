use strum_macros::Display;
use crate::client::models::outbound::SendError;
use crate::message::proto;
use crate::message::validation::ValidationError;
use url::ParseError;
use crate::client::errors;
use crate::client::errors::ConnectionError;

#[derive(Debug, Display)]
pub enum LookupError {
    ServiceNotReady,
    RequestNotSent(SendError),
    Query(Option<proto::ServerError>),
    ResponseValidationError(ValidationError),
    NotFound(String),
    Unexpected(String),
    ConnectionError(errors::ConnectionError)
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

impl From<errors::ConnectionError> for LookupError {
    fn from(e: errors::ConnectionError) -> Self { LookupError::ConnectionError(e) }
}