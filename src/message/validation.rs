use crate::message::codec::Message;
use crate::message::errors::server_error;
use crate::message::proto::ServerError;
use strum_macros::Display;

#[derive(Debug, Display)]
pub enum ValidationError {
    UnexpectedResponse(String),
    PulsarError(Option<ServerError>, Option<String>)
}

pub fn validate_response<F, V>(response: Message, map: F) -> Result<V, ValidationError>
    where F: FnOnce(Message) -> Option<V>
{
    if let Some(e) = response.command.error {
        Err(ValidationError::PulsarError(server_error(e.error), Some(e.message) ))
    } else {
        let cmd = response.command.clone();
        if let Some(val) = map(response) {
            Ok(val)
        } else {
            Err(ValidationError::UnexpectedResponse(format!("{:?}", cmd)))
        }
    }
}