use strum_macros::Display;
use std::error::Error;

#[derive(Debug, Display)]
pub enum SerDeError {
   Io(std::io::Error),
   Custom(String)
}

impl From<serde_json::error::Error> for SerDeError {
   fn from(e: serde_json::error::Error) -> Self {
      SerDeError::Io(std::io::Error::from(e))
   }
}

impl From<std::io::Error> for SerDeError {
   fn from(e: std::io::Error) -> Self {
      SerDeError::Io(e)
   }
}

impl Error for SerDeError {}

#[derive(Display, Debug)]
pub enum FramingError {
   EncodeErr(String),
   DecodeErr(String),
   IoError(String)
}

impl Error for FramingError {}

impl From<std::io::Error> for FramingError {
   fn from(err: std::io::Error) -> FramingError {
       FramingError::IoError(err.to_string())
   }
}

impl From<prost::EncodeError> for FramingError {
   fn from(err: prost::EncodeError) -> FramingError {
      FramingError::EncodeErr(err.to_string())
   }
}

impl From<prost::DecodeError> for FramingError {
   fn from(err: prost::DecodeError) -> FramingError {
      FramingError::DecodeErr(err.to_string())
   }
}

use crate::message::proto::ServerError;

pub(crate) fn server_error(i: i32) -> Option<ServerError> {
   match i {
      0 => Some(ServerError::UnknownError),
      1 => Some(ServerError::MetadataError),
      2 => Some(ServerError::PersistenceError),
      3 => Some(ServerError::AuthenticationError),
      4 => Some(ServerError::AuthorizationError),
      5 => Some(ServerError::ConsumerBusy),
      6 => Some(ServerError::ServiceNotReady),
      7 => Some(ServerError::ProducerBlockedQuotaExceededError),
      8 => Some(ServerError::ProducerBlockedQuotaExceededException),
      9 => Some(ServerError::ChecksumError),
      10 => Some(ServerError::UnsupportedVersionError),
      11 => Some(ServerError::TopicNotFound),
      12 => Some(ServerError::SubscriptionNotFound),
      13 => Some(ServerError::ConsumerNotFound),
      14 => Some(ServerError::TooManyRequests),
      15 => Some(ServerError::TopicTerminatedError),
      16 => Some(ServerError::ProducerBusy),
      17 => Some(ServerError::InvalidTopicName),
      18 => Some(ServerError::IncompatibleSchema),
      19 => Some(ServerError::ConsumerAssignError),
      20 => Some(ServerError::TransactionCoordinatorNotFound),
      21 => Some(ServerError::InvalidTxnStatus),
      _ => None,
   }
}
