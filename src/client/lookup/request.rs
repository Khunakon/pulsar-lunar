use crate::message::codec::Message;
use crate::message::errors::server_error;
use crate::message::proto;
use crate::message::proto::{CommandPartitionedTopicMetadataResponse, CommandLookupTopicResponse};
use crate::message::proto::base_command::Type::PartitionedMetadata;
use crate::client::lookup::response::*;
use crate::client::lookup::GetLookupRequestParam;
use crate::client::lookup::errors::LookupError;
use crate::message::proto::command_lookup_topic_response;
use crate::message::proto::command_partitioned_topic_metadata_response;
use url::Url;
use crate::client::lookup::request::utils::validate_lookup_response;
use crate::client::models::outbound::RequestKey;
use std::sync::Arc;
use crate::client::connection;

#[derive(Clone)]
pub struct GetPartitionTopicMetadata {
    pub topic: String
}

impl GetLookupRequestParam for GetPartitionTopicMetadata {
    type ReturnedVal = PartitionNumbersResponse;
    type ExtractedVal = CommandPartitionedTopicMetadataResponse;

    fn into_message(self, request_id: u64) -> Message {

        let Self { topic } = self;

        Message {
            command: proto::BaseCommand {
                type_: proto::base_command::Type::PartitionedMetadata as i32,
                partition_metadata: Some(proto::CommandPartitionedTopicMetadata {
                    topic,
                    request_id,
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    fn extract(response: Message) -> Option<Self::ExtractedVal> {
        response.command.partition_metadata_response
    }

    fn map(extracted_val: Self::ExtractedVal) -> Result<Self::ReturnedVal, LookupError> {
        let _ = {
            validate_lookup_response(
                extracted_val.response.clone(),
                |code| (command_partitioned_topic_metadata_response::LookupType::Failed as i32) == code
            )?
        };

        if (extracted_val.clone().partitions).is_none() {
            Err(LookupError::Query(extracted_val.error.and_then(server_error)))
        } else {
            Ok(PartitionNumbersResponse(extracted_val))
        }
    }
}

#[derive(Clone)]
pub struct LookupTopic {
    pub topic: String,
    pub authoritative: bool
}

impl GetLookupRequestParam for LookupTopic {

    type ReturnedVal = LookupTopicResponse;
    type ExtractedVal = CommandLookupTopicResponse;

    fn into_message(self, request_id: u64) -> Message {
        let Self { topic, authoritative } = self;
        Message {
            command: proto::BaseCommand {
                type_: proto::base_command::Type::Lookup as i32,
                lookup_topic: Some(proto::CommandLookupTopic {
                    topic,
                    request_id,
                    authoritative: Some(authoritative),
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    fn extract(response: Message) -> Option<Self::ExtractedVal> {
        response.command.lookup_topic_response
    }

    fn map(extracted_val: Self::ExtractedVal) -> Result<Self::ReturnedVal, LookupError> {

        let _ = {
            validate_lookup_response(
                extracted_val.response.clone(),
                |code| (command_lookup_topic_response::LookupType::Failed as i32) == code)?
        };

        let proxy_through_service_url = extracted_val.proxy_through_service_url.unwrap_or(false);
        let is_authoritative = extracted_val.authoritative.unwrap_or(false);
        let redirect = extracted_val.response == Some(command_lookup_topic_response::LookupType::Redirect as i32);

        if extracted_val.broker_service_url.is_none() {
            Err(LookupError::NotFound("The broker service url is not found!".to_string()))
        } else {
            let broker_url = Url::parse(&extracted_val.broker_service_url.clone().unwrap());
            let broker_url = broker_url.map_err(LookupError::from)?;
            let broker_url_tls = match extracted_val.broker_service_url_tls.as_ref() {
                Some(url) => Some(Url::parse(&url).map_err(LookupError::from)?),
                None => None
            };

            Ok(LookupTopicResponse {
                broker_url,
                broker_url_tls,
                proxy_through_service: proxy_through_service_url,
                redirect,
                is_authoritative
            })
        }
    }

}

mod utils {
    use crate::client::lookup::errors::LookupError;
    use crate::message::errors::server_error;
    use crate::message::proto;

    pub fn validate_lookup_response(
        code: Option<i32>,
        is_error: impl FnOnce(i32) -> bool,
    ) -> Result<(), LookupError> {
        match code {
            Some(code) if is_error(code) => {
                if let Some(err) = server_error(code) {
                    if err == proto::ServerError::ServiceNotReady {
                        Err(LookupError::ServiceNotReady)
                    } else {
                        Err(LookupError::Query(Some(err)))
                    }
                } else {
                    Err(LookupError::Query(None))
                }
            },
            None => Err(LookupError::Unexpected("Receive none response code!".to_string())),
            _ => Ok(())
        }
    }
}