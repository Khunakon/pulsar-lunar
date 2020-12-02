use crate::message::codec;
use crate::message::proto;
use std::collections::{BTreeMap, HashMap};
use chrono::Utc;

#[derive(Debug, Clone, Default)]
pub struct Message {
    /// serialized data
    pub payload: Vec<u8>,
    pub properties: HashMap<String, String>,
    /// key to decide partition for the msg
    pub partition_key: ::std::option::Option<String>,
    /// Override namespace's replication
    pub replicate_to: ::std::vec::Vec<String>,
    /// the timestamp that this event occurs. it is typically set by applications.
    /// if this field is omitted, `publish_time` can be used for the purpose of `event_time`.
    pub event_time: ::std::option::Option<u64>,
    pub schema_version: ::std::option::Option<Vec<u8>>,
}
/// internal message type carrying options that must be defined
/// by the producer
#[derive(Debug, Clone, Default)]
pub(crate) struct ProducerMessage {
    pub payload: Vec<u8>,
    pub properties: HashMap<String, String>,
    ///key to decide partition for the msg
    pub partition_key: ::std::option::Option<String>,
    /// Override namespace's replication
    pub replicate_to: ::std::vec::Vec<String>,
    pub compression: ::std::option::Option<i32>,
    pub uncompressed_size: ::std::option::Option<u32>,
    /// Removed below checksum field from Metadata as
    /// it should be part of send-command which keeps checksum of header + payload
    ///optional sfixed64 checksum = 10;
    ///differentiate single and batch message metadata
    pub num_messages_in_batch: ::std::option::Option<i32>,
    pub event_time: ::std::option::Option<u64>,
    /// Contains encryption key name, encrypted key and metadata to describe the key
    pub encryption_keys: ::std::vec::Vec<proto::EncryptionKeys>,
    /// Algorithm used to encrypt data key
    pub encryption_algo: ::std::option::Option<String>,
    /// Additional parameters required by encryption
    pub encryption_param: ::std::option::Option<Vec<u8>>,
    pub schema_version: ::std::option::Option<Vec<u8>>,
}

impl From<Message> for ProducerMessage {
    fn from(m: Message) -> Self {
        ProducerMessage {
            payload: m.payload,
            properties: m.properties,
            partition_key: m.partition_key,
            replicate_to: m.replicate_to,
            event_time: m.event_time,
            schema_version: m.schema_version,
            ..Default::default()
        }
    }
}

#[derive(Clone, Default)]
pub struct ProducerOptions {
    pub encrypted: Option<bool>,
    pub metadata: BTreeMap<String, String>,
    pub schema: Option<proto::Schema>,
    pub batch_size: Option<u32>,
    pub compression: Option<proto::CompressionType>,
}

pub fn create(
    topic: String,
    producer_name: Option<String>,
    producer_id: u64,
    request_id: u64,
    options: ProducerOptions) -> codec::Message {

    codec::Message {
        command: proto::BaseCommand {
            type_: proto::base_command::Type::Producer as i32,
            producer: Some(proto::CommandProducer {
                topic,
                producer_id,
                request_id,
                producer_name,
                encrypted: options.encrypted,
                metadata: options
                    .metadata
                    .iter()
                    .map(|(k, v)| proto::KeyValue {
                        key: k.clone(),
                        value: v.clone(),
                    })
                    .collect(),
                schema: options.schema,
                ..Default::default()
            }),
            ..Default::default()
        },
        payload: None,
    }
}

pub(crate) fn send(
    producer_id: u64,
    producer_name: String,
    sequence_id: u64,
    message: ProducerMessage,
) -> codec::Message {
    let properties = message
        .properties
        .into_iter()
        .map(|(key, value)| proto::KeyValue { key, value })
        .collect();

    codec::Message {
        command: proto::BaseCommand {
            type_: proto::base_command::Type::Send as i32,
            send: Some(proto::CommandSend {
                producer_id,
                sequence_id,
                num_messages: message.num_messages_in_batch,
                ..Default::default()
            }),
            ..Default::default()
        },
        payload: Some(codec::Payload {
            metadata: proto::MessageMetadata {
                producer_name,
                sequence_id,
                properties,
                publish_time: Utc::now().timestamp_millis() as u64,
                replicated_from: None,
                partition_key: message.partition_key,
                replicate_to: message.replicate_to,
                compression: message.compression,
                uncompressed_size: message.uncompressed_size,
                num_messages_in_batch: message.num_messages_in_batch,
                event_time: message.event_time,
                encryption_keys: message.encryption_keys,
                encryption_algo: message.encryption_algo,
                encryption_param: message.encryption_param,
                schema_version: message.schema_version,
                ..Default::default()
            },
            data: message.payload,
        }),
    }
}

pub fn close(producer_id: u64, request_id: u64) -> codec::Message {
    codec::Message {
        command: proto::BaseCommand {
            type_: proto::base_command::Type::CloseProducer as i32,
            close_producer: Some(proto::CommandCloseProducer {
                producer_id,
                request_id,
            }),
            ..Default::default()
        },
        payload: None,
    }
}
