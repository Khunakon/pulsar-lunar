use std::collections::HashMap;
use std::io::Write;

use crate::message::proto;
use crate::message::proto::CompressionType;

// message data that will be sent on a topic
// generated from the [SerializeMessage] trait or [MessageBuilder]
// this is actually a subset of the fields of a message, because batching,
// compression and encryption should be handled by the producer
#[derive(Debug, Clone, Default)]
pub struct Message {
    /// serialized data
    pub payload: Vec<u8>,
    pub properties: HashMap<String, String>,
    /// key to decide partition for the msg
    pub partition_key: Option<String>,
    /// Override namespace's replication
    pub replicate_to: Vec<String>,
    /// the timestamp that this event occurs. it is typically set by applications.
    /// if this field is omitted, `publish_time` can be used for the purpose of `event_time`.
    pub event_time: Option<u64>,
    pub schema_version: Option<Vec<u8>>,
}
/// internal message type carrying options that must be defined
/// by the producer
#[derive(Debug, Clone, Default)]
pub struct ProducerMessage {
    pub payload: Vec<u8>,
    pub properties: HashMap<String, String>,
    ///key to decide partition for the msg
    pub partition_key: Option<String>,
    /// Override namespace's replication
    pub replicate_to: Vec<String>,
    pub compression: Option<i32>,
    pub uncompressed_size: Option<u32>,
    /// Removed below checksum field from Metadata as
    /// it should be part of send-command which keeps checksum of header + payload
    ///optional sfixed64 checksum = 10;
    ///differentiate single and batch message metadata
    pub num_messages_in_batch: Option<i32>,
    pub event_time: Option<u64>,
    /// Contains encryption key name, encrypted key and metadata to describe the key
    pub encryption_keys: Vec<proto::EncryptionKeys>,
    /// Algorithm used to encrypt data key
    pub encryption_algo: Option<String>,
    /// Additional parameters required by encryption
    pub encryption_param: Option<Vec<u8>>,
    pub schema_version: Option<Vec<u8>>,
}

impl ProducerMessage {

    pub fn compress(mut self, compression: Option<CompressionType> ) -> Result<Self, std::io::Error> {
        match compression {
            None | Some(CompressionType::None) => Ok(self),
            Some(CompressionType::Lz4) => {
                #[cfg(not(feature = "lz4"))]
                    return unimplemented!();

                #[cfg(feature = "lz4")]
                    {
                        let v: Vec<u8> = Vec::new();
                        let mut encoder = lz4::EncoderBuilder::new().build(v)?;

                        encoder.write(&self.payload[..])?;
                        let (compressed_payload, result) = encoder.finish();
                        result?;
                        self.payload = compressed_payload;
                        self.compression = Some(1);
                        Ok(self)
                    }
            }
            Some(CompressionType::Zlib) => {
                #[cfg(not(feature = "flate2"))]
                    return unimplemented!();

                #[cfg(feature = "flate2")]
                    {
                        let mut e = flate2::write::ZlibEncoder::new(
                            Vec::new(),
                            flate2::Compression::default()
                        );

                        e.write_all(&self.payload[..])?;
                        let compressed_payload = e.finish()?;

                        self.payload = compressed_payload;
                        self.compression = Some(2);
                        Ok(self)
                    }
            }
            Some(CompressionType::Zstd) => {
                #[cfg(not(feature = "zstd"))]
                    return unimplemented!();

                #[cfg(feature = "zstd")]
                    {
                        let compressed_payload = zstd::encode_all(&self.payload[..], 0)?;
                        self.compression = Some(3);
                        self.payload = compressed_payload;
                        Ok(self)
                    }
            }
            Some(CompressionType::Snappy) => {
                #[cfg(not(feature = "snap"))]
                    return unimplemented!();

                #[cfg(feature = "snap")]
                    {
                        let compressed_payload: Vec<u8> = Vec::new();
                        let mut encoder = snap::write::FrameEncoder::new(compressed_payload);
                        encoder.write(&self.payload[..])?;
                        let compressed_payload = encoder
                            .into_inner()
                            //FIXME
                            .map_err(|e| {
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!("Snappy compression error: {:?}", e),
                                )
                            })?;

                        self.payload = compressed_payload;
                        self.compression = Some(4);
                        Ok(self)
                    }
            }

        }
    }
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

