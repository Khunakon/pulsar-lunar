use crate::message::proto::{MessageMetadata, BaseCommand, SingleMessageMetadata, base_command};
use tokio_util::codec::{Encoder, Decoder};
use crate::message::errors::FramingError;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;
use nom::number::streaming::{be_u16, be_u32};

// import extension for protobuf decoding/ encoding
use prost::{self, Message as ImplProtobuf};

#[derive(Debug)]
pub struct Message {
    pub command: BaseCommand,
    pub payload: Option<Payload>,
}

#[derive(Debug)]
pub struct Payload {
    pub metadata: MessageMetadata,
    pub data: Vec<u8>,
}

pub struct BatchedMessage {
    pub metadata: SingleMessageMetadata,
    pub payload: Vec<u8>,
}

impl BatchedMessage {
    pub(crate) fn serialize(&self, w: &mut Vec<u8>) {
        w.put_u32(self.metadata.encoded_len() as u32);
        let _ = self.metadata.encode(w);
        w.put_slice(&self.payload);
    }

    pub(crate) fn parse(count: u32, payload: &[u8]) -> Result<Vec<BatchedMessage>, FramingError> {
        let (_, result) = nom::multi::count(batched_message, count as usize)(payload)
            .map_err(|e|
                FramingError::DecodeErr(format!("Error decoding batched messages: {:?}", e))
            )?;

        Ok(result)
    }
}

#[rustfmt::skip::macros(named)]
named!(batched_message<BatchedMessage>,
    do_parse!(
        metadata_size: be_u32 >>
        metadata: map_res!(
            take!(metadata_size),
            SingleMessageMetadata::decode
        ) >>
        payload: take!(metadata.payload_size) >>

        (BatchedMessage {
            metadata,
            payload: payload.to_vec(),
        })
    )
);

struct CommandFrame<'a> {
    #[allow(dead_code)]
    total_size: u32,
    #[allow(dead_code)]
    command_size: u32,
    command: &'a [u8],
}

#[rustfmt::skip::macros(named)]
named!(command_frame<CommandFrame>,
    do_parse!(
        total_size: be_u32 >>
        command_size: be_u32 >>
        command: take!(command_size) >>

        (CommandFrame {
            total_size,
            command_size,
            command,
        })
    )
);

struct PayloadFrame<'a> {
    #[allow(dead_code)]
    magic_number: u16,
    #[allow(dead_code)]
    checksum: u32,
    #[allow(dead_code)]
    metadata_size: u32,
    metadata: &'a [u8],
}

#[rustfmt::skip::macros(named)]
named!(payload_frame<PayloadFrame>,
    do_parse!(
        magic_number: be_u16 >>
        checksum: be_u32 >>
        metadata_size: be_u32 >>
        metadata: take!(metadata_size) >>

        (PayloadFrame {
            magic_number,
            checksum,
            metadata_size,
            metadata,
        })
    )
);

pub struct Codec;

impl Encoder<Message> for Codec {
    type Error = FramingError;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), FramingError> {
        let command_size = item.command.encoded_len();
        let metadata_size = item
            .payload
            .as_ref()
            .map(|p| p.metadata.encoded_len())
            .unwrap_or(0);
        let payload_size = item.payload.as_ref().map(|p| p.data.len()).unwrap_or(0);
        let header_size = if item.payload.is_some() { 18 } else { 8 };
        // Total size does not include the size of the 'totalSize' field, so we subtract 4
        let total_size = command_size + metadata_size + payload_size + header_size - 4;
        let mut buf = Vec::with_capacity(total_size + 4);

        // Simple command frame
        buf.put_u32(total_size as u32);
        buf.put_u32(command_size as u32);
        item.command.encode(&mut buf)?;

        // Payload command frame
        if let Some(payload) = &item.payload {
            buf.put_u16(0x0e01);

            let crc_offset = buf.len();
            buf.put_u32(0); // NOTE: Checksum (CRC32c). Overrwritten later to avoid copying.

            let metdata_offset = buf.len();
            buf.put_u32(metadata_size as u32);
            payload.metadata.encode(&mut buf)?;
            buf.put(&payload.data[..]);

            let crc = crc::crc32::checksum_castagnoli(&buf[metdata_offset..]);
            let mut crc_buf: &mut [u8] = &mut buf[crc_offset..metdata_offset];
            crc_buf.put_u32(crc);
        }
        if dst.remaining_mut() < buf.len() {
            dst.reserve(buf.len());
        }
        dst.put_slice(&buf);
        log::trace!("Encoder sending {} bytes", buf.len());
        Ok(())
    }

}

impl Decoder for Codec {
    type Item = Message;
    type Error = FramingError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Message>, FramingError> {
        log::trace!("Decoder received {} bytes", src.len());
        if src.len() >= 4 {
            let mut buf = Cursor::new(src);
            // `messageSize` refers only to _remaining_ message size, so we add 4 to get total frame size
            let message_size = buf.get_u32() as usize + 4;
            let src = buf.into_inner();
            if src.len() >= message_size {
                let msg = {
                    let (buf, command_frame) =
                        command_frame(&src[..message_size]).map_err(|err| {
                            FramingError::DecodeErr(format!("Error decoding command frame: {:?}", err))
                        })?;
                    let command = BaseCommand::decode(command_frame.command)?;

                    let payload = if !buf.is_empty() {
                        let (buf, payload_frame) = payload_frame(buf).map_err(|err| {
                            FramingError::DecodeErr(format!("Error decoding payload frame: {:?}", err))
                        })?;

                        // TODO: Check crc32 of payload data

                        let metadata = MessageMetadata::decode(payload_frame.metadata)?;
                        Some(Payload {
                            metadata,
                            data: buf.to_vec(),
                        })
                    } else {
                        None
                    };

                    Message { command, payload }
                };

                //TODO advance as we read, rather than this weird post thing
                src.advance(message_size);
                return Ok(Some(msg));
            }
        }
        Ok(None)
    }
}

use std::convert::TryFrom;

impl TryFrom<i32> for base_command::Type {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, ()> {
        match value {
             2 => Ok(base_command::Type::Connect),
             3 => Ok(base_command::Type::Connected),
             4 => Ok(base_command::Type::Subscribe),
             5 => Ok(base_command::Type::Producer),
             6 => Ok(base_command::Type::Send),
             7 => Ok(base_command::Type::SendReceipt),
             8 => Ok(base_command::Type::SendError),
             9 => Ok(base_command::Type::Message),
            10 => Ok(base_command::Type::Ack),
            11 => Ok(base_command::Type::Flow),
            12 => Ok(base_command::Type::Unsubscribe),
            13 => Ok(base_command::Type::Success),
            14 => Ok(base_command::Type::Error),
            15 => Ok(base_command::Type::CloseProducer),
            16 => Ok(base_command::Type::CloseConsumer),
            17 => Ok(base_command::Type::ProducerSuccess),
            18 => Ok(base_command::Type::Ping),
            19 => Ok(base_command::Type::Pong),
            20 => Ok(base_command::Type::RedeliverUnacknowledgedMessages),
            21 => Ok(base_command::Type::PartitionedMetadata),
            22 => Ok(base_command::Type::PartitionedMetadataResponse),
            23 => Ok(base_command::Type::Lookup),
            24 => Ok(base_command::Type::LookupResponse),
            25 => Ok(base_command::Type::ConsumerStats),
            26 => Ok(base_command::Type::ConsumerStatsResponse),
            27 => Ok(base_command::Type::ReachedEndOfTopic),
            28 => Ok(base_command::Type::Seek),
            29 => Ok(base_command::Type::GetLastMessageId),
            30 => Ok(base_command::Type::GetLastMessageIdResponse),
            31 => Ok(base_command::Type::ActiveConsumerChange),
            32 => Ok(base_command::Type::GetTopicsOfNamespace),
            33 => Ok(base_command::Type::GetTopicsOfNamespaceResponse),
            34 => Ok(base_command::Type::GetSchema),
            35 => Ok(base_command::Type::GetSchemaResponse),
            _ => Err(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::message::codec::Codec;
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};
    use std::convert::TryFrom;

    #[test]
    fn parse_simple_command() {
        let input: &[u8] = &[
            0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00, 0x1E, 0x08, 0x02, 0x12, 0x1A, 0x0A, 0x10,
            0x32, 0x2E, 0x30, 0x2E, 0x31, 0x2D, 0x69, 0x6E, 0x63, 0x75, 0x62, 0x61, 0x74, 0x69,
            0x6E, 0x67, 0x20, 0x0C, 0x2A, 0x04, 0x6E, 0x6F, 0x6E, 0x65,
        ];

        let message = Codec.decode(&mut input.into()).unwrap().unwrap();

        {
            let connect = message.command.connect.as_ref().unwrap();
            assert_eq!(connect.client_version, "2.0.1-incubating");
            assert_eq!(connect.auth_method_name.as_ref().unwrap(), "none");
            assert_eq!(connect.protocol_version.as_ref().unwrap(), &12);
        }

        let mut output = BytesMut::with_capacity(38);
        Codec.encode(message, &mut output).unwrap();
        assert_eq!(&output, input);
    }

    #[test]
    fn parse_payload_command() {
        let input: &[u8] = &[
            0x00, 0x00, 0x00, 0x3D, 0x00, 0x00, 0x00, 0x08, 0x08, 0x06, 0x32, 0x04, 0x08, 0x00,
            0x10, 0x08, 0x0E, 0x01, 0x42, 0x83, 0x54, 0xB5, 0x00, 0x00, 0x00, 0x19, 0x0A, 0x0E,
            0x73, 0x74, 0x61, 0x6E, 0x64, 0x61, 0x6C, 0x6F, 0x6E, 0x65, 0x2D, 0x30, 0x2D, 0x33,
            0x10, 0x08, 0x18, 0xBE, 0xC0, 0xFC, 0x84, 0xD2, 0x2C, 0x68, 0x65, 0x6C, 0x6C, 0x6F,
            0x2D, 0x70, 0x75, 0x6C, 0x73, 0x61, 0x72, 0x2D, 0x38,
        ];

        let message = Codec.decode(&mut input.into()).unwrap().unwrap();
        {
            let send = message.command.send.as_ref().unwrap();
            assert_eq!(send.producer_id, 0);
            assert_eq!(send.sequence_id, 8);
        }

        {
            let payload = message.payload.as_ref().unwrap();
            assert_eq!(payload.metadata.producer_name, "standalone-0-3");
            assert_eq!(payload.metadata.sequence_id, 8);
            assert_eq!(payload.metadata.publish_time, 1533850624062);
        }

        let mut output = BytesMut::with_capacity(65);
        Codec.encode(message, &mut output).unwrap();
        assert_eq!(&output, input);
    }

    #[test]
    fn base_command_type_parsing() {
        use super::proto::base_command::Type;
        let mut successes = 0;
        for i in 0..40 {
            if let Ok(type_) = Type::try_from(i) {
                successes += 1;
                assert_eq!(type_ as i32, i);
            }
        }
        assert_eq!(successes, 34);
    }
}
