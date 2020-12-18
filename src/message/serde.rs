use crate::message::codec::Payload;
use std::string::FromUtf8Error;
use crate::message::producer;
use crate::message::errors::SerDeError;

pub trait DeserializeMessage {
    type Output: Sized;
    fn deserialize_message(payload: &Payload) -> Self::Output;
}

impl DeserializeMessage for Vec<u8> {
    type Output = Self;
    fn deserialize_message(payload: &Payload) -> Self::Output {
        payload.data.to_vec()
    }
}

impl DeserializeMessage for String {
    type Output = Result<String, FromUtf8Error>;
    fn deserialize_message(payload: &Payload) -> Self::Output {
        String::from_utf8(payload.data.to_vec())
    }
}

/// Helper trait for message serialization
pub trait SerializeMessage {
    fn serialize_message(input: Self) -> Result<producer::Message, SerDeError>;
}

impl SerializeMessage for producer::Message {
    fn serialize_message(input: Self) -> Result<producer::Message, SerDeError> {
        Ok(input)
    }
}

impl<'a> SerializeMessage for &'a [u8] {
    fn serialize_message(input: Self) -> Result<producer::Message, SerDeError> {
        Ok(producer::Message {
            payload: input.to_vec(),
            ..Default::default()
        })
    }
}

impl SerializeMessage for Vec<u8> {
    fn serialize_message(input: Self) -> Result<producer::Message, SerDeError> {
        Ok(producer::Message {
            payload: input,
            ..Default::default()
        })
    }
}

impl SerializeMessage for String {
    fn serialize_message(input: Self) -> Result<producer::Message, SerDeError> {
        let payload = input.into_bytes();
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl<'a> SerializeMessage for &String {
    fn serialize_message(input: Self) -> Result<producer::Message, SerDeError> {
        let payload = input.as_bytes().to_vec();
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl<'a> SerializeMessage for &'a str {
    fn serialize_message(input: Self) -> Result<producer::Message, SerDeError> {
        let payload = input.as_bytes().to_vec();
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}
