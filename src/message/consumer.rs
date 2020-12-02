use crate::message::codec::Message;
use crate::message::proto;

pub fn flow(consumer_id: u64, message_permits: u32) -> Message {
    Message {
        command: proto::BaseCommand {
            type_: proto::base_command::Type::Flow as i32,
            flow: Some(proto::CommandFlow {
                consumer_id,
                message_permits,
            }),
            ..Default::default()
        },
        payload: None,
    }
}
