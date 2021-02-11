pub mod outbound {

    use crate::message::codec::Message;
    use crate::message::proto::*;
    use tokio::sync::oneshot;
    use std::convert::TryFrom;
    use strum_macros::Display;
    use tokio::sync::mpsc;
    use crate::net::errors::SendError;
    use futures::Future;

    #[derive(Debug, Display, Clone, PartialEq, Ord, PartialOrd, Eq)]
    pub enum RequestKey {
        AutoIncrRequestId,
        RequestId(u64),
        ProducerSend { producer_id: u64, sequence_id: u64 },
        Consumer { consumer_id: u64 },
    }

    impl RequestKey {
        pub fn from_message(message: &Message) -> Option<RequestKey> {
            match message.command {
                BaseCommand { subscribe: Some(CommandSubscribe { request_id, .. }), .. } |
                BaseCommand { partition_metadata: Some(CommandPartitionedTopicMetadata { request_id, .. }), .. } |
                BaseCommand { partition_metadata_response: Some(CommandPartitionedTopicMetadataResponse { request_id, .. }), .. } |
                BaseCommand { lookup_topic: Some(CommandLookupTopic { request_id, .. }), .. } |
                BaseCommand { lookup_topic_response: Some(CommandLookupTopicResponse { request_id, .. }), .. } |
                BaseCommand { producer: Some(CommandProducer { request_id, .. }), .. } |
                BaseCommand { producer_success: Some(CommandProducerSuccess { request_id, .. }), .. } |
                BaseCommand { unsubscribe: Some(CommandUnsubscribe { request_id, .. }), .. } |
                BaseCommand { seek: Some(CommandSeek { request_id, .. }), .. } |
                BaseCommand { close_producer: Some(CommandCloseProducer { request_id, .. }), .. } |
                BaseCommand { close_consumer: Some(CommandCloseConsumer { request_id, .. }), .. } |
                BaseCommand { success: Some(CommandSuccess { request_id, .. }), .. } |
                BaseCommand { error: Some(CommandError { request_id, .. }), .. } |
                BaseCommand { consumer_stats: Some(CommandConsumerStats { request_id, .. }), .. } |
                BaseCommand { consumer_stats_response: Some(CommandConsumerStatsResponse { request_id, .. }), .. } |
                BaseCommand { get_last_message_id: Some(CommandGetLastMessageId { request_id, .. }), .. } |
                BaseCommand { get_last_message_id_response: Some(CommandGetLastMessageIdResponse { request_id, .. }), .. } |
                BaseCommand { get_topics_of_namespace: Some(CommandGetTopicsOfNamespace { request_id, .. }), .. } |
                BaseCommand { get_topics_of_namespace_response: Some(CommandGetTopicsOfNamespaceResponse { request_id, .. }), .. } |
                BaseCommand { get_schema: Some(CommandGetSchema { request_id, .. }), .. } |
                BaseCommand { get_schema_response: Some(CommandGetSchemaResponse { request_id, .. }), .. } => {
                    Some(RequestKey::RequestId(request_id))
                }
                BaseCommand { send: Some(CommandSend { producer_id, sequence_id, .. }), .. } |
                BaseCommand { send_error: Some(CommandSendError { producer_id, sequence_id, .. }), .. } |
                BaseCommand { send_receipt: Some(CommandSendReceipt { producer_id, sequence_id, .. }), .. } => {
                    Some(RequestKey::ProducerSend { producer_id, sequence_id})
                }
                BaseCommand { active_consumer_change: Some(CommandActiveConsumerChange { consumer_id, .. }), .. } |
                BaseCommand { message: Some(CommandMessage { consumer_id, .. }), .. } |
                BaseCommand { flow: Some(CommandFlow { consumer_id, .. }), .. } |
                BaseCommand { redeliver_unacknowledged_messages: Some(CommandRedeliverUnacknowledgedMessages { consumer_id, .. }), .. } |
                BaseCommand { reached_end_of_topic: Some(CommandReachedEndOfTopic { consumer_id }), .. } |
                BaseCommand { ack: Some(CommandAck { consumer_id, .. }), .. } => {
                    Some(RequestKey::Consumer { consumer_id: consumer_id })
                },
                _ => {
                    match base_command::Type::try_from(message.command.type_ ) {
                        Ok(type_) => {
                            log::warn!("Unexpected payload for command of type {:?}. This is likely a bug!", type_);
                        }
                        Err(()) => {
                            log::warn!("Received BaseCommand of unexpected type: {}", message.command.type_);
                        }
                    }
                    None
                }
            }
        }
    }

    pub enum RetryReq {
        Default,
        LimitTo { max: u16, back_off_sec: u64 },
        Forever,
    }

    pub enum Request {
        Ask {
            key: RequestKey,
            message: Message,
            tx_response: oneshot::Sender<Result<Message, SendError>>
        },
        Tell {
            key: RequestKey,
            message: Message,
            tx_response: oneshot::Sender<Result<(), SendError>>
        }
    }

    impl Request {
        pub async fn get_response_via<'a, F, Fut>(self, sender: F )
            where
                F: FnOnce(RequestKey, Message) -> Fut + 'a,
                Fut: Future<Output = Result<(RequestKey, &'a mpsc::Sender<PendingResponse>), SendError>>
        {
            match self {
                Self::Ask { key, message, tx_response } => {
                    match sender(key, message).await {
                        Ok((request_key, register_pending_resp_tx)) => {
                            let pending_response = PendingResponse { request_key, tx_response };
                            if let Err(e) = register_pending_resp_tx.send(pending_response).await {
                                panic!(SendError::Unexpected(e.to_string()));
                            }
                        }
                        Err(e) => { tx_response.send(Err(e)).unwrap(); }
                    }
                }
                Self::Tell { key, message, tx_response } => {
                    let result = sender(key, message).await;
                    let _ = tx_response.send(result.map(|_|()));
                }
            };
        }
    }

    pub struct PendingResponse {
        pub request_key: RequestKey,
        pub tx_response: oneshot::Sender<Result<Message, SendError>>
    }

}

pub mod general {

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone)]
    pub struct SerialId(Arc<AtomicUsize>);

    impl SerialId {
        pub fn new() -> Self {
            Self::default()
        }
        pub fn get(&self) -> u64 {
            self.0.fetch_add(1, Ordering::Relaxed) as u64
        }
    }

    impl Default for SerialId {
        fn default() -> Self {
            SerialId(Arc::new(AtomicUsize::new(1)))
        }
    }

}
