use crate::message::serde::DeserializeMessage;
use crate::client::connection::Connection;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use crate::message::proto::command_subscribe;
use crate::message::proto;
use tokio::time::Duration;
use std::collections::BTreeMap;
use crate::client::connection;
use crate::client::errors::ConsumerError;
use crate::client::lookup::response::BrokerLoc;
use tokio::sync::mpsc;

pub struct TopicConsumer<M: DeserializeMessage> {
    consumer_id: u64,
    config: ConsumerConfig,
    connection: Arc<Connection>,
    topic: String,
    message_rx: mpsc::Receiver<M>,
    //messages: Pin<Box<mpsc::Receiver<Result<(proto::MessageIdData, Payload), Error>>>>,
    //ack_tx: mpsc::UnboundedSender<AckMessage>,
    #[allow(unused)]
    //data_type: PhantomData<fn(Payload) -> T::Output>,
    dead_letter_policy: Option<DeadLetterPolicy>,
    last_message_received: Option<DateTime<Utc>>,
    messages_received: u64,
}

impl <M: DeserializeMessage> TopicConsumer<M> {
    async fn new(
        pool: Arc<connection::Pool>,
        topic: String,
        broker_loc: BrokerLoc,
        config: ConsumerConfig
    ) ->Result<TopicConsumer<M>, ConsumerError> {
        let ConsumerConfig{ subscription, sub_type, batch_size, consumer_name, consumer_id,
                            unacked_message_redelivery_delay, options, dead_letter_policy } = config.clone();
        let connection = pool.get_connection(broker_loc.clone()).await.map_err(ConsumerError::from)?;
        //let consumer_id = consumer_id.unwrap_or_else(rand::random());
        unimplemented!()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConsumerConfig {
    subscription: String,
    sub_type: command_subscribe::SubType,
    batch_size: Option<u32>,
    consumer_name: Option<String>,
    consumer_id: Option<u64>,
    unacked_message_redelivery_delay: Option<Duration>,
    options: ConsumerOptions,
    dead_letter_policy: Option<DeadLetterPolicy>,
}

#[derive(Clone, Default, Debug)]
pub struct ConsumerOptions {
    pub priority_level: Option<i32>,
    pub durable: Option<bool>,
    pub start_message_id: Option<proto::MessageIdData>,
    pub metadata: BTreeMap<String, String>,
    pub read_compacted: Option<bool>,
    pub schema: Option<proto::Schema>,
    pub initial_position: Option<i32>,
    pub replicate_subscription_state: Option<bool>,
    pub force_topic_creation: Option<bool>,
    pub start_message_rollback_duration_sec: Option<u64>,
    pub key_shared_meta: Option<proto::KeySharedMeta>
}

#[derive(Debug, Clone)]
pub struct DeadLetterPolicy {
    //Maximum number of times that a message will be redelivered before being sent to the dead letter queue.
    pub max_redeliver_count: usize,
    //Name of the dead topic where the failing messages will be sent.
    pub dead_letter_topic: String,
}

mod request {
    use crate::message::proto::{command_subscribe, CommandSuccess};
    use crate::message::proto;
    use crate::client::consumer::ConsumerOptions;
    use crate::client::connection::GetRequestParam;
    use crate::message::codec::Message;
    use crate::client::models::outbound::RequestKey;
    use std::sync::Arc;
    use crate::client::errors::ConsumerError;

    #[derive(Clone, Debug)]
    pub struct Subscribe {
        topic: String,
        subscription: String,
        sub_type: command_subscribe::SubType,
        consumer_id: u64,
        consumer_name: Option<String>,
        options: Arc<ConsumerOptions>,
    }

    impl GetRequestParam for Subscribe {
        type ReturnedVal = CommandSuccess;
        type Error = ConsumerError;

        fn get_request_key(&self) -> RequestKey {
            RequestKey::Consumer { id: self.consumer_id }
        }

        fn into_message(self, request_id: u64) -> Message {
            let Self { topic, subscription, sub_type, consumer_id, consumer_name, options} = self;
            let ConsumerOptions { priority_level, durable, start_message_id, metadata, read_compacted,
                schema, initial_position, replicate_subscription_state, force_topic_creation,
                start_message_rollback_duration_sec, key_shared_meta } = (*options).clone();

            Message {
                command: proto::BaseCommand {
                    type_: proto::base_command::Type::Subscribe as i32,
                    subscribe: Some(proto::CommandSubscribe {
                        topic,
                        subscription,
                        sub_type: sub_type as i32,
                        consumer_id,
                        request_id,
                        consumer_name,
                        priority_level,
                        durable,
                        metadata: metadata
                            .iter()
                            .map(|(k, v)| proto::KeyValue {
                                key: k.clone(),
                                value: v.clone(),
                            })
                            .collect(),
                        read_compacted,
                        initial_position,
                        replicate_subscription_state,
                        force_topic_creation,
                        start_message_rollback_duration_sec,
                        schema,
                        start_message_id,
                        key_shared_meta,
                    }),
                    ..Default::default()
                },
                payload: None,
            }
        }

        fn map(response: Message) -> Result<Self::ReturnedVal, Self::Error> {
            match response.command.success {
                Some(success) => Ok(success),
                None => Err(ConsumerError::SubscribeFail("Unexecpted none response".to_string()))
            }
        }

        fn can_retry_from_error(_: &Self::Error) -> bool {
            false
        }
    }
}