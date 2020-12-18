use crate::client::models::general::SerialId;
use crate::message::proto;
use std::collections::{BTreeMap, VecDeque};
use crate::client::connection::Connection;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio::time;
use crate::client::errors::ProducerError;
use crate::client::lookup::response::BrokerLoc;
use crate::client::lookup::{lookup_partitioned_topic, LookupTopic};
use crate::client::connection;
use crate::client::producer::request::{CreateProducer, CloseProducer, SendMessage};
use crate::message::serde::SerializeMessage;
use crate::message::proto::CommandSendReceipt;
use crate::message::producer::ProducerMessage;
use request::Batch;

pub async fn create(pool: Arc<connection::Pool>, config: ProducerConfig) -> Result<Producer, ProducerError> {

    let ProducerConfig { topic, producer_name, producer_options } = config;
    let producer_options = Arc::new(producer_options.unwrap_or_default());

    let map = {
        let producer_options = producer_options.clone();
        let pool = pool.clone();
        move |(topic, broker_loc)| {
            let producer_options = producer_options.clone();
            let producer_name = producer_name.clone();
            let pool = pool.clone();
            TopicProducer::new(broker_loc, topic, pool, producer_name, producer_options)
        }
    };

    let mut producers =
        lookup_partitioned_topic(topic.clone(), pool, map)
            .await.map_err(ProducerError::from)?;

    let variant =
        match producers.len() {
            0 => return Err(ProducerError::Custom(format!("Unexpected zero partitioned number for topic: {}", topic))),
            1 => ProducerVariant::Single(producers.pop().unwrap()),
            _ => {
                let mut producers = VecDeque::from(producers);
                producers.rotate_right(1);
                ProducerVariant::Partitioned(PartitionedProducer {
                    producers,
                    topic,
                    options: producer_options
                })
            }
        };
    Ok(Producer { variant })
}

struct TopicProducer {
    pool: Arc<connection::Pool>,
    connection: Arc<Connection>,
    id: u64,
    name: String,
    topic: String,
    message_id: SerialId,
    //putting it in a mutex because we must send multiple messages at once
    // while we might be pushing more messages from elsewhere
    batch: Option<Mutex<Batch>>,
    compression: Option<proto::CompressionType>,
    sig_drop_tx: oneshot::Sender<()>,
    options: Arc<ProducerOptions>,
    epoch: u64
}

impl TopicProducer {

    pub(crate) async fn new<S: Into<String>>(broker_loc: BrokerLoc,
                                             topic: S,
                                             pool: Arc<connection::Pool>,
                                             name: Option<String>,
                                             options: Arc<ProducerOptions>) -> Result<Self, ProducerError> {
        let topic = topic.into();
        let compression = options.validate_compression_option()?;
        let request = CreateProducer {
            topic,
            producer_id: rand::random(),
            producer_name: name,
            options,
            epoch: None
        };

        let connection = pool.get_connection(broker_loc).await.map_err(ProducerError::from)?;
        let success = connection.send_request_with_default_retry(&request).await?;

        let (sig_drop_tx, sig_drop_rx) = oneshot::channel();

        Self::mon_sig_drop(connection.clone(), request.producer_id, sig_drop_rx);

        let CreateProducer {producer_id, topic, options, ..} = request;

        Ok(TopicProducer {
            pool,
            connection,
            id: producer_id,
            name: success.producer_name,
            topic,
            message_id: SerialId::new(),
            batch: options.batch_size.map(Batch::new).map(Mutex::new),
            compression,
            sig_drop_tx,
            options,
            epoch: 0
        })
    }

    fn mon_sig_drop(connection: Arc<Connection>, producer_id: u64, sig_drop_rx: oneshot::Receiver<()>) {
        // Closing this producer when it is dropped by dropping `sig_drop_tx` together.
        // Dropping `sig_drop_tx` causes `sig_drop_rx` to be completed and close producer request is sent.
        tokio::spawn( async move {
            // An Err is received when the `sig_drop_tx` is dropped. But the producer's reconnect logic
            // might send a drop signal to prevent the closing of this producer.
            if let Err(_) = sig_drop_rx.await {
                let result = connection.send_request(CloseProducer { producer_id }).await;
                match result {
                    Ok(_) => log::trace!("Producer is dropped and closed. producer_id: {}", producer_id),
                    Err(e) => log::error!(
                        "Producer is dropped but error occurred while closing producer. producer_id {}, error: {:?}",
                        producer_id, e
                    )
                }
            }
        });
    }

    async fn reconnect(&mut self) -> Result<(), ProducerError> {

        let (sig_drop_tx, sig_drop_rx) = oneshot::channel::<()>();

        let sig_drop_tx = {
            let dest = &mut self.sig_drop_tx;
            std::mem::replace(dest, sig_drop_tx)
        };

        let _ = sig_drop_tx.send(());

        let lookup_topic = LookupTopic {
            topic: self.topic.clone(),
            authoritative: false
        };

        let response = self.pool.main_connection
            .send_request_with_default_retry(&lookup_topic)
            .await.map_err(ProducerError::from)?;

        let broker_loc = response
            .into_broker_loc(self.pool.clone(), self.topic.clone())
            .await.map_err(ProducerError::from)?;

        self.connection = self.pool
            .get_connection(broker_loc)
            .await.map_err(ProducerError::from)?;

        Self::mon_sig_drop(self.connection.clone(), self.id, sig_drop_rx);

        self.epoch += 1;

        let create_producer = CreateProducer {
            topic: self.topic.clone(),
            producer_id: self.id,
            producer_name: Some(self.name.clone()),
            options: self.options.clone(),
            epoch: Some(self.epoch)
        };

        let _ = self.connection
            .send_request_with_default_retry(&create_producer)
            .await.map_err(ProducerError::from)?;

        Ok(())
    }

    async fn send<M: SerializeMessage + Sized>(&mut self, message: M, retry_send: RetrySend) -> Result<CommandSendReceipt, ProducerError> {
        let message = M::serialize_message(message)
            .map_err(ProducerError::from)?
            .into();
        self.send_producer_message(message, retry_send).await
    }

    async fn send_producer_message(&mut self, message: ProducerMessage, retry_send: RetrySend) -> Result<CommandSendReceipt, ProducerError> {
        match self.batch.as_ref() {
            None => {
                let message = message.compress(self.compression).map_err(ProducerError::from)?;
                let sequence_id = self.message_id.get();
                let mut retry_count = 0;

                loop {
                    let request = SendMessage {
                        message: message.clone(),
                        sequence_id,
                        producer_id: self.id,
                        producer_name: self.name.clone()
                    };

                    let result = self.connection.send_request(request).await;

                    match retry_send {
                        RetrySend::Never => {
                            break result
                        }
                        RetrySend::LimitTo { max_retry_count, back_off_sec } => {
                            if result.is_err() && retry_count < max_retry_count {
                                time::sleep(time::Duration::from_secs(back_off_sec)).await;
                                retry_count += 1;
                            } else {
                                break result;
                            }
                        }
                        RetrySend::Forever => {}
                    }

                    if let Err(e) = result {
                        log::error!("Error occurred while sending the message!. error: {:?}, retry...", e);
                        self.reconnect().await?;
                    } else {
                        break result
                    }
                }

            }
            Some(batch) => {
                //todo: support batched message sending
                unimplemented!()
            }
        }
    }

}

#[derive(Default)]
pub struct ProducerConfig {
    pub topic: String,
    pub producer_name: Option<String>,
    pub producer_options: Option<ProducerOptions>
}

pub struct Producer {
    variant: ProducerVariant
}

impl Producer {

    pub fn topic(&self) -> &str {
        match &self.variant {
            ProducerVariant::Single(p) => &p.topic,
            ProducerVariant::Partitioned(p) => &p.topic
        }
    }

    pub fn partitions(&self) -> Option<Vec<String>> {
        match &self.variant {
            ProducerVariant::Single(_) => None,
            ProducerVariant::Partitioned(p) => {
                Some(p.producers.iter().map(|p| p.topic.clone()).collect())
            }
        }
    }

    pub fn options(&self) -> Arc<ProducerOptions> {
        match &self.variant {
            ProducerVariant::Single(p) => p.options.clone(),
            ProducerVariant::Partitioned(p) => p.options.clone()
        }
    }

    pub async fn send<M: SerializeMessage + Sized>(
        &mut self,
        message: M,
        retry_send: RetrySend
    ) -> Result<CommandSendReceipt, ProducerError > {
        match &mut self.variant {
            ProducerVariant::Single(p) => p.send(message, retry_send).await,
            ProducerVariant::Partitioned(p) => p.next().send(message, retry_send).await
        }
    }

    pub async fn send_producer_message(
        &mut self,
        message: ProducerMessage,
        retry_send: RetrySend
    ) -> Result<CommandSendReceipt, ProducerError> {
        match &mut self.variant {
            ProducerVariant::Single(p) => p.send_producer_message(message, retry_send).await,
            ProducerVariant::Partitioned(p) => p.next().send_producer_message(message, retry_send).await
        }
    }

}

enum ProducerVariant {
    Single(TopicProducer),
    Partitioned(PartitionedProducer)
}

struct PartitionedProducer {
    // Guaranteed to be non-empty
    producers: VecDeque<TopicProducer>,
    topic: String,
    options: Arc<ProducerOptions>,
}

impl PartitionedProducer {
    pub fn next(&mut self) -> &mut TopicProducer {
        self.producers.rotate_left(1);
        self.producers.front_mut().unwrap()
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

impl ProducerOptions {

    pub fn validate_compression_option(&self) -> Result<Option<proto::CompressionType>, ProducerError> {
        match self.compression.clone() {
            Some(proto::CompressionType::Lz4) => {
                #[cfg(not(feature = "lz4"))]
                    return Err(ProducerError::Custom("cannot create a producer with LZ4 compression because the 'lz4' cargo feature is not active".to_string()));
            }
            Some(proto::CompressionType::Zlib) => {
                #[cfg(not(feature = "flate2"))]
                    return Err(ProducerError::Custom("cannot create a producer with zlib compression because the 'flate2' cargo feature is not active".to_string()));
            }
            Some(proto::CompressionType::Zstd) => {
                #[cfg(not(feature = "zstd"))]
                    return Err(ProducerError::Custom("cannot create a producer with zstd compression because the 'zstd' cargo feature is not active".to_string()));
            }
            Some(proto::CompressionType::Snappy) => {
                #[cfg(not(feature = "snap"))]
                    return Err(ProducerError::Custom("cannot create a producer with Snappy compression because the 'snap' cargo feature is not active".to_string()));
            },
            _  => {}
        }
        Ok(self.compression.clone())
    }

}

pub enum RetrySend {
    Never,
    Forever,
    LimitTo { max_retry_count: u16, back_off_sec: u64 }
}


mod request {
    use crate::client::connection::GetRequestParam;
    use crate::message::codec::{Message, BatchedMessage, Payload};
    use crate::client::models::outbound::RequestKey;
    use crate::message::proto;
    use crate::client::errors::ProducerError;
    use crate::client::producer::ProducerOptions;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::collections::VecDeque;
    use tokio::sync::oneshot;
    use crate::message::proto::CommandSendReceipt;
    use crate::message::producer::ProducerMessage;
    use chrono::Utc;

    pub struct Batch {
        pub length: u32,
        // put it in a mutex because the design of Producer requires an immutable TopicProducer,
        // so we cannot have a mutable Batch in a send_raw(&mut self, ...)
        pub storage: Mutex<VecDeque<(
            oneshot::Sender<Result<proto::CommandSendReceipt, ProducerError>>, BatchedMessage
        )>>
    }

    impl Batch {
        pub fn new(length: u32) -> Batch {
            Batch {
                length,
                storage: Mutex::new(VecDeque::with_capacity(length as usize)),
            }
        }

        pub fn is_full(&self) -> bool {
            self.storage.lock().unwrap().len() >= self.length as usize
        }

        pub fn push_back(&self, msg: (
            oneshot::Sender<Result<proto::CommandSendReceipt, ProducerError>>,
            ProducerMessage
        )) {
            let (tx, message) = msg;

            let properties = message
                .properties
                .into_iter()
                .map(|(key, value)| proto::KeyValue { key, value })
                .collect();

            let batched = BatchedMessage {
                metadata: proto::SingleMessageMetadata {
                    properties,
                    partition_key: message.partition_key,
                    payload_size: message.payload.len() as i32,
                    ..Default::default()
                },
                payload: message.payload,
            };

            self.storage.lock().unwrap().push_back((tx, batched))
        }

        pub fn get_messages(
            &self,
        ) -> Vec<(
            oneshot::Sender<Result<proto::CommandSendReceipt, ProducerError>>,
            BatchedMessage,
        )> {
            self.storage.lock().unwrap().drain(..).collect()
        }
    }

    #[derive(Clone)]
    pub struct CreateProducer {
        pub topic: String,
        pub producer_id: u64,
        pub producer_name: Option<String>,
        pub options: Arc<ProducerOptions>,
        /// If producer reconnect to broker, the epoch of this producer will +1
        pub epoch: Option<u64>
    }

    impl GetRequestParam for CreateProducer {
        type ReturnedVal = proto::CommandProducerSuccess;
        type Error = ProducerError;

        fn get_request_key(&self) -> RequestKey { RequestKey::AutoIncrRequestId }
        fn into_message(self, request_id: u64) -> Message {
            let Self { topic, producer_id, producer_name, options, epoch } = self;
            let user_provided_producer_name = if producer_name.is_some() { Some(true) } else { None };
            Message {
                command: proto::BaseCommand {
                    type_: proto::base_command::Type::Producer as i32,
                    producer: Some(proto::CommandProducer {
                        topic,
                        producer_id,
                        request_id,
                        producer_name,
                        encrypted: options.encrypted.clone(),
                        metadata: options
                            .metadata
                            .iter()
                            .map(|(k, v)| proto::KeyValue {
                                key: k.clone(),
                                value: v.clone(),
                            })
                            .collect(),
                        schema: options.schema.clone(),
                        epoch,
                        user_provided_producer_name
                    }),
                    ..Default::default()
                },
                payload: None,
            }
        }

        fn map(response: Message) -> Result<Self::ReturnedVal, Self::Error> {
            match (response.command.producer_success, response.command.error) {
                (Some(cmd), _) => Ok(cmd),
                (None, Some(error)) => Err(ProducerError::CreationFail(
                    format!("Can't create a producer, error_id: {}, error_message: {}", error.error, error.message))
                ),
                _ => Err(ProducerError::CreationFail("Can't create a producer".to_string()))
            }
        }

        fn can_retry_from_error(_: &Self::Error) -> bool {
            false
        }
    }

    #[derive(Clone)]
    pub struct CloseProducer {
        pub producer_id: u64
    }

    impl GetRequestParam for CloseProducer {
        type ReturnedVal = proto::CommandSuccess;
        type Error = ProducerError;

        fn get_request_key(&self) -> RequestKey {
            RequestKey::AutoIncrRequestId
        }

        fn into_message(self, request_id: u64) -> Message {
            let Self { producer_id } = self;
            Message {
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

        fn map(response: Message) -> Result<Self::ReturnedVal, Self::Error> {
            match (response.command.success, response.command.error) {
                (Some(success), _) => Ok(success),
                (None, Some(error)) => Err(ProducerError::CloseFail(
                    format!("error_id: {}, error_message: {}", error.error, error.message)
                )),
                _ => Err(ProducerError::CloseFail("unknown error!".to_string()))
            }
        }

        fn can_retry_from_error(_: &Self::Error) -> bool {
            false
        }
    }

    pub struct SendMessage {
        pub message: ProducerMessage,
        pub sequence_id: u64,
        pub producer_id: u64,
        pub producer_name: String,
    }

    impl GetRequestParam for SendMessage {
        type ReturnedVal = CommandSendReceipt;
        type Error = ProducerError;

        fn get_request_key(&self) -> RequestKey {
            RequestKey::ProducerSend {
                producer_id: self.producer_id,
                sequence_id: self.sequence_id
            }
        }

        fn into_message(self, _: u64) -> Message {
            let Self { message, sequence_id, producer_id, producer_name } = self;
            let ProducerMessage { payload, properties, partition_key, replicate_to, compression,
                uncompressed_size, num_messages_in_batch, event_time, encryption_keys,
                encryption_algo, encryption_param, schema_version } = message;

            let properties = properties
                .into_iter()
                .map(|(key, value)| proto::KeyValue { key, value })
                .collect();

            Message {
                command: proto::BaseCommand {
                    type_: proto::base_command::Type::Send as i32,
                    send: Some(proto::CommandSend {
                        producer_id: producer_id,
                        sequence_id: sequence_id,
                        num_messages: num_messages_in_batch.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                payload: Some(Payload {
                    metadata: proto::MessageMetadata {
                        producer_name,
                        sequence_id,
                        properties,
                        publish_time: Utc::now().timestamp_millis() as u64,
                        replicated_from: None,
                        partition_key,
                        replicate_to,
                        compression,
                        uncompressed_size,
                        num_messages_in_batch,
                        event_time,
                        encryption_keys,
                        encryption_algo,
                        encryption_param,
                        schema_version,
                        ..Default::default()
                    },
                    data: payload,
                }),
            }
        }

        fn map(response: Message) -> Result<Self::ReturnedVal, Self::Error> {
            match response.command.send_receipt {
                Some(cmd) => Ok(cmd),
                None => Err(ProducerError::ErrorSendMessage("Unexpected message receipt confirmation : None".to_string()))
            }
        }

        fn can_retry_from_error(error: &Self::Error) -> bool {
            match error {
                ProducerError::ErrorSendMessage(..) => true,
                _ => false
            }
        }
    }
}