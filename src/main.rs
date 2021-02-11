use std::sync::Arc;

use futures::FutureExt;
use serde::{Deserialize, Serialize};
use url::Url;

use pulsar_lunar::netflow::config::ConnectionConfig;
use pulsar_lunar::netflow::connection;
use pulsar_lunar::entity;
use pulsar_lunar::entity::producer::{ProducerConfig, RetrySend};
use pulsar_lunar::message::errors::SerDeError;
use pulsar_lunar::message::producer::Message;
use pulsar_lunar::message::serde::SerializeMessage;

#[tokio::main]
async fn main() {
    log4rs::init_file("conf/log4rs.yaml", Default::default()).unwrap();

    let conn_config = ConnectionConfig::new(Url::parse("pulsar+ssl://pulsar-a-proxy.pulsar-a.svc.cluster.local").unwrap(),
                                            "secret/ca.crt".to_string(),
                                            None,
                                            None,
                                            0,
                                            5).unwrap();

    let pool = connection::Pool::new(conn_config, |conn_config| {
        connection::connect_tls(conn_config, 10, 5).boxed()
    }).await.unwrap();

    let pool = Arc::new(pool);

    let topic = "persistent://global.gravity/test/pulsar-lunar".to_string();
    let mut producer = entity::producer::create(pool, ProducerConfig { topic, ..Default::default() } ).await.unwrap();

    let message  = TestMessage { name: "koo".to_string(), color: "green".to_string() };

    let receipt = producer.send(message, RetrySend::LimitTo { max_retry_count: 5, back_off_sec: 1 }).await.unwrap();

    log::debug!("{:?}", receipt);

    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(3000)).await;
    }).await;
}

#[derive(Serialize, Deserialize)]
pub struct TestMessage {
    name: String,
    color: String
}

impl SerializeMessage for TestMessage {
    fn serialize_message(input: Self) -> Result<Message, SerDeError> {
        let message = Message {
            payload: serde_json::to_vec(&input).map_err(SerDeError::from)?,
            ..Default::default()
        };
        Ok(message)
    }
}

