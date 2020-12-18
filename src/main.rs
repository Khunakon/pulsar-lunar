use pulsar_lunar::client::config::ConnectionConfig;
use url::Url;
use pulsar_lunar::stream::socket;
use tokio::net::TcpStream;
use tokio_native_tls::TlsStream;
use pulsar_lunar::client::connection;
use pulsar_lunar::stream::errors::TcpSockError;
use pulsar_lunar::client::Dispatcher;
use pulsar_lunar::client::lookup::GetPartitionTopicMetadata;
use pulsar_lunar::client;
use futures::FutureExt;
use pulsar_lunar::client::producer::{ProducerConfig, RetrySend};
use std::sync::Arc;
use pulsar_lunar::message::serde::SerializeMessage;
use pulsar_lunar::message::producer::Message;
use pulsar_lunar::message::errors::SerDeError;
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() {
    log4rs::init_file("conf/log4rs.yaml", Default::default()).unwrap();

    let conn_config = ConnectionConfig::new(Url::parse("pulsar+ssl://pulsar-a-proxy.pulsar-a.svc.cluster.local").unwrap(),
                                            "secret/ca.crt".to_string(),
                                            None,
                                            None,
                                            0,
                                            5).unwrap();

    let pool = connection::Pool::new(conn_config, |config| {
        connection::connect(tls_sock,
                            config,
                            Dispatcher::new,
                            10,
                            5).boxed()
    }).await.unwrap();

    let pool = Arc::new(pool);

    let topic = "persistent://global.gravity/test/pulsar-lunar".to_string();
    let mut producer = client::producer::create(pool, ProducerConfig { topic, ..Default::default() } ).await.unwrap();
    let message  = TestMessage { name: "koo".to_string(), color: "green".to_string() };

    let receipt = producer.send(message, RetrySend::LimitTo { max_retry_count: 5, back_off_sec: 1 }).await.unwrap();

    log::debug!("{:?}", receipt);

    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(3000)).await;
    }).await;
}

async fn tls_sock(config: ConnectionConfig) -> Result<TlsStream<TcpStream>, TcpSockError> {
    let address = config.resolve_address().await.map_err(TcpSockError::from)?;
    let host_name = config.get_host_name().await.map_err(TcpSockError::from)?;
    socket::tls(address, host_name, &config.cert_chain).await
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

