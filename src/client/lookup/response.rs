use crate::message::proto::CommandPartitionedTopicMetadataResponse;
use url::Url;
use crate::client::connection;
use crate::client::lookup::LookupTopic;
use crate::client::lookup::errors::LookupError;
use std::sync::Arc;

pub struct PartitionNumbersResponse(pub CommandPartitionedTopicMetadataResponse);

impl PartitionNumbersResponse {
    pub fn value(self) -> u32 { self.0.partitions.unwrap() }
}

pub struct LookupTopicResponse {
    pub broker_url: Url,
    pub broker_url_tls: Option<Url>,
    pub proxy_through_service: bool,
    pub redirect: bool,
    pub is_authoritative: bool,
}

impl LookupTopicResponse {
    pub async fn into_broker_loc(self, pool: Arc<connection::Pool>, topic: String) -> Result<BrokerLoc, LookupError> {
        let mut response: LookupTopicResponse;
        response = self;

        loop {
            let LookupTopicResponse { broker_url, broker_url_tls, proxy_through_service, redirect, is_authoritative} = response;
            let (broker_url, use_tls) =
                if let Some(tls_url) = broker_url_tls.clone() {
                    (tls_url, true)
                } else {
                    (broker_url.clone(), false)
                };

            let broker_loc = BrokerLoc { broker_url, use_proxy: proxy_through_service, use_tls };

            if redirect {
                let connection = pool.get_connection(broker_loc).await.map_err(LookupError::from)?;
                let lookup_topic = LookupTopic { topic: topic.clone(), authoritative: is_authoritative };
                response = connection.send_request_with_retry(&lookup_topic, 5, 0).await?
            } else {
                break Ok(broker_loc)
            }
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct BrokerLoc {
    /// pulsar URL for the broker we're actually contacting
    /// this must follow the IP:port format
    pub broker_url: Url,
    /// Client can ask to be proxied to a specific broker
    /// This is only honored by a Pulsar proxy
    pub use_proxy: bool,

    pub use_tls: bool
}

impl BrokerLoc {
    pub fn broker_url_string(&self) -> String {
        let host_name = self.broker_url.host_str().unwrap();
        let default_port = if self.use_tls { 6651 } else { 6650 };
        let port = self.broker_url.port().unwrap_or(default_port);
        format!("{}:{}", host_name, port)
    }
}
