use url::Url;

use crate::message::proto::CommandPartitionedTopicMetadataResponse;

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

