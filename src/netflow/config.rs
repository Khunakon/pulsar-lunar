use std::net::SocketAddr;

use native_tls::Certificate;
use url::Url;

use crate::net;
use crate::net::NetOpError;

#[derive(Clone)]
pub struct ConnectionConfig {
    pub pulsar_url: Url,
    pub cert_chain: Vec<Certificate>,
    pub auth_data: Option<AuthData>,
    pub proxy_to_broker_url: Option<String>,
    pub default_max_retry_count: u16,
    pub default_retry_backoff_sec: u64
}

impl ConnectionConfig {

    pub async fn resolve_address(&self) -> Result<SocketAddr, NetOpError> {
        net::resolve_address(self.pulsar_url.clone(), self.get_port()).await
    }

    pub fn use_tls(&self) -> bool { self.use_tls_and_port().0 }
    pub fn get_port(&self) -> u16 { self.use_tls_and_port().1 }
    pub fn use_tls_and_port(&self) -> (bool, u16) {
        net::use_tls_and_port(&self.pulsar_url).unwrap()
    }

    pub async fn get_host_name(&self) -> Result<String, NetOpError> {
        let address = self.resolve_address().await?;
        Ok(self.pulsar_url.host_str().unwrap_or(address.ip().to_string().as_str()).into())
    }

    pub fn new(pulsar_url: Url,
               cert_file: String,
               auth_data: Option<AuthData>,
               proxy_to_broker_url: Option<String>,
               default_max_retry_count: u16,
               default_retry_backoff_sec: u64 ) -> Result<ConnectionConfig, NetOpError> {
        let cert_chain = net::read_cert_chain_from_file(&cert_file)?;
        let _ = net::use_tls_and_port(&pulsar_url)?; // check port and tls
        Ok(ConnectionConfig { pulsar_url, cert_chain, auth_data, proxy_to_broker_url, default_max_retry_count, default_retry_backoff_sec })
    }
}

#[derive(Clone)]
pub struct AuthData {
    pub name: String,
    pub data: Vec<u8>,
}