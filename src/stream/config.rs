use url::Url;
use std::net::SocketAddr;
use tokio::task;
use crate::stream::errors::TcpSockError::{CannotResolveHostName, InvalidUrlScheme};
use crate::stream::errors::TcpSockError;
use rand::Rng;

pub struct TcpSockConfig {
    pub use_tls: bool,
    pub address: SocketAddr,
    pub host_name: String,
}

impl TcpSockConfig {

    pub async fn new(pulsar_url: Url) -> Result<TcpSockConfig, TcpSockError> {
        match TcpSockConfig::use_tls_and_port(&pulsar_url) {
            Err(e) => Err(e),
            Ok((use_tls, port)) => {
                TcpSockConfig::resolve_address(pulsar_url.clone(), port).await
                    .map(|address| {
                        TcpSockConfig {
                            use_tls,
                            address,
                            host_name: pulsar_url.host_str().unwrap_or(address.ip().to_string().as_str()).into()}
                    })
            }
        }
    }

    fn use_tls_and_port(pulsar_url: &Url) -> Result<(bool, u16), TcpSockError> {
        match pulsar_url.scheme() {
            "pulsar" => Ok((false, 6650)),
            "pulsar+ssl" => Ok((true, 6651)),
            s => Err(InvalidUrlScheme(s.into()))
        }
    }

    async fn resolve_address(pulsar_url: Url, port: u16) -> Result<SocketAddr, TcpSockError> {
        task::spawn_blocking(move || {
            pulsar_url
                .socket_addrs(|| Some(port))
                .map_err(|e| CannotResolveHostName(e.to_string()))
                .and_then(|addrs| {
                    if addrs.is_empty() {
                        Err(CannotResolveHostName(pulsar_url.host_str().unwrap_or("null").into()))
                    } else {
                        let mut rng = rand::thread_rng();
                        let idx: usize = rng.gen_range(0, addrs.len());
                        Ok(addrs.get(idx).copied().unwrap())
                    }
                })
        }).await.unwrap()
    }

}

