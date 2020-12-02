use pulsar_lunar::stream::config::TcpSockConfig;
use url::Url;
use pulsar_lunar::stream::socket;
use native_tls::{Certificate};
use tokio::net::TcpStream;
use tokio_native_tls::TlsStream;
use pulsar_lunar::client::connection;
use pulsar_lunar::stream::errors::TcpSockError;
use pulsar_lunar::client::Dispatcher;

#[tokio::main]
async fn main() {
    log4rs::init_file("conf/log4rs.yaml", Default::default()).unwrap();
    let connection = connection::connect(tls_sock, Dispatcher::new, 10, 5).await.unwrap();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(3000)).await;
    }).await;
}

async fn tls_sock() -> Result<TlsStream<TcpStream>, TcpSockError> {
    let url = Url::parse("pulsar+ssl://pulsar-a-proxy.pulsar-a.svc.cluster.local").unwrap();
    let config = TcpSockConfig::new(url).await.unwrap();
    let cert_chain = read_cert_chain_from_file("secret/ca.crt");
    socket::tls(config, &cert_chain).await
}

fn read_cert_chain_from_file(path: &str) -> Vec<Certificate> {

    use std::io::Read;

    let mut file = std::fs::File::open(path).unwrap();
    let mut buffer = vec![];
    file.read_to_end(&mut buffer).unwrap();

    let mut certs = vec![];

    for cert in pem::parse_many(&buffer).iter().rev() {
        certs.push(
            Certificate::from_der(&cert.contents[..]).unwrap(),
        )
    }

    certs

}

