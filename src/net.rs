use url::Url;
use std::net::SocketAddr;
use native_tls::Certificate;
use strum_macros::Display;
use rand::Rng;

#[derive(Debug, Display)]
pub enum NetOpError {
    InvalidUrlScheme(String),
    UnableToResolveHostName(String),
    UnableToReadCert(String)
}

pub fn use_tls_and_port(pulsar_url: &Url) -> Result<(bool, u16), NetOpError> {
    match pulsar_url.scheme() {
        "pulsar" => Ok((false, 6650)),
        "pulsar+ssl" => Ok((true, 6651)),
        s => Err(NetOpError::InvalidUrlScheme(s.into()))
    }
}

pub async fn resolve_address(pulsar_url: Url, port: u16) -> Result<SocketAddr, NetOpError> {
    tokio::task::spawn_blocking(move || {
        pulsar_url
            .socket_addrs(|| Some(port))
            .map_err(|e| NetOpError::UnableToResolveHostName(e.to_string()))
            .and_then(|addrs| {
                if addrs.is_empty() {
                    Err(NetOpError::UnableToResolveHostName(pulsar_url.host_str().unwrap_or("null").into()))
                } else {
                    let mut rng = rand::thread_rng();
                    let idx: usize = rng.gen_range(0, addrs.len());
                    Ok(addrs.get(idx).copied().unwrap())
                }
            })
    }).await.unwrap()
}

pub fn read_cert_chain_from_file(path: &str) -> Result<Vec<Certificate>, NetOpError> {
    use std::io::Read;

    let mut file = std::fs::File::open(path).map_err(|e| NetOpError::UnableToReadCert(e.to_string()))?;
    let mut buffer = vec![];
    file.read_to_end(&mut buffer).map_err(|e| NetOpError::UnableToReadCert(e.to_string()))?;

    let mut certs = vec![];

    for cert in pem::parse_many(&buffer).iter().rev() {
        match Certificate::from_der(&cert.contents[..]) {
            Ok(value) => certs.push(value),
            Err(e) => return Err(NetOpError::UnableToReadCert(e.to_string()))
        }
    }

    Ok(certs)

}

