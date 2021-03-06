use futures::{SinkExt, StreamExt};
use native_tls::Certificate;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc::channel;
use tokio::sync::oneshot;
use tokio_native_tls::TlsConnector;
use tokio_native_tls::TlsStream;
use tokio_util::codec::Framed;

use crate::message::codec::{Codec, Message};
use crate::netflow::{Term, SocketFlow, In, Out};
use crate::netflow::errors::TcpSockError;
use std::net::SocketAddr;

pub async fn tcp(address: SocketAddr) -> Result<TcpStream, TcpSockError> {
    let stream = TcpStream::connect(address)
        .await
        .map_err(|e| TcpSockError::IoError(e.to_string()))?;
    Ok(stream)
}

pub async fn tls(address: SocketAddr, host_name: String,  cert_chain: &[Certificate]) -> Result<TlsStream<TcpStream>, TcpSockError> {
    let stream = TcpStream::connect(address)
        .await
        .map_err( |e| TcpSockError::IoError(e.to_string()) )?;

    let mut builder = native_tls::TlsConnector::builder();

    for cert in cert_chain {
        builder.add_root_certificate(cert.clone());
    }

    let ctx = builder
        .build()
        .map_err(|e| TcpSockError::TlsError(e.to_string()))?;

    let stream = TlsConnector::from(ctx)
        .connect(&host_name, stream)
        .await
        .map_err(|e| TcpSockError::IoError(e.to_string()))?;

    Ok(stream)
}

impl SocketFlow for TlsStream<TcpStream> {
    fn ports(self) -> (Term, In, Out) {
        into_protocol_flow(self)
    }
}

impl SocketFlow for TcpStream {
    fn ports(self) -> (Term, In, Out) {
        into_protocol_flow(self)
    }
}

fn into_protocol_flow<S>(stream: S) -> (Term, In, Out)
    where
        S: AsyncRead + AsyncWrite,
        S: Sized + Send + Unpin + 'static
{

    let (mut sink, mut source) = Framed::new(stream, Codec).split();
    let (tx_sigterm, rx_sigterm) = oneshot::channel::<()>();
    let (tx_sigterm_1, rx_sigterm_1) = oneshot::channel::<()>();
    let (tx_sigterm_2, rx_sigterm_2) = oneshot::channel::<()>();
    let (tx_inbound, rx_inbound) = channel::<Message>(1);
    let (tx_outbound, mut rx_outbound) = channel::<Message>(1);

    tokio::spawn(async move {
        let _ = rx_sigterm.await;
        let _ = tx_sigterm_1.send(());
        let _ = tx_sigterm_2.send(());
    });

    tokio::spawn(async move {

        tokio::select! {
            _ = async {
                    while let Some(result) = source.next().await {
                        match result {
                            Ok(msg) => {
                                if let Err(e) = tx_inbound.send(msg).await {
                                    log::error!("Can't send inbound message. {:?}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                log::error!("Error occurred while reading packet. {:?}", e)
                            }
                        }
                    }
                    log::debug!("The input socket is closed!");
                } => {},
            _ = tx_inbound.closed() => {},
            _ = rx_sigterm_1 => {
                log::debug!("Sigterm received on source stream");
            }
        }

    });

    tokio::spawn(async move {

        tokio::select! {
            _ = async {
                    while let Some(msg) = rx_outbound.recv().await {
                        if let Err(e) = sink.send(msg).await {
                            log::error!("Error occurred while sending outbound message. {:?}", e);
                            break;
                        }
                    }
            } => {} ,

            _ = rx_sigterm_2 => {
                log::debug!("Sigterm received on sink stream");
            }
        }

        let _ = sink.close().await; //ignore any other error.
    });

    (Term { tx_sigterm }, In { rx: rx_inbound }, Out { tx: tx_outbound } )

}
