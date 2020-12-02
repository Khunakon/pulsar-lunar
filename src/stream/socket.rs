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
use crate::stream::{FlowHandle, PulsarProtocolFlow, InboundReceiver, OutboundSender};
use crate::stream::config::TcpSockConfig;
use crate::stream::errors::TcpSockError;

pub async fn tcp(config: TcpSockConfig) -> Result<TcpStream, TcpSockError> {
    let stream = TcpStream::connect(&config.address)
        .await
        .map_err(|e| TcpSockError::IoError(e.to_string()))?;
    Ok(stream)
}

pub async fn tls(config: TcpSockConfig, cert_chain: &[Certificate]) -> Result<TlsStream<TcpStream>, TcpSockError> {
    let stream = TcpStream::connect(&config.address)
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
        .connect(&config.host_name, stream)
        .await
        .map_err(|e| TcpSockError::IoError(e.to_string()))?;

    Ok(stream)
}

impl PulsarProtocolFlow for TlsStream<TcpStream> {
    fn into_protocol_flow(self) -> (FlowHandle, InboundReceiver, OutboundSender) {
        into_protocol_flow(self)
    }
}

impl PulsarProtocolFlow for TcpStream {
    fn into_protocol_flow(self) -> (FlowHandle, InboundReceiver, OutboundSender) {
        into_protocol_flow(self)
    }
}

fn into_protocol_flow<S>(stream: S) -> (FlowHandle, InboundReceiver, OutboundSender)
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

    (FlowHandle { tx_sigterm }, InboundReceiver { rx: rx_inbound }, OutboundSender { tx: tx_outbound } )

}
