use crate::message::codec::Message;
use tokio::sync::mpsc::{Sender, channel};
use tokio::sync::oneshot;
use crate::message::proto;
use crate::client::errors::ConnectionError;
use crate::stream::{PulsarProtocolFlow, FlowHandle };
use tokio::time;
use futures::Future;
use std::fmt::{Debug, Display};
use crate::client::Dispatcher;
use crate::client::dispatcher;

pub async fn connect<SF, Fut, S, E, DF>(create_socket: SF, create_dispatcher: DF, buffer_size: usize, outbound_timeout_sec: u8) -> Result<Connection, ConnectionError>
    where
        SF: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, E>> + Send + 'static,
        S: PulsarProtocolFlow,
        E: Into<ConnectionError> + Debug + Display,
        DF: Fn(Sender<Message>, usize, u8) -> Dispatcher + Send + Sync + 'static,
{
    let create_dispatcher = move |tx: &Sender<Message>| { IsCurrentDispatcher::No( create_dispatcher(tx.clone(), buffer_size, outbound_timeout_sec) ) };
    let outbound = handle_connection(create_socket, create_dispatcher).await?;
    Ok(Connection { outbound })
}

async fn handle_connection<SF, Fut, S, E, DF>(create_socket: SF, create_dispatcher: DF) -> Result<dispatcher::Outbound, ConnectionError>
    where
        SF: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, E>> + Send + 'static,
        S: PulsarProtocolFlow,
        E: Into<ConnectionError> + Debug + Display,
        DF: FnOnce(&Sender<Message>) -> IsCurrentDispatcher + Send + Sync + 'static,
{
    //todo: add retry logic.
    let (flow, mut inbound_rcv, outbound_snd) = create_socket().await
        .map_err(|e| e.into())?
        .into_protocol_flow();

    outbound_snd.tx
        .send(ctrl_message::connect(None)).await
        .map_err(|e| ConnectionError::Unexpected(e.to_string()))?;

    inbound_rcv.rx
        .recv().await
        .map_or_else(|| Err(ConnectionError::Disconnected), ctrl_message::check_connection_response)?;

    log::debug!("connected!");

    let dispatcher = match create_dispatcher(&outbound_snd.tx) {
        IsCurrentDispatcher::No(dispatcher) => dispatcher,
        IsCurrentDispatcher::Yes(dispatcher) => {
            dispatcher.change_connection(outbound_snd.tx.clone().into() ).await;
            dispatcher
        }
    };

    let dispatcher_inbound_tx = dispatcher.inbound.tx.clone();
    let dispatcher_outbound_tx = dispatcher.outbound.tx.clone();

    let heartbeat = {
        let reconnection = ReConnection::new(create_socket, flow, 10, dispatcher);
        Heartbeat::new(reconnection.tx_sig_alert, outbound_snd.tx.clone())
    };


    tokio::spawn(async move {
        let tx_outbound = outbound_snd.tx.clone();

        while let Some(msg) = inbound_rcv.rx.recv().await {
            log::debug!("before hb send");
            heartbeat.send();
            log::debug!("after hb sent");

            if msg.command.type_ == (proto::base_command::Type::Ping as i32) {
                log::debug!("Ping received!");
                let _ = tx_outbound.send(ctrl_message::pong()).await.unwrap();
                log::debug!("Pong!");
            } else if msg.command.type_ == (proto::base_command::Type::Pong as i32) {
                log::debug!("Pong received!");
            } else {
                dispatcher_inbound_tx.send(msg).await.unwrap();
            }
        }
    });

    Ok(dispatcher_outbound_tx.into())
}

pub struct Connection {
    outbound: dispatcher::Outbound
}

impl Connection {

}

struct ReConnection {
    tx_sig_alert: oneshot::Sender<()>
}

impl ReConnection {

    fn new<F, Fut, S, E>(create_socket: F, flow: FlowHandle, wait_sec: u64, dispatcher: Dispatcher) -> ReConnection
        where
            F: Fn() -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<S, E>> + Send + 'static,
            S: PulsarProtocolFlow,
            E: Into<ConnectionError> + Debug + Display,
    {

        let (tx_sig, rx_sig) = oneshot::channel::<()>();

        let reconnection = ReConnection { tx_sig_alert: tx_sig };

        tokio::spawn(async move {
            let _ = rx_sig.await;
            flow.terminate();
            time::sleep(time::Duration::from_secs(wait_sec)).await;
            let _ = handle_connection(create_socket, move |_| IsCurrentDispatcher::Yes(dispatcher)).await;
        });

        reconnection
    }

}

enum IsCurrentDispatcher {
    Yes(Dispatcher),
    No(Dispatcher)
}

struct Heartbeat {
    tx: Sender<()>,
    tx_sigterm: Sender<()>
}

impl Heartbeat {

    fn terminate(self) {
        let _ = self.tx_sigterm.send(());
    }

    fn send(&self) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            tx.send(()).await.unwrap();
        });
    }

    fn new(tx_alert: oneshot::Sender<()>, tx_outbound: Sender<Message>) -> Heartbeat {

        let (tx_heartbeat, mut rx_heartbeat) = channel(1);
        let (tx_sigterm, mut rx_sigterm) = channel::<()>(1);

        tokio::spawn(async move {
            let mut ping_sent_count: u8 = 0;

            loop {
                tokio::select! {
                    does_continue = async {
                            log::debug!("wait before ping server!");
                            time::sleep(time::Duration::from_secs( if ping_sent_count == 0 { 20 } else { 5 } )).await;
                            log::debug!("Ping server!");
                            // The ping receiver, rx_outbound, might not be able to sink a ping message to its socket due to IOError.
                            // This causes blocking on the ping sender side, tx_outbound. The logic moves forward by stop waiting for sender
                            // after timeout.
                            tokio::select! {
                                _ = async {
                                        let _ = tx_outbound.send(ctrl_message::ping()).await;
                                    } => { true }

                                _ = time::sleep(time::Duration::from_secs(5)) => { false }
                            }

                        } => {
                            if does_continue && ping_sent_count < 3 {
                                ping_sent_count += 1;
                            } else {
                                let _ = tx_alert.send(());
                                break;
                            }
                        },

                    heartbeat = rx_heartbeat.recv() => {
                            if heartbeat.is_none() {
                                let _ = tx_alert.send(());
                                break;
                            } else {
                                log::debug!("heartbeat received");
                                ping_sent_count = 0;
                            }
                        },
                    _ = rx_sigterm.recv() => { break }
                }
            }


        });

        Heartbeat { tx: tx_heartbeat, tx_sigterm }

    }
}

mod ctrl_message {

    use crate::message::proto;
    use crate::message::errors::server_error;
    use crate::message::codec::Message;
    use crate::client::errors::ConnectionError;

    pub fn check_connection_response(m: Message) -> Result<(), ConnectionError> {
        match m {
            Message {
                command:
                proto::BaseCommand {
                    error: Some(error), ..
                },
                ..
            } => {
                Err(ConnectionError::PulsarError { server_error: server_error(error.error), message: Some(error.message)})
            }
            msg => {
                if let Some(_) = msg.command.connected {
                    Ok(())
                } else {
                    let cmd = msg.command.clone();
                    Err(ConnectionError::Unexpected(format!("Unknown connection error: {:?}", cmd)))
                }
            }
        }
    }

    pub fn connect(proxy_to_broker_url: Option<String>) -> Message {
        Message {
            command: proto::BaseCommand {
                type_ : proto::base_command::Type::Connect as i32,
                connect: Some(proto::CommandConnect {
                    auth_method_name: None,
                    auth_data: None,
                    proxy_to_broker_url,
                    client_version: String::from("2.0.1-incubating"),
                    protocol_version: Some(12),
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn ping() -> Message {
        Message {
            command: proto::BaseCommand {
                type_: proto::base_command::Type::Ping as i32,
                ping: Some(proto::CommandPing {}),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn pong() -> Message {
        Message {
            command: proto::BaseCommand {
                type_: proto::base_command::Type::Pong as i32,
                pong: Some(proto::CommandPong {}),
                ..Default::default()
            },
            payload: None,
        }
    }
}