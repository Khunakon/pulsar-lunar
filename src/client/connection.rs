use std::fmt::{Debug, Display};
use std::sync::Arc;

use futures::Future;
use nom::lib::std::collections::HashMap;
use tokio::time;
use tokio::sync::{Mutex, oneshot};
use tokio::sync::mpsc::Sender;
use url::Url;

use model::*;

use crate::client::config::ConnectionConfig;
use crate::client::Dispatcher;
use crate::client::dispatcher;
use crate::client::errors::ConnectionError;
use crate::client::lookup::response::BrokerLoc;
use crate::client::models::general::SerialId;
use crate::client::models::outbound::{Request, RequestKey, SendError};
use crate::message::codec::Message;
use crate::stream::PulsarStream;
use futures::future::Map;
use tokio::sync::watch;
use crate::net::NetOpError;
use crate::net;
use std::pin::Pin;

pub async fn connect<SF, Fut, S, E, DF>(create_socket: SF,
                                        conn_config: ConnectionConfig,
                                        create_dispatcher: DF,
                                        buffer_size: usize,
                                        outbound_timeout_sec: u8) -> Result<Connection, ConnectionError>
    where
        SF: Fn(ConnectionConfig) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, E>> + Send + 'static,
        S: PulsarStream,
        E: Into<ConnectionError> + Debug + Display,
        DF: Fn(Sender<Message>, watch::Sender<()>, usize, u8) -> Dispatcher + Send + Sync + 'static,
{
    let (sig_reconnect_tx, sig_reconnect_rx) = watch::channel(());
    let create_dispatcher = move |tx: &Sender<Message>| {
        IsCurrentDispatcher::No(
            create_dispatcher(tx.clone(), sig_reconnect_tx, buffer_size, outbound_timeout_sec)
        )
    };
    let outbound = handler::run_loop(create_socket, conn_config.clone(), create_dispatcher).await?;
    Ok(Connection { outbound, auto_incr: SerialId::new(), config: conn_config , sig_reconnect_rx})
}

pub struct Connection {
    pub outbound: dispatcher::Outbound,
    pub auto_incr: SerialId,
    pub config: ConnectionConfig,
    pub sig_reconnect_rx: watch::Receiver<()>
}

impl Connection {

    pub async fn send_request_with_default_retry<A>(
        &self,
        request_param: &A
    ) -> Result<A::ReturnedVal, A::Error>
        where
            A: GetRetryableRequestParam + 'static,
            <A as GetRetryableRequestParam>::Error: From<SendError>
    {
        self.send_request_with_retry(request_param,
                                     self.config.default_retry_backoff_sec,
                                     self.config.default_max_retry_count).await
    }

    pub async fn send_request_with_retry<A>(
        &self,
        request_param: &A,
        back_off_sec: u64,
        max_retry_count: u16
    ) -> Result<A::ReturnedVal, A::Error>
        where
            A: GetRetryableRequestParam + 'static,
            <A as GetRetryableRequestParam>::Error: From<SendError>
    {
        let mut retry_count: u16 = 0;
        let get_message = |request_id| request_param.get_message(request_id);

        loop {
            let key = request_param.get_request_key();
            let response = self._send_request(key, get_message, A::map).await;
            if response.is_ok() {
                break response;
            } else {
                if response.is_ok() {
                    break response;
                } else {
                    let err = response.err().unwrap();
                    if A::can_retry_from_error(&err) &&
                        (max_retry_count == 0 || retry_count < max_retry_count) {
                        retry_count += 1;
                        time::sleep(time::Duration::from_secs(back_off_sec)).await;
                    } else {
                        break Err(err);
                    }
                }
            }
        }

    }

    pub async fn send_request<A: GetRequestParam + 'static>(&self, request_param: A) -> Result<A::ReturnedVal, A::Error>
        where
            <A as GetRequestParam>::Error: From<SendError>
    {
        let key = request_param.get_request_key();
        self._send_request(key, |request_id| request_param.into_message(request_id), A::map).await
    }

    async fn _send_request<R, E: From<SendError>>(
        &self,
        key: RequestKey,
        get_message: impl FnOnce(u64) -> Message,
        map: impl FnOnce(Message) -> Result<R, E>) -> Result<R, E>
    {
        let (tx_response, rx_response) = oneshot::channel();

        let (key, request_id) = match key {
            RequestKey::AutoIncrRequestId => {
                let id = self.auto_incr.get();
                (RequestKey::RequestId(id), id)
            },
            _ => (key, 0)
        };

        let message = get_message(request_id);

        let request = Request { key, message, tx_response };

        self.outbound.tx
            .send(request).await
            .map_err(|e| E::from(SendError::MessageNotSent(e.to_string())))?;

        match rx_response.await {
            Ok(Ok(msg)) => map(msg),
            Ok(Err(e)) => Err(e.into()),
            Err(e) => Err(E::from(SendError::Unexpected(e.to_string()).into()))
        }

    }

}

pub trait GetRequestParam {
    type ReturnedVal;
    type Error;

    fn get_request_key(&self) -> RequestKey;
    fn into_message(self, request_id: u64) -> Message;
    fn map(response: Message) -> Result<Self::ReturnedVal, Self::Error>;
    fn can_retry_from_error(error: &Self::Error) -> bool;
}

pub trait GetRetryableRequestParam {
    type ReturnedVal;
    type Error;
    fn get_request_key(&self) -> RequestKey;
    fn get_message(&self, request_id: u64) -> Message;
    fn map(response: Message) -> Result<Self::ReturnedVal, Self::Error>;
    fn can_retry_from_error(error: &Self::Error) -> bool;
}

impl <Req: GetRequestParam + Clone> GetRetryableRequestParam for Req {
    type ReturnedVal = Req::ReturnedVal;
    type Error = Req::Error;

    fn get_request_key(&self) -> RequestKey {
        self.get_request_key()
    }

    fn get_message(&self, request_id: u64) -> Message {
        let this = self.clone();
        this.into_message(request_id)
    }

    fn map(response: Message) -> Result<Self::ReturnedVal, Self::Error> {
        Req::map(response)
    }

    fn can_retry_from_error(error: &Self::Error) -> bool {
        Req::can_retry_from_error(error)
    }
}

pub type ConnResult = Pin<Box<dyn Future<Output = Result<Connection, ConnectionError>> + Send>>;

pub struct Pool {
    pub main_connection: Arc<Connection>,
    main_conn_config: ConnectionConfig,
    registry: Arc<Mutex<HashMap<BrokerLoc, ConnectionState>>>,
    connect: Arc<Box<dyn Fn(ConnectionConfig) -> ConnResult + Send + Sync>>
}

impl Pool {

    pub async fn new<F>(conn_config: ConnectionConfig, connect: F) -> Result<Pool, ConnectionError>
        where F: Fn(ConnectionConfig) -> ConnResult + Send + Sync + 'static
    {
        let broker_loc = BrokerLoc {
            broker_url: conn_config.pulsar_url.clone(),
            use_proxy: false,
            use_tls: conn_config.use_tls()
        };

        let connection = Arc::new(connect(conn_config.clone()).await?);
        let mut registry = HashMap::default();
        registry.insert(broker_loc, ConnectionState::Ready(connection.clone()));

        Ok(Pool {
            main_connection: connection,
            main_conn_config: conn_config,
            registry: Arc::new(Mutex::new(registry)),
            connect: Arc::new(Box::new(connect))
        })
    }

    pub async fn get_connection(&self, broker_loc: BrokerLoc) -> Result<Arc<Connection>, ConnectionError> {
        let mut locked_registry = self.registry.lock().await;
        let state = locked_registry.get(&broker_loc).map(|opt| opt.clone());

        match state {
            Some(ConnectionState::Ready(connection)) => {
                Ok(connection)
            },
            Some(ConnectionState::Connecting(mut rx)) => {
                std::mem::drop(locked_registry); //unlock
                let _ = rx.changed().await.unwrap();
                Ok(rx.borrow().as_ref().unwrap().clone())
            },
            None => {
                let (tx, rx) = watch::channel(None);
                locked_registry.insert(broker_loc.clone(), ConnectionState::Connecting(rx));
                std::mem::drop(locked_registry); //unlock
                let mut conn_config = self.main_conn_config.clone();

                if broker_loc.use_proxy {
                    conn_config.proxy_to_broker_url = Some(broker_loc.broker_url_string());
                } else {
                    conn_config.pulsar_url = broker_loc.broker_url.clone();
                    conn_config.proxy_to_broker_url = None;
                }

                let registry = self.registry.clone();
                let connect = self.connect.clone();
                let connection = connect(conn_config).await?;
                let connection = Arc::new(connection);
                let mut locked_registry = registry.lock().await;
                locked_registry.insert(broker_loc, ConnectionState::Ready(connection.clone()));
                let _ = tx.send(Some(connection.clone()));
                Ok(connection)
            }
        }
    }
}

mod handler {
    use std::fmt::{Debug, Display};
    use std::future::Future;

    use tokio::sync::{Mutex, oneshot};
    use tokio::sync::mpsc::{channel, Sender};
    use tokio::time;

    use crate::client::{dispatcher, Dispatcher};
    use crate::client::config::ConnectionConfig;
    use crate::client::connection::ctrl_message;
    use crate::client::connection::model::IsCurrentDispatcher;
    use crate::client::errors::ConnectionError;
    use crate::message::codec::Message;
    use crate::message::proto;
    use crate::stream::{FlowHandle, PulsarStream};

    pub async fn run_loop<SF, Fut, S, E, DF>(create_socket: SF,
                                             connection_config: ConnectionConfig,
                                             create_dispatcher: DF) -> Result<dispatcher::Outbound, ConnectionError>
        where
            SF: Fn(ConnectionConfig) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<S, E>> + Send + 'static,
            S: PulsarStream,
            E: Into<ConnectionError> + Debug + Display,
            DF: FnOnce(&Sender<Message>) -> IsCurrentDispatcher + Send + Sync + 'static,
    {
        //todo: add retry logic.
        let (flow, mut inbound_rcv, outbound_snd) = create_socket(connection_config.clone()).await
            .map_err(|e| e.into())?
            .into_io_flow();

        outbound_snd.tx
            .send(ctrl_message::connect(connection_config.proxy_to_broker_url.clone())).await
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
            let reconnection = ReConnection::new(create_socket, connection_config, flow, 10, dispatcher);
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

    struct ReConnection {
        tx_sig_alert: oneshot::Sender<()>
    }

    impl ReConnection {

        fn new<F, Fut, S, E>(create_socket: F,
                             connection_config: ConnectionConfig,
                             flow: FlowHandle,
                             wait_sec: u64,
                             dispatcher: Dispatcher) -> ReConnection
            where
                F: Fn(ConnectionConfig) -> Fut + Send + Sync + 'static,
                Fut: Future<Output = Result<S, E>> + Send + 'static,
                S: PulsarStream,
                E: Into<ConnectionError> + Debug + Display,
        {

            let (tx_sig, rx_sig) = oneshot::channel::<()>();

            let reconnection = ReConnection { tx_sig_alert: tx_sig };

            tokio::spawn(async move {
                let _ = rx_sig.await;
                flow.terminate();
                time::sleep(time::Duration::from_secs(wait_sec)).await;
                let _ = run_loop(create_socket, connection_config, move |_| IsCurrentDispatcher::Yes(dispatcher)).await;
            });

            reconnection
        }

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
}

mod model {
    use crate::client::Dispatcher;
    use crate::client::connection::Connection;
    use tokio::sync::watch;
    use std::sync::Arc;

    #[derive(Clone)]
    pub enum ConnectionState {
        Ready(Arc<Connection>),
        Connecting(watch::Receiver<Option<Arc<Connection>>>)
    }

    pub enum IsCurrentDispatcher {
        Yes(Dispatcher),
        No(Dispatcher)
    }
}

mod ctrl_message {
    use crate::client::errors::ConnectionError;
    use crate::message::codec::Message;
    use crate::message::errors::server_error;
    use crate::message::proto;

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