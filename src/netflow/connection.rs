use std::fmt::{Debug, Display};
use std::pin::Pin;
use std::sync::Arc;

use futures::Future;
use nom::lib::std::collections::HashMap;
use tokio::sync::{Mutex, oneshot};
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::time;

use model::*;
use proto::*;

use crate::netflow::config::ConnectionConfig;
use crate::netflow::dispatcher;
use crate::netflow::dispatcher::DispatcherConfig;
use crate::netflow::errors::{ConnectionError, SendError};
use crate::netflow::models::general::SerialId;
use crate::netflow::models::outbound::{Request, RequestKey, RetryReq};
use crate::netflow::SocketFlow;
use crate::discovery::response::BrokerLoc;
use crate::entity::Dispatcher;
use crate::message::codec::Message;

pub type ConnResult = Pin<Box<dyn Future<Output = Result<Connection, ConnectionError>> + Send>>;

pub async fn connect_tls(conn_config: ConnectionConfig,
                         buffer_size: usize,
                         outbound_timeout_sec: u8) -> Result<Connection, ConnectionError> {
    connect(engine::tls_sock, conn_config, Dispatcher::new, buffer_size, outbound_timeout_sec).await
}

//todo: implement facade for connect_plain

pub async fn connect<SF, Fut, S, E, DF>(create_socket: SF,
                                        conn_config: ConnectionConfig,
                                        create_dispatcher: DF,
                                        buffer_size: usize,
                                        outbound_timeout_sec: u8) -> Result<Connection, ConnectionError>
    where
        SF: Fn(ConnectionConfig) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, E>> + Send + 'static,
        S: SocketFlow,
        E: Into<ConnectionError> + Debug + Display,
        DF: Fn(DispatcherConfig) -> Dispatcher + Send + Sync + 'static,
{
    let (sig_reconnect_tx, sig_reconnect_rx) = watch::channel(());
    let create_dispatcher = move |tx: &Sender<Message>| {
        IsCurrentDispatcher::No(
            create_dispatcher(
                DispatcherConfig {
                    tx_connection: tx.clone(),
                    sig_reconnect_tx,
                    buffer_size,
                    outbound_timeout_sec
                }
            )
        )
    };
    let outbound = engine::run_loop(create_socket, conn_config.clone(), create_dispatcher).await?;
    Ok(Connection { outbound, auto_incr: SerialId::new(), config: conn_config , sig_reconnect_rx})
}

pub struct Connection {
    pub outbound: dispatcher::Outbound,
    pub auto_incr: SerialId,
    pub config: ConnectionConfig,
    pub sig_reconnect_rx: watch::Receiver<()>
}

impl Connection {

    pub async fn ask<A>(&self, request: A) -> Result<A::ReturnedVal, A::Error>
        where
            A: AskProto + 'static,
            <A as AskProto>::Error: From<SendError>
    {
        let key = request.get_request_key();
        let get_message = |request_id| request.into_message(request_id);
        self._ask(key, get_message, A::map).await
    }

    pub async fn ask_retry<A>(&self, request: &A, retry_req: RetryReq ) -> Result<A::ReturnedVal, A::Error>
        where
            A: RetryableAskProto + 'static,
            <A as RetryableAskProto>::Error: From<SendError>
    {
        let send_req = || {
            let get_message = |request_id| request.get_message(request_id);
            let key = request.get_request_key();
            self._ask(key, get_message, A::map)
        };

        self.retry_request_on_err(send_req, retry_req, A::can_retry_from_error).await
    }

    pub async fn tell<A>(&self, request: A) -> Result<(), A::Error>
        where
            A: TellProto + 'static,
            <A as TellProto>::Error: From<SendError>
    {
        let key = request.get_request_key();
        let get_message = |request_id| request.into_message(request_id);
        self._tell(key, get_message).await
    }

    pub async fn tell_retry<A>(&self, request: &A, retry_req: RetryReq) -> Result<(), A::Error>
        where
            A: RetryableTellProto + 'static,
            <A as RetryableTellProto>::Error: From<SendError>
    {
        let send_req = || {
            let key = request.get_request_key();
            let get_message = |request_id| request.get_message(request_id);
            self._tell(key, get_message)
        };

        self.retry_request_on_err(send_req, retry_req, A::can_retry_from_error).await
    }

    async fn retry_request_on_err<'s, F, Fut, R, E>(
        &'s self,
        send_req: F,
        retry_req: RetryReq,
        can_retry_from_error: impl Fn(&E) -> bool
    ) -> Result<R, E>
        where
            F: Fn() -> Fut + 's,
            Fut: Future<Output = Result<R, E>>,
    {
        let (max_retry_count , back_off_sec) = match retry_req {
            RetryReq::Default => (self.config.default_max_retry_count, self.config.default_retry_backoff_sec),
            RetryReq::LimitTo { max, back_off_sec } => ( max, back_off_sec ),
            RetryReq::Forever => (0, self.config.default_retry_backoff_sec)
        };

        let mut retry_count: u16 = 0;

        loop {
            let response = send_req().await;
            if response.is_ok() {
                break response;
            } else {
                if response.is_ok() {
                    break response;
                } else {
                    let err = response.err().unwrap();
                    if can_retry_from_error(&err) &&
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

    async fn _tell<E: From<SendError>>(
        &self,
        key: RequestKey,
        get_message: impl FnOnce(u64) -> Message
    ) -> Result<(), E> {

        let (tx_response, rx_response) = oneshot::channel();
        let (key, request_id) = self.get_request_id(key);
        let message = get_message(request_id);
        let request = Request::Tell { key, message, tx_response };

        self.outbound.tx
            .send(request).await
            .map_err(|e| E::from(SendError::MessageNotSent(e.to_string())))?;

        match rx_response.await {
            Ok(r) => r.map_err(E::from),
            Err(e) => Err(E::from(SendError::Unexpected(e.to_string()).into()))
        }
    }

    async fn _ask<R, E: From<SendError>>(
        &self,
        key: RequestKey,
        get_message: impl FnOnce(u64) -> Message,
        map: impl FnOnce(Message) -> Result<R, E>,
    ) -> Result<R, E> {
        let (tx_response, rx_response) = oneshot::channel();
        let (key, request_id) = self.get_request_id(key);
        let message = get_message(request_id);
        let request = Request::Ask { key, message, tx_response };

        self.outbound.tx
            .send(request).await
            .map_err(|e| E::from(SendError::MessageNotSent(e.to_string())))?;

        match rx_response.await {
            Ok(Ok(msg)) => map(msg),
            Ok(Err(e)) => Err(e.into()),
            Err(e) => Err(E::from(SendError::Unexpected(e.to_string()).into()))
        }

    }

    fn get_request_id(&self, key: RequestKey) -> (RequestKey, u64) {
        match key {
            RequestKey::AutoIncrRequestId => {
                let id = self.auto_incr.get();
                (RequestKey::RequestId(id), id)
            },
            _ => (key, 0)
        }
    }

}


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

pub mod proto {
    use crate::netflow::models::outbound::RequestKey;
    use crate::message::codec::Message;

    pub trait TellProto {
        type Error;
        fn get_request_key(&self) -> RequestKey;
        fn into_message(self, request_id: u64) -> Message;
        fn can_retry_from_error(error: &Self::Error) -> bool;
    }

    impl <Req: TellProto + Clone> RetryableTellProto for Req {
        type Error = Req::Error;

        fn get_request_key(&self) -> RequestKey {
            self.get_request_key()
        }

        fn get_message(&self, request_id: u64) -> Message {
            let this = self.clone();
            this.into_message(request_id)
        }

        fn can_retry_from_error(error: &Self::Error) -> bool {
            Req::can_retry_from_error(error)
        }
    }

    pub trait RetryableTellProto {
        type Error;
        fn get_request_key(&self) -> RequestKey;
        fn get_message(&self, request_id: u64) -> Message;
        fn can_retry_from_error(error: &Self::Error) -> bool;
    }

    pub trait AskProto {
        type ReturnedVal;
        type Error;

        fn get_request_key(&self) -> RequestKey;
        fn into_message(self, request_id: u64) -> Message;
        fn map(response: Message) -> Result<Self::ReturnedVal, Self::Error>;
        fn can_retry_from_error(error: &Self::Error) -> bool;
    }

    pub trait RetryableAskProto {
        type ReturnedVal;
        type Error;
        fn get_request_key(&self) -> RequestKey;
        fn get_message(&self, request_id: u64) -> Message;
        fn map(response: Message) -> Result<Self::ReturnedVal, Self::Error>;
        fn can_retry_from_error(error: &Self::Error) -> bool;
    }

    impl <Req: AskProto + Clone> RetryableAskProto for Req {
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
}

mod engine {
    use std::fmt::{Debug, Display};
    use std::future::Future;

    use tokio::net::TcpStream;
    use tokio::sync::oneshot;
    use tokio::sync::mpsc::{channel, Sender};
    use tokio::time;
    use tokio_native_tls::TlsStream;

    use crate::netflow::{dispatcher, SocketFlow, Term};
    use crate::netflow::config::ConnectionConfig;
    use crate::netflow::connection::message_utils;
    use crate::netflow::connection::model::IsCurrentDispatcher;
    use crate::netflow::errors::ConnectionError;
    use crate::netflow::errors::TcpSockError;
    use crate::netflow::socket;
    use crate::entity::Dispatcher;
    use crate::message::codec::Message;
    use crate::message::proto;

    pub async fn tls_sock(config: ConnectionConfig) -> Result<TlsStream<TcpStream>, TcpSockError> {
        let address = config.resolve_address().await.map_err(TcpSockError::from)?;
        let host_name = config.get_host_name().await.map_err(TcpSockError::from)?;
        socket::tls(address, host_name, &config.cert_chain).await
    }


    pub async fn run_loop<SF, Fut, S, E, DF>(create_socket: SF,
                                             connection_config: ConnectionConfig,
                                             create_dispatcher: DF) -> Result<dispatcher::Outbound, ConnectionError>
        where
            SF: Fn(ConnectionConfig) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<S, E>> + Send + 'static,
            S: SocketFlow,
            E: Into<ConnectionError> + Debug + Display,
            DF: FnOnce(&Sender<Message>) -> IsCurrentDispatcher + Send + Sync + 'static,
    {
        //todo: add retry logic.
        let (sock_term, mut inbound_rcv, outbound_snd) = create_socket(connection_config.clone()).await
            .map_err(|e| e.into())?
            .ports();

        outbound_snd.tx
            .send(message_utils::create_connect_cmd(connection_config.proxy_to_broker_url.clone())).await
            .map_err(|e| ConnectionError::Unexpected(e.to_string()))?;

        inbound_rcv.rx
            .recv().await
            .map_or_else(|| Err(ConnectionError::Disconnected), message_utils::check_connection_response)?;

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
            let reconnection = ReConnection::new(create_socket, connection_config, sock_term, 10, dispatcher);
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
                    let _ = tx_outbound.send(message_utils::create_pong()).await.unwrap();
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
                             sock_term: Term,
                             wait_sec: u64,
                             dispatcher: Dispatcher) -> ReConnection
            where
                F: Fn(ConnectionConfig) -> Fut + Send + Sync + 'static,
                Fut: Future<Output = Result<S, E>> + Send + 'static,
                S: SocketFlow,
                E: Into<ConnectionError> + Debug + Display,
        {

            let (tx_sig, rx_sig) = oneshot::channel::<()>();

            let reconnection = ReConnection { tx_sig_alert: tx_sig };

            tokio::spawn(async move {
                let _ = rx_sig.await;
                sock_term.terminate();
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
                                /**
                                 * The ping receiver, rx_outbound, might not be able to sink a ping message to its socket due to IOError.
                                 * This causes blocking on the ping sender side, tx_outbound. The logic moves forward by stop waiting for sender
                                 * after timeout.
                                 */
                                tokio::select! {
                                    _ = async {
                                            let _ = tx_outbound.send(message_utils::create_ping()).await;
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
    use std::sync::Arc;

    use tokio::sync::watch;

    use crate::netflow::connection::Connection;
    use crate::entity::Dispatcher;

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

mod message_utils {
    use crate::netflow::errors::ConnectionError;
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

    //todo: support auth plugin (https://pulsar.apache.org/docs/en/develop-binary-protocol/#connection-establishment)
    pub fn create_connect_cmd(proxy_to_broker_url: Option<String>) -> Message {
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

    pub fn create_ping() -> Message {
        Message {
            command: proto::BaseCommand {
                type_: proto::base_command::Type::Ping as i32,
                ping: Some(proto::CommandPing {}),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn create_pong() -> Message {
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