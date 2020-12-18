use tokio::sync::{mpsc, oneshot, Mutex, watch};
use tokio::time;
use crate::message::codec::Message;
use crate::client::models::outbound::{PendingResponse, SendError, Request, RequestKey};
use nom::lib::std::collections::BTreeMap;
use std::sync::Arc;
use std::mem;

pub struct Dispatcher {
    pub outbound: Outbound,
    pub inbound: Inbound,
    tx_connection_change: mpsc::Sender<ConnectionTx>
}

impl Dispatcher {

    pub fn new(tx_connection: mpsc::Sender<Message>,
               sig_reconnect_tx: watch::Sender<()> ,
               buffer_size: usize,
               outbound_timeout_sec: u8) -> Self {
        let (tx_connection_change, rx_connection_change) = mpsc::channel(1);
        let (tx_pending_response, rx_pending_response) = mpsc::channel(buffer_size);
        let connection_tx = ConnectionTx { tx: tx_connection };
        let outbound = Outbound::new(connection_tx, tx_pending_response, rx_connection_change, sig_reconnect_tx, buffer_size, outbound_timeout_sec);
        let inbound = Inbound::new(buffer_size, rx_pending_response);
        Dispatcher { outbound, inbound, tx_connection_change }
    }

    pub async fn change_connection(&self, tx: ConnectionTx) {
        if let Err(e) = self.tx_connection_change.send(tx).await {
            panic!("Can't send a connection change event. {}", e.to_string());
        }
    }
}

#[derive(Clone)]
pub struct ConnectionTx {
    tx: mpsc::Sender<Message>
}

impl From<mpsc::Sender<Message>> for ConnectionTx {
    fn from(tx: mpsc::Sender<Message>) -> Self {
        ConnectionTx { tx }
    }
}

#[derive(Clone)]
pub struct Outbound {
    pub tx: mpsc::Sender<Request>,
}

impl From<mpsc::Sender<Request>> for Outbound {
    fn from(tx: mpsc::Sender<Request>) -> Self {
        Outbound { tx }
    }
}

impl Outbound {
    fn new(connection_tx: ConnectionTx,
           tx_pending_response: mpsc::Sender<PendingResponse>,
           rx_connection_change: mpsc::Receiver<ConnectionTx>,
           sig_reconnect_tx: watch::Sender<()>,
           buffer_size: usize,
           timeout_sec: u8) -> Self {

        let (tx, rx) = mpsc::channel(buffer_size);

        Outbound::handle(connection_tx, tx_pending_response, rx, rx_connection_change, sig_reconnect_tx, timeout_sec);
        Outbound { tx }
    }

    fn handle(connection_tx: ConnectionTx,
              tx_pending_response: mpsc::Sender<PendingResponse>,
              mut rx: mpsc::Receiver<Request>,
              mut rx_connection_change: mpsc::Receiver<ConnectionTx>,
              sig_reconnect_tx: watch::Sender<()>,
              timeout_sec: u8) {

        //todo: solve request message might be loss and sender might await for the response forever.
        tokio::spawn(async move {
            tokio::select! {
                _ = async {
                        while let Some(request) = rx.recv().await {
                            let Request { key, message, tx_response } = request;
                            let result = tokio::select! {
                                result = async { connection_tx.tx.send(message).await } => {
                                    if let Err(e) = result {
                                        Err(SendError::MessageNotSent(e.to_string()))
                                    } else {
                                        Ok(key)
                                    }
                                },
                                _ = time::sleep(time::Duration::from_secs(timeout_sec as u64)) => {
                                    Err(SendError::SendTimeout(key))
                                }
                            };
                            match result {
                                Ok(request_key) => {
                                    let pending_response = PendingResponse { request_key, tx_response };
                                    if let Err(e) = tx_pending_response.send(pending_response).await {
                                        panic!(SendError::Unexpected(e.to_string()));
                                    }
                                }
                                Err(e) => { tx_response.send(Err(e)).unwrap(); }
                            }
                        }
                    } => {},

                Some(new_connection_tx) = rx_connection_change.recv() => {
                    sig_reconnect_tx.send(()).unwrap();
                    Outbound::handle(new_connection_tx, tx_pending_response, rx, rx_connection_change, sig_reconnect_tx, timeout_sec)
                }
            }
        });
    }
}

type PendingResponseRegistry = Arc<Mutex<BTreeMap<RequestKey, oneshot::Sender<Result<Message, SendError>>>>>;
type PendingDispatchRegistry = Arc<Mutex<BTreeMap<RequestKey, Message>>>;

pub struct Inbound {
    pub tx: mpsc::Sender<Message>
}

impl Inbound {

    fn new(buffer_size: usize, rx_pending_response: mpsc::Receiver<PendingResponse>) -> Self {

        let (tx, rx) = mpsc::channel::<Message>(buffer_size);
        let pending_response_registry = Arc::new(Mutex::new(BTreeMap::new()));
        let pending_dispatch_registry = Arc::new(Mutex::new(BTreeMap::new()));

        Inbound::handle_inbound_message(rx,
                                        pending_response_registry.clone(),
                                        pending_dispatch_registry.clone());

        Inbound::handle_pending_response(rx_pending_response,
                                         pending_response_registry.clone(),
                                         pending_dispatch_registry.clone());

        Inbound { tx }
    }

    fn handle_pending_response(mut rx_pending_response: mpsc::Receiver<PendingResponse>,
                               pending_response_registry: PendingResponseRegistry,
                               pending_dispatch_registry: PendingDispatchRegistry) {

        tokio::spawn(async move {
            while let Some(PendingResponse { request_key, tx_response } ) = rx_pending_response.recv().await {
                let pending_dispatch_registry = pending_dispatch_registry.clone();
                let mut pending_dispatch_registry = pending_dispatch_registry.lock().await;
                let message_opt = pending_dispatch_registry.remove(&request_key);

                mem::drop(pending_dispatch_registry);

                if let Some(msg) = message_opt {
                    let _ = tx_response.send(Ok(msg));
                } else {
                    let pending_response_registry = pending_response_registry.clone();
                    let mut pending_response_registry = pending_response_registry.lock().await;
                    pending_response_registry.insert(request_key, tx_response);
                }
            }
        });
    }

    fn handle_inbound_message(mut rx: mpsc::Receiver<Message>,
                              pending_response_registry: PendingResponseRegistry,
                              pending_dispatch_registry: PendingDispatchRegistry) {

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match RequestKey::from_message(&msg) {
                    Some(key) => {
                        let pending_response_registry = pending_response_registry.clone();
                        let mut pending_response_registry = pending_response_registry.lock().await;
                        let resolver_opt = pending_response_registry.remove(&key);

                        mem::drop(pending_response_registry); //release lock.

                        if let Some(resolver) = resolver_opt {
                            let _ = resolver.send(Ok(msg));
                        } else {
                            let pending_dispatch_registry = pending_dispatch_registry.clone();
                            let mut pending_dispatch_registry = pending_dispatch_registry.lock().await;
                            pending_dispatch_registry.insert(key, msg);
                        }
                    }
                    None => {
                        log::warn!("Drop message with no request id. {:?}", msg);
                    }
                }
            }
        });
    }
}
