use tokio::sync::{mpsc, oneshot, Mutex, watch};
use tokio::time;
use crate::message::codec::Message;
use crate::netflow::errors::SendError;
use crate::netflow::models::outbound::{PendingResponse, Request, RequestKey};
use nom::lib::std::collections::BTreeMap;
use std::sync::Arc;

pub struct Dispatcher {
    pub outbound: Outbound,
    pub inbound: Inbound,
    tx_connection_change: mpsc::Sender<ConnectionTx>
}

pub struct DispatcherConfig {
    pub tx_connection: mpsc::Sender<Message>,
    pub sig_reconnect_tx: watch::Sender<()>,
    pub buffer_size: usize,
    pub outbound_timeout_sec: u8
}

impl Dispatcher {

    pub fn new(config: DispatcherConfig) -> Self {
        let DispatcherConfig {
            tx_connection,
            sig_reconnect_tx,
            buffer_size,
            outbound_timeout_sec
        } = config;

        let (tx_connection_change, rx_connection_change) = mpsc::channel(1);
        let (register_pending_resp_tx, register_pending_resp_rx) = mpsc::channel(buffer_size);
        let connection_tx = ConnectionTx { tx: tx_connection };

        let consumer_rx_registry = Arc::new(Mutex::new(BTreeMap::new()));

        let outbound = Outbound::new(connection_tx,
                                     register_pending_resp_tx,
                                     rx_connection_change,
                                     consumer_rx_registry.clone(),
                                     sig_reconnect_tx,
                                     buffer_size,
                                     outbound_timeout_sec);

        let inbound = Inbound::new(buffer_size,
                                   register_pending_resp_rx,
                                   consumer_rx_registry);

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
           register_pending_resp_tx: mpsc::Sender<PendingResponse>,
           rx_connection_change: mpsc::Receiver<ConnectionTx>,
           consumer_rx_registry: Arc<Mutex<BTreeMap<u64, mpsc::Sender<Message>>>>,
           sig_reconnect_tx: watch::Sender<()>,
           buffer_size: usize,
           timeout_sec: u8) -> Self {

        let (tx, rx) = mpsc::channel(buffer_size);

        Outbound::handle(connection_tx, register_pending_resp_tx, rx, rx_connection_change, consumer_rx_registry, sig_reconnect_tx, timeout_sec);
        Outbound { tx }
    }


    fn handle(connection_tx: ConnectionTx,
              register_pending_resp_tx: mpsc::Sender<PendingResponse>,
              mut rx: mpsc::Receiver<Request>,
              mut rx_connection_change: mpsc::Receiver<ConnectionTx>,
              consumer_rx_registry: Arc<Mutex<BTreeMap<u64, mpsc::Sender<Message>>>>,
              sig_reconnect_tx: watch::Sender<()>,
              timeout_sec: u8) {

        tokio::spawn(async move {
            let tx = &connection_tx.tx;
            let register_pending_resp_tx_ref = &register_pending_resp_tx;
            tokio::select! {
                _ = async {
                        while let Some(request) = rx.recv().await {
                            request.get_response_via(|key, message| async move {
                                tokio::select! {
                                    result = async { tx.send(message).await } => {
                                        if let Err(e) = result {
                                            Err(SendError::MessageNotSent(e.to_string()))
                                        } else {
                                            Ok((key, register_pending_resp_tx_ref))
                                        }
                                    },
                                    _ = time::sleep(time::Duration::from_secs(timeout_sec as u64)) => {
                                        Err(SendError::SendTimeout(key))
                                    }
                                }
                            }).await
                        }
                    } => {},

                Some(new_connection_tx) = rx_connection_change.recv() => {
                    sig_reconnect_tx.send(()).unwrap();
                    Outbound::handle(new_connection_tx, register_pending_resp_tx, rx, rx_connection_change, consumer_rx_registry, sig_reconnect_tx, timeout_sec)
                }
            }
        });
    }

}


pub struct Inbound {
    pub tx: mpsc::Sender<Message>
}

impl Inbound {

    fn new(
        buffer_size: usize,
        register_pending_resp_rx: mpsc::Receiver<PendingResponse>,
        consumer_rx_registry: Arc<Mutex<BTreeMap<u64, mpsc::Sender<Message>>>>
    ) -> Self {

        let (tx, rx) = mpsc::channel::<Message>(buffer_size);
        let pending_resp_reg = PendingRespReg::new();
        let pending_dispatch_reg = PendingDispatchReg::new();

        Inbound::handle_inbound_message(rx,
                                        pending_resp_reg.clone(),
                                        pending_dispatch_reg.clone(),
                                        consumer_rx_registry);

        Inbound::handle_pending_response(register_pending_resp_rx,
                                         pending_resp_reg.clone(),
                                         pending_dispatch_reg.clone());

        Inbound { tx }
    }

    fn handle_pending_response(mut register_pending_resp_rx: mpsc::Receiver<PendingResponse>,
                               pending_resp_reg: PendingRespReg,
                               pending_dispatch_reg: PendingDispatchReg) {

        tokio::spawn(async move {
            while let Some(PendingResponse { request_key, tx_response }) = register_pending_resp_rx.recv().await {
                let pending_dispatch_reg = pending_dispatch_reg.clone();
                if let Some(msg) = pending_dispatch_reg.check_out(&request_key).await {
                    let _ = tx_response.send(Ok(msg));
                } else {
                    let reg = pending_resp_reg.clone();
                    reg.check_in(request_key, tx_response).await;
                }
            }
        });
    }

    fn handle_inbound_message(mut rx: mpsc::Receiver<Message>,
                              pending_resp_reg: PendingRespReg,
                              pending_dispatch_reg: PendingDispatchReg,
                              consumer_rx_registry: Arc<Mutex<BTreeMap<u64, mpsc::Sender<Message>>>>) {

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match RequestKey::from_message(&msg) {
                    Some(RequestKey::Consumer { consumer_id }) => {
                        let rx_registry = consumer_rx_registry.lock().await;
                        match rx_registry.get(&consumer_id) {
                            Some(tx) => {
                                let tx = tx.clone();
                                std::mem::drop(rx_registry); //unlock;
                                let _ = tx.send(msg).await;
                            }
                            None => {
                                log::warn!("Cannot dispatch the message to the consumer. \
                                No mapping channel found in registry. consumer_id: {}", consumer_id);
                            }
                        }
                    }
                    Some(key) => {
                        let reg = pending_resp_reg.clone();

                        if let Some(resolver) = reg.check_out(&key).await {
                            let _ = resolver.send(Ok(msg));
                        } else {
                            let pending_dispatch_reg = pending_dispatch_reg.clone();
                            pending_dispatch_reg.check_in(key, msg).await;
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


struct PendingDispatchReg {
    inner: Arc<Mutex<BTreeMap<RequestKey, Message>>>
}

impl PendingDispatchReg {
    pub fn new() -> PendingDispatchReg {
        PendingDispatchReg {
            inner: Arc::new(Mutex::new(BTreeMap::new()))
        }
    }
    pub async fn check_out(&self, key: &RequestKey) -> Option<Message> {
        let mut reg = self.inner.lock().await;
        reg.remove(key)
    }
    pub async fn check_in(&self, key: RequestKey, message: Message) {
        let mut reg = self.inner.lock().await;
        reg.insert(key, message);
    }
}

impl Clone for PendingDispatchReg {
    fn clone(&self) -> Self {
        PendingDispatchReg { inner: self.inner.clone() }
    }
}

struct PendingRespReg {
   inner: Arc<Mutex<BTreeMap<RequestKey, oneshot::Sender<Result<Message, SendError>>>>>
}

impl PendingRespReg {

    pub fn new() -> PendingRespReg {
        PendingRespReg {
            inner: Arc::new(Mutex::new(BTreeMap::new()))
        }
    }

    pub async fn check_out(&self, key: &RequestKey) -> Option<oneshot::Sender<Result<Message, SendError>>> {
        let mut reg = self.inner.lock().await;
        reg.remove(key)
    }

    pub async fn check_in(&self, key: RequestKey, resolver: oneshot::Sender<Result<Message, SendError>>) {
        let mut reg = self.inner.lock().await;
        reg.insert(key, resolver);
    }
}

impl Clone for PendingRespReg {
    fn clone(&self) -> Self {
        PendingRespReg { inner: self.inner.clone() }
    }
}