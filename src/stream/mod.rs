pub mod errors;
pub mod socket;

use crate::message::codec::Message;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::oneshot;
use url::Url;
use std::net::SocketAddr;

pub struct FlowHandle {
    tx_sigterm: oneshot::Sender<()>
}

impl FlowHandle {
    pub fn terminate(self) {
        let _ = self.tx_sigterm.send(());
    }
}

pub struct InboundReceiver {
    pub rx: Receiver<Message>
}

pub struct OutboundSender {
    pub tx: Sender<Message>
}

pub trait PulsarStream {
    fn into_io_flow(self) -> (FlowHandle, InboundReceiver, OutboundSender);
}

