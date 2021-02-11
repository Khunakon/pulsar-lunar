use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;

use crate::message::codec::Message;

pub mod errors;
pub mod socket;
pub mod connection;
pub mod config;
pub(crate) mod dispatcher;
pub(crate) mod models;

pub struct Term {
    tx_sigterm: oneshot::Sender<()>
}

impl Term {
    pub fn terminate(self) {
        let _ = self.tx_sigterm.send(());
    }
}

pub struct In {
    pub rx: Receiver<Message>
}

pub struct Out {
    pub tx: Sender<Message>
}

pub trait SocketFlow {
    fn ports(self) -> (Term, In, Out);
}

