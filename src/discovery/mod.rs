use std::sync::Arc;

use futures::Future;
use tokio::sync::mpsc;

use crate::net::connection::{proto::AskProto};
use crate::net::connection;
use crate::net::models::outbound::{RequestKey, RetryReq};
use crate::discovery::errors::LookupError;
pub use crate::discovery::request::GetPartitionTopicMetadata;
pub use crate::discovery::request::LookupTopic;
use crate::discovery::response::{BrokerLoc, LookupTopicResponse};
use crate::message::codec::Message;
use crate::message::validation::validate_response;

pub mod request;
pub mod response;
pub mod errors;

pub async fn find_broker_loc(mut response: LookupTopicResponse, pool: Arc<connection::Pool>, topic: String) -> Result<BrokerLoc, LookupError> {

    loop {
        let LookupTopicResponse { broker_url, broker_url_tls, proxy_through_service, redirect, is_authoritative} = response;
        let (broker_url, use_tls) =
            if let Some(tls_url) = broker_url_tls.clone() {
                (tls_url, true)
            } else {
                (broker_url.clone(), false)
            };

        let broker_loc = BrokerLoc { broker_url, use_proxy: proxy_through_service, use_tls };

        if redirect {
            let connection = pool.get_connection(broker_loc).await.map_err(LookupError::from)?;
            let lookup_topic = LookupTopic { topic: topic.clone(), authoritative: is_authoritative };
            response = connection.ask_retry(&lookup_topic, RetryReq::Default).await?
        } else {
            break Ok(broker_loc)
        }
    }

}

pub async fn lookup_topic<S, F, B, E, Fut>(
    topic: S,
    pool: Arc<connection::Pool>,
    map: F,
) -> Result<Vec<B>, E>
    where
        S: Into<String>,
        F: Fn((String, BrokerLoc)) -> Fut + Send + Sync + 'static ,
        Fut: Future<Output = Result<B, E>> + Send,
        E: From<LookupError> + Send + Sync + 'static,
        B: Send + Sync + 'static
{
    let topic = topic.into();
    let request = GetPartitionTopicMetadata { topic: topic.clone() };
    let partitions = pool.main_connection.ask_retry(&request, RetryReq::Default).await?.value();

    if partitions == 0 {
        let request = LookupTopic { topic: topic.clone(), authoritative: false };
        let response = pool.main_connection
            .ask_retry(&request, RetryReq::Default)
            .await.map_err(E::from)?;

        let broker_loc = find_broker_loc(response, pool, topic.clone()).await.map_err(E::from)?;
        let b = map((topic, broker_loc)).await?;
        Ok(vec![b])

    } else {
        let topics: Vec<String> = (0..partitions).map(|n| format!("{}-partition-{}", &topic, n)).collect();
        let mut topics = topics.into_iter();
        let (tx, mut rx) = mpsc::channel(partitions as usize);
        let map = Arc::new(map);

        while let Some(topic) = topics.next() {
            let pool = pool.clone();
            let tx = tx.clone();
            let map = map.clone();

            tokio::spawn(async move {
                let request = LookupTopic { topic: topic.clone(), authoritative: false };
                let result = pool.main_connection.ask_retry(&request, RetryReq::Default).await;
                match result {
                    Ok(response) => {
                        match find_broker_loc(response, pool.clone(), topic.clone()).await {
                            Ok(broker_loc) => tx.send(map((topic, broker_loc)).await).await,
                            Err(e) => tx.send(Err(E::from(e))).await
                        }
                    },
                    Err(e) => tx.send(Err(E::from(e))).await
                }
            });
        }

        let mut pairs = Vec::default();

        loop {
            if let Some(result) = rx.recv().await {
                match result {
                    Ok(pair) => pairs.push(pair),
                    Err(e) => break Err(e)
                }
            } else {
                break Ok(pairs)
            }
        }
    }

}

pub trait GetLookupRequestParam {

    type ReturnedVal;
    type ExtractedVal;

    fn into_message(self, request_id: u64) -> Message;
    fn extract(response: Message) -> Option<Self::ExtractedVal>;
    fn map(extracted: Self::ExtractedVal) -> Result<Self::ReturnedVal, LookupError>;

}

impl <L: GetLookupRequestParam> AskProto for L {
    type ReturnedVal = <L as GetLookupRequestParam>::ReturnedVal;
    type Error = LookupError;

    fn get_request_key(&self) -> RequestKey {
        RequestKey::AutoIncrRequestId
    }

    fn into_message(self, request_id: u64) -> Message {
        self.into_message(request_id)
    }

    fn map(response: Message) -> Result<Self::ReturnedVal, LookupError> {
        let extracted_val = validate_response(response, L::extract).map_err(LookupError::from)?;
        L::map(extracted_val)
    }

    fn can_retry_from_error(error: &Self::Error) -> bool {
        LookupError::can_be_retry(&error)
    }
}