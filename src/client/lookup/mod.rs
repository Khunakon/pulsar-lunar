use crate::client::models::outbound::{SendError, RequestKey};
use crate::message::proto;
use crate::message::codec::Message;
use crate::client::connection::{GetRequestParam, Connection};
use crate::client::lookup::errors::LookupError;
use crate::message::validation::validate_response;

pub mod request;
pub mod response;
pub mod errors;

pub use crate::client::lookup::request::GetPartitionTopicMetadata;
pub use crate::client::lookup::request::LookupTopic;
use crate::client::lookup::response::{BrokerLoc, LookupTopicResponse};
use nom::Map;
use nom::lib::std::ops::Range;
use crate::client::connection;
use std::sync::Arc;
use futures::{FutureExt, Future};
use tokio::sync::mpsc;


pub trait GetLookupRequestParam {

    type ReturnedVal;
    type ExtractedVal;

    fn into_message(self, request_id: u64) -> Message;
    fn extract(response: Message) -> Option<Self::ExtractedVal>;
    fn map(extracted: Self::ExtractedVal) -> Result<Self::ReturnedVal, LookupError>;

}

impl <L: GetLookupRequestParam> GetRequestParam for L {
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

pub async fn lookup_partitioned_topic<S, F, B, E, Fut>(
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
    let partitions = pool.main_connection.send_request_with_default_retry(&request).await?.value();

    if partitions == 0 {
        let request = LookupTopic { topic: topic.clone(), authoritative: false };
        let response = pool.main_connection
            .send_request_with_default_retry(&request)
            .await.map_err(E::from)?;

        let broker_loc = response
            .into_broker_loc(pool, topic.clone())
            .await.map_err(E::from)?;

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
                let result = pool.main_connection.send_request_with_default_retry(&request).await;
                match result {
                    Ok(response) => {
                        match response.into_broker_loc(pool.clone(), topic.clone()).await {
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