use std::{collections::HashMap, marker::PhantomData};

use async_trait::async_trait;
use rdkafka::message::{BorrowedMessage, Headers, Message as KafkaMessage, Timestamp};
use serde::de::DeserializeOwned;

use crate::error::HandlerError;

#[derive(Debug, Clone)]
pub struct Message {
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub headers: HashMap<String, Vec<u8>>,
}

impl Message {
    pub fn from_borrowed(msg: &BorrowedMessage<'_>) -> Self {
        let mut headers_map = HashMap::new();
        if let Some(headers) = msg.headers() {
            for header in headers.iter() {
                if let Some(value) = header.value {
                    headers_map.insert(header.key.to_string(), value.to_vec());
                }
            }
        }

        let timestamp = match msg.timestamp() {
            Timestamp::NotAvailable => None,
            Timestamp::CreateTime(ts) | Timestamp::LogAppendTime(ts) => Some(ts),
        };

        Self {
            key: msg.key().map(ToOwned::to_owned),
            payload: msg.payload().map(ToOwned::to_owned),
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            timestamp,
            headers: headers_map,
        }
    }

    pub fn metadata(&self) -> MessageMetadata {
        MessageMetadata {
            topic: self.topic.clone(),
            partition: self.partition,
            offset: self.offset,
            timestamp: self.timestamp,
            headers: self.headers.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub headers: HashMap<String, Vec<u8>>,
}

#[async_trait]
pub trait MessageHandler: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn handle(&self, message: Message) -> Result<(), Self::Error>;
}

#[async_trait]
pub trait JsonHandler<T: DeserializeOwned>: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn handle(
        &self,
        key: Option<Vec<u8>>,
        value: T,
        metadata: MessageMetadata,
    ) -> Result<(), Self::Error>;
}

pub struct JsonMessageHandlerAdapter<H, T> {
    inner: H,
    marker: PhantomData<T>,
}

impl<H, T> JsonMessageHandlerAdapter<H, T> {
    pub fn new(inner: H) -> Self {
        Self {
            inner,
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<H, T> MessageHandler for JsonMessageHandlerAdapter<H, T>
where
    H: JsonHandler<T>,
    T: DeserializeOwned + Send + Sync + 'static,
{
    type Error = HandlerError;

    async fn handle(&self, message: Message) -> Result<(), Self::Error> {
        let payload = message
            .payload
            .clone()
            .ok_or(HandlerError::MissingPayload)?;
        let value = serde_json::from_slice::<T>(&payload)?;

        self.inner
            .handle(message.key.clone(), value, message.metadata())
            .await
            .map_err(HandlerError::processing)
    }
}
