use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rdkafka::{
    config::ClientConfig,
    message::{Header, Message as KafkaMessage, OwnedHeaders},
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
};

use crate::{config::AppConfig, error::DlqError, handler::Message};

#[derive(Clone)]
pub struct DlqProducer {
    producer: FutureProducer,
    topic: String,
    include_metadata: bool,
    flush_timeout: Duration,
}

impl DlqProducer {
    pub fn from_config(config: &AppConfig) -> Result<Option<Self>, DlqError> {
        if !config.dlq.enabled {
            return Ok(None);
        }

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka.brokers)
            .set("message.timeout.ms", "5000")
            .create()?;

        Ok(Some(Self {
            producer,
            topic: config.dlq.topic.clone(),
            include_metadata: config.dlq.include_metadata,
            flush_timeout: Duration::from_millis(config.dlq.flush_timeout_ms),
        }))
    }

    pub async fn send(
        &self,
        message: &Message,
        error_message: &str,
        retry_count: u32,
    ) -> Result<(), DlqError> {
        let mut record = FutureRecord::to(&self.topic);
        if let Some(payload) = &message.payload {
            record = record.payload(payload);
        }
        if let Some(key) = &message.key {
            record = record.key(key);
        }
        if self.include_metadata {
            record = record.headers(Self::build_metadata_headers(
                message,
                error_message,
                retry_count,
            ));
        }

        self.producer
            .send(record, Timeout::After(Duration::from_secs(5)))
            .await
            .map_err(|(err, failed_message)| {
                tracing::error!(
                    error = %err,
                    failed_topic = %failed_message.topic(),
                    source_topic = %message.topic,
                    source_partition = message.partition,
                    source_offset = message.offset,
                    "failed to deliver message to DLQ"
                );
                DlqError::Delivery(err.to_string())
            })?;

        Ok(())
    }

    pub fn flush(&self) -> Result<(), DlqError> {
        self.producer
            .flush(Timeout::After(self.flush_timeout))
            .map_err(DlqError::Flush)
    }

    pub fn build_metadata_headers(
        message: &Message,
        error_message: &str,
        retry_count: u32,
    ) -> OwnedHeaders {
        let failed_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis().to_string())
            .unwrap_or_else(|_| "0".to_string());

        let mut headers = OwnedHeaders::new();
        headers = headers.insert(Header {
            key: "original_topic",
            value: Some(message.topic.as_bytes()),
        });
        headers = headers.insert(Header {
            key: "original_partition",
            value: Some(message.partition.to_string().as_bytes()),
        });
        headers = headers.insert(Header {
            key: "original_offset",
            value: Some(message.offset.to_string().as_bytes()),
        });
        headers = headers.insert(Header {
            key: "error_message",
            value: Some(error_message.as_bytes()),
        });
        headers = headers.insert(Header {
            key: "retry_count",
            value: Some(retry_count.to_string().as_bytes()),
        });
        headers.insert(Header {
            key: "failed_at",
            value: Some(failed_at.as_bytes()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::Message;
    use rdkafka::message::Headers;
    use std::collections::HashMap;

    #[test]
    fn builds_dlq_headers_with_required_metadata() {
        let message = Message {
            key: None,
            payload: Some(vec![1, 2, 3]),
            topic: "orders".to_string(),
            partition: 2,
            offset: 44,
            timestamp: Some(1700000000),
            headers: HashMap::new(),
        };

        let headers = DlqProducer::build_metadata_headers(&message, "boom", 3);
        assert_eq!(headers.count(), 6);
    }
}
