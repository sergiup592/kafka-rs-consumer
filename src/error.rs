use rdkafka::error::KafkaError as RdKafkaError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, FrameworkError>;

#[derive(Debug, Error)]
pub enum FrameworkError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Kafka(#[from] KafkaError),
    #[error(transparent)]
    Dlq(#[from] DlqError),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    Handler(#[from] HandlerError),
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    ReadFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse TOML config: {0}")]
    ParseToml(#[from] toml::de::Error),
    #[error("invalid configuration: {0}")]
    Validation(String),
}

#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("kafka client creation failed: {0}")]
    CreateClient(#[from] RdKafkaError),
    #[error("failed to subscribe to topics {topics:?}: {source}")]
    Subscribe {
        topics: Vec<String>,
        #[source]
        source: RdKafkaError,
    },
    #[error("kafka poll failed: {0}")]
    Poll(#[source] RdKafkaError),
    #[error("kafka commit failed: {0}")]
    Commit(#[source] RdKafkaError),
    #[error("kafka infrastructure error: {message}: {source}")]
    Infrastructure {
        message: String,
        #[source]
        source: RdKafkaError,
    },
}

#[derive(Debug, Error)]
pub enum HandlerError {
    #[error("handler failed: {message}")]
    Processing { message: String },
    #[error("json deserialization failed: {0}")]
    JsonDeserialization(#[from] serde_json::Error),
    #[error("message missing payload")]
    MissingPayload,
}

impl HandlerError {
    pub fn processing<E>(error: E) -> Self
    where
        E: std::fmt::Display,
    {
        Self::Processing {
            message: error.to_string(),
        }
    }
}

#[derive(Debug, Error)]
pub enum DlqError {
    #[error("DLQ disabled in configuration")]
    Disabled,
    #[error("failed to build DLQ producer: {0}")]
    CreateProducer(#[from] RdKafkaError),
    #[error("failed to deliver message to DLQ: {0}")]
    Delivery(String),
    #[error("failed to flush DLQ producer: {0}")]
    Flush(#[source] RdKafkaError),
}

#[derive(Debug, Error)]
pub enum ShutdownError {
    #[error("failed to install SIGTERM handler: {0}")]
    SigTerm(std::io::Error),
    #[error("failed to wait for shutdown signal: {0}")]
    Signal(std::io::Error),
}
