pub mod backpressure;
pub mod config;
pub mod consumer;
pub mod dlq;
pub mod error;
pub mod handler;
pub mod health;
pub mod metrics;
pub mod offset;
pub mod shutdown;

pub use config::AppConfig;
pub use consumer::KafkaConsumerFramework;
pub use error::{ConfigError, DlqError, FrameworkError, HandlerError, KafkaError, ShutdownError};
pub use handler::{
    JsonHandler, JsonMessageHandlerAdapter, Message, MessageHandler, MessageMetadata,
};
pub use metrics::{Metrics, MetricsSnapshot};
