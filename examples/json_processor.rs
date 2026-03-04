use std::path::PathBuf;

use async_trait::async_trait;
use clap::Parser;
use kafka_rs_consumer::{
    handler::{JsonHandler, JsonMessageHandlerAdapter, MessageMetadata},
    shutdown::spawn_shutdown_listener,
    AppConfig, KafkaConsumerFramework,
};
use serde::Deserialize;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "config.toml")]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
struct OrderEvent {
    order_id: String,
    customer_id: String,
    amount: f64,
}

#[derive(Debug, Error)]
enum ProcessorError {
    #[error("invalid amount {0}")]
    InvalidAmount(f64),
}

struct OrderProcessor;

#[async_trait]
impl JsonHandler<OrderEvent> for OrderProcessor {
    type Error = ProcessorError;

    async fn handle(
        &self,
        _key: Option<Vec<u8>>,
        value: OrderEvent,
        metadata: MessageMetadata,
    ) -> Result<(), Self::Error> {
        if value.amount.is_sign_negative() {
            return Err(ProcessorError::InvalidAmount(value.amount));
        }

        tracing::info!(
            order_id = %value.order_id,
            customer_id = %value.customer_id,
            amount = value.amount,
            topic = %metadata.topic,
            partition = metadata.partition,
            offset = metadata.offset,
            "processed order event"
        );

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .init();

    let args = Args::parse();
    let config = AppConfig::from_file(args.config)?;

    let handler = JsonMessageHandlerAdapter::<OrderProcessor, OrderEvent>::new(OrderProcessor);
    let framework = KafkaConsumerFramework::new(config, handler)?;

    let shutdown = CancellationToken::new();
    let _signal = spawn_shutdown_listener(shutdown.clone());

    framework.run(shutdown).await?;
    Ok(())
}
