use std::path::PathBuf;

use async_trait::async_trait;
use clap::Parser;
use kafka_rs_consumer::{
    handler::{Message, MessageHandler},
    shutdown::spawn_shutdown_listener,
    AppConfig, KafkaConsumerFramework,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "config.toml")]
    config: PathBuf,
}

#[derive(Debug, Error)]
#[error("print handler does not fail")]
struct PrintError;

struct PrintHandler;

#[async_trait]
impl MessageHandler for PrintHandler {
    type Error = PrintError;

    async fn handle(&self, message: Message) -> Result<(), Self::Error> {
        let payload = message
            .payload
            .as_ref()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
            .unwrap_or_else(|| "<empty>".to_string());

        tracing::info!(
            topic = %message.topic,
            partition = message.partition,
            offset = message.offset,
            payload = %payload,
            "message received"
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
    let framework = KafkaConsumerFramework::new(config, PrintHandler)?;

    let shutdown = CancellationToken::new();
    let _signal = spawn_shutdown_listener(shutdown.clone());

    framework.run(shutdown).await?;
    Ok(())
}
