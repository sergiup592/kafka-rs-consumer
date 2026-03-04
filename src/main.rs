use std::path::PathBuf;

use anyhow::Context;
use async_trait::async_trait;
use clap::Parser;
use kafka_rs_consumer::{
    config::AppConfig,
    handler::{Message, MessageHandler},
    shutdown::spawn_shutdown_listener,
    KafkaConsumerFramework,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(author, version, about = "Production-grade Kafka consumer framework")]
struct Cli {
    #[arg(short, long, default_value = "config.toml", env = "KAFKA_RS_CONFIG")]
    config: PathBuf,

    #[arg(long, default_value = "info", env = "RUST_LOG")]
    log_level: String,

    #[arg(long)]
    json_logs: bool,
}

#[derive(Debug, Error)]
#[error("print handler does not fail")]
struct PrintHandlerError;

struct PrintHandler;

#[async_trait]
impl MessageHandler for PrintHandler {
    type Error = PrintHandlerError;

    async fn handle(&self, message: Message) -> Result<(), Self::Error> {
        tracing::info!(
            topic = %message.topic,
            partition = message.partition,
            offset = message.offset,
            payload_len = message.payload.as_ref().map_or(0, Vec::len),
            "received message"
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&cli.log_level));

    if cli.json_logs {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(env_filter).init();
    }

    let config = AppConfig::from_file(&cli.config)
        .with_context(|| format!("failed to load config from {}", cli.config.display()))?;

    let framework = KafkaConsumerFramework::new(config, PrintHandler)
        .context("failed to initialize Kafka consumer framework")?;

    let shutdown_token = CancellationToken::new();
    let _signal_listener = spawn_shutdown_listener(shutdown_token.clone());

    framework
        .run(shutdown_token)
        .await
        .context("consumer runtime exited with failure")
}
