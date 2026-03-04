use std::{path::PathBuf, time::Duration};

use async_trait::async_trait;
use clap::Parser;
use kafka_rs_consumer::{
    handler::{Message, MessageHandler},
    shutdown::spawn_shutdown_listener,
    AppConfig, KafkaConsumerFramework,
};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "config.toml")]
    config: PathBuf,

    #[arg(long, default_value_t = 100)]
    batch_size: usize,
}

#[derive(Debug, Error, Clone)]
enum BatchError {
    #[error("batch worker stopped")]
    WorkerStopped,
}

struct BatchItem {
    message: Message,
    ack: oneshot::Sender<Result<(), BatchError>>,
}

struct BatchingHandler {
    tx: mpsc::Sender<BatchItem>,
}

impl BatchingHandler {
    fn new(batch_size: usize) -> Self {
        let (tx, mut rx) = mpsc::channel::<BatchItem>(10_000);

        tokio::spawn(async move {
            let mut batch = Vec::with_capacity(batch_size);
            let mut acks = Vec::with_capacity(batch_size);

            loop {
                let item = if batch.is_empty() {
                    rx.recv().await
                } else {
                    tokio::time::timeout(Duration::from_secs(1), rx.recv())
                        .await
                        .unwrap_or_default()
                };
                let stream_closed = item.is_none();

                match item {
                    Some(item) => {
                        batch.push(item.message);
                        acks.push(item.ack);
                    }
                    None => {
                        if batch.is_empty() {
                            break;
                        }
                    }
                }

                if batch.len() >= batch_size || (stream_closed && !batch.is_empty()) {
                    let result = process_batch(&batch).await;
                    for ack in acks.drain(..) {
                        let _ = ack.send(result.clone());
                    }
                    batch.clear();
                }
            }
        });

        Self { tx }
    }
}

#[async_trait]
impl MessageHandler for BatchingHandler {
    type Error = BatchError;

    async fn handle(&self, message: Message) -> Result<(), Self::Error> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(BatchItem {
                message,
                ack: ack_tx,
            })
            .await
            .map_err(|_| BatchError::WorkerStopped)?;

        ack_rx.await.map_err(|_| BatchError::WorkerStopped)?
    }
}

async fn process_batch(batch: &[Message]) -> Result<(), BatchError> {
    tracing::info!(batch_size = batch.len(), "processing batch");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .init();

    let args = Args::parse();
    let config = AppConfig::from_file(args.config)?;
    let handler = BatchingHandler::new(args.batch_size);
    let framework = KafkaConsumerFramework::new(config, handler)?;

    let shutdown = CancellationToken::new();
    let _signal = spawn_shutdown_listener(shutdown.clone());

    framework.run(shutdown).await?;
    Ok(())
}
