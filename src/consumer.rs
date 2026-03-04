use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use futures_util::StreamExt;
use rdkafka::{
    config::ClientConfig,
    consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaError as RdKafkaError,
    statistics::Statistics,
    ClientContext,
};
use tokio::{task::JoinSet, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    backpressure::{BackpressureController, BackpressurePermit},
    config::{AppConfig, CommitStrategy},
    dlq::DlqProducer,
    error::{KafkaError, Result},
    handler::{Message, MessageHandler},
    health::{spawn_health_server, HealthState},
    metrics::Metrics,
    offset::OffsetTracker,
};

#[derive(Clone)]
struct RuntimeContext {
    health: HealthState,
    metrics: Metrics,
    rebalance_commit_requested: Arc<AtomicBool>,
}

impl ClientContext for RuntimeContext {
    fn stats(&self, statistics: Statistics) {
        let mut lag_total = 0_i64;
        let mut lag_samples = 0_u64;

        for topic in statistics.topics.values() {
            for partition in topic.partitions.values() {
                let lag = partition.consumer_lag;
                if lag >= 0 {
                    lag_total += lag;
                    lag_samples += 1;
                }
            }
        }

        if lag_samples > 0 {
            self.metrics.set_consumer_lag(Some(lag_total));
        }
    }
}

impl ConsumerContext for RuntimeContext {
    fn pre_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        self.health.rebalance_started();

        if matches!(rebalance, Rebalance::Revoke(_)) {
            self.rebalance_commit_requested
                .store(true, Ordering::SeqCst);
        }
    }

    fn post_rebalance<'a>(&self, _: &Rebalance<'a>) {
        self.health.rebalance_finished();
    }
}

type FrameworkConsumer = StreamConsumer<RuntimeContext>;

#[derive(Clone)]
struct TaskResult {
    topic: String,
    partition: i32,
    offset: i64,
    ack: bool,
    failed: bool,
    dlqed: bool,
    latency: Duration,
    error_message: Option<String>,
}

pub struct KafkaConsumerFramework<H>
where
    H: MessageHandler,
{
    config: AppConfig,
    consumer: FrameworkConsumer,
    handler: Arc<H>,
    offset_tracker: Arc<OffsetTracker>,
    dlq: Option<DlqProducer>,
    backpressure: BackpressureController,
    metrics: Metrics,
    health: HealthState,
    rebalance_commit_requested: Arc<AtomicBool>,
}

impl<H> KafkaConsumerFramework<H>
where
    H: MessageHandler,
{
    pub fn new(config: AppConfig, handler: H) -> Result<Self> {
        let metrics = Metrics::new();
        let offset_tracker = Arc::new(OffsetTracker::new(
            config.consumer.commit_strategy.clone(),
            config.consumer.manual_commit_batch_size,
        ));
        let health = HealthState::new(
            config.health.max_poll_gap_secs,
            config.health.max_rebalance_secs,
        );
        let rebalance_commit_requested = Arc::new(AtomicBool::new(false));

        let context = RuntimeContext {
            health: health.clone(),
            metrics: metrics.clone(),
            rebalance_commit_requested: Arc::clone(&rebalance_commit_requested),
        };

        let enable_auto_commit = if config.consumer.commit_strategy == CommitStrategy::Auto {
            "true"
        } else {
            "false"
        };

        let consumer: FrameworkConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka.brokers)
            .set("group.id", &config.kafka.group_id)
            .set("enable.partition.eof", "false")
            .set(
                "session.timeout.ms",
                config.kafka.session_timeout_ms.to_string(),
            )
            .set(
                "max.poll.interval.ms",
                config.kafka.max_poll_interval_ms.to_string(),
            )
            .set("auto.offset.reset", config.kafka.auto_offset_reset.as_str())
            .set("enable.auto.commit", enable_auto_commit)
            .set(
                "auto.commit.interval.ms",
                config.consumer.auto_commit_interval_ms.to_string(),
            )
            .set(
                "statistics.interval.ms",
                config.kafka.statistics_interval_ms.to_string(),
            )
            .create_with_context(context)
            .map_err(KafkaError::CreateClient)?;

        consumer
            .subscribe(
                &config
                    .kafka
                    .topics
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
            )
            .map_err(|source| KafkaError::Subscribe {
                topics: config.kafka.topics.clone(),
                source,
            })?;

        let dlq = DlqProducer::from_config(&config)?;

        Ok(Self {
            backpressure: BackpressureController::new(config.consumer.concurrency),
            config,
            consumer,
            handler: Arc::new(handler),
            offset_tracker,
            dlq,
            metrics,
            health,
            rebalance_commit_requested,
        })
    }

    pub fn metrics(&self) -> Metrics {
        self.metrics.clone()
    }

    pub fn health_state(&self) -> HealthState {
        self.health.clone()
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        tracing::info!(
            brokers = %self.config.kafka.brokers,
            group_id = %self.config.kafka.group_id,
            topics = ?self.config.kafka.topics,
            concurrency = self.backpressure.max_in_flight(),
            commit_strategy = ?self.config.consumer.commit_strategy,
            "starting kafka consumer framework"
        );

        let health_shutdown = shutdown.child_token();
        let health_handle = spawn_health_server(
            &self.config.health,
            self.health.clone(),
            self.metrics.clone(),
            health_shutdown.clone(),
        );

        let mut tasks: JoinSet<TaskResult> = JoinSet::new();
        let mut stream = self.consumer.stream();
        let infrastructure_backoff =
            Duration::from_millis(self.config.consumer.infrastructure_retry_backoff_ms);

        loop {
            self.commit_on_rebalance_request()?;

            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("shutdown signal received, stopping kafka polling");
                    break;
                }
                joined = tasks.join_next(), if !tasks.is_empty() => {
                    self.handle_task_join(joined)?;
                }
                maybe_message = stream.next() => {
                    match maybe_message {
                        Some(Ok(borrowed_message)) => {
                            self.health.record_poll();

                            let message = Message::from_borrowed(&borrowed_message);
                            let permit = self.acquire_handler_permit().await;
                            self.spawn_message_task(&mut tasks, message, permit);
                        }
                        Some(Err(err)) => {
                            self.handle_poll_error(err);
                            tokio::time::sleep(infrastructure_backoff).await;
                        }
                        None => {
                            tracing::warn!("kafka consumer stream ended unexpectedly");
                            break;
                        }
                    }
                }
            }
        }

        self.drain_in_flight(
            &mut tasks,
            Duration::from_secs(self.config.consumer.shutdown_timeout_secs),
        )
        .await?;

        self.commit_final_offsets()?;
        self.flush_dlq()?;
        health_shutdown.cancel();

        if let Some(handle) = health_handle {
            let _ = handle.await;
        }

        let summary = self.metrics.snapshot();
        tracing::info!(
            messages_processed = summary.messages_processed,
            messages_failed = summary.messages_failed,
            messages_dlq = summary.messages_dlq,
            processing_latency_ms_avg = summary.processing_latency_ms_avg,
            consumer_lag = ?summary.consumer_lag,
            "consumer shutdown summary"
        );

        Ok(())
    }

    fn commit_on_rebalance_request(&self) -> Result<()> {
        if !self
            .rebalance_commit_requested
            .swap(false, Ordering::SeqCst)
        {
            return Ok(());
        }

        if self.offset_tracker.strategy() != CommitStrategy::Manual {
            return Ok(());
        }

        let committed = self
            .offset_tracker
            .commit_pending(&self.consumer, CommitMode::Sync)?;

        if committed > 0 {
            tracing::info!(
                committed_partitions = committed,
                "committed pending offsets around rebalance"
            );
        }

        Ok(())
    }

    async fn acquire_handler_permit(&self) -> BackpressurePermit {
        match self.backpressure.acquire().await {
            Ok(permit) => {
                self.metrics.set_in_flight(self.backpressure.in_flight());
                permit
            }
            Err(err) => {
                panic!("handler semaphore closed unexpectedly: {err}");
            }
        }
    }

    fn spawn_message_task(
        &self,
        tasks: &mut JoinSet<TaskResult>,
        message: Message,
        permit: BackpressurePermit,
    ) {
        let handler = Arc::clone(&self.handler);
        let dlq = self.dlq.clone();
        let max_retries = self.config.consumer.max_retries;
        let retry_backoff_ms = self.config.consumer.retry_backoff_ms;

        tasks.spawn(async move {
            let _permit = permit;
            process_message(handler, dlq, message, max_retries, retry_backoff_ms).await
        });
    }

    fn handle_task_join(
        &self,
        joined: Option<std::result::Result<TaskResult, tokio::task::JoinError>>,
    ) -> Result<()> {
        let Some(joined) = joined else {
            return Ok(());
        };

        self.metrics.set_in_flight(self.backpressure.in_flight());

        match joined {
            Ok(result) => {
                self.metrics.record_latency(result.latency);

                if result.failed {
                    self.metrics.inc_failed();
                    if let Some(error_message) = &result.error_message {
                        tracing::error!(
                            topic = %result.topic,
                            partition = result.partition,
                            offset = result.offset,
                            error = %error_message,
                            "message handling failed"
                        );
                    }
                }
                if result.dlqed {
                    self.metrics.inc_dlq();
                }
                if result.ack {
                    self.metrics.inc_processed();
                    self.offset_tracker.track_success(
                        &result.topic,
                        result.partition,
                        result.offset,
                    );
                    self.commit_if_threshold_reached()?;
                }
            }
            Err(join_err) => {
                tracing::error!(error = %join_err, "message task panicked or was cancelled");
                self.metrics.inc_failed();
            }
        }

        Ok(())
    }

    async fn drain_in_flight(
        &self,
        tasks: &mut JoinSet<TaskResult>,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;

        while !tasks.is_empty() {
            let now = Instant::now();
            if now >= deadline {
                tracing::warn!(
                    timeout_secs = timeout.as_secs(),
                    remaining_tasks = tasks.len(),
                    "shutdown timeout reached, aborting remaining handler tasks"
                );
                tasks.abort_all();
                break;
            }

            let remaining = deadline - now;
            match tokio::time::timeout(remaining, tasks.join_next()).await {
                Ok(joined) => {
                    self.handle_task_join(joined)?;
                }
                Err(_) => {
                    tracing::warn!("timed out waiting for in-flight handlers");
                    tasks.abort_all();
                    break;
                }
            }
        }

        while let Some(joined) = tasks.join_next().await {
            self.handle_task_join(Some(joined))?;
        }

        Ok(())
    }

    fn commit_if_threshold_reached(&self) -> Result<()> {
        if self.offset_tracker.strategy() != CommitStrategy::Manual {
            return Ok(());
        }

        if !self.offset_tracker.should_commit() {
            return Ok(());
        }

        let committed = self
            .offset_tracker
            .commit_pending(&self.consumer, CommitMode::Async)?;

        if committed > 0 {
            tracing::debug!(
                committed_partitions = committed,
                "committed batched offsets"
            );
        }

        Ok(())
    }

    fn commit_final_offsets(&self) -> Result<()> {
        if self.offset_tracker.strategy() != CommitStrategy::Manual {
            return Ok(());
        }

        let committed = self
            .offset_tracker
            .commit_pending(&self.consumer, CommitMode::Sync)?;

        if committed > 0 {
            tracing::info!(committed_partitions = committed, "committed final offsets");
        }

        Ok(())
    }

    fn flush_dlq(&self) -> Result<()> {
        if let Some(dlq) = &self.dlq {
            dlq.flush()?;
        }
        Ok(())
    }

    fn handle_poll_error(&self, err: RdKafkaError) {
        let kafka_error = KafkaError::Poll(err);
        tracing::error!(error = %kafka_error, "poll error");
    }
}

async fn process_message<H>(
    handler: Arc<H>,
    dlq: Option<DlqProducer>,
    message: Message,
    max_retries: u32,
    retry_backoff_ms: u64,
) -> TaskResult
where
    H: MessageHandler,
{
    let started_at = Instant::now();
    let topic = message.topic.clone();
    let partition = message.partition;
    let offset = message.offset;

    let mut retry_count = 0_u32;
    let mut backoff = Duration::from_millis(retry_backoff_ms.max(1));

    loop {
        match handler.handle(message.clone()).await {
            Ok(()) => {
                return TaskResult {
                    topic,
                    partition,
                    offset,
                    ack: true,
                    failed: false,
                    dlqed: false,
                    latency: started_at.elapsed(),
                    error_message: None,
                };
            }
            Err(err) => {
                let error_text = err.to_string();

                if retry_count < max_retries {
                    retry_count += 1;
                    tracing::warn!(
                        topic = %message.topic,
                        partition = message.partition,
                        offset = message.offset,
                        retry = retry_count,
                        max_retries = max_retries,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %error_text,
                        "handler error; retrying"
                    );

                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, Duration::from_secs(60));
                    continue;
                }

                if let Some(dlq_producer) = &dlq {
                    match dlq_producer.send(&message, &error_text, retry_count).await {
                        Ok(()) => {
                            tracing::warn!(
                                topic = %message.topic,
                                partition = message.partition,
                                offset = message.offset,
                                retries = retry_count,
                                "message moved to DLQ"
                            );

                            return TaskResult {
                                topic,
                                partition,
                                offset,
                                ack: true,
                                failed: true,
                                dlqed: true,
                                latency: started_at.elapsed(),
                                error_message: Some(error_text),
                            };
                        }
                        Err(dlq_error) => {
                            let combined_error = format!(
                                "handler failed after retries: {}; dlq delivery also failed: {}",
                                error_text, dlq_error
                            );

                            return TaskResult {
                                topic,
                                partition,
                                offset,
                                ack: false,
                                failed: true,
                                dlqed: false,
                                latency: started_at.elapsed(),
                                error_message: Some(combined_error),
                            };
                        }
                    }
                }

                tracing::warn!(
                    topic = %message.topic,
                    partition = message.partition,
                    offset = message.offset,
                    "max retries exhausted and DLQ disabled; skipping message"
                );

                return TaskResult {
                    topic,
                    partition,
                    offset,
                    ack: true,
                    failed: true,
                    dlqed: false,
                    latency: started_at.elapsed(),
                    error_message: Some(error_text),
                };
            }
        }
    }
}
