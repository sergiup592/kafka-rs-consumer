use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use futures_util::StreamExt;
use kafka_rs_consumer::{
    handler::{Message, MessageHandler},
    AppConfig, KafkaConsumerFramework,
};
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    message::Message as KafkaMessage,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use thiserror::Error;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Error)]
#[error("test handler failed")]
struct TestHandlerError;

struct CountingHandler {
    processed: Arc<AtomicUsize>,
}

#[async_trait]
impl MessageHandler for CountingHandler {
    type Error = TestHandlerError;

    async fn handle(&self, _message: Message) -> Result<(), Self::Error> {
        self.processed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct FailingHandler {
    attempts: Arc<AtomicUsize>,
}

#[async_trait]
impl MessageHandler for FailingHandler {
    type Error = TestHandlerError;

    async fn handle(&self, _message: Message) -> Result<(), Self::Error> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(TestHandlerError)
    }
}

struct SlowHandler {
    processed: Arc<AtomicUsize>,
    current: Arc<AtomicUsize>,
    max_seen: Arc<AtomicUsize>,
}

#[async_trait]
impl MessageHandler for SlowHandler {
    type Error = TestHandlerError;

    async fn handle(&self, _message: Message) -> Result<(), Self::Error> {
        let running = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        update_max(&self.max_seen, running);

        tokio::time::sleep(Duration::from_millis(150)).await;

        self.current.fetch_sub(1, Ordering::SeqCst);
        self.processed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Kafka at localhost:9092 (run docker-compose up -d)"]
async fn produce_consume_end_to_end() {
    let topic = format!("consumer-test-{}", Uuid::new_v4());
    let group_id = format!("group-{}", Uuid::new_v4());

    let config = test_config(&topic, &group_id, "auto", 4, false, "unused.dlq");

    let processed = Arc::new(AtomicUsize::new(0));
    let framework = KafkaConsumerFramework::new(
        config,
        CountingHandler {
            processed: Arc::clone(&processed),
        },
    )
    .expect("framework should initialize");

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let runtime = tokio::spawn(async move { framework.run(runtime_shutdown).await });

    produce_messages(&topic, 100).await;

    wait_for_counter(&processed, 100, Duration::from_secs(30)).await;

    shutdown.cancel();
    let result = runtime.await.expect("runtime task should join");
    assert!(
        result.is_ok(),
        "framework runtime should succeed: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Kafka at localhost:9092 (run docker-compose up -d)"]
async fn failing_messages_are_sent_to_dlq_after_retries() {
    let topic = format!("consumer-retry-test-{}", Uuid::new_v4());
    let dlq_topic = format!("consumer-retry-test-{}.dlq", Uuid::new_v4());
    let group_id = format!("group-{}", Uuid::new_v4());

    let mut config = test_config(&topic, &group_id, "manual", 2, true, &dlq_topic);
    config.consumer.max_retries = 2;
    config.consumer.retry_backoff_ms = 100;

    let attempts = Arc::new(AtomicUsize::new(0));

    let framework = KafkaConsumerFramework::new(
        config,
        FailingHandler {
            attempts: Arc::clone(&attempts),
        },
    )
    .expect("framework should initialize");

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let runtime = tokio::spawn(async move { framework.run(runtime_shutdown).await });

    produce_messages(&topic, 1).await;

    let message = consume_one_message(&dlq_topic, Duration::from_secs(30)).await;
    assert!(message.is_some(), "expected message in DLQ topic");
    assert!(
        attempts.load(Ordering::SeqCst) >= 3,
        "expected at least initial attempt + retries"
    );

    shutdown.cancel();
    let result = runtime.await.expect("runtime task should join");
    assert!(
        result.is_ok(),
        "framework runtime should succeed: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Kafka at localhost:9092 (run docker-compose up -d)"]
async fn graceful_shutdown_commits_offsets_for_restart() {
    let topic = format!("consumer-shutdown-test-{}", Uuid::new_v4());
    let group_id = format!("group-{}", Uuid::new_v4());

    let mut first_config = test_config(&topic, &group_id, "manual", 2, false, "unused.dlq");
    first_config.consumer.manual_commit_batch_size = 1;

    let first_processed = Arc::new(AtomicUsize::new(0));
    let first_framework = KafkaConsumerFramework::new(
        first_config,
        CountingHandler {
            processed: Arc::clone(&first_processed),
        },
    )
    .expect("framework should initialize");

    let first_shutdown = CancellationToken::new();
    let first_runtime_shutdown = first_shutdown.clone();
    let first_runtime =
        tokio::spawn(async move { first_framework.run(first_runtime_shutdown).await });

    produce_messages(&topic, 20).await;

    wait_for_counter(&first_processed, 20, Duration::from_secs(30)).await;

    first_shutdown.cancel();
    let first_result = first_runtime.await.expect("runtime task should join");
    assert!(
        first_result.is_ok(),
        "first runtime should succeed: {first_result:?}"
    );

    let second_config = test_config(&topic, &group_id, "manual", 2, false, "unused.dlq");
    let second_processed = Arc::new(AtomicUsize::new(0));
    let second_framework = KafkaConsumerFramework::new(
        second_config,
        CountingHandler {
            processed: Arc::clone(&second_processed),
        },
    )
    .expect("framework should initialize");

    let second_shutdown = CancellationToken::new();
    let second_runtime_shutdown = second_shutdown.clone();
    let second_runtime =
        tokio::spawn(async move { second_framework.run(second_runtime_shutdown).await });

    tokio::time::sleep(Duration::from_secs(8)).await;

    second_shutdown.cancel();
    let second_result = second_runtime.await.expect("runtime task should join");
    assert!(
        second_result.is_ok(),
        "second runtime should succeed: {second_result:?}"
    );

    assert_eq!(
        second_processed.load(Ordering::SeqCst),
        0,
        "no messages should be reprocessed after restart"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Kafka at localhost:9092 (run docker-compose up -d)"]
async fn semaphore_limits_handler_parallelism() {
    let topic = format!("consumer-concurrency-test-{}", Uuid::new_v4());
    let group_id = format!("group-{}", Uuid::new_v4());

    let config = test_config(&topic, &group_id, "auto", 3, false, "unused.dlq");

    let processed = Arc::new(AtomicUsize::new(0));
    let current = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));

    let framework = KafkaConsumerFramework::new(
        config,
        SlowHandler {
            processed: Arc::clone(&processed),
            current: Arc::clone(&current),
            max_seen: Arc::clone(&max_seen),
        },
    )
    .expect("framework should initialize");

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let runtime = tokio::spawn(async move { framework.run(runtime_shutdown).await });

    produce_messages(&topic, 25).await;

    wait_for_counter(&processed, 25, Duration::from_secs(40)).await;

    shutdown.cancel();
    let result = runtime.await.expect("runtime task should join");
    assert!(
        result.is_ok(),
        "framework runtime should succeed: {result:?}"
    );

    assert!(
        max_seen.load(Ordering::SeqCst) <= 3,
        "max parallel handlers exceeded configured concurrency"
    );
}

fn test_config(
    topic: &str,
    group_id: &str,
    commit_strategy: &str,
    concurrency: usize,
    dlq_enabled: bool,
    dlq_topic: &str,
) -> AppConfig {
    let raw = format!(
        r#"
            [kafka]
            brokers = "localhost:9092"
            group_id = "{group_id}"
            topics = ["{topic}"]
            auto_offset_reset = "earliest"
            session_timeout_ms = 30000
            max_poll_interval_ms = 300000
            statistics_interval_ms = 5000

            [consumer]
            concurrency = {concurrency}
            commit_strategy = "{commit_strategy}"
            auto_commit_interval_ms = 5000
            manual_commit_batch_size = 1
            max_retries = 3
            retry_backoff_ms = 200
            shutdown_timeout_secs = 20
            infrastructure_retry_backoff_ms = 500

            [dlq]
            enabled = {dlq_enabled}
            topic = "{dlq_topic}"
            include_metadata = true
            flush_timeout_ms = 10000

            [health]
            enabled = false
            bind_address = "127.0.0.1:0"
            max_poll_gap_secs = 60
            max_rebalance_secs = 120
        "#
    );

    AppConfig::from_toml_str(&raw).expect("test config should parse")
}

async fn produce_messages(topic: &str, count: usize) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("producer should be created");

    for i in 0..count {
        let payload = format!("{{\"id\":{i},\"event\":\"test\"}}");
        producer
            .send(
                FutureRecord::to(topic)
                    .key(&i.to_string())
                    .payload(&payload),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .expect("message should be produced");
    }
}

async fn consume_one_message(topic: &str, timeout: Duration) -> Option<String> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", format!("dlq-checker-{}", Uuid::new_v4()))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "true")
        .create()
        .expect("consumer should be created");

    consumer
        .subscribe(&[topic])
        .expect("subscribe should succeed");

    let mut stream = consumer.stream();
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        if let Ok(Some(Ok(message))) =
            tokio::time::timeout(Duration::from_secs(1), stream.next()).await
        {
            return message
                .payload()
                .map(|bytes| String::from_utf8_lossy(bytes).to_string());
        }
    }

    None
}

async fn wait_for_counter(counter: &Arc<AtomicUsize>, expected: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if counter.load(Ordering::SeqCst) >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    panic!(
        "timed out waiting for counter: expected at least {}, got {}",
        expected,
        counter.load(Ordering::SeqCst)
    );
}

fn update_max(max_seen: &Arc<AtomicUsize>, candidate: usize) {
    let mut current = max_seen.load(Ordering::SeqCst);
    while candidate > current {
        match max_seen.compare_exchange(current, candidate, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}
