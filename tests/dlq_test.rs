use std::collections::HashMap;

use kafka_rs_consumer::{config::AppConfig, dlq::DlqProducer, handler::Message};
use rdkafka::message::Headers;

#[test]
fn dlq_metadata_headers_include_required_fields() {
    let message = Message {
        key: Some(b"k".to_vec()),
        payload: Some(b"payload".to_vec()),
        topic: "orders".to_string(),
        partition: 2,
        offset: 17,
        timestamp: Some(1700000000),
        headers: HashMap::new(),
    };

    let headers = DlqProducer::build_metadata_headers(&message, "validation failed", 3);

    let mut header_map = HashMap::new();
    for header in headers.iter() {
        header_map.insert(
            header.key.to_string(),
            header
                .value
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .unwrap_or_default(),
        );
    }

    assert_eq!(
        header_map.get("original_topic"),
        Some(&"orders".to_string())
    );
    assert_eq!(header_map.get("original_partition"), Some(&"2".to_string()));
    assert_eq!(header_map.get("original_offset"), Some(&"17".to_string()));
    assert_eq!(
        header_map.get("error_message"),
        Some(&"validation failed".to_string())
    );
    assert_eq!(header_map.get("retry_count"), Some(&"3".to_string()));
    assert!(header_map.contains_key("failed_at"));
}

#[test]
fn dlq_is_not_created_when_disabled() {
    let config = AppConfig::from_toml_str(
        r#"
            [kafka]
            brokers = "localhost:9092"
            group_id = "group-a"
            topics = ["events"]
            auto_offset_reset = "earliest"
            session_timeout_ms = 30000
            max_poll_interval_ms = 300000

            [consumer]
            concurrency = 2
            commit_strategy = "auto"
            auto_commit_interval_ms = 5000
            manual_commit_batch_size = 1
            max_retries = 3
            retry_backoff_ms = 1000
            shutdown_timeout_secs = 30
            infrastructure_retry_backoff_ms = 2000

            [dlq]
            enabled = false
            topic = "group-a.dlq"
            include_metadata = true
            flush_timeout_ms = 10000

            [health]
            enabled = false
            bind_address = "127.0.0.1:9090"
            max_poll_gap_secs = 60
            max_rebalance_secs = 120
        "#,
    )
    .expect("config should parse");

    let dlq = DlqProducer::from_config(&config).expect("construction should succeed");
    assert!(dlq.is_none());
}
