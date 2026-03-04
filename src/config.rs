use std::{fs, path::Path};

use serde::Deserialize;

use crate::error::ConfigError;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
    pub consumer: ConsumerConfig,
    pub dlq: DlqConfig,
    pub health: HealthConfig,
}

impl AppConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path_ref = path.as_ref();
        let content = fs::read_to_string(path_ref).map_err(|source| ConfigError::ReadFile {
            path: path_ref.display().to_string(),
            source,
        })?;

        Self::from_toml_str(&content)
    }

    pub fn from_toml_str(raw: &str) -> Result<Self, ConfigError> {
        let cfg: AppConfig = toml::from_str(raw)?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.kafka.topics.is_empty() {
            return Err(ConfigError::Validation(
                "kafka.topics must include at least one topic".to_string(),
            ));
        }
        if self.consumer.concurrency == 0 {
            return Err(ConfigError::Validation(
                "consumer.concurrency must be greater than 0".to_string(),
            ));
        }
        if self.consumer.manual_commit_batch_size == 0 {
            return Err(ConfigError::Validation(
                "consumer.manual_commit_batch_size must be greater than 0".to_string(),
            ));
        }
        if self.health.max_poll_gap_secs == 0 {
            return Err(ConfigError::Validation(
                "health.max_poll_gap_secs must be greater than 0".to_string(),
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
    pub topics: Vec<String>,
    pub auto_offset_reset: AutoOffsetReset,
    pub session_timeout_ms: u64,
    pub max_poll_interval_ms: u64,
    #[serde(default = "KafkaConfig::default_statistics_interval_ms")]
    pub statistics_interval_ms: u64,
}

impl KafkaConfig {
    const fn default_statistics_interval_ms() -> u64 {
        30_000
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AutoOffsetReset {
    Earliest,
    Latest,
}

impl AutoOffsetReset {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsumerConfig {
    pub concurrency: usize,
    pub commit_strategy: CommitStrategy,
    #[serde(default = "ConsumerConfig::default_auto_commit_interval_ms")]
    pub auto_commit_interval_ms: u64,
    #[serde(default = "ConsumerConfig::default_manual_commit_batch_size")]
    pub manual_commit_batch_size: usize,
    #[serde(default = "ConsumerConfig::default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "ConsumerConfig::default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default = "ConsumerConfig::default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
    #[serde(default = "ConsumerConfig::default_infrastructure_retry_backoff_ms")]
    pub infrastructure_retry_backoff_ms: u64,
}

impl ConsumerConfig {
    const fn default_auto_commit_interval_ms() -> u64 {
        5_000
    }

    const fn default_manual_commit_batch_size() -> usize {
        1
    }

    const fn default_max_retries() -> u32 {
        3
    }

    const fn default_retry_backoff_ms() -> u64 {
        1_000
    }

    const fn default_shutdown_timeout_secs() -> u64 {
        30
    }

    const fn default_infrastructure_retry_backoff_ms() -> u64 {
        2_000
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CommitStrategy {
    Auto,
    Manual,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DlqConfig {
    pub enabled: bool,
    pub topic: String,
    #[serde(default = "DlqConfig::default_include_metadata")]
    pub include_metadata: bool,
    #[serde(default = "DlqConfig::default_flush_timeout_ms")]
    pub flush_timeout_ms: u64,
}

impl DlqConfig {
    const fn default_include_metadata() -> bool {
        true
    }

    const fn default_flush_timeout_ms() -> u64 {
        10_000
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct HealthConfig {
    pub enabled: bool,
    pub bind_address: String,
    #[serde(default = "HealthConfig::default_max_poll_gap_secs")]
    pub max_poll_gap_secs: u64,
    #[serde(default = "HealthConfig::default_max_rebalance_secs")]
    pub max_rebalance_secs: u64,
}

impl HealthConfig {
    const fn default_max_poll_gap_secs() -> u64 {
        60
    }

    const fn default_max_rebalance_secs() -> u64 {
        120
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_toml() -> &'static str {
        r#"
            [kafka]
            brokers = "localhost:9092"
            group_id = "group-a"
            topics = ["events"]
            auto_offset_reset = "earliest"
            session_timeout_ms = 30000
            max_poll_interval_ms = 300000
            statistics_interval_ms = 30000

            [consumer]
            concurrency = 4
            commit_strategy = "manual"
            auto_commit_interval_ms = 5000
            manual_commit_batch_size = 5
            max_retries = 3
            retry_backoff_ms = 1000
            shutdown_timeout_secs = 30
            infrastructure_retry_backoff_ms = 2000

            [dlq]
            enabled = true
            topic = "group-a.dlq"
            include_metadata = true
            flush_timeout_ms = 10000

            [health]
            enabled = true
            bind_address = "0.0.0.0:9090"
            max_poll_gap_secs = 60
            max_rebalance_secs = 120
        "#
    }

    #[test]
    fn parses_valid_config() {
        let cfg = AppConfig::from_toml_str(valid_toml()).expect("valid config should parse");
        assert_eq!(cfg.kafka.group_id, "group-a");
        assert_eq!(cfg.consumer.concurrency, 4);
        assert_eq!(cfg.consumer.commit_strategy, CommitStrategy::Manual);
    }

    #[test]
    fn rejects_empty_topics() {
        let raw = valid_toml().replace("topics = [\"events\"]", "topics = []");
        let err = AppConfig::from_toml_str(&raw).expect_err("empty topics must fail");
        assert!(matches!(err, ConfigError::Validation(_)));
    }

    #[test]
    fn rejects_zero_concurrency() {
        let raw = valid_toml().replace("concurrency = 4", "concurrency = 0");
        let err = AppConfig::from_toml_str(&raw).expect_err("zero concurrency must fail");
        assert!(matches!(err, ConfigError::Validation(_)));
    }
}
