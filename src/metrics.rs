use std::{
    sync::{
        atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use serde::Serialize;

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    messages_processed: AtomicU64,
    messages_failed: AtomicU64,
    messages_dlq: AtomicU64,
    latency_samples: AtomicU64,
    latency_total_ms: AtomicU64,
    consumer_lag: AtomicI64,
    in_flight_handlers: AtomicUsize,
    started_at: Instant,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                messages_processed: AtomicU64::new(0),
                messages_failed: AtomicU64::new(0),
                messages_dlq: AtomicU64::new(0),
                latency_samples: AtomicU64::new(0),
                latency_total_ms: AtomicU64::new(0),
                consumer_lag: AtomicI64::new(-1),
                in_flight_handlers: AtomicUsize::new(0),
                started_at: Instant::now(),
            }),
        }
    }

    pub fn inc_processed(&self) {
        self.inner
            .messages_processed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_failed(&self) {
        self.inner.messages_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_dlq(&self) {
        self.inner.messages_dlq.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_latency(&self, latency: Duration) {
        self.inner.latency_samples.fetch_add(1, Ordering::Relaxed);
        self.inner
            .latency_total_ms
            .fetch_add(latency.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn set_in_flight(&self, in_flight: usize) {
        self.inner
            .in_flight_handlers
            .store(in_flight, Ordering::Relaxed);
    }

    pub fn set_consumer_lag(&self, lag: Option<i64>) {
        let value = lag.unwrap_or(-1);
        self.inner.consumer_lag.store(value, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let samples = self.inner.latency_samples.load(Ordering::Relaxed);
        let total = self.inner.latency_total_ms.load(Ordering::Relaxed);
        let avg_latency = if samples == 0 {
            0.0
        } else {
            total as f64 / samples as f64
        };

        let lag_raw = self.inner.consumer_lag.load(Ordering::Relaxed);
        let consumer_lag = if lag_raw < 0 {
            None
        } else {
            Some(lag_raw as u64)
        };

        MetricsSnapshot {
            messages_processed: self.inner.messages_processed.load(Ordering::Relaxed),
            messages_failed: self.inner.messages_failed.load(Ordering::Relaxed),
            messages_dlq: self.inner.messages_dlq.load(Ordering::Relaxed),
            processing_latency_ms_avg: avg_latency,
            consumer_lag,
            in_flight_handlers: self.inner.in_flight_handlers.load(Ordering::Relaxed) as u64,
            uptime_secs: self.inner.started_at.elapsed().as_secs(),
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub messages_processed: u64,
    pub messages_failed: u64,
    pub messages_dlq: u64,
    pub processing_latency_ms_avg: f64,
    pub consumer_lag: Option<u64>,
    pub in_flight_handlers: u64,
    pub uptime_secs: u64,
}
