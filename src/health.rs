use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use serde::Serialize;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{config::HealthConfig, metrics::Metrics};

#[derive(Clone)]
pub struct HealthState {
    last_poll_epoch_secs: Arc<AtomicU64>,
    rebalance_started_epoch_secs: Arc<AtomicU64>,
    max_poll_gap_secs: u64,
    max_rebalance_secs: u64,
}

impl HealthState {
    pub fn new(max_poll_gap_secs: u64, max_rebalance_secs: u64) -> Self {
        let now = now_epoch_secs();
        Self {
            last_poll_epoch_secs: Arc::new(AtomicU64::new(now)),
            rebalance_started_epoch_secs: Arc::new(AtomicU64::new(0)),
            max_poll_gap_secs,
            max_rebalance_secs,
        }
    }

    pub fn record_poll(&self) {
        self.last_poll_epoch_secs
            .store(now_epoch_secs(), Ordering::Relaxed);
    }

    pub fn rebalance_started(&self) {
        self.rebalance_started_epoch_secs
            .store(now_epoch_secs(), Ordering::Relaxed);
    }

    pub fn rebalance_finished(&self) {
        self.rebalance_started_epoch_secs
            .store(0, Ordering::Relaxed);
    }

    pub fn last_poll_secs_ago(&self) -> u64 {
        now_epoch_secs().saturating_sub(self.last_poll_epoch_secs.load(Ordering::Relaxed))
    }

    fn unhealthy_reason(&self) -> Option<String> {
        let last_poll_gap = self.last_poll_secs_ago();
        if last_poll_gap > self.max_poll_gap_secs {
            return Some(format!(
                "no poll in {}s (threshold {}s)",
                last_poll_gap, self.max_poll_gap_secs
            ));
        }

        let rebalance_started = self.rebalance_started_epoch_secs.load(Ordering::Relaxed);
        if rebalance_started > 0 {
            let rebalance_secs = now_epoch_secs().saturating_sub(rebalance_started);
            if rebalance_secs > self.max_rebalance_secs {
                return Some(format!(
                    "consumer rebalancing for {}s (threshold {}s)",
                    rebalance_secs, self.max_rebalance_secs
                ));
            }
        }

        None
    }
}

#[derive(Clone)]
struct HealthAppState {
    health: HealthState,
    metrics: Metrics,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: String,
    reason: Option<String>,
    last_poll_secs_ago: u64,
    in_flight: u64,
    messages_processed: u64,
}

pub fn spawn_health_server(
    config: &HealthConfig,
    health: HealthState,
    metrics: Metrics,
    shutdown: CancellationToken,
) -> Option<JoinHandle<()>> {
    if !config.enabled {
        return None;
    }

    let bind_address = config.bind_address.clone();

    Some(tokio::spawn(async move {
        let addr: SocketAddr = match bind_address.parse() {
            Ok(addr) => addr,
            Err(err) => {
                tracing::error!(address = %bind_address, error = %err, "invalid health bind address");
                return;
            }
        };

        let app_state = HealthAppState { health, metrics };
        let app = Router::new()
            .route("/health", get(health_handler))
            .with_state(app_state);

        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => listener,
            Err(err) => {
                tracing::error!(address = %addr, error = %err, "failed to bind health server");
                return;
            }
        };

        tracing::info!(address = %addr, "health server started");

        let server = axum::serve(listener, app).with_graceful_shutdown(shutdown.cancelled_owned());

        if let Err(err) = server.await {
            tracing::error!(error = %err, "health server stopped with error");
        }
    }))
}

async fn health_handler(State(state): State<HealthAppState>) -> impl IntoResponse {
    let snapshot = state.metrics.snapshot();
    let reason = state.health.unhealthy_reason();
    let body = HealthResponse {
        status: if reason.is_some() {
            "unhealthy".to_string()
        } else {
            "ok".to_string()
        },
        reason,
        last_poll_secs_ago: state.health.last_poll_secs_ago(),
        in_flight: snapshot.in_flight_handlers,
        messages_processed: snapshot.messages_processed,
    };

    let code = if body.status == "ok" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (code, Json(body))
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
