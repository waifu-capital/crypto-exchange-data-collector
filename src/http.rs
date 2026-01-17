use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use prometheus::{Encoder, TextEncoder};
use serde_json::json;
use tracing::{error, info};

use crate::metrics::{APP_START_TIMESTAMP, MESSAGES_DROPPED};
use crate::models::ConnectionState;

/// Default port for the metrics HTTP server
const DEFAULT_METRICS_PORT: u16 = 9090;

/// Handler for /metrics endpoint - returns Prometheus format
async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();

    match encoder.encode(&metric_families, &mut buffer) {
        Ok(_) => {
            let output = String::from_utf8(buffer).unwrap_or_default();
            (
                StatusCode::OK,
                [("content-type", "text/plain; charset=utf-8")],
                output,
            )
        }
        Err(e) => {
            error!(error = %e, "Failed to encode metrics");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [("content-type", "text/plain; charset=utf-8")],
                format!("Failed to encode metrics: {}", e),
            )
        }
    }
}

/// Handler for /health endpoint - returns JSON health status
async fn health_handler(State(conn_state): State<ConnectionState>) -> impl IntoResponse {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();

    let start_time = APP_START_TIMESTAMP.get();
    let uptime_secs = if start_time > 0.0 {
        now - start_time
    } else {
        0.0
    };

    // Check WebSocket connections from shared state
    let state = conn_state.read().await;
    let connections_total = state.len();
    let connections_up = state.values().filter(|&&v| v).count();
    drop(state);

    let messages_dropped = MESSAGES_DROPPED.get() as u64;

    // Determine overall health status
    let status = if connections_total > 0 && connections_up == connections_total {
        "healthy"
    } else if connections_up > 0 {
        "degraded"
    } else if connections_total == 0 {
        "starting"
    } else {
        "unhealthy"
    };

    let health = json!({
        "status": status,
        "uptime_seconds": uptime_secs as u64,
        "connections": {
            "up": connections_up,
            "total": connections_total
        },
        "messages_dropped": messages_dropped
    });

    let status_code = match status {
        "healthy" | "starting" => StatusCode::OK,
        "degraded" => StatusCode::OK, // Still return 200 for degraded
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };

    (status_code, Json(health))
}

/// Handler for /ready endpoint - simple readiness probe
async fn ready_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

/// Run the HTTP server for metrics and health endpoints
pub async fn run_http_server(port: Option<u16>, conn_state: ConnectionState) {
    let port = port.unwrap_or(DEFAULT_METRICS_PORT);

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .with_state(conn_state);

    let addr = format!("0.0.0.0:{}", port);

    info!(port, "Starting metrics HTTP server");

    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, addr, "Failed to bind metrics HTTP server");
            return;
        }
    };

    if let Err(e) = axum::serve(listener, app).await {
        error!(error = %e, "Metrics HTTP server error");
    }
}
