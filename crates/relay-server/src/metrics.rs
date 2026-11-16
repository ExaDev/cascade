//! Lightweight relay-server metrics.
//!
//! Counters are stored as atomic integers and exposed through a plain-text
//! `Prometheus`-compatible endpoint when the `metrics` feature is enabled.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

/// Shared counter handle.
#[derive(Debug, Default)]
pub struct Counters {
    /// Currently active (paired) sessions.
    pub sessions_active: AtomicI64,
    /// Total number of sessions that successfully paired since startup.
    pub sessions_paired_total: AtomicU64,
    /// Total number of sessions reaped due to peer-timeout.
    pub sessions_timed_out_total: AtomicU64,
    /// Total number of sessions rejected because the registry was full.
    pub sessions_rejected_total: AtomicU64,
    /// Total number of handshake failures (bad HMAC, malformed frame, etc).
    pub auth_failures_total: AtomicU64,
    /// Cumulative bytes ferried across all paired sessions.
    pub bytes_relayed_total: AtomicU64,
    /// Client connection tasks currently open, in any state (handshaking,
    /// parked or paired). A paired session holds one: the second peer's stream
    /// is handed to the first peer's task. A draining relay exits once this
    /// reaches zero.
    pub connections_open: AtomicI64,
    /// Set once the relay has stopped accepting connections and is waiting
    /// for the open ones to finish. `/health` reports it as unhealthy so a
    /// health gate never counts a draining instance as ready.
    pub draining: AtomicBool,
}

impl Counters {
    /// Construct a fresh counter set with all values at zero.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Render the counters as text in `Prometheus` exposition format.
    #[must_use]
    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "# HELP cascade_relay_sessions_active Currently paired sessions."
        );
        let _ = writeln!(out, "# TYPE cascade_relay_sessions_active gauge");
        let _ = writeln!(
            out,
            "cascade_relay_sessions_active {}",
            self.sessions_active.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP cascade_relay_sessions_paired_total Sessions that successfully paired."
        );
        let _ = writeln!(out, "# TYPE cascade_relay_sessions_paired_total counter");
        let _ = writeln!(
            out,
            "cascade_relay_sessions_paired_total {}",
            self.sessions_paired_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP cascade_relay_sessions_timed_out_total Parked sessions reaped due to timeout."
        );
        let _ = writeln!(out, "# TYPE cascade_relay_sessions_timed_out_total counter");
        let _ = writeln!(
            out,
            "cascade_relay_sessions_timed_out_total {}",
            self.sessions_timed_out_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP cascade_relay_sessions_rejected_total Sessions rejected because the registry was full."
        );
        let _ = writeln!(out, "# TYPE cascade_relay_sessions_rejected_total counter");
        let _ = writeln!(
            out,
            "cascade_relay_sessions_rejected_total {}",
            self.sessions_rejected_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP cascade_relay_auth_failures_total Handshake verification failures."
        );
        let _ = writeln!(out, "# TYPE cascade_relay_auth_failures_total counter");
        let _ = writeln!(
            out,
            "cascade_relay_auth_failures_total {}",
            self.auth_failures_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP cascade_relay_bytes_relayed_total Total bytes shuttled between paired peers."
        );
        let _ = writeln!(out, "# TYPE cascade_relay_bytes_relayed_total counter");
        let _ = writeln!(
            out,
            "cascade_relay_bytes_relayed_total {}",
            self.bytes_relayed_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP cascade_relay_connections_open Client connection tasks currently open."
        );
        let _ = writeln!(out, "# TYPE cascade_relay_connections_open gauge");
        let _ = writeln!(
            out,
            "cascade_relay_connections_open {}",
            self.connections_open.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP cascade_relay_draining 1 once the relay has stopped accepting connections."
        );
        let _ = writeln!(out, "# TYPE cascade_relay_draining gauge");
        let _ = writeln!(
            out,
            "cascade_relay_draining {}",
            u8::from(self.draining.load(Ordering::Relaxed))
        );

        out
    }
}

#[cfg(feature = "metrics")]
mod axum_endpoint {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use anyhow::{Context, Result};
    use axum::Router;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::http::header::CONTENT_TYPE;
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;
    use std::sync::atomic::Ordering;

    use tokio::net::TcpListener;

    use super::Counters;

    /// Bind a small `HTTP` server on `bind` that exposes `/metrics` and `/health`.
    ///
    /// `/metrics` serves the Prometheus text exposition for the relay counters.
    /// `/health` is an unauthenticated liveness endpoint that returns `200 OK`
    /// with the body `ok` once the relay is accepting connections. Its only
    /// job is to let a load balancer or container orchestrator determine that
    /// the process is up; it carries no sensitive information.
    ///
    /// Returns the actual bound address (useful when binding to port 0) and
    /// a handle that keeps the server alive until dropped.
    pub async fn serve_metrics(
        bind: SocketAddr,
        counters: Arc<Counters>,
    ) -> Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
        let listener = TcpListener::bind(bind)
            .await
            .with_context(|| format!("binding metrics endpoint to {bind}"))?;
        let local = listener
            .local_addr()
            .context("reading local address for bound metrics listener")?;

        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/health", get(health_handler))
            .with_state(counters);

        let join = tokio::spawn(async move {
            if let Err(err) = axum::serve(listener, app).await {
                tracing::error!(error = %err, "metrics server exited with error");
            }
        });
        Ok((local, join))
    }

    async fn metrics_handler(State(counters): State<Arc<Counters>>) -> Response {
        let body = counters.render_prometheus();
        ([(CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response()
    }

    /// Unauthenticated readiness probe. Returns `200 OK` with the body `ok`
    /// while the relay accepts connections, and `503` with the body
    /// `draining` once it has stopped accepting them and is waiting for open
    /// connections to finish, so an update's health gate never mistakes an
    /// outgoing instance for a ready one. A failure to reach the endpoint
    /// means the process is down.
    pub(super) async fn health_handler(State(counters): State<Arc<Counters>>) -> Response {
        if counters.draining.load(Ordering::Relaxed) {
            (StatusCode::SERVICE_UNAVAILABLE, "draining").into_response()
        } else {
            (StatusCode::OK, "ok").into_response()
        }
    }
}

#[cfg(feature = "metrics")]
pub use axum_endpoint::serve_metrics;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn rendered_metrics_contain_each_counter_name() {
        let counters = Counters::new();
        counters.sessions_paired_total.store(7, Ordering::Relaxed);
        counters.bytes_relayed_total.store(1024, Ordering::Relaxed);
        let body = counters.render_prometheus();

        for needle in [
            "cascade_relay_sessions_active",
            "cascade_relay_sessions_paired_total 7",
            "cascade_relay_sessions_timed_out_total",
            "cascade_relay_sessions_rejected_total",
            "cascade_relay_auth_failures_total",
            "cascade_relay_bytes_relayed_total 1024",
        ] {
            assert!(body.contains(needle), "missing {needle} in {body}");
        }
    }

    /// The `/health` handler returns `200 OK` with the body `ok` while the
    /// relay accepts connections. It is called directly so the test needs no
    /// network round-trip.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn health_handler_returns_200_ok() {
        use axum::body::to_bytes;
        use axum::http::StatusCode;

        let counters = Counters::new();
        let response = axum_endpoint::health_handler(axum::extract::State(counters)).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), 16).await.unwrap();
        assert_eq!(body.as_ref(), b"ok");
    }

    /// Once draining starts, `/health` turns to `503` so a health gate never
    /// treats the outgoing instance as ready.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn health_handler_reports_draining_as_unavailable() {
        use axum::body::to_bytes;
        use axum::http::StatusCode;

        let counters = Counters::new();
        counters.draining.store(true, Ordering::Relaxed);
        let response = axum_endpoint::health_handler(axum::extract::State(counters)).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = to_bytes(response.into_body(), 16).await.unwrap();
        assert_eq!(body.as_ref(), b"draining");
    }
}
