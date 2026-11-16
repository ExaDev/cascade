#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::string_slice
    )
)]
//! `cascade-relay` binary entry point.
//!
//! Parses CLI flags via clap, configures `tracing` for structured logging,
//! and hands the resulting [`RelayConfig`] to [`cascade_relay_server::run_relay`].

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use cascade_relay_server::RelayConfig;
use cascade_relay_server::server::{RelayHandle, spawn};
use clap::Parser;
use tracing_subscriber::EnvFilter;

/// `cascade-relay` — opaque byte-pipe relay for cascade peers behind NATs.
///
/// The relay shuttles already-encrypted bytes between two peers that have
/// authenticated with a shared HMAC secret. The relay never inspects the
/// payload — peers establish their own end-to-end TLS over the tunnel.
#[derive(Debug, Parser)]
#[command(name = "cascade-relay", version, about)]
struct Cli {
    /// Address the byte-pipe listener binds to.
    #[arg(long, default_value = "0.0.0.0:9999")]
    bind: SocketAddr,

    /// 64-character hexadecimal shared secret (32 bytes after decoding).
    /// Generate with `openssl rand -hex 32`. Both peers and the relay must
    /// hold the same secret.
    ///
    /// Can also be supplied via the `CASCADE_RELAY_SHARED_SECRET` environment
    /// variable to avoid the secret appearing on the process command line.
    #[arg(long, env = "CASCADE_RELAY_SHARED_SECRET")]
    shared_secret: String,

    /// How long the first peer of a session may wait for its partner
    /// before the server times the session out, in seconds.
    #[arg(long, default_value_t = 60)]
    session_timeout_seconds: u32,

    /// Maximum number of in-flight sessions (paired or parked). New
    /// sessions are rejected when full.
    #[arg(long, default_value_t = 1024)]
    max_sessions: u32,

    /// Optional address for a `/metrics` HTTP endpoint. When unset the
    /// metrics endpoint is disabled (counters are still kept in process
    /// memory but not exposed).
    #[arg(long)]
    metrics_bind: Option<SocketAddr>,

    /// Optional address for the announce/lookup directory HTTP endpoint.
    /// When unset the relay serves only the byte-pipe. Requires the
    /// `announce` build feature; without it a set value logs a warning and
    /// the endpoint stays disabled.
    #[arg(long)]
    announce_bind: Option<SocketAddr>,

    /// How long a relay told to stop (SIGTERM or Ctrl-C) keeps serving its
    /// open connections before exiting, in seconds. It stops accepting new
    /// connections at once; `/health` reports `503` while it drains.
    #[arg(long, default_value_t = 300)]
    drain_timeout_seconds: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Default to INFO; honour `RUST_LOG` when set. Structured logs go to
    // stderr so the relay can be combined with other tooling that uses
    // stdout for protocol data.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let shared_secret =
        RelayConfig::parse_shared_secret(&cli.shared_secret).context("invalid --shared-secret")?;

    let config = RelayConfig {
        bind: cli.bind,
        shared_secret,
        session_timeout: Duration::from_secs(u64::from(cli.session_timeout_seconds)),
        max_sessions: cli.max_sessions,
        metrics_bind: cli.metrics_bind,
        announce_bind: cli.announce_bind,
    };

    let handle = start(config).await?;
    wait_for_stop_signal().await?;
    tracing::info!("stop signal received; draining open connections");
    let remaining = handle
        .drain(Duration::from_secs(u64::from(cli.drain_timeout_seconds)))
        .await?;
    if remaining > 0 {
        tracing::warn!(
            remaining,
            "drain timeout reached with connections still open"
        );
    } else {
        tracing::info!("drained cleanly");
    }
    Ok(())
}

/// Start the relay on a socket inherited from systemd when one is passed
/// (`LISTEN_FDS`), otherwise on `--bind`. An inherited socket lets a second
/// instance accept on the same listener during an update, so no connection
/// is refused while one instance stops and the other starts.
async fn start(config: RelayConfig) -> Result<RelayHandle> {
    #[cfg(unix)]
    {
        let mut inherited = listenfd::ListenFd::from_env();
        if let Some(std_listener) = inherited
            .take_tcp_listener(0)
            .context("reading the inherited listening socket")?
        {
            tracing::info!("serving on the listening socket inherited from systemd");
            return cascade_relay_server::server::spawn_with_listener(config, std_listener).await;
        }
    }
    spawn(config).await
}

/// Resolve on SIGTERM (unix) or Ctrl-C.
async fn wait_for_stop_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).context("installing SIGTERM handler")?;
        tokio::select! {
            _ = term.recv() => Ok(()),
            result = tokio::signal::ctrl_c() => result.context("waiting for Ctrl-C"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("waiting for Ctrl-C")
    }
}
