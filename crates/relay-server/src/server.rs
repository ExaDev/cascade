//! Relay server entry point.
//!
//! Accepts incoming `WebSocket` connections on the configured bind address,
//! extracts the session ID from the URL path (`/join/<session_id>`), runs
//! the `HMAC` handshake, and either parks the client awaiting a peer or
//! pairs it with an already-parked peer and hands both sockets to the
//! byte-pipe.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use http::{Response, StatusCode};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request};
use tracing::{info, warn};

use crate::auth::{HandshakeError, verify_handshake};
use crate::config::{RelayConfig, SHARED_SECRET_LEN};
use crate::metrics::Counters;
use crate::session::{RegisterOutcome, SessionRegistry};

/// Path prefix the relay accepts. Mirrors the existing client in
/// [`cascade-p2p`](../../../p2p/src/relay.rs).
pub const RELAY_JOIN_PATH: &str = "/join/";

/// Handle returned by [`spawn`]. Drop to stop the server.
#[derive(Debug)]
pub struct RelayHandle {
    /// Local address the relay is bound to.
    pub local_addr: SocketAddr,
    /// Optional local address the metrics endpoint is bound to.
    pub metrics_addr: Option<SocketAddr>,
    /// Optional local address the announce endpoint is bound to.
    pub announce_addr: Option<SocketAddr>,
    /// Shared counters — useful for tests asserting against metric values.
    pub counters: Arc<Counters>,
    /// Channel that, when signalled or dropped, instructs the server loop to
    /// stop accepting at the next accept boundary.
    shutdown: mpsc::Sender<()>,
    listener_task: tokio::task::JoinHandle<()>,
    metrics_task: Option<tokio::task::JoinHandle<()>>,
    announce_task: Option<tokio::task::JoinHandle<()>>,
    /// Kept alive so the listener task's reference to it stays valid;
    /// access goes through cloned references inside the listener.
    _registry: SessionRegistry,
}

impl RelayHandle {
    /// Block until the listener task exits. Mainly useful for the binary
    /// entry point — tests drop the handle and rely on `Drop` cleanup.
    pub async fn join(self) -> Result<()> {
        self.listener_task
            .await
            .context("relay listener task panicked")?;
        if let Some(metrics) = self.metrics_task {
            metrics.await.context("metrics task panicked")?;
        }
        if let Some(announce) = self.announce_task {
            announce.await.context("announce task panicked")?;
        }
        Ok(())
    }

    /// Stop accepting connections and wait for the open ones to finish.
    ///
    /// Marks the relay as draining (`/health` turns to `503`), closes the
    /// listener, releases parked peers so they reconnect elsewhere, then waits
    /// until no client connection remains or `timeout` elapses. Returns the
    /// number of connections still open, which is zero for a clean drain.
    /// Dropping the listener only closes this process's copy of the socket, so
    /// with an inherited listener the other instance keeps accepting and no
    /// queued connection is lost.
    pub async fn drain(self, timeout: Duration) -> Result<i64> {
        self.counters.draining.store(true, Ordering::Relaxed);
        // A closed channel means the listener task already stopped.
        let _ = self.shutdown.send(()).await;
        self.listener_task
            .await
            .context("relay listener task panicked")?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let open = self.counters.connections_open.load(Ordering::Relaxed);
            if open <= 0 || tokio::time::Instant::now() >= deadline {
                return Ok(open.max(0));
            }
            tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
        }
    }
}

/// How often a draining relay re-reads its open connection count.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Run the relay until the returned handle is dropped or `join`'d.
pub async fn spawn(config: RelayConfig) -> Result<RelayHandle> {
    let listener = TcpListener::bind(config.bind)
        .await
        .with_context(|| format!("binding relay listener to {}", config.bind))?;
    spawn_serving(config, Incoming::Owned(listener)).await
}

/// Like [`spawn`], serving on an already-bound `listener`.
///
/// `config.bind` is ignored. Used to serve on a socket inherited from systemd,
/// so two relay instances can accept on one socket during an update.
///
/// The listener's open file description is shared with every other instance,
/// and systemd puts it in blocking mode each time it starts one, so its
/// `O_NONBLOCK` flag cannot be relied on. Accepting therefore runs on a
/// dedicated thread that tolerates either mode.
#[cfg(unix)]
pub async fn spawn_with_listener(
    config: RelayConfig,
    listener: std::net::TcpListener,
) -> Result<RelayHandle> {
    let incoming = Incoming::from_shared(listener)?;
    spawn_serving(config, incoming).await
}

async fn spawn_serving(config: RelayConfig, incoming: Incoming) -> Result<RelayHandle> {
    let local_addr = incoming.local_addr()?;

    let registry = SessionRegistry::new();
    let counters = Counters::new();

    let (metrics_addr, metrics_task) = spawn_metrics_endpoint(&config, counters.clone()).await?;
    let (announce_addr, announce_task) = spawn_announce_endpoint(&config).await?;

    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
    let listener_registry = registry.clone();
    let listener_counters = counters.clone();
    let listener_config = config;

    let listener_task = tokio::spawn(async move {
        run_listener(
            incoming,
            listener_config,
            listener_registry,
            listener_counters,
            &mut shutdown_rx,
        )
        .await;
    });

    info!(addr = %local_addr, metrics = ?metrics_addr, announce = ?announce_addr, "cascade-relay listening");

    Ok(RelayHandle {
        local_addr,
        metrics_addr,
        announce_addr,
        counters,
        shutdown: shutdown_tx,
        listener_task,
        metrics_task,
        announce_task,
        _registry: registry,
    })
}

/// Convenience wrapper used by `main` — runs to completion.
pub async fn run_relay(config: RelayConfig) -> Result<()> {
    let handle = spawn(config).await?;
    handle.join().await
}

#[cfg(feature = "metrics")]
async fn spawn_metrics_endpoint(
    config: &RelayConfig,
    counters: Arc<Counters>,
) -> Result<(Option<SocketAddr>, Option<tokio::task::JoinHandle<()>>)> {
    if let Some(bind) = config.metrics_bind {
        let (addr, handle) = crate::metrics::serve_metrics(bind, counters).await?;
        Ok((Some(addr), Some(handle)))
    } else {
        Ok((None, None))
    }
}

// The `async` keyword is required here so this stub has the same signature as
// the `cfg(feature = "metrics")` variant above — the call site awaits the
// return value unconditionally regardless of which variant is selected.
#[cfg(not(feature = "metrics"))]
#[allow(clippy::unused_async)]
async fn spawn_metrics_endpoint(
    config: &RelayConfig,
    _counters: Arc<Counters>,
) -> Result<(Option<SocketAddr>, Option<tokio::task::JoinHandle<()>>)> {
    if config.metrics_bind.is_some() {
        warn!("metrics_bind set but cascade-relay-server was built without the metrics feature");
    }
    Ok((None, None))
}

#[cfg(feature = "announce")]
async fn spawn_announce_endpoint(
    config: &RelayConfig,
) -> Result<(Option<SocketAddr>, Option<tokio::task::JoinHandle<()>>)> {
    if let Some(bind) = config.announce_bind {
        let directory = crate::announce::AnnounceDirectory::new();
        let (addr, handle) =
            crate::announce::serve_announce(bind, directory, config.shared_secret).await?;
        Ok((Some(addr), Some(handle)))
    } else {
        Ok((None, None))
    }
}

// The `async` keyword keeps this stub's signature identical to the
// `cfg(feature = "announce")` variant so the call site awaits the return
// value unconditionally regardless of which variant is selected.
#[cfg(not(feature = "announce"))]
#[allow(clippy::unused_async)]
async fn spawn_announce_endpoint(
    config: &RelayConfig,
) -> Result<(Option<SocketAddr>, Option<tokio::task::JoinHandle<()>>)> {
    if config.announce_bind.is_some() {
        warn!("announce_bind set but cascade-relay-server was built without the announce feature");
    }
    Ok((None, None))
}

async fn run_listener(
    mut incoming: Incoming,
    config: RelayConfig,
    registry: SessionRegistry,
    counters: Arc<Counters>,
    shutdown: &mut mpsc::Receiver<()>,
) {
    let reaper_registry = registry.clone();
    let reaper_counters = counters.clone();
    let reaper_interval = std::cmp::max(Duration::from_millis(250), config.session_timeout / 4);
    let reaper = tokio::spawn(async move {
        loop {
            tokio::time::sleep(reaper_interval).await;
            let reaped = reaper_registry.reap_expired().await;
            if reaped > 0 {
                reaper_counters
                    .sessions_timed_out_total
                    .fetch_add(reaped as u64, Ordering::Relaxed);
            }
        }
    });

    loop {
        tokio::select! {
            _ = shutdown.recv() => {
                info!("relay shutdown signal received");
                break;
            }
            accept = incoming.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        serve_stream(stream, peer, config, &registry, &counters);
                    }
                    Err(err) => {
                        warn!(error = %err, "accept failed");
                    }
                }
            }
        }
    }

    // Connections the acceptor took before it stopped are served, not reset.
    for (stream, peer) in incoming.stop().await {
        serve_stream(stream, peer, config, &registry, &counters);
    }

    registry.shutdown().await;
    reaper.abort();
}

fn serve_stream(
    stream: TcpStream,
    peer: SocketAddr,
    config: RelayConfig,
    registry: &SessionRegistry,
    counters: &Arc<Counters>,
) {
    let registry = registry.clone();
    let counters = counters.clone();
    counters.connections_open.fetch_add(1, Ordering::Relaxed);
    tokio::spawn(async move {
        let _open = OpenConnection(counters.clone());
        if let Err(err) = handle_connection(stream, peer, &config, &registry, &counters).await {
            warn!(error = %err, %peer, "relay connection ended with error");
        }
    });
}

/// Where accepted connections come from.
enum Incoming {
    /// A listener this process bound itself, accepted on the runtime.
    Owned(TcpListener),
    /// A listener shared with other processes, accepted on a dedicated thread.
    #[cfg(unix)]
    Shared(SharedAcceptor),
}

/// How long the acceptor thread blocks in `accept` before it re-checks
/// whether it was told to stop. Bounds how long a draining instance keeps
/// taking connections after the stop request.
#[cfg(unix)]
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[cfg(unix)]
struct SharedAcceptor {
    local_addr: SocketAddr,
    streams: mpsc::UnboundedReceiver<std::io::Result<(std::net::TcpStream, SocketAddr)>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Incoming {
    /// Take a listener shared with other instances.
    ///
    /// The socket is put in blocking mode with a receive timeout, which also
    /// bounds `accept` on Linux, so `accept` returns control every
    /// [`ACCEPT_POLL_INTERVAL`] whoever else touches the shared flags. A
    /// blocking `accept` that loses a race for a connection to the other
    /// instance simply waits out the timeout.
    #[cfg(unix)]
    fn from_shared(listener: std::net::TcpListener) -> Result<Self> {
        use nix::sys::socket::{setsockopt, sockopt::ReceiveTimeout};
        use nix::sys::time::{TimeVal, TimeValLike};
        use std::sync::atomic::AtomicBool;

        let local_addr = listener
            .local_addr()
            .context("reading local address for the shared relay listener")?;
        listener
            .set_nonblocking(false)
            .context("putting the shared listener in blocking mode")?;
        let timeout = TimeVal::microseconds(
            i64::try_from(ACCEPT_POLL_INTERVAL.as_micros()).context("accept interval")?,
        );
        setsockopt(&listener, ReceiveTimeout, &timeout)
            .context("setting the shared listener's accept timeout")?;

        let (tx, streams) = mpsc::unbounded_channel();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let thread = std::thread::Builder::new()
            .name("relay-accept".to_owned())
            .spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok(accepted) => {
                            if tx.send(Ok(accepted)).is_err() {
                                break;
                            }
                        }
                        Err(err)
                            if matches!(
                                err.kind(),
                                std::io::ErrorKind::WouldBlock
                                    | std::io::ErrorKind::TimedOut
                                    | std::io::ErrorKind::Interrupted
                            ) => {}
                        Err(err) => {
                            if tx.send(Err(err)).is_err() {
                                break;
                            }
                            std::thread::sleep(ACCEPT_POLL_INTERVAL);
                        }
                    }
                }
            })
            .context("starting the accept thread")?;
        Ok(Self::Shared(SharedAcceptor {
            local_addr,
            streams,
            stop,
            thread: Some(thread),
        }))
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        match self {
            Self::Owned(listener) => listener
                .local_addr()
                .context("reading local address for bound relay listener"),
            #[cfg(unix)]
            Self::Shared(shared) => Ok(shared.local_addr),
        }
    }

    async fn accept(&mut self) -> std::io::Result<(TcpStream, SocketAddr)> {
        match self {
            Self::Owned(listener) => listener.accept().await,
            #[cfg(unix)]
            Self::Shared(shared) => match shared.streams.recv().await {
                Some(Ok((stream, peer))) => {
                    stream.set_nonblocking(true)?;
                    Ok((TcpStream::from_std(stream)?, peer))
                }
                Some(Err(err)) => Err(err),
                // The thread only ends after `stop`, so a closed channel
                // while accepting is unreachable; wait forever rather than
                // spin if it ever happens.
                None => std::future::pending().await,
            },
        }
    }

    /// Stop accepting and return the connections accepted but not yet served.
    ///
    /// For a shared listener this waits for the acceptor thread to exit, so
    /// this process's copy of the socket is closed before it returns and the
    /// other instance owns every later connection.
    async fn stop(self) -> Vec<(TcpStream, SocketAddr)> {
        #[cfg(not(unix))]
        {
            let Self::Owned(_) = self;
            Vec::new()
        }
        #[cfg(unix)]
        {
            self.stop_shared().await
        }
    }

    #[cfg(unix)]
    async fn stop_shared(self) -> Vec<(TcpStream, SocketAddr)> {
        let Self::Shared(mut shared) = self else {
            return Vec::new();
        };
        shared.stop.store(true, Ordering::Release);
        if let Some(thread) = shared.thread.take() {
            let _ = tokio::task::spawn_blocking(move || thread.join()).await;
        }
        let mut pending = Vec::new();
        while let Ok(Ok((stream, peer))) = shared.streams.try_recv() {
            if stream.set_nonblocking(true).is_err() {
                continue;
            }
            if let Ok(stream) = TcpStream::from_std(stream) {
                pending.push((stream, peer));
            }
        }
        pending
    }
}

/// Decrements the open-connection gauge when a connection task ends, on every
/// exit path including a panic.
struct OpenConnection(Arc<Counters>);

impl Drop for OpenConnection {
    fn drop(&mut self) {
        self.0.connections_open.fetch_sub(1, Ordering::Relaxed);
    }
}

#[allow(clippy::result_large_err)]
// The `accept_hdr_async` callback is required to return
// `Result<Response<()>, ErrorResponse>` by the tungstenite API. The `Err`
// variant is a tungstenite `ErrorResponse` (an alias for
// `http::Response<Option<String>>`) whose size we cannot reduce without
// changing the third-party trait bound.
async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    config: &RelayConfig,
    registry: &SessionRegistry,
    counters: &Arc<Counters>,
) -> Result<()> {
    let mut path_holder: Option<String> = None;
    let path_ref = &mut path_holder;

    let ws = accept_hdr_async(stream, |req: &Request, response| {
        let path = req.uri().path().to_owned();
        parse_session_path(&path).map_or_else(
            || {
                let err_response = Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Some("relay session path must be /join/<session_id>".into()))
                    .unwrap_or_else(|_| {
                        let mut empty = Response::new(None);
                        *empty.status_mut() = StatusCode::NOT_FOUND;
                        empty
                    });
                Err(ErrorResponse::from(err_response))
            },
            |session| {
                *path_ref = Some(session.to_owned());
                Ok(response)
            },
        )
    })
    .await
    .with_context(|| format!("WebSocket handshake from {peer}"))?;

    let Some(session_id) = path_holder else {
        anyhow::bail!("WebSocket handshake succeeded without a session path");
    };

    info!(%peer, %session_id, "client connected");

    let outcome = run_session(ws, &session_id, config, registry, counters).await;
    if let Err(ref err) = outcome {
        warn!(%peer, %session_id, error = %err, "session ended with error");
    } else {
        info!(%peer, %session_id, "session ended");
    }
    outcome
}

async fn run_session(
    mut ws: WebSocketStream<TcpStream>,
    session_id: &str,
    config: &RelayConfig,
    registry: &SessionRegistry,
    counters: &Arc<Counters>,
) -> Result<()> {
    // First binary frame is the handshake.
    let handshake_frame = match ws.next().await {
        Some(Ok(Message::Binary(payload))) => payload,
        Some(Ok(_)) => {
            counters.auth_failures_total.fetch_add(1, Ordering::Relaxed);
            let _ = ws.send(Message::Close(None)).await;
            anyhow::bail!("first frame must be binary handshake");
        }
        Some(Err(err)) => {
            counters.auth_failures_total.fetch_add(1, Ordering::Relaxed);
            return Err(err).context("reading handshake frame");
        }
        None => {
            counters.auth_failures_total.fetch_add(1, Ordering::Relaxed);
            anyhow::bail!("client closed before sending handshake");
        }
    };

    let verified = match verify_handshake(&handshake_frame, session_id, &config.shared_secret) {
        Ok(v) => v,
        Err(err) => {
            counters.auth_failures_total.fetch_add(1, Ordering::Relaxed);
            let _ = ws.send(Message::Close(None)).await;
            return Err(handshake_to_anyhow(err));
        }
    };

    info!(device_id = %verified.device_id, session_id = %verified.session_id, "client authenticated");

    // Register against the session registry — either pair immediately or
    // park awaiting a peer.
    let outcome = registry
        .register(session_id, config.session_timeout, config.max_sessions)
        .await;

    match outcome {
        RegisterOutcome::AtCapacity => {
            counters
                .sessions_rejected_total
                .fetch_add(1, Ordering::Relaxed);
            warn!(%session_id, "rejecting session: registry at capacity");
            let _ = ws.send(Message::Close(None)).await;
            anyhow::bail!("relay at session capacity");
        }
        RegisterOutcome::Paired => {
            // We are the second peer to arrive — but we don't yet have the
            // first peer's `WebSocketStream`. The first peer's session-runner
            // is parked in the `pair` channel waiting for us. We use a tiny
            // companion mechanism in the registry: instead of duplicating
            // the channel plumbing, we re-park ourselves, send a "second"
            // marker through the channel, and let a coordinator pair us.
            //
            // The implementation uses the registry only as a rendezvous
            // signal — the actual byte-pipe is established here, by passing
            // the second peer's `WebSocketStream` over a side channel back
            // to the first parked peer's task.
            //
            // To keep the surface area small, the registry simply notifies
            // the parked peer with `Paired`; the parked peer then awaits a
            // second handoff on a fresh channel inserted by us. See
            // `pairing` module.
            pairing::pair_into(session_id, ws, counters).await
        }
        RegisterOutcome::Parked { wait_for_peer, .. } => {
            pairing::wait_for_peer(session_id, ws, wait_for_peer, registry, counters).await
        }
    }
}

fn handshake_to_anyhow(err: HandshakeError) -> anyhow::Error {
    anyhow::anyhow!(err).context("handshake verification failed")
}

fn parse_session_path(path: &str) -> Option<&str> {
    let stripped = path.strip_prefix(RELAY_JOIN_PATH)?;
    if stripped.is_empty() || stripped.contains('/') {
        None
    } else {
        Some(stripped)
    }
}

/// Validate a shared-secret length at compile time.
const _: () = {
    assert!(SHARED_SECRET_LEN == 32, "shared secret must be 32 bytes");
};

mod pairing {
    //! Side-channel that bridges the two halves of a session.
    //!
    //! When the second peer arrives the registry hands a `Paired` event
    //! to the first peer's task, but the first peer's task still needs to
    //! obtain the second peer's `WebSocketStream`. The pairing module
    //! keeps a single global map keyed by session ID where the second peer
    //! deposits its stream and the first peer collects it.

    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::OnceLock;
    use std::sync::atomic::Ordering;

    use anyhow::{Context, Result};
    use tokio::net::TcpStream;
    use tokio::sync::Mutex;
    use tokio::sync::oneshot;
    use tokio_tungstenite::WebSocketStream;

    use crate::metrics::Counters;
    use crate::pipe::run_pipe;
    use crate::session::{SessionEvent, SessionRegistry};

    type Slot = oneshot::Sender<WebSocketStream<TcpStream>>;

    static PAIRING: OnceLock<Mutex<HashMap<String, Slot>>> = OnceLock::new();

    fn pairing_map() -> &'static Mutex<HashMap<String, Slot>> {
        PAIRING.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub(super) async fn pair_into(
        session_id: &str,
        second_ws: WebSocketStream<TcpStream>,
        counters: &Arc<Counters>,
    ) -> Result<()> {
        let slot = {
            let mut map = pairing_map().lock().await;
            map.remove(session_id)
        };
        let Some(slot) = slot else {
            // The first peer disappeared between the registry notifying it
            // and us trying to hand off the stream. Drop our socket.
            anyhow::bail!("first peer dropped before hand-off");
        };
        slot.send(second_ws).map_err(|_| {
            anyhow::anyhow!("first peer task dropped before receiving second stream")
        })?;
        // The first peer's task is responsible for the byte-pipe; we're
        // done. Counters are updated by the first peer's task.
        let _ = counters;
        Ok(())
    }

    pub(super) async fn wait_for_peer(
        session_id: &str,
        first_ws: WebSocketStream<TcpStream>,
        wait_for_peer: oneshot::Receiver<SessionEvent>,
        registry: &SessionRegistry,
        counters: &Arc<Counters>,
    ) -> Result<()> {
        // Install our slot in the pairing map *before* awaiting the event
        // so the second peer never races us.
        let (slot_tx, slot_rx) = oneshot::channel();
        {
            let mut map = pairing_map().lock().await;
            map.insert(session_id.to_owned(), slot_tx);
        }

        let Ok(event) = wait_for_peer.await else {
            // Registry was dropped. Clean up our pairing slot.
            pairing_map().lock().await.remove(session_id);
            anyhow::bail!("session registry signalled an unrecoverable failure");
        };

        match event {
            SessionEvent::Paired => {
                // Receive the second peer's stream.
                let second_ws = slot_rx
                    .await
                    .context("waiting for second peer's stream handoff")?;
                counters.sessions_active.fetch_add(1, Ordering::Relaxed);
                counters
                    .sessions_paired_total
                    .fetch_add(1, Ordering::Relaxed);
                let (_a, _b) = run_pipe(first_ws, second_ws, counters.clone()).await;
                counters.sessions_active.fetch_sub(1, Ordering::Relaxed);
                Ok(())
            }
            SessionEvent::TimedOut => {
                pairing_map().lock().await.remove(session_id);
                let _ = first_ws;
                anyhow::bail!("session timed out before peer arrived");
            }
            SessionEvent::ServerShutdown => {
                pairing_map().lock().await.remove(session_id);
                registry.drop_parked(session_id).await;
                let _ = first_ws;
                anyhow::bail!("server shutting down");
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_session_path() {
        assert_eq!(parse_session_path("/join/abc"), Some("abc"));
        assert_eq!(parse_session_path("/join/AbC-123"), Some("AbC-123"));
    }

    #[test]
    fn rejects_invalid_session_path() {
        assert_eq!(parse_session_path("/join/"), None);
        assert_eq!(parse_session_path("/join/a/b"), None);
        assert_eq!(parse_session_path("/other"), None);
        assert_eq!(parse_session_path("join/abc"), None);
    }
}
