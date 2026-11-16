# cascade-relay deployment

`cascade-relay` is a stateless byte-pipe relay for Cascade peers that cannot reach each other directly. Peers connect to the relay and authenticate with a shared HMAC secret; the relay pairs them and shuttles already-encrypted bytes between them without inspecting the payload.

Three distribution paths are supported: building from source with `cargo install`, installing a pre-built binary via Homebrew, and running the official Docker image from `ghcr.io`.

## Prerequisites

- A shared HMAC secret shared between the relay and every peer that will use it. Generate one with:

  ```sh
  openssl rand -hex 32
  ```

  The output is a 64-character hexadecimal string (32 bytes). Keep it out of shell history — pass it via a file, environment variable, or secrets manager.

- The relay needs a publicly-reachable TCP address so peers can connect. The default port is `9999`. Open it in your firewall or security group.

## Install paths

### From source (cargo install)

```sh
cargo install --git https://github.com/Mearman/cascade --bin cascade-relay
```

This builds and installs the `cascade-relay` binary from the latest `main` branch. Use a pinned tag for reproducibility:

```sh
cargo install --git https://github.com/Mearman/cascade --tag cascade-relay-v0.1.37 --bin cascade-relay
```

### Homebrew

```sh
brew tap Mearman/cascade
brew install Mearman/cascade/cascade-relay
```

This installs a pre-built binary from the latest `cascade-relay-v*` release. The formula covers macOS (aarch64 and x86_64) and Linux (x86_64 and aarch64) via the same tap used by the main `cascade` formula.

### Docker (ghcr.io)

```sh
docker pull ghcr.io/mearman/cascade-relay:latest
```

Versioned tags match the relay release version (e.g. `ghcr.io/mearman/cascade-relay:0.1.37`). The image is multi-platform (`linux/amd64` and `linux/arm64`).

## Running the relay

### Direct invocation

```sh
cascade-relay \
  --bind 0.0.0.0:9999 \
  --shared-secret "$(cat /etc/cascade/relay.secret)" \
  --metrics-bind 0.0.0.0:9998
```

All flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--bind` | `0.0.0.0:9999` | Address the byte-pipe listener binds to. |
| `--shared-secret` | (required) | 64-char hex HMAC secret. |
| `--session-timeout-seconds` | `60` | How long the first peer may wait for its partner before the session is reaped. |
| `--max-sessions` | `1024` | Maximum simultaneous in-flight sessions. New sessions are rejected when this limit is reached. |
| `--metrics-bind` | (disabled) | Address for the Prometheus `/metrics` HTTP endpoint. |

### Docker

Pass configuration via environment variables:

```sh
docker run -d \
  --name cascade-relay \
  --restart unless-stopped \
  -p 9999:9999 \
  -p 9998:9998 \
  -e CASCADE_RELAY_SHARED_SECRET="$(cat /etc/cascade/relay.secret)" \
  -e CASCADE_RELAY_LISTEN="0.0.0.0:9999" \
  -e CASCADE_RELAY_METRICS="0.0.0.0:9998" \
  ghcr.io/mearman/cascade-relay:latest
```

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `CASCADE_RELAY_SHARED_SECRET` | (required) | 64-char hex HMAC secret. |
| `CASCADE_RELAY_LISTEN` | `0.0.0.0:9999` | Bind address for the peer listener. |
| `CASCADE_RELAY_METRICS` | (disabled) | Bind address for the `/metrics` endpoint. |

Additional CLI flags (e.g. `--max-sessions`) can be passed as `CMD` arguments after the image name:

```sh
docker run ... ghcr.io/mearman/cascade-relay:latest \
  --session-timeout-seconds 120 \
  --max-sessions 512
```

### Docker Compose

```yaml
services:
  relay:
    image: ghcr.io/mearman/cascade-relay:latest
    restart: unless-stopped
    ports:
      - "9999:9999"
      - "9998:9998"
    environment:
      CASCADE_RELAY_SHARED_SECRET: "${CASCADE_RELAY_SHARED_SECRET}"
      CASCADE_RELAY_LISTEN: "0.0.0.0:9999"
      CASCADE_RELAY_METRICS: "0.0.0.0:9998"
```

Store the secret in a `.env` file (not committed to version control) or inject it via your orchestrator's secrets mechanism.

## Updating without dropping connections

The relay keeps no state on disk, but a restart ends every open session and refuses connections while the new process starts. On a systemd host the update path below avoids both: the new version starts beside the old one on the same listening port, passes a health gate, and only then does the old one stop accepting and drain. If the new version fails its gate, it is stopped and the old one carries on untouched.

### Why a socket handoff

The relay binds one TCP port and a reverse proxy cannot be assumed, so two instances must accept on the same port. A systemd-owned socket (`cascade-relay-handoff.socket`) gives that without a proxy: both instances inherit the one listening socket and accept from it. `SO_REUSEPORT` was rejected because closing one listener resets the connections still queued on it. Docker's `deploy.update_config.order: start-first` is not an option for the same reason: a published host port can be bound by one container only, and the setting is honoured by Swarm, not by `docker compose`.

### How it works

Two slots, `blue` and `green`, run as `cascade-relay-handoff@blue.service` and `cascade-relay-handoff@green.service`. Each slot has its own binary under `/usr/local/lib/cascade-relay/<slot>/` and its own `/health` address.

1. `cascade-relay-update.sh NEW_BINARY` installs the binary into the idle slot and starts it. It accepts connections from the shared socket as soon as it is up, so starting it is the switch.
2. The health gate polls the new slot's `/health` until it returns `200` within `--health-timeout` (default 30 seconds), then requires it to stay active and healthy for `--settle` seconds (default 5).
3. The old slot is sent `SIGTERM`. It stops accepting, `/health` turns to `503` with the body `draining`, and it keeps serving its open sessions until they end or `--drain-timeout-seconds` (default 300) passes. The script returns when the old slot has exited.
4. If the gate fails at any point (the new slot exits, never becomes healthy or is killed), the script stops it, resets its unit, prints why, exits with status `1` and leaves the old slot serving. Open sessions stay where they were and new connections keep going to the old slot.

systemd puts an inherited socket in blocking mode whenever it starts an instance, so the relay cannot rely on the socket's non-blocking flag. It accepts on a dedicated thread with a 100 millisecond accept timeout, and on stop it serves every connection it had already taken before closing its copy of the socket.

A connection the new slot accepted before it failed is reset, and the peer reconnects, as it does after any relay restart. The gate keeps that window to the slot's first moments.

### Install

```sh
useradd --system --no-create-home --shell /usr/sbin/nologin cascade-relay
install -m 0755 crates/relay-server/cascade-relay-start.sh /usr/local/bin/
install -m 0755 crates/relay-server/cascade-relay-update.sh /usr/local/bin/
cp crates/relay-server/cascade-relay-handoff.socket \
   crates/relay-server/cascade-relay-handoff.service \
   crates/relay-server/cascade-relay-handoff@.service /etc/systemd/system/

# /etc/cascade/relay.env holds CASCADE_RELAY_SHARED_SECRET (see relay.env.example).
# relay-blue.env and relay-green.env each give their slot a distinct
# CASCADE_RELAY_METRICS address (see relay-slot.env.example).
cp crates/relay-server/relay-slot.env.example /etc/cascade/relay-blue.env
sed 's/9998/9996/' crates/relay-server/relay-slot.env.example > /etc/cascade/relay-green.env

systemctl daemon-reload
systemctl enable --now cascade-relay-handoff.socket cascade-relay-handoff.service
cascade-relay-update.sh /path/to/cascade-relay    # first run starts blue
```

The socket listens on `9999`; edit `ListenStream=` to change it. `CASCADE_RELAY_LISTEN` is ignored while the socket is inherited. The slot units use `TimeoutStopSec=330s` so systemd does not kill a slot that is still draining.

### Update

```sh
cascade-relay-update.sh /path/to/new/cascade-relay
```

Add `--user` to drive a per-user systemd manager. The script refuses to run when both slots are active, which means an earlier update was interrupted: stop the slot that should not serve and run it again.

### Test

`test/relay-update/test_update.py` runs the shipped units and script under `systemctl --user` against a real relay binary, with a probe connecting to the shared port throughout:

```sh
cargo build --release -p cascade-relay-server
CASCADE_RELAY_TEST_BINARY=target/release/cascade-relay python3 test/relay-update/test_update.py
```

- A clean update: the old slot drains while its open session keeps relaying, a connection made mid-drain lands on the new slot, no probe fails, and the old slot exits once its session ends.
- The new slot `SIGKILL`ed inside the settle window: the update rolls back, and the old slot keeps relaying its session, answers every probe and accepts new sessions.
- A new binary that exits at once: the update rolls back with no probe failing.

It needs Linux with a running user manager, `curl` and `python3`.

### Docker and other hosts

A container cannot share a published port with a second one, so a Docker or Compose deployment has no create-before-destroy path without a proxy in front: replacing the container refuses connections for as long as the new one takes to start, and open sessions end. Soften it with:

- `stop_grace_period` (Compose) or `--stop-timeout` (`docker run`) of at least the drain timeout, so the old container drains before Docker kills it: the relay stops accepting at `SIGTERM` and `/health` reports `503 draining`, which the shipped healthcheck treats as unhealthy.
- A reverse proxy or load balancer that health-checks `/health` and runs two relay containers; then update them one at a time, waiting for each to report healthy.

A host that can run the relay binary directly should prefer the systemd path above.

## Pointing peers at the relay

In the peer's `.cascade` config or backend configuration, set the relay endpoint and the same shared secret:

```toml
[p2p]
relay_endpoints = ["relay.example.com:9999"]
relay_shared_secret = "your-64-char-hex-secret"
```

Both peers must share the same secret as the relay. The relay validates the HMAC handshake before pairing sessions; a mismatched secret causes authentication to fail.

## Observability

When `--metrics-bind` (or `CASCADE_RELAY_METRICS`) is set, the relay serves Prometheus metrics at `http://<metrics-bind>/metrics`. Exposed counters include:

- `relay_sessions_total` — cumulative sessions created.
- `relay_sessions_active` — sessions currently in the paired or waiting state.
- `relay_bytes_relayed_total` — bytes forwarded across all sessions.
- `cascade_relay_connections_open`: client connection tasks currently open; a paired session holds one.
- `cascade_relay_draining`: `1` once the relay has stopped accepting connections ahead of exit.

## Security notes

- The relay never decrypts peer traffic. Peers establish end-to-end TLS over the byte pipe; the relay sees only opaque bytes after the initial HMAC handshake.
- Keep the shared secret private. Anyone with it can open relay sessions against your server. Use a unique secret per relay deployment.
- Run the relay behind a firewall that allows only `9999/tcp` inbound (and optionally `9998/tcp` for metrics, restricted to your monitoring network).
