#!/usr/bin/env python3
"""End-to-end test of the relay's side-by-side update path.

Runs the shipped handoff units and cascade-relay-update.sh under a real
per-user systemd manager (systemd --user), with the unit files rewritten only
to drop system-only sandboxing and to point at a temporary directory and free
ports. Requires Linux with a running user manager, curl and a built relay.

    CASCADE_RELAY_TEST_BINARY=target/release/cascade-relay \
        python3 test/relay-update/test_update.py

Scenarios:
  * a clean update drains the old slot, keeps its open session alive, sends
    new connections to the new slot and refuses no connection;
  * the new slot killed with SIGKILL mid-update is rolled back and the old
    slot keeps serving its session and new connections;
  * a new binary that cannot start is rolled back without the old slot ever
    losing a connection.
"""

import base64
import hashlib
import hmac
import os
import re
import shutil
import socket
import stat
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RELAY_DIR = REPO / "crates" / "relay-server"
BINARY = Path(os.environ.get("CASCADE_RELAY_TEST_BINARY", REPO / "target" / "release" / "cascade-relay"))
SECRET_HEX = "ab" * 32
SECRET = bytes.fromhex(SECRET_HEX)
# Lines of the shipped system units that a user manager cannot honour.
SYSTEM_ONLY = re.compile(
    r"^(User|Group|NoNewPrivileges|ProtectSystem|ProtectHome|PrivateTmp|PrivateDevices|"
    r"ProtectKernelTunables|ProtectControlGroups|RestrictSUIDSGID|RestrictAddressFamilies)="
)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def ws_connect(port: int, session: str, device: str, timeout: float = 5.0) -> socket.socket:
    """Open a relay WebSocket, send the HMAC handshake and return the socket."""
    sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    key = base64.b64encode(os.urandom(16)).decode()
    sock.sendall(
        (
            f"GET /join/{session} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
            f"Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode()
    )
    head = b""
    while b"\r\n\r\n" not in head:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("closed during upgrade")
        head += chunk
    if not head.startswith(b"HTTP/1.1 101"):
        raise ConnectionError(f"upgrade refused: {head[:40]!r}")
    dev, sess = device.encode(), session.encode()
    tag = hmac.new(SECRET, dev + sess, hashlib.sha256).digest()
    ws_send(sock, struct.pack(">BH", 1, len(dev)) + dev + struct.pack(">H", len(sess)) + sess + tag)
    return sock


def ws_send(sock: socket.socket, payload: bytes) -> None:
    """Send one masked binary frame."""
    mask = os.urandom(4)
    length = len(payload)
    header = bytes([0x82])
    if length < 126:
        header += bytes([0x80 | length])
    else:
        header += bytes([0x80 | 126]) + struct.pack(">H", length)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    sock.sendall(header + mask + masked)


def ws_recv(sock: socket.socket) -> bytes:
    """Read one (unmasked, server-to-client) frame and return its payload."""

    def exact(count: int) -> bytes:
        data = b""
        while len(data) < count:
            chunk = sock.recv(count - len(data))
            if not chunk:
                raise ConnectionError("closed while reading a frame")
            data += chunk
        return data

    first, second = exact(2)
    length = second & 0x7F
    if length == 126:
        (length,) = struct.unpack(">H", exact(2))
    return exact(length)


def serves(port: int) -> str | None:
    """None when the relay answers a WebSocket upgrade for a bad path with 404, else why not."""
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2) as sock:
            sock.sendall(
                b"GET /nope HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
            )
            reply = sock.recv(64)
            return None if reply.startswith(b"HTTP/1.1 404") else f"unexpected reply {reply!r}"
    except OSError as err:
        return repr(err)


class Prober(threading.Thread):
    """Probes the shared listener continuously and counts failures."""

    def __init__(self, port: int) -> None:
        super().__init__(daemon=True)
        self.port = port
        self.ok = 0
        self.failed = 0
        self.reasons: list[str] = []
        self._halt = threading.Event()

    def run(self) -> None:
        while not self._halt.is_set():
            reason = serves(self.port)
            if reason is None:
                self.ok += 1
            else:
                self.failed += 1
                self.reasons.append(reason)
            time.sleep(0.01)

    def finish(self) -> None:
        self._halt.set()
        self.join()


class UpdateTest(unittest.TestCase):
    def setUp(self) -> None:
        if not BINARY.is_file():
            self.fail(f"relay binary not found at {BINARY}; build it or set CASCADE_RELAY_TEST_BINARY")
        self.tmp = Path(tempfile.mkdtemp(prefix="relay-update-"))
        self.unit = f"crhtest{os.getpid()}"
        self.port = free_port()
        self.metrics = {"blue": free_port(), "green": free_port()}
        self.etc, self.lib, self.bin = (self.tmp / d for d in ("etc", "lib", "bin"))
        for directory in (self.etc, self.lib, self.bin):
            directory.mkdir()
        (self.etc / "relay.env").write_text(f"CASCADE_RELAY_SHARED_SECRET={SECRET_HEX}\n")
        for slot, port in self.metrics.items():
            (self.etc / f"relay-{slot}.env").write_text(f"CASCADE_RELAY_METRICS=127.0.0.1:{port}\n")
        launcher = self.bin / "cascade-relay-start.sh"
        shutil.copy(RELAY_DIR / "cascade-relay-start.sh", launcher)
        launcher.chmod(0o755)
        self.units = Path.home() / ".config" / "systemd" / "user"
        self.units.mkdir(parents=True, exist_ok=True)
        self.unit_files: list[Path] = []
        for name in ("cascade-relay-handoff.socket", "cascade-relay-handoff.service", "cascade-relay-handoff@.service"):
            text = (RELAY_DIR / name).read_text()
            text = "\n".join(line for line in text.splitlines() if not SYSTEM_ONLY.match(line))
            text = text.replace("cascade-relay-handoff", self.unit)
            text = text.replace("/etc/cascade", str(self.etc))
            text = text.replace("/usr/local/lib/cascade-relay", str(self.lib))
            text = text.replace("/usr/local/bin/cascade-relay-start.sh", str(launcher))
            text = text.replace("ListenStream=9999", f"ListenStream=127.0.0.1:{self.port}")
            target = self.units / name.replace("cascade-relay-handoff", self.unit)
            target.write_text(text + "\n")
            self.unit_files.append(target)
        self.ctl("daemon-reload")
        self.ctl("start", f"{self.unit}.socket", f"{self.unit}.service")
        self.update_env = {
            **os.environ,
            "CASCADE_RELAY_LIB_DIR": str(self.lib),
            "CASCADE_RELAY_ETC_DIR": str(self.etc),
            "CASCADE_RELAY_UNIT": self.unit,
        }
        # Install and start version one through the update path itself.
        result = self.update(BINARY, "--settle", "1")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(self.active("blue"))
        self.sockets: list[socket.socket] = []

    def tearDown(self) -> None:
        for sock in self.sockets:
            sock.close()
        for slot in ("blue", "green"):
            self.ctl("stop", f"{self.unit}@{slot}.service", check=False)
        self.ctl("stop", f"{self.unit}.service", f"{self.unit}.socket", check=False)
        for path in self.unit_files:
            path.unlink(missing_ok=True)
        self.ctl("daemon-reload", check=False)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def ctl(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["systemctl", "--user", *args], capture_output=True, text=True, check=check)

    def active(self, slot: str) -> bool:
        return self.ctl("is-active", "--quiet", f"{self.unit}@{slot}.service", check=False).returncode == 0

    def update(self, binary: Path, *flags: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(RELAY_DIR / "cascade-relay-update.sh"), "--user", *flags, str(binary)],
            env=self.update_env,
            capture_output=True,
            text=True,
        )

    def start_update(self, binary: Path, *flags: str) -> subprocess.Popen[str]:
        return subprocess.Popen(
            [str(RELAY_DIR / "cascade-relay-update.sh"), "--user", *flags, str(binary)],
            env=self.update_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

    def metric(self, slot: str, name: str) -> float:
        body = subprocess.run(
            ["curl", "-s", "--max-time", "2", f"http://127.0.0.1:{self.metrics[slot]}/metrics"],
            capture_output=True,
            text=True,
        ).stdout
        match = re.search(rf"^{name} (\S+)$", body, re.MULTILINE)
        self.assertIsNotNone(match, f"{name} missing from {slot} metrics")
        return float(match.group(1))

    def wait_for(self, condition, what: str, timeout: float = 30.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if condition():
                return
            time.sleep(0.1)
        self.fail(f"timed out waiting for {what}")

    def open_pair(self, session: str) -> tuple[socket.socket, socket.socket]:
        a = ws_connect(self.port, session, "device-a")
        b = ws_connect(self.port, session, "device-b")
        self.sockets += [a, b]
        return a, b

    def assert_pair_relays(self, a: socket.socket, b: socket.socket) -> None:
        ws_send(a, b"ping")
        self.assertEqual(ws_recv(b), b"ping")
        ws_send(b, b"pong")
        self.assertEqual(ws_recv(a), b"pong")

    def test_clean_update_drains_old_slot_without_refusing_a_connection(self) -> None:
        a, b = self.open_pair("held-before-update")
        self.assert_pair_relays(a, b)
        prober = Prober(self.port)
        prober.start()
        update = self.start_update(BINARY, "--settle", "2")
        self.wait_for(lambda: self.active("green") and self.metrics_ok("green"), "green to serve")
        # The old slot drains but keeps the open session until it ends.
        self.wait_for(lambda: self.metric("blue", "cascade_relay_draining") == 1, "blue to start draining")
        # A paired session holds one connection task: the second peer's stream
        # is handed to the first peer's task.
        self.assertEqual(self.metric("blue", "cascade_relay_connections_open"), 1)
        self.assert_pair_relays(a, b)
        # A connection made after the old slot stopped accepting reaches green.
        before = self.metric("green", "cascade_relay_connections_open")
        c, d = self.open_pair("opened-during-drain")
        self.assert_pair_relays(c, d)
        self.assertGreater(self.metric("green", "cascade_relay_connections_open"), before)
        self.assertIsNone(update.poll(), "update must wait for the old slot to drain")
        # Ending the old session lets the old slot exit and the update finish.
        a.close()
        b.close()
        output, _ = update.communicate(timeout=30)
        prober.finish()
        self.assertEqual(update.returncode, 0, output)
        self.assertFalse(self.active("blue"))
        self.assertTrue(self.active("green"))
        self.assertGreater(prober.ok, 0)
        self.assertEqual(prober.failed, 0, f"{prober.failed} of {prober.ok + prober.failed} probes failed: {prober.reasons[:3]}")
        self.assert_pair_relays(c, d)

    def metrics_ok(self, slot: str) -> bool:
        code = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "2",
             f"http://127.0.0.1:{self.metrics[slot]}/health"],
            capture_output=True,
            text=True,
        ).stdout
        return code == "200"

    def test_new_slot_killed_mid_update_rolls_back_and_old_keeps_serving(self) -> None:
        a, b = self.open_pair("held-across-failed-update")
        self.assert_pair_relays(a, b)
        update = self.start_update(BINARY, "--settle", "20")
        # Wait for the new slot to pass its health gate, then kill it inside
        # the settle window.
        self.wait_for(lambda: self.active("green") and self.metrics_ok("green"), "green to be healthy")
        pid = self.ctl("show", "-p", "MainPID", "--value", f"{self.unit}@green.service").stdout.strip()
        self.assertNotIn(pid, ("", "0"))
        os.kill(int(pid), 9)
        output, _ = update.communicate(timeout=60)
        self.assertEqual(update.returncode, 1, output)
        self.assertIn("leaving blue serving", output)
        self.assertTrue(self.active("blue"))
        self.assertFalse(self.active("green"))
        self.assertTrue(self.metrics_ok("blue"))
        # The old slot kept its session and still takes new connections.
        self.assert_pair_relays(a, b)
        prober = Prober(self.port)
        prober.start()
        time.sleep(1)
        prober.finish()
        self.assertGreater(prober.ok, 0)
        self.assertEqual(prober.failed, 0, prober.reasons[:3])
        c, d = self.open_pair("opened-after-rollback")
        self.assert_pair_relays(c, d)

    def test_unstartable_new_binary_is_rolled_back_without_touching_the_old_slot(self) -> None:
        a, b = self.open_pair("held-across-bad-binary")
        broken = self.tmp / "broken-relay"
        broken.write_text("#!/bin/sh\nexit 1\n")
        broken.chmod(broken.stat().st_mode | stat.S_IXUSR)
        prober = Prober(self.port)
        prober.start()
        result = self.update(broken, "--health-timeout", "6", "--settle", "1")
        prober.finish()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertTrue(self.active("blue"))
        self.assertFalse(self.active("green"))
        self.assertGreater(prober.ok, 0)
        self.assertEqual(prober.failed, 0, f"a slot that never started must never receive a connection: {prober.reasons[:3]}")
        self.assert_pair_relays(a, b)


if __name__ == "__main__":
    sys.exit(unittest.main(verbosity=2))
