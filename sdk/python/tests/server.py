"""Test harness: build the Rust ``ledge`` binary once, then spawn it on an
ephemeral port over a tmp data dir, poll ``/healthz`` until ready, and hand back
a base URL + a stop handle. Mirrors ``sdk/ts/test/server.ts``.
"""

from __future__ import annotations

import os
import random
import shutil
import signal
import subprocess
import tempfile
import time
import urllib.request
from dataclasses import dataclass

# sdk/python/tests -> repo root is three levels up.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.normpath(os.path.join(_HERE, "..", "..", ".."))
_BIN_PATH = os.path.join(_REPO_ROOT, "target", "debug", "ledge")


def build_server() -> None:
    """Build the ``ledge`` binary once (cargo is incremental; no-op if current)."""
    r = subprocess.run(
        ["cargo", "build", "--bin", "ledge"], cwd=_REPO_ROOT
    )
    if r.returncode != 0:
        raise RuntimeError(f"cargo build --bin ledge failed (status {r.returncode})")


@dataclass
class RunningServer:
    base_url: str
    _proc: subprocess.Popen
    _data_dir: str

    def stop(self) -> None:
        if self._proc.poll() is None:
            self._proc.send_signal(signal.SIGKILL)
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:  # pragma: no cover
                pass
        shutil.rmtree(self._data_dir, ignore_errors=True)


def _random_port() -> int:
    # Ephemeral-ish range; we retry on bind failure so collisions are harmless.
    return 20000 + random.randint(0, 39999)


def _wait_for_health(base_url: str, deadline: float) -> bool:
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/healthz", timeout=1) as r:
                if 200 <= r.status < 300:
                    return True
        except Exception:
            pass  # Not up yet; back off and retry.
        time.sleep(0.05)
    return False


def start_server() -> RunningServer:
    """Spawn the prebuilt server on a free port over a fresh tmp dir; await health."""
    if not os.path.exists(_BIN_PATH):
        raise RuntimeError(
            f"ledge binary not found at {_BIN_PATH}; run build_server() first"
        )
    last_err: Exception | None = None
    for _ in range(8):
        port = _random_port()
        data_dir = tempfile.mkdtemp(prefix="ledge-sdk-py-")
        addr = f"127.0.0.1:{port}"
        base_url = f"http://{addr}"
        env = {**os.environ, "RUST_LOG": "warn"}
        proc = subprocess.Popen(
            [_BIN_PATH, "start", "--addr", addr, "--data-dir", data_dir],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )
        ready = _wait_for_health(base_url, time.monotonic() + 10.0)
        if ready and proc.poll() is None:
            return RunningServer(base_url=base_url, _proc=proc, _data_dir=data_dir)
        if proc.poll() is None:
            proc.send_signal(signal.SIGKILL)
            proc.wait(timeout=5)
        shutil.rmtree(data_dir, ignore_errors=True)
        last_err = RuntimeError(f"server failed to become healthy on {addr}")
    raise last_err or RuntimeError("server failed to start")
