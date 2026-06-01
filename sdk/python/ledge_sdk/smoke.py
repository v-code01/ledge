"""Runnable smoke check: ``python -m ledge_sdk.smoke``.

Spawns the Rust ``ledge`` server as a child process and exercises the core
agent flow through :class:`LedgeClient`: write_object -> read_object identical
bytes, list_workspaces empty, run_gc stats, and fork([]) -> get_workspace ->
release. Exits non-zero on any failure. This is the same coverage as the pytest
suite, packaged as a standalone runner for environments without pytest.
"""

from __future__ import annotations

import os
import sys

# Allow `python -m ledge_sdk.smoke` to find the sibling test harness.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "tests")))

from ledge_sdk import CommitMapping, LedgeClient, LedgeRpcError  # noqa: E402


def main() -> int:
    from server import build_server, start_server  # local test harness

    build_server()
    server = start_server()
    try:
        client = LedgeClient(server.base_url)

        content = b"hello ledge over capnp/http (python smoke)"
        obj_id = client.write_object(3, content)
        assert len(obj_id) == 32, "object id must be 32 bytes"
        assert client.read_object(obj_id) == content, "roundtrip mismatch"

        assert client.list_workspaces() == [], "fresh server must have no workspaces"

        stats = client.run_gc()
        assert stats.reachable <= stats.scanned, "gc accounting invariant"

        ws = client.fork([], 120)
        assert client.get_workspace(ws.id).id == ws.id, "getWorkspace id mismatch"
        outcomes = client.commit(ws.id, [])
        assert outcomes == [], "empty commit must yield no outcomes"
        client.release(ws.id)
        assert ws.id not in [w.id for w in client.list_workspaces()], "release failed"

        try:
            client.read_object(bytes([0xAB]) * 32)
        except LedgeRpcError:
            pass
        else:  # pragma: no cover - server contract violation
            raise AssertionError("missing object should raise LedgeRpcError")

        print("ledge_sdk smoke: OK")
        return 0
    finally:
        server.stop()


if __name__ == "__main__":
    raise SystemExit(main())
