"""Smoke test: the real Rust ``ledge`` server, spawned as a child process,
driven entirely through the Python :class:`LedgeClient` over capnp ``POST /rpc``.
Every assertion crosses the HTTP boundary and the capnp encode/decode path.
"""

from __future__ import annotations

import os
import sys

import pytest

# Make `ledge_sdk` and the local `server` harness importable.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ledge_sdk import CommitMapping, LedgeClient, LedgeRpcError  # noqa: E402

from server import RunningServer, build_server, start_server  # noqa: E402


@pytest.fixture(scope="module")
def client() -> LedgeClient:
    build_server()
    server: RunningServer = start_server()
    try:
        yield LedgeClient(server.base_url)
    finally:
        server.stop()


def test_write_read_roundtrip(client: LedgeClient) -> None:
    content = b"hello ledge over capnp/http (python)"
    obj_id = client.write_object(3, content)  # git blob type = 3
    assert isinstance(obj_id, bytes)
    assert len(obj_id) == 32
    got = client.read_object(obj_id)
    assert got == content


def test_content_addressed(client: LedgeClient) -> None:
    content = b"deterministic payload"
    a = client.write_object(3, content)
    b = client.write_object(3, content)
    assert a == b


def test_read_missing_raises(client: LedgeClient) -> None:
    missing = bytes([0xAB]) * 32
    with pytest.raises(LedgeRpcError):
        client.read_object(missing)


def test_list_workspaces_empty(client: LedgeClient) -> None:
    assert client.list_workspaces() == []


def test_run_gc_returns_stats(client: LedgeClient) -> None:
    stats = client.run_gc()
    assert isinstance(stats.scanned, int)
    assert isinstance(stats.reachable, int)
    assert isinstance(stats.reclaimed, int)
    assert isinstance(stats.bytes_freed, int)
    assert stats.reachable <= stats.scanned


def test_fork_get_release_lifecycle(client: LedgeClient) -> None:
    ws = client.fork([], 120)
    assert ws.refs == []
    assert ws.expires_at_ms > 0

    assert ws.id in [w.id for w in client.list_workspaces()]

    fetched = client.get_workspace(ws.id)
    assert fetched.id == ws.id

    lease = client.renew(ws.id, 300)
    assert lease.id == ws.id
    assert lease.generation >= 2
    assert lease.expires_at_ms >= ws.expires_at_ms

    client.release(ws.id)
    assert ws.id not in [w.id for w in client.list_workspaces()]


def test_commit_empty_mappings(client: LedgeClient) -> None:
    ws = client.fork([], 60)
    outcomes = client.commit(ws.id, [])
    assert outcomes == []
    client.release(ws.id)


def test_get_unknown_workspace_raises(client: LedgeClient) -> None:
    with pytest.raises(LedgeRpcError):
        client.get_workspace("ab" * 16)
