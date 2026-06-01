"""Typed Ledge control-plane client over Cap'n Proto ``POST /rpc``.

A :class:`LedgeClient` encodes a Cap'n Proto ``Request`` (the shared
``sdk/schema/ledge.capnp`` contract the Rust server is generated from), POSTs it
as a standard framed (unpacked) capnp message with Content-Type
``application/x-ledge-capnp``, then decodes the framed capnp ``Response``.

Wire-format note: the Rust server uses ``capnp::serialize`` (unpacked, framed
segments). pycapnp ``Message.to_bytes()`` emits exactly that framing and
``Schema.from_bytes(...)`` reads it -- no packing on either side. The packed
variants (``to_bytes_packed`` / ``from_bytes_packed``) would NOT interoperate
and are intentionally unused.

Each method mirrors one arm of the ``Request`` union. Server-side business
errors arrive in the ``Response.error`` variant and are raised as
:class:`LedgeRpcError`; non-2xx HTTP responses raise :class:`LedgeTransportError`.
"""

# pyright: reportAttributeAccessIssue=false
#
# pycapnp builds message/reader classes (``Request``, ``Response``, and every
# struct/union arm) at *runtime* from the dynamically loaded ``ledge.capnp``
# schema (see ``capnp.load`` below). There is no static type information for any
# of those generated classes, so every field access on a capnp builder/reader
# (``req.init(...)``, ``resp.which()``, ``resp.error``, ``w.expiresAtMs``, ...)
# is invisible to a static checker. The builder/reader params are deliberately
# typed ``object`` for documentation; the accesses are correct against the
# schema and are exercised by the smoke test against the live server. This
# pragma scopes the suppression to exactly this category (dynamic capnp
# attribute access) for this module only; all other diagnostics stay on.

from __future__ import annotations

import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Callable, List, Sequence, Union

import capnp

# Wire content type for capnp `/rpc` request and response bodies.
_CONTENT_TYPE = "application/x-ledge-capnp"

# Load the canonical schema dynamically. The schema lives at
# `sdk/schema/ledge.capnp`, three directories up from this file
# (`sdk/python/ledge_sdk/client.py`). pycapnp parses it at import time; no
# codegen step is involved.
_SCHEMA_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "schema", "ledge.capnp")
)
_ledge = capnp.load(_SCHEMA_PATH)


# ── plain owned value types (decoded out of the capnp readers) ──────────────


@dataclass(frozen=True)
class RefEntry:
    """A single versioned ref state (decoded from the capnp ``RefEntry``)."""

    target: bytes  # 32-byte BLAKE3 content address the ref points at.
    hlc: int  # Hybrid logical clock stamp of the write.
    version: int  # Monotonic per-ref version for optimistic concurrency.


@dataclass(frozen=True)
class NamedRef:
    """A named ref carried by a workspace view (e.g. ``refs/heads/main``)."""

    name: str
    entry: RefEntry


@dataclass(frozen=True)
class WorkspaceInfo:
    """A point-in-time view returned by ``fork`` / ``get_workspace``."""

    id: str
    expires_at_ms: int
    refs: List[NamedRef]


@dataclass(frozen=True)
class Lease:
    """A workspace lease returned by ``renew``."""

    id: str
    expires_at_ms: int
    created_at_ms: int
    generation: int


@dataclass(frozen=True)
class GcStats:
    """Per-pass garbage-collection accounting returned by ``run_gc``."""

    scanned: int
    reachable: int
    reclaimed: int
    bytes_freed: int


@dataclass(frozen=True)
class CommitMapping:
    """One (workspace ref -> durable ref) promotion request for ``commit``."""

    workspace_ref: str
    durable_ref: str


@dataclass(frozen=True)
class CommitOutcome:
    """The result of promoting one workspace ref to a durable ref."""

    target: str
    ok: bool  # True if the promotion landed; False on a concurrent-write conflict.
    version: int  # On ok, the promoted version; on conflict, the live version.


# ── errors ──────────────────────────────────────────────────────────────────


class LedgeRpcError(Exception):
    """Raised when the server encodes a business error in ``Response.error``."""


class LedgeTransportError(Exception):
    """Raised when the HTTP layer rejects the request (e.g. 400 malformed body)."""

    def __init__(self, message: str, status: int) -> None:
        super().__init__(message)
        self.status = status


# ── client ────────────────────────────────────────────────────────────────


class LedgeClient:
    """A typed client for the Ledge control plane over Cap'n Proto ``POST /rpc``.

    Construct with the server base URL (no trailing ``/rpc``)::

        client = LedgeClient("http://127.0.0.1:8080")
    """

    def __init__(self, base_url: str, timeout: float = 30.0) -> None:
        # Strip trailing slashes so `${base}/rpc` is well-formed.
        self._rpc_url = base_url.rstrip("/") + "/rpc"
        self._timeout = timeout

    # public API — one method per Request union arm ---------------------------

    def write_object(self, git_type: int, content: bytes) -> bytes:
        """Store a git object body. Returns its 32-byte BLAKE3 id."""

        def build(req: object) -> None:
            wo = req.init("writeObject")
            wo.gitType = git_type
            wo.content = bytes(content)

        with self._call(build) as resp:
            self._expect(resp, "objectId")
            return bytes(resp.objectId.bytes)

    def read_object(self, object_id: bytes) -> bytes:
        """Read an object's content by its 32-byte id."""
        _require_len(object_id, 32, "object id")

        def build(req: object) -> None:
            ro = req.init("readObject")
            ro.id.bytes = bytes(object_id)

        with self._call(build) as resp:
            self._expect(resp, "objectContent")
            return bytes(resp.objectContent)

    def fork(self, sources: Sequence[str], ttl_seconds: int = 0) -> WorkspaceInfo:
        """Fork a fresh workspace seeded from ``sources`` (durable ref names).

        Leased for ``ttl_seconds`` (0 = server default). Returns the new view.
        """

        def build(req: object) -> None:
            f = req.init("fork")
            lst = f.init("sources", len(sources))
            for i, s in enumerate(sources):
                lst[i] = s
            f.ttlSeconds = int(ttl_seconds)

        with self._call(build) as resp:
            self._expect(resp, "workspace")
            return _decode_workspace(resp.workspace)

    def commit(
        self, workspace_id: str, mappings: Sequence[CommitMapping]
    ) -> List[CommitOutcome]:
        """Promote workspace refs to durable refs. One outcome per mapping, in order."""

        def build(req: object) -> None:
            c = req.init("commit")
            c.workspaceId = workspace_id
            lst = c.init("mappings", len(mappings))
            for i, m in enumerate(mappings):
                lst[i].workspaceRef = m.workspace_ref
                lst[i].durableRef = m.durable_ref

        with self._call(build) as resp:
            self._expect(resp, "commitOutcomes")
            return [_decode_commit_outcome(o) for o in resp.commitOutcomes]

    def renew(self, workspace_id: str, ttl_seconds: int = 0) -> Lease:
        """Extend a workspace's lease by ``ttl_seconds`` (0 = server default)."""

        def build(req: object) -> None:
            r = req.init("renew")
            r.workspaceId = workspace_id
            r.ttlSeconds = int(ttl_seconds)

        with self._call(build) as resp:
            self._expect(resp, "lease")
            lease = resp.lease
            return Lease(
                id=lease.id,
                expires_at_ms=lease.expiresAtMs,
                created_at_ms=lease.createdAtMs,
                generation=lease.generation,
            )

    def release(self, workspace_id: str) -> None:
        """Release a workspace and its lease."""

        def build(req: object) -> None:
            r = req.init("release")
            r.workspaceId = workspace_id

        with self._call(build) as resp:
            self._expect(resp, "ok")

    def get_workspace(self, workspace_id: str) -> WorkspaceInfo:
        """Fetch a point-in-time view of a workspace by id."""

        def build(req: object) -> None:
            g = req.init("getWorkspace")
            g.workspaceId = workspace_id

        with self._call(build) as resp:
            self._expect(resp, "workspace")
            return _decode_workspace(resp.workspace)

    def list_workspaces(self) -> List[WorkspaceInfo]:
        """List every live workspace."""

        def build(req: object) -> None:
            req.listWorkspaces = None

        with self._call(build) as resp:
            self._expect(resp, "workspaceList")
            return [_decode_workspace(w) for w in resp.workspaceList]

    def run_gc(self) -> GcStats:
        """Run one mark-and-sweep garbage-collection pass; returns its accounting."""

        def build(req: object) -> None:
            req.runGc = None

        with self._call(build) as resp:
            self._expect(resp, "gcStats")
            return _decode_gc_stats(resp.gcStats)

    # transport ---------------------------------------------------------------

    def _call(self, build: Callable[[object], None]):
        """Encode a ``Request`` via ``build``, POST it, return a Response reader.

        Returns a context manager yielding the decoded ``Response`` reader; the
        caller must extract owned values inside the ``with`` block because the
        reader pins the underlying receive buffer. Raises
        :class:`LedgeTransportError` on a non-2xx status; the ``error`` variant is
        surfaced by :meth:`_expect` (or below, if the caller does not check it).
        """
        req = _ledge.Request.new_message()
        build(req)
        body = req.to_bytes()  # unpacked framed, matching Rust `capnp::serialize`.

        http_req = urllib.request.Request(
            self._rpc_url,
            data=body,
            method="POST",
            headers={"Content-Type": _CONTENT_TYPE},
        )
        try:
            with urllib.request.urlopen(http_req, timeout=self._timeout) as http_resp:
                resp_body = http_resp.read()
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8", "replace")
            except Exception:  # pragma: no cover - best-effort error body
                pass
            raise LedgeTransportError(
                f"POST {self._rpc_url} -> HTTP {e.code}"
                + (f": {detail}" if detail else ""),
                e.code,
            ) from e

        # `from_bytes` reads the unpacked framing the server wrote.
        return _ledge.Response.from_bytes(resp_body)

    @staticmethod
    def _expect(resp: object, want: str) -> None:
        """Assert the response union tag, surfacing a server ``error`` if present."""
        got = resp.which()
        if got == want:
            return
        if got == "error":
            raise LedgeRpcError(resp.error)
        raise LedgeRpcError(f"unexpected response variant {got!r} (wanted {want!r})")


# ── decode helpers (capnp readers -> plain owned values) ────────────────────


def _decode_workspace(w: object) -> WorkspaceInfo:
    return WorkspaceInfo(
        id=w.id,
        expires_at_ms=w.expiresAtMs,
        refs=[
            NamedRef(
                name=nr.name,
                entry=RefEntry(
                    target=bytes(nr.entry.target.bytes),
                    hlc=nr.entry.hlc,
                    version=nr.entry.version,
                ),
            )
            for nr in w.refs
        ],
    )


def _decode_commit_outcome(o: object) -> CommitOutcome:
    return CommitOutcome(target=o.target, ok=o.ok, version=o.version)


def _decode_gc_stats(g: object) -> GcStats:
    return GcStats(
        scanned=g.scanned,
        reachable=g.reachable,
        reclaimed=g.reclaimed,
        bytes_freed=g.bytesFreed,
    )


def _require_len(buf: Union[bytes, bytearray], length: int, what: str) -> None:
    if len(buf) != length:
        raise ValueError(f"{what} must be {length} bytes, got {len(buf)}")
