"""Ledge Python SDK (Phase 2b, Tier 3).

A thin, typed client for the Ledge control plane over the binary
Cap'n Proto ``POST /rpc`` endpoint. The wire contract is the shared
``sdk/schema/ledge.capnp`` schema, loaded dynamically by ``pycapnp`` (no
codegen step). See :class:`ledge_sdk.client.LedgeClient`.
"""

from .client import (
    CommitMapping,
    CommitOutcome,
    GcStats,
    Lease,
    LedgeClient,
    LedgeRpcError,
    LedgeTransportError,
    NamedRef,
    RefEntry,
    WorkspaceInfo,
)

__all__ = [
    "CommitMapping",
    "CommitOutcome",
    "GcStats",
    "Lease",
    "LedgeClient",
    "LedgeRpcError",
    "LedgeTransportError",
    "NamedRef",
    "RefEntry",
    "WorkspaceInfo",
]
