//! `ledge-rpc` — the Cap'n Proto wire contract and request dispatch core for the
//! Ledge control plane's binary `POST /rpc` endpoint (Phase 2b, Tier 1).
//!
//! [`dispatch`] decodes a capnp [`ledge_capnp::request`], invokes the matching
//! object-store / workspace-manager / GC operation behind an [`RpcCtx`], and
//! encodes a capnp [`ledge_capnp::response`]. Business errors (unknown
//! workspace, commit conflict, missing object) are encoded into the
//! `Response.error` / `Response.commitOutcomes` variants — `dispatch` only
//! returns `Err` for a genuinely malformed message that cannot be decoded.

// `deny` (not `forbid`) so the generated capnp module can locally re-permit any
// `unsafe` its emitted code needs; all hand-written code in this crate stays
// `unsafe`-free.
#![deny(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use ledge_core::RefStore;
use ledge_object_store::DiskObjectStore;
use ledge_workspace::{Gc, WorkspaceManager};

/// Generated Cap'n Proto types for `sdk/schema/ledge.capnp`. The capnpc-emitted
/// code is the only `unsafe`-containing code in this crate (we `#![deny]` it —
/// `deny` not `forbid`, so this generated module can scope an
/// `#[allow(unsafe_code)]`); it is generated, reviewed-by-construction code.
#[allow(clippy::all)]
#[allow(unsafe_code)]
pub mod ledge_capnp {
    include!(concat!(env!("OUT_DIR"), "/ledge_capnp.rs"));
}

mod dispatch;
pub use dispatch::{dispatch, method_name};

/// Shared handles the dispatcher needs to service every request variant.
///
/// Cloned cheaply (all fields are `Arc`); one is built per `/rpc` call from the
/// server's `AppState`.
#[derive(Clone)]
pub struct RpcCtx {
    /// Concrete object store: the dispatcher's `writeObject` path needs
    /// `DiskObjectStore::write_git_object` (the git type-tagged write), which is
    /// not on the [`ledge_core::ObjectStore`] trait. In cluster mode this is the
    /// node-local disk store (the git/object wire path is node-local in Phase 3).
    pub objects: Arc<DiskObjectStore>,
    /// Ref store seam: `Arc<dyn RefStore>` so the clustered `ClusterRefStore`
    /// slots in unchanged. (The dispatcher reaches refs only via `workspaces`;
    /// this handle is carried for symmetry / future direct-ref RPCs.)
    pub refs: Arc<dyn RefStore>,
    pub workspaces: Arc<WorkspaceManager>,
    pub gc: Arc<Gc>,
    /// Fallback TTL (seconds) applied when a `fork` request sends `ttlSeconds == 0`.
    pub default_ttl_secs: u64,
    /// The caller's tenant (Phase 4d-2 spec §3.4). A plain `String` — NOT the
    /// server's `Principal` — so `ledge-rpc` gains no `ledge-server` dependency
    /// (which would close a crate cycle). Threaded into every workspace op so the
    /// RPC path enforces the SAME per-tenant isolation as REST. `"root"` (or "")
    /// is the global/legacy namespace.
    pub tenant_id: String,
}

impl RpcCtx {
    /// Resolve a request TTL: `0` means "use the configured default".
    fn resolve_ttl(&self, ttl_seconds: u64) -> Duration {
        let secs = if ttl_seconds == 0 {
            self.default_ttl_secs
        } else {
            ttl_seconds
        };
        Duration::from_secs(secs)
    }
}
