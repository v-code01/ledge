//! `ClusterRefStore` + `ClusterLeaseStore` — the §2.2 seam over sharded Raft.
//!
//! Both stores route every mutation through a per-shard Raft group (selected by
//! the [`ShardRouter`]) and serve reads from applied state. `ClusterRefStore`
//! implements [`ledge_core::RefStore`] so the server/workspace/RPC layers that
//! depend on `Arc<dyn RefStore>` work unchanged whether storage is single-node
//! or clustered.
//!
//! # openraft 0.9.24 (verified against the resolved crate source)
//! - `client_write(app_data) -> Result<ClientWriteResponse<C>, RaftError<.., ClientWriteError<..>>>`;
//!   the app response is `ClientWriteResponse.data` (`= LedgeResp`). `client_write`
//!   is generic over the responder error `E`; we pin it to `Infallible` via the
//!   typed `client_write_op` wrapper below.
//! - `ensure_linearizable()` takes **no arguments** and returns
//!   `Result<Option<LogId>, RaftError<.., CheckIsLeaderError>>`. It only succeeds
//!   on the leader, so strong reads route through `leader_of` just like writes.
//! - `RaftError::forward_to_leader() -> Option<&ForwardToLeader>` lets us detect
//!   that a write landed on a follower.
//! - `Raft::metrics().borrow().current_leader: Option<NodeId>` gives the elected
//!   leader for leader discovery (V4).
//!
//! # Production note (in-process registry vs RPC forward)
//! The in-process harness gives the cluster store handles to *all* replicas of a
//! shard so `leader_of` can call the leader's `Raft` handle directly. In a real
//! multi-host cluster a node holds only its own replica plus a peer address
//! table; `ForwardToLeader { leader_node }` is resolved by re-issuing the write
//! over the HTTP `RaftNetwork` to `leader_node.addr` (Task 6). The semantics are
//! identical (the write always lands on the leader); only the transport differs.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use ledge_core::{
    HLC, LedgeError, ObjectId, RefEntry, RefName, RefSnapshot, RefStore, Result,
};
use ledge_raft::{LedgeOp, LedgeResp, ReadHandle, TypeConfig};
use ledge_workspace::{id::WorkspaceId, lease::Lease};
use openraft::Raft;

use crate::router::{ShardId, ShardRouter, ShardSpan};

/// Raft handle type for one shard replica.
type RaftHandle = Raft<TypeConfig>;

/// One shard replica reachable from THIS node: its Raft handle, a read handle
/// onto its applied state, the shard-local HLC source, and its node id.
///
/// PRODUCTION: a node holds only its own replica here; peers are reached by RPC.
/// In the in-process harness the cluster store holds every replica of a shard so
/// [`ClusterRefStore::leader_of`] can pick the leader's handle locally.
#[derive(Clone)]
pub struct ShardHandle {
    /// Which shard this replica belongs to.
    pub shard: ShardId,
    /// The node id hosting this replica (used for leader matching / local reads).
    pub node_id: u64,
    /// The replica's Raft handle.
    pub raft: RaftHandle,
    /// Read-only view of this replica's applied state (refs + leases).
    pub sm: ReadHandle,
    /// Shard-local HLC; the leader ticks it at propose time (the value travels
    /// in the op so every replica applies the identical timestamp).
    pub hlc: Arc<HLC>,
}

/// Read consistency for `get`/`list`/lease reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsistencyMode {
    /// `ensure_linearizable()` on the shard leader, then read the leader's
    /// applied SM. The default: a strong, cluster-linearizable read.
    Linearizable,
    /// Read THIS node's local applied SM without a linearizability barrier —
    /// cheap, possibly stale, for read-heavy callers that tolerate it.
    Stale,
}

/// Maximum leader-discovery polling attempts before giving up.
const LEADER_POLL_ATTEMPTS: usize = 50;

/// Transient infrastructure fault → `LedgeError::Unavailable` (retryable).
///
/// Every fault funnelled through here is an *availability* failure, not a data
/// integrity failure: no shard leader elected yet, an unreachable shard/peer, a
/// failed linearizability barrier, or a transient Raft `client_write` error. The
/// data is intact, so the caller must learn this is retryable (→ HTTP 503) and
/// must NOT mistake it for [`LedgeError::Corruption`], which signals a fatal,
/// non-retryable integrity failure.
fn infra(msg: impl Into<String>) -> LedgeError {
    LedgeError::Unavailable(msg.into())
}

/// Resolve the current leader handle of `shard` from a replica set, polling the
/// metrics watch with bounded backoff until a leader is observed.
///
/// PRODUCTION: replaced by reading `ForwardToLeader.leader_node.addr` and
/// forwarding the RPC. Here every replica is locally reachable so we return the
/// leader's `&ShardHandle` directly.
async fn leader_handle(replicas: &[ShardHandle], shard: ShardId) -> Result<&ShardHandle> {
    for attempt in 0..LEADER_POLL_ATTEMPTS {
        for h in replicas {
            // V4: `current_leader: Option<NodeId>` on RaftMetrics in 0.9.24.
            // Match the Option directly: `Some(id)` is a real elected leader for
            // ANY id value, `None` means no leader yet. (The old code aliased
            // `Some(0)` to "no leader" via `unwrap_or(&0)` + `!= 0`, conflating a
            // node whose id is 0 with the no-leader case.)
            if let Some(leader) = h.raft.metrics().borrow().current_leader {
                if let Some(lead) = replicas.iter().find(|r| r.node_id == leader) {
                    // A `current_leader` pointer can be STALE: after the prior
                    // leader crashes, a lagging follower (or the crashed node's own
                    // frozen metrics) may still name the dead node. Only trust the
                    // pointer if the named replica itself confirms it is the leader
                    // — a crashed / deposed node never reports `Leader`. This keeps
                    // failover deterministic (no write/read landing on a corpse) and
                    // is strictly correct in production too (the resolved leader must
                    // be live to accept the write).
                    if lead.raft.metrics().borrow().state == openraft::ServerState::Leader {
                        return Ok(lead);
                    }
                }
            }
        }
        // Bounded backoff: caps at ~110ms/iteration; total < ~5s worst case.
        let backoff = 10 * (attempt.min(10) as u64 + 1);
        tokio::time::sleep(Duration::from_millis(backoff)).await;
    }
    Err(infra(format!("shard {shard:?}: no leader elected")))
}

/// Propose `op` on `leader`'s Raft, returning the applied `LedgeResp`.
///
/// `client_write` is generic over the responder receiver error `E`; for the
/// oneshot responder openraft uses, `E = tokio::sync::oneshot::error::RecvError`.
/// We name it explicitly so type inference is deterministic at the call site
/// (V1: the app response is `ClientWriteResponse.data`).
async fn client_write_op(
    leader: &ShardHandle,
    op: LedgeOp,
) -> std::result::Result<LedgeResp, ClientWriteErr> {
    leader
        .raft
        .client_write::<tokio::sync::oneshot::error::RecvError>(op)
        .await
        .map(|resp| resp.data)
        .map_err(ClientWriteErr)
}

/// Newtype over the openraft client-write error so we can inspect
/// `ForwardToLeader` without leaking openraft types into the public API.
struct ClientWriteErr(
    openraft::error::RaftError<u64, openraft::error::ClientWriteError<u64, ledge_raft::Node>>,
);

impl ClientWriteErr {
    /// True if the write landed on a follower (must be retried on the leader).
    fn is_forward_to_leader(&self) -> bool {
        self.0.forward_to_leader().is_some()
    }
}

impl std::fmt::Display for ClientWriteErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Clustered `RefStore`: routes mutations through per-shard Raft, serves reads
/// from applied state. Implements [`ledge_core::RefStore`] (the §2.2 seam).
///
/// `shards` holds *all* replicas of each shard reachable in this process so
/// `leader_of` can address the leader (in-process registry). In production this
/// would be a single own-replica per shard plus a peer table (see module docs).
pub struct ClusterRefStore {
    node_id: u64,
    router: ShardRouter,
    shards: BTreeMap<ShardId, Vec<ShardHandle>>,
    mode: ConsistencyMode,
}

impl ClusterRefStore {
    /// Construct over a node's view of the cluster. Defaults to linearizable reads.
    pub fn new(
        node_id: u64,
        router: ShardRouter,
        shards: BTreeMap<ShardId, Vec<ShardHandle>>,
    ) -> Self {
        Self {
            node_id,
            router,
            shards,
            mode: ConsistencyMode::Linearizable,
        }
    }

    /// Override the read consistency mode (builder-style).
    pub fn with_mode(mut self, mode: ConsistencyMode) -> Self {
        self.mode = mode;
        self
    }

    /// The router this store partitions through (for tests / introspection).
    pub fn router(&self) -> &ShardRouter {
        &self.router
    }

    fn replicas(&self, shard: ShardId) -> Result<&[ShardHandle]> {
        self.shards
            .get(&shard)
            .map(|v| v.as_slice())
            .ok_or_else(|| infra(format!("unknown shard {shard:?}")))
    }

    /// Resolve the current leader handle for `shard` (in-process registry).
    async fn leader_of(&self, shard: ShardId) -> Result<&ShardHandle> {
        leader_handle(self.replicas(shard)?, shard).await
    }

    /// This node's local replica of `shard` (for stale reads / local snapshot).
    fn local_handle(&self, shard: ShardId) -> Result<&ShardHandle> {
        self.replicas(shard)?
            .iter()
            .find(|r| r.node_id == self.node_id)
            .ok_or_else(|| infra(format!("no local replica of shard {shard:?}")))
    }

    /// Single chokepoint for mutations: lands the write on the leader, surviving
    /// `ForwardToLeader` and a mid-call leader change (spec §4 leader-failure).
    async fn client_write_routed(&self, shard: ShardId, op: LedgeOp) -> Result<LedgeResp> {
        let leader = self.leader_of(shard).await?;
        match client_write_op(leader, op.clone()).await {
            Ok(resp) => Ok(resp),
            Err(e) if e.is_forward_to_leader() => {
                // Leader moved between leader_of() and the call: re-resolve once
                // (waiting for a fresh leader if needed) and retry.
                let leader = self.leader_of(shard).await?;
                client_write_op(leader, op)
                    .await
                    .map_err(|e| infra(format!("client_write after forward: {e}")))
            }
            Err(e) => Err(infra(format!("raft client_write: {e}"))),
        }
    }

    /// Read applied refs for `shard`, honoring the consistency mode. For
    /// `Linearizable`, run `ensure_linearizable()` on the leader then read the
    /// leader's SM; for `Stale`, read the local replica without a barrier.
    async fn read_refs(&self, shard: ShardId, prefix: &str) -> Result<Vec<(RefName, RefEntry)>> {
        match self.mode {
            ConsistencyMode::Linearizable => {
                let leader = self.leader_of(shard).await?;
                leader
                    .raft
                    .ensure_linearizable()
                    .await
                    .map_err(|e| infra(format!("ensure_linearizable: {e}")))?;
                Ok(leader.sm.applied_refs_with_prefix(prefix).await)
            }
            ConsistencyMode::Stale => {
                let local = self.local_handle(shard)?;
                Ok(local.sm.applied_refs_with_prefix(prefix).await)
            }
        }
    }

    /// Map a `LedgeResp` from a `RefUpdate` proposal to the trait result.
    fn map_update_resp(resp: LedgeResp, target: ObjectId) -> Result<RefEntry> {
        match resp {
            LedgeResp::RefUpdated(entry) => Ok(entry),
            LedgeResp::Conflict(current) => Err(LedgeError::Conflict { current }),
            // NotFound carries the object the caller tried to install (mirrors the
            // single-node store, which reports the new target on a missing ref).
            LedgeResp::NotFound => Err(LedgeError::NotFound(target)),
            other => Err(infra(format!("unexpected resp for update: {other:?}"))),
        }
    }
}

#[async_trait]
impl RefStore for ClusterRefStore {
    async fn get(&self, name: &RefName) -> Result<Option<RefEntry>> {
        let shard = self.router.shard_for(name.as_str());
        match self.mode {
            ConsistencyMode::Linearizable => {
                let leader = self.leader_of(shard).await?;
                leader
                    .raft
                    .ensure_linearizable()
                    .await
                    .map_err(|e| infra(format!("ensure_linearizable: {e}")))?;
                Ok(leader.sm.applied_ref(name.as_str()).await)
            }
            ConsistencyMode::Stale => {
                let local = self.local_handle(shard)?;
                Ok(local.sm.applied_ref(name.as_str()).await)
            }
        }
    }

    async fn update(
        &self,
        name: &RefName,
        new: ObjectId,
        expected: Option<ObjectId>,
    ) -> Result<RefEntry> {
        let shard = self.router.shard_for(name.as_str());
        // HLC is ticked on the LEADER at propose time (spec §2.3): the proposing
        // node is the timestamp source, and the value travels in the op.
        let leader = self.leader_of(shard).await?;
        let hlc = leader.hlc.tick();
        let op = LedgeOp::RefUpdate {
            name: name.as_str().to_string(),
            target_bytes: *new.as_bytes(),
            expected_bytes: expected.map(|e| *e.as_bytes()),
            hlc,
        };
        let resp = self.client_write_routed(shard, op).await?;
        Self::map_update_resp(resp, new)
    }

    async fn delete(&self, name: &RefName, expected: ObjectId) -> Result<()> {
        let shard = self.router.shard_for(name.as_str());
        let leader = self.leader_of(shard).await?;
        let hlc = leader.hlc.tick();
        let op = LedgeOp::RefDelete {
            name: name.as_str().to_string(),
            expected_bytes: *expected.as_bytes(),
            hlc,
        };
        match self.client_write_routed(shard, op).await? {
            LedgeResp::Deleted => Ok(()),
            LedgeResp::Conflict(current) => Err(LedgeError::Conflict { current }),
            LedgeResp::NotFound => Err(LedgeError::NotFound(expected)),
            other => Err(infra(format!("unexpected resp for delete: {other:?}"))),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<(RefName, RefEntry)>> {
        let shards: Vec<ShardId> = match self.router.shards_for_prefix(prefix) {
            ShardSpan::One(s) => vec![s],
            ShardSpan::All => self.shards.keys().copied().collect(),
        };
        let mut out: Vec<(RefName, RefEntry)> = Vec::new();
        for shard in shards {
            out.extend(self.read_refs(shard, prefix).await?);
        }
        // Shards are name-disjoint, so the merge is collision-free; sort for a
        // stable order and dedup defensively. Broad cross-shard list is
        // per-shard-linearizable, NOT a single global atomic snapshot.
        out.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
        out.dedup_by(|a, b| a.0 == b.0);
        Ok(out)
    }

    fn snapshot(&self) -> Arc<dyn RefSnapshot> {
        // The trait method is sync, so this reads LOCAL applied state only (no
        // `.await`, no linearizability barrier): a point-in-time, per-shard view.
        // Each shard's applied map is read through the ref store's lock-free
        // sync `snapshot()` (one atomic load), so no executor/blocking is needed.
        // Keyed by the ref's string form: `RefName` is not `Ord` (it is
        // `Arc<str>`-backed and intentionally only `Hash`/`Eq`), so a BTreeMap
        // keyed on the canonical string gives a stable sorted snapshot.
        let mut refs: BTreeMap<String, (RefName, RefEntry)> = BTreeMap::new();
        for (shard, replicas) in &self.shards {
            let h = replicas
                .iter()
                .find(|r| r.node_id == self.node_id)
                .or_else(|| replicas.first());
            if let Some(h) = h {
                for (name, entry) in h.sm.applied_ref_map_sync() {
                    // shards disjoint ⇒ no key collision
                    refs.insert(name.as_str().to_string(), (name, entry));
                }
            }
            let _ = shard;
        }
        Arc::new(MapRefSnapshot { refs })
    }
}

/// Point-in-time, map-backed [`RefSnapshot`] merging all shards' applied state.
/// Non-linearized by design (snapshots are point-in-time; the trait promises no
/// cross-shard atomicity). Keyed by the canonical ref string (RefName is not
/// `Ord`).
pub struct MapRefSnapshot {
    refs: BTreeMap<String, (RefName, RefEntry)>,
}

impl RefSnapshot for MapRefSnapshot {
    fn get(&self, name: &RefName) -> Option<RefEntry> {
        self.refs.get(name.as_str()).map(|(_, e)| e.clone())
    }

    fn list(&self, prefix: &str) -> Vec<(RefName, RefEntry)> {
        self.refs
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(_, (n, e))| (n.clone(), e.clone()))
            .collect()
    }
}

/// Clustered lease store: leases route by workspace id through the same
/// [`ShardRouter`] (D5), co-locating a workspace's lease with its refs on one
/// Raft group so the workspace lifecycle is single-shard linearizable.
pub struct ClusterLeaseStore {
    node_id: u64,
    router: ShardRouter,
    shards: BTreeMap<ShardId, Vec<ShardHandle>>,
    mode: ConsistencyMode,
}

impl ClusterLeaseStore {
    /// Construct over a node's view of the cluster. Defaults to linearizable reads.
    pub fn new(
        node_id: u64,
        router: ShardRouter,
        shards: BTreeMap<ShardId, Vec<ShardHandle>>,
    ) -> Self {
        Self {
            node_id,
            router,
            shards,
            mode: ConsistencyMode::Linearizable,
        }
    }

    fn replicas(&self, shard: ShardId) -> Result<&[ShardHandle]> {
        self.shards
            .get(&shard)
            .map(|v| v.as_slice())
            .ok_or_else(|| infra(format!("unknown shard {shard:?}")))
    }

    async fn leader_of(&self, shard: ShardId) -> Result<&ShardHandle> {
        leader_handle(self.replicas(shard)?, shard).await
    }

    fn local_handle(&self, shard: ShardId) -> Result<&ShardHandle> {
        self.replicas(shard)?
            .iter()
            .find(|r| r.node_id == self.node_id)
            .ok_or_else(|| infra(format!("no local replica of shard {shard:?}")))
    }

    async fn client_write_routed(&self, shard: ShardId, op: LedgeOp) -> Result<LedgeResp> {
        let leader = self.leader_of(shard).await?;
        match client_write_op(leader, op.clone()).await {
            Ok(resp) => Ok(resp),
            Err(e) if e.is_forward_to_leader() => {
                let leader = self.leader_of(shard).await?;
                client_write_op(leader, op)
                    .await
                    .map_err(|e| infra(format!("client_write after forward: {e}")))
            }
            Err(e) => Err(infra(format!("raft client_write: {e}"))),
        }
    }

    /// The handle to read from for `shard`, applying the linearizability barrier
    /// when in `Linearizable` mode.
    async fn read_handle_for(&self, shard: ShardId) -> Result<&ShardHandle> {
        match self.mode {
            ConsistencyMode::Linearizable => {
                let leader = self.leader_of(shard).await?;
                leader
                    .raft
                    .ensure_linearizable()
                    .await
                    .map_err(|e| infra(format!("ensure_linearizable: {e}")))?;
                Ok(leader)
            }
            ConsistencyMode::Stale => self.local_handle(shard),
        }
    }

    /// Upsert a lease, routed to the workspace's shard (co-located with its refs).
    pub async fn put(&self, lease: Lease) -> Result<()> {
        let shard = self.router.shard_for_workspace(&lease.id);
        match self
            .client_write_routed(shard, LedgeOp::LeasePut { lease })
            .await?
        {
            LedgeResp::LeaseOk => Ok(()),
            other => Err(infra(format!("unexpected resp for lease put: {other:?}"))),
        }
    }

    /// The current lease for `id`, or `None` if absent/tombstoned.
    pub async fn get(&self, id: &WorkspaceId) -> Result<Option<Lease>> {
        let shard = self.router.shard_for_workspace(id);
        let h = self.read_handle_for(shard).await?;
        Ok(h.sm.applied_lease(*id).await)
    }

    /// Tombstone the lease for `id`.
    pub async fn tombstone(&self, id: &WorkspaceId) -> Result<()> {
        let shard = self.router.shard_for_workspace(id);
        let leader = self.leader_of(shard).await?;
        let hlc = leader.hlc.tick();
        match self
            .client_write_routed(shard, LedgeOp::LeaseTombstone { id: *id, hlc })
            .await?
        {
            LedgeResp::LeaseOk => Ok(()),
            other => Err(infra(format!(
                "unexpected resp for lease tombstone: {other:?}"
            ))),
        }
    }

    /// All leases live at `now_ms` (expiry strictly after `now_ms`), across all
    /// shards (leases spread by workspace id).
    pub async fn live(&self, now_ms: u64) -> Result<Vec<Lease>> {
        let mut out = Vec::new();
        for shard in self.shards.keys().copied().collect::<Vec<_>>() {
            let h = self.read_handle_for(shard).await?;
            out.extend(h.sm.applied_leases_live(now_ms).await);
        }
        Ok(out)
    }

    /// All leases expired at `now_ms` (expiry at or before `now_ms`), across all
    /// shards.
    pub async fn expired(&self, now_ms: u64) -> Result<Vec<Lease>> {
        let mut out = Vec::new();
        for shard in self.shards.keys().copied().collect::<Vec<_>>() {
            let h = self.read_handle_for(shard).await?;
            out.extend(h.sm.applied_leases_expired(now_ms).await);
        }
        Ok(out)
    }
}
