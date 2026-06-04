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

use crate::forward::{ClusterOp, LocalApplier, RefOpForwarder, RefOpResponse};
use crate::router::{ShardId, ShardRouter, ShardSpan};
use crate::shard_map::ShardMap;

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

/// Forwarder used when a store is built without placement (Phase-3 in-process
/// harness / single-node). It is never invoked because every shard is locally
/// hosted; if it ever is, that is a configuration bug → `Unavailable`.
struct RejectAllForwarder;

#[async_trait]
impl RefOpForwarder for RejectAllForwarder {
    async fn forward(&self, shard: ShardId, _op: ClusterOp) -> Result<RefOpResponse> {
        Err(infra(format!(
            "shard {shard:?} not locally hosted and no forwarder configured"
        )))
    }
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
    /// LOCALLY-HOSTED shards only (an absent shard ⇒ not hosted ⇒ forward).
    shards: BTreeMap<ShardId, Vec<ShardHandle>>,
    mode: ConsistencyMode,
    /// The placement map (for "do I host this shard?" + forward-target choice).
    /// `Default` (empty) in single-node / Phase-3 in-process mode, where every
    /// shard is present in `shards` so the forward branch is never taken.
    map: ShardMap,
    /// Forwarder for non-locally-hosted shards. Defaults to a reject-all stub so
    /// a store built without placement (Phase-3 harness) behaves exactly as
    /// before for its locally-present shards.
    forwarder: Arc<dyn RefOpForwarder>,
}

impl ClusterRefStore {
    /// Construct over a node's view of the cluster. Defaults to linearizable
    /// reads, an empty placement map, and a reject-all forwarder — i.e. the
    /// Phase-3 behavior where `shards` holds every shard and no op is ever
    /// forwarded. Use [`with_placement`](Self::with_placement) to enable the
    /// remote path.
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
            map: ShardMap::default(),
            forwarder: Arc::new(RejectAllForwarder),
        }
    }

    /// Construct with placement: a LOCAL-ONLY handle map (absent shards are
    /// forwarded), the cluster's `ShardMap`, and a `forwarder` for remote shards.
    /// This is the production / multi-host constructor (`build_cluster_stack`)
    /// and the in-memory forwarding test's constructor.
    pub fn with_placement(
        node_id: u64,
        router: ShardRouter,
        shards: BTreeMap<ShardId, Vec<ShardHandle>>,
        map: ShardMap,
        forwarder: Arc<dyn RefOpForwarder>,
    ) -> Self {
        Self {
            node_id,
            router,
            shards,
            mode: ConsistencyMode::Linearizable,
            map,
            forwarder,
        }
    }

    /// Attach a placement map + forwarder, enabling the remote-shard path for
    /// shards absent from the local handle map (builder-style).
    pub fn with_forwarder(mut self, forwarder: Arc<dyn RefOpForwarder>, map: ShardMap) -> Self {
        self.forwarder = forwarder;
        self.map = map;
        self
    }

    /// Override the read consistency mode (builder-style).
    pub fn with_mode(mut self, mode: ConsistencyMode) -> Self {
        self.mode = mode;
        self
    }

    /// Whether THIS node hosts `shard` (it has a local handle for it). Public so
    /// the [`crate::txn::TxnResolver`] can gate coordinator-record finalization on
    /// local coord-shard ownership.
    pub fn hosts_locally(&self, shard: ShardId) -> bool {
        self.shards.contains_key(&shard)
    }

    /// The placement map this store was built with (introspection / status).
    pub fn map(&self) -> &ShardMap {
        &self.map
    }

    /// This node's id. Needed by the `/cluster/ref-op` handler to ask the shard
    /// map "do I (this node) host the target shard?" before applying locally.
    pub fn node_id(&self) -> u64 {
        self.node_id
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
        if !self.hosts_locally(shard) {
            return match self
                .forwarder
                .forward(shard, ClusterOp::Get { name: name.as_str().to_string() })
                .await?
            {
                RefOpResponse::Entry(e) => Ok(e),
                other => Err(infra(format!("unexpected forward resp for get: {other:?}"))),
            };
        }
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
        if !self.hosts_locally(shard) {
            // Remote shard: ship target+expected; the HOST assigns the HLC.
            let resp = self
                .forwarder
                .forward(
                    shard,
                    ClusterOp::Update {
                        name: name.as_str().to_string(),
                        target_bytes: *new.as_bytes(),
                        expected_bytes: expected.map(|e| *e.as_bytes()),
                    },
                )
                .await?;
            return match resp {
                RefOpResponse::Updated(e) => Ok(e),
                RefOpResponse::Conflict(c) => Err(LedgeError::Conflict { current: c }),
                RefOpResponse::NotFound => Err(LedgeError::NotFound(new)),
                other => Err(infra(format!("unexpected forward resp for update: {other:?}"))),
            };
        }
        // Local shard: existing Phase-3 fast path (HLC ticked on the leader at
        // propose time — spec §2.3 — the value travels in the op).
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
        if !self.hosts_locally(shard) {
            let resp = self
                .forwarder
                .forward(
                    shard,
                    ClusterOp::Delete {
                        name: name.as_str().to_string(),
                        expected_bytes: *expected.as_bytes(),
                    },
                )
                .await?;
            return match resp {
                RefOpResponse::Deleted => Ok(()),
                RefOpResponse::Conflict(c) => Err(LedgeError::Conflict { current: c }),
                RefOpResponse::NotFound => Err(LedgeError::NotFound(expected)),
                other => Err(infra(format!("unexpected forward resp for delete: {other:?}"))),
            };
        }
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
        // SCOPE (spec §10): `list`/`snapshot` read only the shards THIS node hosts.
        // Cross-shard fan-out (forward `ClusterOp::List` to non-hosted shards and
        // merge) is DEFERRED — it is not wired in Phase 4a. In practice the blast
        // radius is nil: workspace/ref prefixes route to a single shard, so a
        // single-tenant `list` already resolves to one (locally-or-remotely-owned)
        // shard; only a broad multi-shard `list` on a partial-placement node sees a
        // subset. Forward-and-merge lands with the cross-shard work in 4b.
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

impl ClusterRefStore {
    /// Apply a shard-targeted op on `shard` from THIS node's vantage: locally if
    /// hosted, else forwarded. The unified seam the `TxnCoordinator` drives
    /// prepare/commit/abort/txn-status through without caring where the shard
    /// lives (spec §4.3).
    pub async fn op_on_shard(&self, shard: ShardId, op: ClusterOp) -> Result<RefOpResponse> {
        if self.hosts_locally(shard) {
            self.apply_local_op(shard, op).await
        } else {
            self.forwarder.forward(shard, op).await
        }
    }

    /// Apply a coordinator-shard txn-record op (`TxnBegin`/`TxnDecide`/`TxnEnd`)
    /// on the LOCALLY-hosted coordinator shard's leader. Errors if this node does
    /// not host `coord_shard` (the coordinator must run where it hosts the coord
    /// shard — guaranteed by 4a placement; spec §3.1). Returns the applied resp.
    ///
    /// These ops carry no HLC: `TxnBegin`/`TxnEnd` are pure record lifecycle and
    /// `TxnDecide` is a boolean decision, so there is nothing to stamp.
    pub async fn apply_txn_record_op(
        &self,
        coord_shard: ShardId,
        op: LedgeOp,
    ) -> Result<LedgeResp> {
        if !self.hosts_locally(coord_shard) {
            return Err(infra(format!(
                "coordinator must host coord shard {coord_shard:?}"
            )));
        }
        self.client_write_routed(coord_shard, op).await
    }

    /// Enumerate prepared 2PC locks across every LOCALLY-hosted shard, reading
    /// each shard's leader SM under a linearizable barrier so the resolver sees
    /// committed lock state (spec §3.4). Returns `(shard, [(name, intent)])` only
    /// for shards that currently hold at least one lock. Backs [`TxnResolver`].
    pub async fn prepared_locks_by_shard(
        &self,
    ) -> Result<Vec<(ShardId, Vec<(String, ledge_raft::PreparedIntent)>)>> {
        let mut out = Vec::new();
        // Snapshot the shard ids first (avoid holding a borrow across `.await`).
        let shards: Vec<ShardId> = self.shards.keys().copied().collect();
        for shard in shards {
            let leader = self.leader_of(shard).await?;
            leader
                .raft
                .ensure_linearizable()
                .await
                .map_err(|e| infra(format!("ensure_linearizable: {e}")))?;
            let locks = leader.sm.prepared_locks();
            if !locks.is_empty() {
                out.push((shard, locks));
            }
        }
        Ok(out)
    }

    /// The shard-local HLC source for `shard` (for generating a txn id on the
    /// coordinator shard). Errors if the shard is not locally hosted.
    pub fn hlc_for(&self, shard: ShardId) -> Result<&Arc<HLC>> {
        let reps = self.replicas(shard)?;
        // Prefer this node's own replica (single-ownership in production); any
        // replica's HLC works in-process since the value travels in the op.
        reps.iter()
            .find(|r| r.node_id == self.node_id)
            .or_else(|| reps.first())
            .map(|h| &h.hlc)
            .ok_or_else(|| infra(format!("no hlc for shard {shard:?}")))
    }

    /// Apply a single-shard atomic `RefBatch` (spec §3.5) on `shard`. The host
    /// leader stamps each op's HLC at apply (deterministic: the value travels in
    /// the log entry). Returns the per-ref outcomes in input order. Errors if the
    /// shard is not locally hosted (remote single-shard batch forwarding is not
    /// exercised under the 4a coordinator placement, where the single mapped
    /// shard is always local).
    pub async fn apply_batch_on_shard(
        &self,
        shard: ShardId,
        ops: Vec<(String, [u8; 32], Option<[u8; 32]>)>,
    ) -> Result<Vec<ledge_raft::BatchOutcome>> {
        if !self.hosts_locally(shard) {
            return Err(infra(
                "remote single-shard batch forwarding not yet wired",
            ));
        }
        let leader = self.leader_of(shard).await?;
        // Build the wire batch, stamping a monotonically increasing HLC per op.
        let batch: Vec<ledge_raft::BatchOp> = ops
            .into_iter()
            .map(|(name, target, expected)| ledge_raft::BatchOp {
                name,
                target,
                expected,
                hlc: leader.hlc.tick(),
            })
            .collect();
        match self
            .client_write_routed(shard, LedgeOp::RefBatch { ops: batch })
            .await?
        {
            LedgeResp::BatchResult(v) => Ok(v),
            other => Err(infra(format!("unexpected resp for batch: {other:?}"))),
        }
    }

    /// Apply an ALREADY shard-targeted op via the LOCAL shard handle (no
    /// re-routing): the entry point for the in-memory forwarder and the
    /// `/cluster/ref-op` HTTP handler. Errors if this node does not host
    /// `shard` (the caller misdirected the op — spec §4.4).
    pub async fn apply_local_op(&self, shard: ShardId, op: ClusterOp) -> Result<RefOpResponse> {
        if !self.hosts_locally(shard) {
            return Err(infra(format!(
                "misdirected: shard {shard:?} not hosted here"
            )));
        }
        match op {
            ClusterOp::Update {
                name,
                target_bytes,
                expected_bytes,
            } => {
                // Leader-assigned HLC on the HOST (not pre-assigned on the
                // forwarding node — matches the local-path semantics).
                let leader = self.leader_of(shard).await?;
                let hlc = leader.hlc.tick();
                let lop = LedgeOp::RefUpdate {
                    name,
                    target_bytes,
                    expected_bytes,
                    hlc,
                };
                match self.client_write_routed(shard, lop).await? {
                    LedgeResp::RefUpdated(e) => Ok(RefOpResponse::Updated(e)),
                    LedgeResp::Conflict(c) => Ok(RefOpResponse::Conflict(c)),
                    LedgeResp::NotFound => Ok(RefOpResponse::NotFound),
                    other => Err(infra(format!("unexpected resp for update: {other:?}"))),
                }
            }
            ClusterOp::Delete {
                name,
                expected_bytes,
            } => {
                let leader = self.leader_of(shard).await?;
                let hlc = leader.hlc.tick();
                let lop = LedgeOp::RefDelete {
                    name,
                    expected_bytes,
                    hlc,
                };
                match self.client_write_routed(shard, lop).await? {
                    LedgeResp::Deleted => Ok(RefOpResponse::Deleted),
                    LedgeResp::Conflict(c) => Ok(RefOpResponse::Conflict(c)),
                    LedgeResp::NotFound => Ok(RefOpResponse::NotFound),
                    other => Err(infra(format!("unexpected resp for delete: {other:?}"))),
                }
            }
            ClusterOp::Get { name } => {
                // Linearizable single-ref read on the host (mirror RefStore::get).
                let entry = match self.mode {
                    ConsistencyMode::Linearizable => {
                        let leader = self.leader_of(shard).await?;
                        leader
                            .raft
                            .ensure_linearizable()
                            .await
                            .map_err(|e| infra(format!("ensure_linearizable: {e}")))?;
                        leader.sm.applied_ref(&name).await
                    }
                    ConsistencyMode::Stale => self.local_handle(shard)?.sm.applied_ref(&name).await,
                };
                Ok(RefOpResponse::Entry(entry))
            }
            ClusterOp::List { prefix } => {
                let refs = self.read_refs(shard, &prefix).await?;
                Ok(RefOpResponse::Refs(
                    refs.into_iter()
                        .map(|(n, e)| (n.as_str().to_string(), e))
                        .collect(),
                ))
            }
            ClusterOp::Prepare {
                txn_id,
                coord_shard,
                name,
                target_bytes,
                expected_bytes,
            } => {
                // Leader-assigned staged HLC on the HOST (like Update). The lock
                // + vote are computed deterministically in the SM apply; the HLC
                // travels in the op so every replica stages the identical value.
                let leader = self.leader_of(shard).await?;
                let hlc = leader.hlc.tick();
                let lop = LedgeOp::RefPrepare {
                    txn_id,
                    coord_shard,
                    name,
                    target: target_bytes,
                    expected: expected_bytes,
                    hlc,
                };
                match self.client_write_routed(shard, lop).await? {
                    LedgeResp::Vote(v) => Ok(RefOpResponse::Vote(v)),
                    other => Err(infra(format!("unexpected resp for prepare: {other:?}"))),
                }
            }
            ClusterOp::CommitPrepared { txn_id, name } => {
                let lop = LedgeOp::RefCommitPrepared { txn_id, name };
                match self.client_write_routed(shard, lop).await? {
                    // The SM rolls the staged value forward and returns the new
                    // committed entry (or the idempotent current committed).
                    LedgeResp::CommittedPrepared(e) => Ok(RefOpResponse::CommittedPrepared(e)),
                    // BENIGN IDEMPOTENT ACK (resolver foundation, spec §3.4): the
                    // SM returns `AbortedPrepared` from a CommitPrepared apply when
                    // this txn's prepared lock has already vanished — the slot was
                    // removed by a prior AbortPrepared / GC, or the ref was already
                    // resolved by an earlier apply. That is NOT an error: a
                    // duplicate / already-resolved CommitPrepared is a no-op. We
                    // surface it as `AbortedPrepared` so resolver/coordinator
                    // retries are safe (idempotent re-resolve). Callers that want
                    // the rolled-forward entry already got it on the first apply.
                    LedgeResp::AbortedPrepared => Ok(RefOpResponse::AbortedPrepared),
                    other => {
                        Err(infra(format!("unexpected resp for commit-prepared: {other:?}")))
                    }
                }
            }
            ClusterOp::AbortPrepared { txn_id, name } => {
                let lop = LedgeOp::RefAbortPrepared { txn_id, name };
                match self.client_write_routed(shard, lop).await? {
                    // AbortPrepared just releases the lock; a vanished/already-
                    // cleared lock is the idempotent no-op (the SM's store-level
                    // apply always returns `AbortedPrepared`, whether it released a
                    // lock or found none). This makes presumed-abort retries safe.
                    LedgeResp::AbortedPrepared => Ok(RefOpResponse::AbortedPrepared),
                    other => Err(infra(format!("unexpected resp for abort-prepared: {other:?}"))),
                }
            }
            ClusterOp::TxnStatus {
                txn_id,
                coord_shard: _,
            } => {
                // The decision lives in THIS shard's txn-record SM (the caller
                // already routed to coord_shard). Linearizable read on the leader.
                let entry = match self.mode {
                    ConsistencyMode::Linearizable => {
                        let leader = self.leader_of(shard).await?;
                        leader
                            .raft
                            .ensure_linearizable()
                            .await
                            .map_err(|e| infra(format!("ensure_linearizable: {e}")))?;
                        leader.sm.txn_decision(txn_id)
                    }
                    ConsistencyMode::Stale => self.local_handle(shard)?.sm.txn_decision(txn_id),
                };
                Ok(RefOpResponse::TxnDecisionResp(entry))
            }
        }
    }
}

/// `Arc`-wrapped [`LocalApplier`] so a store can be handed to the in-memory
/// forwarder registry / the HTTP handler as `Arc<dyn LocalApplier>`. The wrapped
/// store applies ops directly to its local shard handles (it never forwards).
pub struct StoreApplier(pub Arc<ClusterRefStore>);

#[async_trait]
impl LocalApplier for StoreApplier {
    async fn apply_local(&self, shard: ShardId, op: ClusterOp) -> Result<RefOpResponse> {
        self.0.apply_local_op(shard, op).await
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
