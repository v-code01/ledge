//! Cluster control-plane HTTP endpoints (Phase 3, Task 7B).
//!
//! These routes are **only meaningful in cluster mode** (`cluster.enabled`).
//! Single-node mode leaves [`AppState::raft_shards`] as `None`, so every handler
//! here short-circuits to `503 Service Unavailable` ("not clustered"). Adding
//! these routes therefore does NOT change single-node behavior — the existing
//! git/workspace/RPC routes and their tests are untouched.
//!
//! - `POST /raft/{shard}/{append|vote|snapshot}` — feed an inbound Raft RPC into
//!   the local node's per-shard Raft handle via
//!   [`ledge_cluster::net_http::handle_rpc`]. This is the server side of the
//!   Task 6 [`ledge_cluster::net_http::HttpRaftNetwork`] transport.
//! - `POST /cluster/init` — bootstrap a shard's membership (`Raft::initialize`).
//! - `GET /cluster/status` — per-shard leader/term/members/last-applied,
//!   projected from each shard's `Raft::metrics()`.

use std::collections::BTreeMap;

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::warn;

use ledge_cluster::net_http::{handle_rpc, HttpObjectPeer, RpcKind};
use ledge_cluster::{ClusterOp, ObjectPeer, Replica, ShardId, ShardMap};
use ledge_raft::{Node, NodeId};
use ledge_workspace::GcStats;

use crate::routes::{AppState, ClusterHandles};

/// 503 body for a request that hit a cluster route while running single-node.
fn not_clustered() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "cluster mode disabled (single-node): cluster endpoints are inert",
    )
        .into_response()
}

/// Borrow the per-shard Raft handle map, or `None` in single-node mode. Each
/// handler maps `None` to [`not_clustered`] (a 503). (Returns `Option`, not
/// `Result<_, Response>`, because a `Response` is a large error variant.)
fn shards(state: &AppState) -> Option<&ClusterHandles> {
    state.raft_shards.as_deref()
}

/// `POST /raft/{shard}/{kind}` — feed one Raft RPC into the local shard handle.
///
/// `kind` ∈ {`append`,`vote`,`snapshot`}. The body is the bincode of the
/// matching openraft request type; the 200 body is the bincode `WireResult`
/// envelope produced by [`handle_rpc`]. Returns:
/// - `503` if not clustered,
/// - `404` if `kind` is unknown or the shard id is not hosted here,
/// - `400` if the body cannot be decoded into the expected request type.
pub async fn raft_rpc(
    State(state): State<AppState>,
    Path((shard, kind_seg)): Path<(u32, String)>,
    body: Bytes,
) -> Response {
    let shards = match shards(&state) {
        Some(s) => s,
        None => return not_clustered(),
    };
    let kind = match RpcKind::from_path(&kind_seg) {
        Some(k) => k,
        None => return (StatusCode::NOT_FOUND, "unknown raft rpc kind").into_response(),
    };
    let shard_id = ShardId(shard);
    let raft = match shards.get(&shard_id) {
        Some(r) => r,
        None => return (StatusCode::NOT_FOUND, "shard not hosted on this node").into_response(),
    };
    match handle_rpc(shard_id, kind, &body, raft).await {
        Ok(out) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
            out,
        )
            .into_response(),
        Err(e) => {
            warn!(error = %e, shard, kind = %kind_seg, "raft rpc handler error");
            (StatusCode::BAD_REQUEST, e.to_string()).into_response()
        }
    }
}

/// `POST /cluster/init` body: bootstrap shard `shard` with `members`
/// (`{ "<node_id>": "<addr>" }`).
#[derive(Debug, Deserialize)]
pub struct InitRequest {
    pub shard: u32,
    /// node id → base URL. A `BTreeMap<String, String>` on the wire (JSON object
    /// keys are strings); parsed to `BTreeMap<NodeId, Node>` for `initialize`.
    pub members: BTreeMap<String, String>,
}

/// `POST /cluster/init` — bootstrap a shard's membership. Idempotent-ish: a
/// second init on an already-initialized shard maps the openraft
/// "already initialized" error to `409 Conflict`.
pub async fn cluster_init(State(state): State<AppState>, Json(req): Json<InitRequest>) -> Response {
    let shards = match shards(&state) {
        Some(s) => s,
        None => return not_clustered(),
    };
    let shard_id = ShardId(req.shard);
    let raft = match shards.get(&shard_id) {
        Some(r) => r,
        None => return (StatusCode::NOT_FOUND, "shard not hosted on this node").into_response(),
    };

    let mut members: BTreeMap<NodeId, Node> = BTreeMap::new();
    for (id_str, addr) in req.members {
        match id_str.parse::<NodeId>() {
            Ok(id) => {
                members.insert(id, Node::new(addr));
            }
            Err(_) => {
                return (StatusCode::BAD_REQUEST, format!("bad node id: {id_str}")).into_response();
            }
        }
    }

    match raft.initialize(members).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => {
            // A re-init of an already-bootstrapped shard is a benign conflict,
            // not a server fault: map it to 409 so init is safely retryable.
            let msg = e.to_string();
            if msg.contains("already") || msg.contains("initialize") {
                (StatusCode::CONFLICT, msg).into_response()
            } else {
                warn!(error = %msg, shard = req.shard, "cluster init failed");
                (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
            }
        }
    }
}

/// `POST /cluster/{shard}/reconfigure` body: the target voter set for `shard`.
#[derive(Debug, Deserialize)]
pub struct ReconfigureRequest {
    /// Target voter set: `node_id` (string, JSON object keys are strings) → base
    /// URL. This REPLACES the shard's current voter set (no key reshuffle — the
    /// cluster's `num_shards` is unchanged, only this shard's membership moves).
    pub members: BTreeMap<String, String>,
}

/// `POST /cluster/{shard}/reconfigure` — change a shard's replica set live.
///
/// **MUST be sent to the shard LEADER.** An openraft membership change
/// (`change_membership`) only succeeds on the leader; on a follower openraft
/// returns a `ForwardToLeader` error which [`ledge_cluster::reconfigure_shard`]
/// surfaces as `LedgeError::Unavailable`, which this handler maps to `503`. The
/// operator / live harness is responsible for POSTing to the current leader (read
/// it from `GET /cluster/status`'s `leader`).
///
/// On success it (1) drives openraft (`add_learner` → `ReplaceAllVoters`), then
/// atomically (2) swaps the live placement map for this shard so client ref
/// routing + the forwarder follow the new topology, and (3) rebuilds THIS node's
/// object-replication peer set from the new voters so object writes fan out to the
/// new members. `num_shards` is unchanged (no key reshuffle). Idempotent: a
/// reconfigure to the already-current voter set is a no-op diff.
///
/// Responses:
/// - `503` if not clustered (single-node), or if this node is not the leader /
///   the change is transiently unavailable.
/// - `404` if the shard is not hosted on this node.
/// - `400` on a bad node id or an empty target set.
/// - `200` `{shard, added, removed, voters}` on success.
pub async fn cluster_reconfigure(
    State(state): State<AppState>,
    Path(shard): Path<u32>,
    Json(req): Json<ReconfigureRequest>,
) -> Response {
    let shards = match shards(&state) {
        Some(s) => s,
        None => return not_clustered(),
    };
    let shard_id = ShardId(shard);
    let raft = match shards.get(&shard_id) {
        Some(r) => r,
        None => return (StatusCode::NOT_FOUND, "shard not hosted on this node").into_response(),
    };

    // Parse the wire `node_id` (string) → addr into a typed target voter set.
    let mut target: BTreeMap<NodeId, String> = BTreeMap::new();
    for (id_str, addr) in req.members {
        match id_str.parse::<NodeId>() {
            Ok(id) => {
                target.insert(id, addr);
            }
            Err(_) => {
                return (StatusCode::BAD_REQUEST, format!("bad node id: {id_str}")).into_response()
            }
        }
    }
    if target.is_empty() {
        return (StatusCode::BAD_REQUEST, "empty target member set").into_response();
    }

    // 1) Drive the openraft membership change (leader-only). A non-leader /
    //    transiently-unavailable shard surfaces as `Unavailable` → 503 retryable.
    let outcome = match ledge_cluster::reconfigure_shard(raft, target.clone()).await {
        Ok(o) => o,
        Err(e) => {
            warn!(error = %e, shard, "reconfigure failed");
            return (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response();
        }
    };

    // 2) Swap the live placement map for this shard so ref routing + the forwarder
    //    follow the new topology. Rebuild from the current entries, replacing only
    //    this shard's replica set; all other shards are carried unchanged.
    if let Some(refs) = &state.cluster_refs {
        let mut entries: Vec<(ShardId, Vec<Replica>)> = refs
            .map()
            .entries()
            .into_iter()
            .filter(|(s, _)| *s != shard_id)
            .collect();
        entries.push((
            shard_id,
            target
                .iter()
                .map(|(id, addr)| Replica {
                    node_id: *id,
                    addr: addr.clone(),
                })
                .collect(),
        ));
        match ShardMap::from_entries(entries) {
            Ok(new_map) => refs.swap_placement(new_map),
            Err(e) => warn!(error = %e, shard, "rebuilt placement map invalid; map not swapped"),
        }
    }

    // 3) Rebuild THIS node's object-replication peer set from the new voters (skip
    //    self; carry the cluster_secret bearer so peers' `/objects/*` INTERNAL auth
    //    passes). New voters also obtain existing objects via on-demand
    //    anti-entropy pull, so object availability does not depend on this push set.
    if let Some(objs) = &state.cluster_objects {
        let self_node = state
            .cluster_refs
            .as_ref()
            .map(|r| r.node_id())
            .unwrap_or(0);
        let client = build_internal_client(state.auth.cluster_secret.as_deref());
        let peers: Vec<std::sync::Arc<dyn ObjectPeer>> = target
            .iter()
            .filter(|(id, _)| **id != self_node)
            .map(|(_, addr)| {
                std::sync::Arc::new(HttpObjectPeer::with_client(
                    addr.clone(),
                    shard_id,
                    client.clone(),
                )) as std::sync::Arc<dyn ObjectPeer>
            })
            .collect();
        objs.set_peers(peers);
    }

    Json(serde_json::json!({
        "shard": shard,
        "added": outcome.added,
        "removed": outcome.removed,
        "voters": outcome.final_voters,
    }))
    .into_response()
}

/// A reqwest client carrying the `cluster_secret` bearer for INTERNAL peer calls
/// (object replication). NOTE (v1 limitation): does NOT carry the per-node TLS
/// client config — reconfigure-rebuilt object peers work for non-TLS clusters; a
/// TLS cluster re-derives full TLS peers on restart (threading the boot client is
/// a documented follow-on). New voters also obtain existing objects via on-demand
/// anti-entropy pull, so object availability does not depend on this push set.
fn build_internal_client(secret: Option<&str>) -> reqwest::Client {
    match secret {
        Some(s) => reqwest::header::HeaderValue::from_str(&format!("Bearer {s}"))
            .ok()
            .and_then(|val| {
                let mut h = reqwest::header::HeaderMap::new();
                h.insert(reqwest::header::AUTHORIZATION, val);
                reqwest::Client::builder().default_headers(h).build().ok()
            })
            .unwrap_or_default(),
        None => reqwest::Client::new(),
    }
}

/// `POST /cluster/ref-op` request: a shard-targeted ref op. The bincode wire
/// body is the tuple `(ShardId, ClusterOp)` exactly as [`ledge_cluster::HttpForwarder`]
/// encodes it (spec §4.4) — `ClusterOp` carries non-JSON-safe `[u8; 32]` arrays,
/// so bincode (the Raft wire codec), not JSON, is the format. This named struct
/// exists for documentation + a serde round-trip test; the handler decodes the
/// `(ShardId, ClusterOp)` tuple directly to stay byte-compatible with the
/// forwarder's `encode_to_vec((shard, &op))`.
#[derive(Debug, Serialize, Deserialize)]
pub struct RefOpRequest {
    /// The target shard the op is addressed to (already shard-routed).
    pub shard: u32,
    /// The shard-targeted op to apply on the locally-hosted shard.
    pub op: ClusterOp,
}

/// `POST /cluster/ref-op` — apply a shard-targeted ref op to a LOCALLY-hosted
/// shard. This is the internal endpoint a remote node's `HttpForwarder` POSTs to
/// when it routes a ref to a shard it does not host (spec §4.3/§4.4).
///
/// Wire body: bincode `(ShardId, ClusterOp)` (the forwarder's exact encoding).
/// Responses:
/// - `503` if not clustered (single-node) — both `cluster_refs` and `shard_map`
///   are `None`, exactly like the sibling cluster routes.
/// - `400` if the body is not a valid bincode `(ShardId, ClusterOp)`.
/// - `421 Misdirected Request` if THIS node does NOT host `shard`: the body is
///   the bincode of the hosting members (`Vec<(node_id, addr)>`) so the caller
///   can retry against a real host. (421 is the precise HTTP semantic for "you
///   sent this to the wrong node"; the spec also permits 404.)
/// - `200` with the bincode `RefOpResponse` on success. A domain conflict is a
///   `RefOpResponse::Conflict` INSIDE the 200 body (mirroring the Raft wire
///   envelope), NOT an HTTP error.
pub async fn cluster_ref_op(State(state): State<AppState>, body: Bytes) -> Response {
    // Cluster-only: both are Some together (set in main.rs's cluster branch).
    let (refs, map) = match (&state.cluster_refs, &state.shard_map) {
        (Some(r), Some(m)) => (r, m),
        _ => return not_clustered(),
    };
    let cfg = bincode::config::standard();
    // Decode the forwarder's `(ShardId, ClusterOp)` tuple (NOT a wrapper struct):
    // this is byte-identical to `HttpForwarder::forward`'s encode side.
    let (shard, op): (ShardId, ClusterOp) = match bincode::serde::decode_from_slice(&body, cfg) {
        Ok((v, _)) => v,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("bad ref-op body: {e}")).into_response();
        }
    };

    // Verify THIS node hosts the target shard. If not, 421 + hosting members so
    // the caller retries against a real host (it picked a wrong forward target).
    if !map.hosts(shard, refs.node_id()) {
        let members: Vec<(u64, String)> = map
            .members(shard)
            .iter()
            .map(|r| (r.node_id, r.addr.clone()))
            .collect();
        let enc = bincode::serde::encode_to_vec(&members, cfg).unwrap_or_default();
        return (
            StatusCode::MISDIRECTED_REQUEST, // 421
            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
            enc,
        )
            .into_response();
    }

    // Apply DIRECTLY to the local shard handle — no re-route, no re-forward
    // (§4.4: the op is already shard-targeted; a re-forward landing here would
    // loop). `apply_local_op` lands the write on the shard leader.
    match refs.apply_local_op(shard, op).await {
        Ok(resp) => {
            crate::metrics::record_ref_op_applied(shard.0);
            match bincode::serde::encode_to_vec(&resp, cfg) {
                Ok(out) => (
                    StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                    out,
                )
                    .into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("encode resp: {e}"),
                )
                    .into_response(),
            }
        }
        Err(e) => {
            // An availability fault (no leader yet, unreachable) → 503 retryable;
            // mirrors how the dyn-RefStore path maps `LedgeError::Unavailable`.
            warn!(error = %e, shard = shard.0, "cluster ref-op apply failed");
            (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
        }
    }
}

/// Per-shard status projected from `Raft::metrics()`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardStatus {
    pub shard: u32,
    /// Declared replica member ids for this shard (sorted ascending). Sourced
    /// from the authoritative [`ledge_cluster::ShardMap`] when available (so it is
    /// reported for EVERY shard, hosted or not); falls back to the live Raft
    /// `voter_ids` for the hosted shards when no map is in `AppState`.
    pub members: Vec<u64>,
    /// Does THIS node host (build a Raft group for) this shard? `true` ⇒ the
    /// leader/term/applied fields below are live; `false` ⇒ they are `None`/`0`.
    pub hosted: bool,
    /// Present (live) ONLY for hosted shards; `None` for shards this node does
    /// not host (we have no Raft handle to read their metrics from).
    pub leader: Option<u64>,
    pub term: u64,
    pub last_applied: Option<u64>,
    /// Mirror of `last_applied`'s index: the highest log index applied to this
    /// node's state machine (openraft has no separate public `commit_index` on
    /// `RaftMetrics`; applied is the committed-and-applied marker we expose).
    pub commit_index: Option<u64>,
    /// The LIVE openraft voter set for this shard (sorted ascending), read from
    /// `membership_config.membership().voter_ids()`. Distinct from `members`
    /// (the *declared* placement): after a `/cluster/{shard}/reconfigure` these
    /// reflect the actual committed membership change. Empty for shards this node
    /// does not host (we have no Raft handle to read live membership from).
    #[serde(default)]
    pub voters: Vec<u64>,
}

/// `GET /cluster/status` response shape.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterStatus {
    pub shards: Vec<ShardStatus>,
}

/// Project a hosted shard's live `Raft::metrics()` into a [`ShardStatus`].
/// `members` is supplied by the caller (from the map when available, else the
/// live `voter_ids`), so this only fills the leader/term/applied fields.
fn hosted_status(
    shard: u32,
    members: Vec<u64>,
    raft: &openraft::Raft<ledge_raft::TypeConfig>,
) -> ShardStatus {
    // `metrics()` returns a `watch::Receiver`; `borrow()` is a cheap, lock-free
    // read of the latest published metrics snapshot.
    let m = raft.metrics().borrow().clone();
    // Live committed voter set (distinct from the declared placement `members`):
    // reflects the actual openraft membership after any reconfigure.
    let mut voters: Vec<u64> = m.membership_config.membership().voter_ids().collect();
    voters.sort_unstable();
    ShardStatus {
        shard,
        members,
        hosted: true,
        leader: m.current_leader,
        term: m.current_term,
        last_applied: m.last_applied.map(|l| l.index),
        commit_index: m.last_applied.map(|l| l.index),
        voters,
    }
}

/// `GET /cluster/status` — placement + per-hosted-shard leader/term/last-applied.
///
/// In single-node mode returns `503` (not clustered). In cluster mode it lists
/// EVERY shard the [`ledge_cluster::ShardMap`] declares (not just the ones this
/// node hosts), reporting each shard's declared `members`, whether THIS node
/// `hosted`s it, and — for hosted shards only — the live leader/term/applied.
///
/// If `AppState.shard_map` is absent (the route-handler test harness passes only
/// `raft_shards`), it falls back to the pre-placement behavior: list the locally
/// hosted shards with members from their live `voter_ids`.
pub async fn cluster_status(State(state): State<AppState>) -> Response {
    let shards = match shards(&state) {
        Some(s) => s,
        None => return not_clustered(),
    };
    let mut out = match &state.shard_map {
        // Placement-aware: iterate the authoritative map so unhosted shards are
        // reported too (declared members, no leader info).
        Some(map) => {
            let mut out = Vec::with_capacity(map.num_shards() as usize);
            for s in 0..map.num_shards() {
                let shard = ShardId(s);
                let mut members: Vec<u64> = map.members(shard).iter().map(|r| r.node_id).collect();
                members.sort_unstable();
                match shards.get(&shard) {
                    Some(raft) => out.push(hosted_status(s, members, raft)),
                    None => out.push(ShardStatus {
                        shard: s,
                        members,
                        hosted: false,
                        leader: None,
                        term: 0,
                        last_applied: None,
                        commit_index: None,
                        // No Raft handle for an unhosted shard ⇒ no live voters.
                        voters: Vec::new(),
                    }),
                }
            }
            out
        }
        // No map (test harness): hosted shards only, members from voter_ids.
        None => {
            let mut out = Vec::with_capacity(shards.len());
            for (shard, raft) in shards.iter() {
                let mut members: Vec<u64> = raft
                    .metrics()
                    .borrow()
                    .membership_config
                    .voter_ids()
                    .collect();
                members.sort_unstable();
                out.push(hosted_status(shard.0, members, raft));
            }
            out
        }
    };
    out.sort_by_key(|s| s.shard);
    (StatusCode::OK, Json(ClusterStatus { shards: out })).into_response()
}

/// Per-node outcome of a `/cluster/gc` fan-out leg: a node's `GcStats`, or an
/// error string when that node was unreachable / its pass failed. A downed node
/// is an `Error` entry — it does NOT fail the whole sweep (each node's pass is
/// independent and safe in isolation, spec §4.6).
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", content = "value", rename_all = "lowercase")]
pub enum NodeGcOutcome {
    Ok(GcStats),
    Error(String),
}

/// `POST /cluster/gc` response: every node's outcome + the summed totals.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterGcStats {
    /// `(node_id, outcome)` for every node in the shard map.
    pub per_node: Vec<(u64, NodeGcOutcome)>,
    /// Sum over the `Ok` nodes (error nodes contribute nothing).
    pub totals: GcStats,
}

impl ClusterGcStats {
    /// Build from per-node outcomes, summing the `Ok` ones into `totals`.
    pub fn from_entries(per_node: Vec<(u64, NodeGcOutcome)>) -> Self {
        let mut totals = GcStats::default();
        for (_node, outcome) in &per_node {
            if let NodeGcOutcome::Ok(s) = outcome {
                totals.scanned += s.scanned;
                totals.reachable += s.reachable;
                totals.reclaimed += s.reclaimed;
                totals.bytes_freed += s.bytes_freed;
                totals.skipped_grace += s.skipped_grace;
            }
        }
        Self { per_node, totals }
    }
}

/// Query flag: a `?local=true` leg runs ONLY this node's local pass and returns
/// its `GcStats` (no further fan-out), avoiding infinite re-fan.
#[derive(Debug, Deserialize, Default)]
pub struct GcQuery {
    #[serde(default)]
    pub local: bool,
}

/// Run THIS node's local `ClusterGc::run` with a wall-clock `now` (seconds).
/// Returns the pass `GcStats`, or an error string mapped by the caller.
async fn run_local_gc(state: &AppState) -> std::result::Result<GcStats, String> {
    let gc = state
        .cluster_gc
        .as_ref()
        .ok_or_else(|| "not clustered".to_string())?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // No route-level `record_gc_run` here: `ClusterGc::run` emits the `ledge_gc_*`
    // series (incl. `GC_RUNS_TOTAL`) at its true site. Recording here too would
    // double-count the run counter for the cluster path.
    let stats = gc.run(now).await.map_err(|e| e.to_string())?;
    Ok(stats)
}

/// `POST /cluster/gc` — trigger distributed GC. Without `?local=true` this is the
/// coordinator leg: run THIS node's pass, then fan `POST /cluster/gc?local=true`
/// out to every OTHER node in the map and aggregate. With `?local=true` it runs
/// only the local pass and returns its `GcStats` JSON (no further fan-out).
///
/// - `503` if not clustered.
/// - `200` `GcStats` JSON for a `?local=true` leg.
/// - `200` `ClusterGcStats` JSON for the coordinator leg (a downed peer is an
///   error entry, never a whole-sweep failure).
pub async fn cluster_gc(
    State(state): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<GcQuery>,
) -> Response {
    let map = match (&state.cluster_gc, &state.shard_map) {
        (Some(_), Some(m)) => m.clone(),
        _ => return not_clustered(),
    };

    // `?local=true` leg: just this node's pass.
    if q.local {
        return match run_local_gc(&state).await {
            Ok(stats) => (StatusCode::OK, Json(stats)).into_response(),
            Err(e) => {
                warn!(error = %e, "local cluster gc failed");
                (StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
            }
        };
    }

    // Coordinator leg: this node's pass first.
    let me = state
        .cluster_refs
        .as_ref()
        .map(|r| r.node_id())
        .unwrap_or(0);
    let mut per_node: Vec<(u64, NodeGcOutcome)> = Vec::new();
    match run_local_gc(&state).await {
        Ok(stats) => per_node.push((me, NodeGcOutcome::Ok(stats))),
        Err(e) => per_node.push((me, NodeGcOutcome::Error(e))),
    }

    // Fan `?local=true` out to every OTHER node, deduped by addr. A downed peer
    // becomes an `Error` entry — it never aborts the sweep (each node's pass is
    // independent and safe in isolation).
    let client = reqwest::Client::new();
    let mut seen: std::collections::HashSet<String> = Default::default();
    for s in 0..map.num_shards() {
        for rep in map.members(ShardId(s)) {
            if rep.node_id == me || !seen.insert(rep.addr.clone()) {
                continue;
            }
            let url = format!("{}/cluster/gc?local=true", rep.addr.trim_end_matches('/'));
            let outcome = match client.post(&url).send().await {
                Ok(resp) if resp.status().is_success() => match resp.json::<GcStats>().await {
                    Ok(stats) => NodeGcOutcome::Ok(stats),
                    Err(e) => NodeGcOutcome::Error(format!("decode {}: {e}", rep.addr)),
                },
                Ok(resp) => NodeGcOutcome::Error(format!("{} status {}", rep.addr, resp.status())),
                Err(e) => NodeGcOutcome::Error(format!("{} unreachable: {e}", rep.addr)),
            };
            per_node.push((rep.node_id, outcome));
        }
    }

    per_node.sort_by_key(|(n, _)| *n);
    (StatusCode::OK, Json(ClusterGcStats::from_entries(per_node))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_status_shape_roundtrips() {
        let s = ClusterStatus {
            shards: vec![ShardStatus {
                shard: 0,
                members: vec![1],
                hosted: true,
                leader: Some(1),
                term: 2,
                last_applied: Some(5),
                commit_index: Some(5),
                voters: vec![1],
            }],
        };
        let j = serde_json::to_string(&s).unwrap();
        let back: ClusterStatus = serde_json::from_str(&j).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn cluster_status_placement_shape_roundtrips() {
        let s = ClusterStatus {
            shards: vec![
                ShardStatus {
                    shard: 0,
                    members: vec![1, 2, 3],
                    hosted: true,
                    leader: Some(2),
                    term: 4,
                    last_applied: Some(9),
                    commit_index: Some(9),
                    voters: vec![1, 2, 3],
                },
                // A shard this node does NOT host: declared members, no leader.
                ShardStatus {
                    shard: 1,
                    members: vec![2, 3, 4],
                    hosted: false,
                    leader: None,
                    term: 0,
                    last_applied: None,
                    commit_index: None,
                    voters: Vec::new(),
                },
            ],
        };
        let j = serde_json::to_string(&s).unwrap();
        let back: ClusterStatus = serde_json::from_str(&j).unwrap();
        assert_eq!(back, s);
        // The unhosted shard reports its declared members but no leader.
        assert!(!back.shards[1].hosted && back.shards[1].leader.is_none());
        assert_eq!(back.shards[1].members, vec![2, 3, 4]);
    }

    #[test]
    fn ref_op_request_bincode_roundtrips() {
        let req = RefOpRequest {
            shard: 1,
            op: ClusterOp::Update {
                name: "refs/heads/x".into(),
                target_bytes: [9u8; 32],
                expected_bytes: None,
            },
        };
        let cfg = bincode::config::standard();
        let bytes = bincode::serde::encode_to_vec(&req, cfg).unwrap();
        let (back, _): (RefOpRequest, usize) =
            bincode::serde::decode_from_slice(&bytes, cfg).unwrap();
        assert_eq!(back.shard, 1);
        assert!(matches!(back.op, ClusterOp::Update { .. }));
    }

    /// The named `RefOpRequest` is byte-identical to the `(ShardId, ClusterOp)`
    /// tuple `HttpForwarder` actually sends, so the handler can decode the tuple
    /// (its real wire form) and a `RefOpRequest`-shaped client interoperates.
    #[test]
    fn ref_op_request_wire_matches_forwarder_tuple() {
        let op = ClusterOp::Update {
            name: "refs/heads/forwarded".into(),
            target_bytes: [0x5a; 32],
            expected_bytes: Some([0x11; 32]),
        };
        let cfg = bincode::config::standard();
        // The struct form and the tuple `(ShardId, ClusterOp)` form encode to the
        // same bytes (struct = ordered fields; tuple = ordered elements; bincode
        // is positional, and `ShardId(1)` is a transparent newtype over `u32`).
        let struct_bytes = bincode::serde::encode_to_vec(
            &RefOpRequest {
                shard: 1,
                op: op.clone(),
            },
            cfg,
        )
        .unwrap();
        let tuple_bytes = bincode::serde::encode_to_vec((ShardId(1), &op), cfg).unwrap();
        assert_eq!(struct_bytes, tuple_bytes);
        // And the handler's decode target round-trips that wire form.
        let (decoded, _): ((ShardId, ClusterOp), usize) =
            bincode::serde::decode_from_slice(&tuple_bytes, cfg).unwrap();
        assert_eq!(decoded.0, ShardId(1));
    }

    #[test]
    fn cluster_gc_stats_aggregates_totals() {
        use ledge_workspace::GcStats;
        let a = GcStats {
            scanned: 5,
            reachable: 3,
            reclaimed: 2,
            bytes_freed: 100,
            skipped_grace: 1,
        };
        let b = GcStats {
            scanned: 4,
            reachable: 4,
            reclaimed: 0,
            bytes_freed: 0,
            skipped_grace: 0,
        };
        let agg = ClusterGcStats::from_entries(vec![
            (1, NodeGcOutcome::Ok(a.clone())),
            (2, NodeGcOutcome::Ok(b.clone())),
            (3, NodeGcOutcome::Error("unreachable".into())),
        ]);
        assert_eq!(agg.totals.reclaimed, 2, "totals sum only the Ok nodes");
        assert_eq!(agg.totals.bytes_freed, 100);
        assert_eq!(agg.totals.skipped_grace, 1);
        assert_eq!(
            agg.per_node.len(),
            3,
            "a downed node is an entry, not a failure"
        );
        let j = serde_json::to_string(&agg).unwrap();
        let back: ClusterGcStats = serde_json::from_str(&j).unwrap();
        assert_eq!(back.totals.reclaimed, 2);
    }

    #[test]
    fn init_request_parses() {
        let r: InitRequest = serde_json::from_str(
            r#"{"shard":0,"members":{"1":"http://h1:4001","2":"http://h2:4001"}}"#,
        )
        .unwrap();
        assert_eq!(r.shard, 0);
        assert_eq!(r.members.len(), 2);
        assert_eq!(
            r.members.get("1").map(String::as_str),
            Some("http://h1:4001")
        );
    }
}
