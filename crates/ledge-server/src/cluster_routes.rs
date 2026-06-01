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

use ledge_cluster::net_http::{handle_rpc, RpcKind};
use ledge_cluster::ShardId;
use ledge_raft::{Node, NodeId};

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

/// Per-shard status projected from `Raft::metrics()`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardStatus {
    pub shard: u32,
    pub leader: Option<u64>,
    pub term: u64,
    pub members: Vec<u64>,
    pub last_applied: Option<u64>,
    /// Mirror of `last_applied`'s index: the highest log index applied to this
    /// node's state machine (openraft has no separate public `commit_index` on
    /// `RaftMetrics`; applied is the committed-and-applied marker we expose).
    pub commit_index: Option<u64>,
}

/// `GET /cluster/status` response shape.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterStatus {
    pub shards: Vec<ShardStatus>,
}

/// `GET /cluster/status` — per-shard leader/term/members/last-applied. In
/// single-node mode returns `503` (not clustered).
pub async fn cluster_status(State(state): State<AppState>) -> Response {
    let shards = match shards(&state) {
        Some(s) => s,
        None => return not_clustered(),
    };
    let mut out = Vec::with_capacity(shards.len());
    for (shard, raft) in shards.iter() {
        // `metrics()` returns a `watch::Receiver`; `borrow()` is a cheap, lock-
        // free read of the latest published metrics snapshot.
        let m = raft.metrics().borrow().clone();
        let mut members: Vec<u64> = m.membership_config.voter_ids().collect();
        members.sort_unstable();
        out.push(ShardStatus {
            shard: shard.0,
            leader: m.current_leader,
            term: m.current_term,
            members,
            last_applied: m.last_applied.map(|l| l.index),
            commit_index: m.last_applied.map(|l| l.index),
        });
    }
    out.sort_by_key(|s| s.shard);
    (StatusCode::OK, Json(ClusterStatus { shards: out })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_status_shape_roundtrips() {
        let s = ClusterStatus {
            shards: vec![ShardStatus {
                shard: 0,
                leader: Some(1),
                term: 2,
                members: vec![1],
                last_applied: Some(5),
                commit_index: Some(5),
            }],
        };
        let j = serde_json::to_string(&s).unwrap();
        let back: ClusterStatus = serde_json::from_str(&j).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn init_request_parses() {
        let r: InitRequest = serde_json::from_str(
            r#"{"shard":0,"members":{"1":"http://h1:4001","2":"http://h2:4001"}}"#,
        )
        .unwrap();
        assert_eq!(r.shard, 0);
        assert_eq!(r.members.len(), 2);
        assert_eq!(r.members.get("1").map(String::as_str), Some("http://h1:4001"));
    }
}
