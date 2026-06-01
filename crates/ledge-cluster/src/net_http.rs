//! HTTP-transport Raft network (`HttpRaftNetwork`) for cross-process clusters.
//!
//! This is the production counterpart to the in-process [`crate::net_mem`]
//! network (Task 3). Where `net_mem` calls the target's `Raft` handle directly,
//! `HttpRaftNetwork` serializes each RPC and POSTs it to the peer's HTTP
//! endpoint, where a server-side handler (the routes are wired in Task 7) feeds
//! it back into the local `Raft`.
//!
//! # Wire format
//! Each RPC request/response is bincode (`bincode::config::standard()`, the same
//! config as the WAL) — binary, compact, and free of JSON float/precision
//! hazards. The request is POSTed to
//! `{base_url}/raft/{shard}/{append|vote|snapshot}` with
//! `Content-Type: application/octet-stream`. The body is the bincode of the
//! request type. The 200 response body is a bincode [`WireResult`] envelope that
//! carries *either* the bincode of the success response *or* the bincode of the
//! served peer's `RaftError`, so a served-but-erroring peer round-trips its
//! domain error back to the caller as an `RPCError::RemoteError` (driving
//! openraft's normal handling) while transport failures map to
//! `RPCError::Network` (driving backoff + retry).
//!
//! # openraft 0.9.24 trait surface (verified against the resolved crate source)
//! - v1 `RaftNetwork` + `RaftNetworkFactory` (NOT `RaftNetworkV2`), generated via
//!   `add_async_trait`, so impls are plain `async fn` taking `&mut self` +
//!   `RPCOption`.
//! - Request types are generic over the *config* `C` when they carry entries
//!   (`AppendEntriesRequest<C>`, `InstallSnapshotRequest<C>`) and over the
//!   *node id* otherwise (`VoteRequest<NID>`); responses are over the node id
//!   (`AppendEntriesResponse<NID>`, `VoteResponse<NID>`,
//!   `InstallSnapshotResponse<NID>`).
//! - The `Raft<C>` handle methods are `append_entries(rpc)`, `vote(rpc)`,
//!   `install_snapshot(rpc)` returning `Result<Resp, RaftError<NID[, E]>>`.

// openraft's `RaftNetwork` trait methods return `Result<_, RPCError<..>>` by
// contract; `RPCError` is a large enum (>256 B) and we cannot box it without
// violating the trait signature. The error path is cold (only on RPC failure),
// so the large-Result size is irrelevant here. (Mirrors `net_mem`.)
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use ledge_core::{LedgeError, ObjectId};
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::router::ShardId;
use ledge_raft::{Node, NodeId, TypeConfig};

/// An RPC-side error addressed to a peer (`RaftError<NodeId>` payload).
type NetRpcError = RPCError<NodeId, Node, RaftError<NodeId>>;

/// An install-snapshot RPC-side error (carries `InstallSnapshotError`).
type SnapRpcError = RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>;

/// bincode standard config — the single wire codec, shared with the WAL.
fn cfg() -> bincode::config::Configuration {
    bincode::config::standard()
}

/// Encode any serde value to a bincode `Vec<u8>`.
fn enc<T: Serialize>(v: &T) -> Result<Vec<u8>, bincode::error::EncodeError> {
    bincode::serde::encode_to_vec(v, cfg())
}

/// Decode a bincode `&[u8]` back to `T`.
fn dec<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, bincode::error::DecodeError> {
    bincode::serde::decode_from_slice(bytes, cfg()).map(|(v, _)| v)
}

/// The response envelope returned by the server-side handler. Carries the
/// bincode of the success response or the bincode of the served peer's
/// `RaftError`, so the caller reconstructs the exact outcome from a single 200.
#[derive(Serialize, Deserialize)]
enum WireResult {
    /// Success: bincode of the RPC response type.
    Ok(Vec<u8>),
    /// The served Raft returned a domain error: bincode of its `RaftError`.
    Err(Vec<u8>),
}

/// Which RPC a request body is. Selected by the URL path segment so the
/// server-side handler can dispatch without a separate tag in the body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcKind {
    /// `append_entries`.
    Append,
    /// `vote`.
    Vote,
    /// `install_snapshot`.
    Snapshot,
}

impl RpcKind {
    /// Parse the trailing URL path segment (`append`/`vote`/`snapshot`).
    pub fn from_path(seg: &str) -> Option<Self> {
        match seg {
            "append" => Some(Self::Append),
            "vote" => Some(Self::Vote),
            "snapshot" => Some(Self::Snapshot),
            _ => None,
        }
    }
}

// ── Server-side handler (Task 7 wires Axum routes onto this) ──────────────────

/// Errors the server-side handler can surface to its HTTP layer. Distinct from a
/// served `RaftError` (which is encoded *into* the 200 body as `WireResult::Err`)
/// — these mean the request itself was malformed and warrant a 4xx.
#[derive(Debug)]
pub enum HandlerError {
    /// The request body could not be bincode-decoded into the expected type.
    Decode(String),
    /// The response (or error) could not be bincode-encoded.
    Encode(String),
}

impl std::fmt::Display for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerError::Decode(m) => write!(f, "request decode error: {m}"),
            HandlerError::Encode(m) => write!(f, "response encode error: {m}"),
        }
    }
}

impl std::error::Error for HandlerError {}

/// Feed one incoming Raft RPC into a local `Raft` and produce the bincode
/// [`WireResult`] body to return as a `200`.
///
/// This is the load-bearing server-side entrypoint: Task 7's Axum route handler
/// is a thin wrapper that extracts `(shard, kind, body)` from the request and
/// calls this, writing the returned bytes back with
/// `Content-Type: application/octet-stream`. The `shard` is accepted for routing
/// symmetry / logging; the caller is responsible for selecting the correct
/// `raft` handle for that shard before calling here.
///
/// # Errors
/// Returns [`HandlerError::Decode`] if `body` is not a valid request of `kind`,
/// or [`HandlerError::Encode`] if the response cannot be serialized. A domain
/// error from the served `Raft` is NOT an error here — it is encoded into the
/// returned bytes as `WireResult::Err`.
pub async fn handle_rpc(
    _shard: ShardId,
    kind: RpcKind,
    body: &[u8],
    raft: &openraft::Raft<TypeConfig>,
) -> Result<Vec<u8>, HandlerError> {
    let wire = match kind {
        RpcKind::Append => {
            let rpc: AppendEntriesRequest<TypeConfig> =
                dec(body).map_err(|e| HandlerError::Decode(e.to_string()))?;
            match raft.append_entries(rpc).await {
                Ok(resp) => WireResult::Ok(enc(&resp).map_err(enc_err)?),
                Err(e) => WireResult::Err(enc(&e).map_err(enc_err)?),
            }
        }
        RpcKind::Vote => {
            let rpc: VoteRequest<NodeId> =
                dec(body).map_err(|e| HandlerError::Decode(e.to_string()))?;
            match raft.vote(rpc).await {
                Ok(resp) => WireResult::Ok(enc(&resp).map_err(enc_err)?),
                Err(e) => WireResult::Err(enc(&e).map_err(enc_err)?),
            }
        }
        RpcKind::Snapshot => {
            let rpc: InstallSnapshotRequest<TypeConfig> =
                dec(body).map_err(|e| HandlerError::Decode(e.to_string()))?;
            match raft.install_snapshot(rpc).await {
                Ok(resp) => WireResult::Ok(enc(&resp).map_err(enc_err)?),
                Err(e) => WireResult::Err(enc(&e).map_err(enc_err)?),
            }
        }
    };
    enc(&wire).map_err(enc_err)
}

fn enc_err(e: bincode::error::EncodeError) -> HandlerError {
    HandlerError::Encode(e.to_string())
}

// ── Client side: factory + per-target connection ─────────────────────────────

/// Address of a peer: the base URL (e.g. `http://127.0.0.1:8080`) it serves
/// `/raft/{shard}/*` on.
pub type PeerAddr = String;

/// Per-node factory: hands out [`HttpRaftNetwork`] connections for this node's
/// shard, closing over the peer address table and a pooled `reqwest::Client`.
#[derive(Clone)]
pub struct HttpRaftNetworkFactory {
    shard: ShardId,
    peers: Arc<HashMap<NodeId, PeerAddr>>,
    client: reqwest::Client,
}

impl HttpRaftNetworkFactory {
    /// Build a factory for `shard` over the given `node_id -> base_url` table.
    /// The `reqwest::Client` is pooled (keep-alive) and cloned per connection.
    pub fn new(shard: ShardId, peers: HashMap<NodeId, PeerAddr>) -> Self {
        Self {
            shard,
            peers: Arc::new(peers),
            client: reqwest::Client::new(),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for HttpRaftNetworkFactory {
    type Network = HttpRaftNetwork;

    // VERIFIED 0.9.24: async fn new_client(&mut self, target, node: &Node) -> Network
    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        // Prefer the configured peer address; fall back to the membership node's
        // own addr (BasicNode carries an addr string) so a node added at runtime
        // without a static table entry is still reachable.
        let base = self
            .peers
            .get(&target)
            .cloned()
            .unwrap_or_else(|| node.addr.clone());
        HttpRaftNetwork {
            shard: self.shard,
            target,
            base_url: base,
            client: self.client.clone(),
        }
    }
}

/// One logical HTTP connection to `target` within `shard`. Holds the pooled
/// client and the resolved base URL; each RPC is a single POST.
pub struct HttpRaftNetwork {
    shard: ShardId,
    target: NodeId,
    base_url: String,
    client: reqwest::Client,
}

impl HttpRaftNetwork {
    /// POST a bincode `req` to `/raft/{shard}/{path}` and decode the
    /// [`WireResult`] envelope into either `Ok(Resp)` or the served peer's
    /// `RaftError<NID, E>`. Transport failures become `RPCError::Network`.
    async fn post<Req, Resp, E>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp, RPCError<NodeId, Node, RaftError<NodeId, E>>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        E: std::error::Error + DeserializeOwned + openraft::OptionalSend,
    {
        let url = format!("{}/raft/{}/{}", self.base_url, self.shard.0, path);
        let body = enc(req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let resp = self
            .client
            .post(&url)
            .header("content-type", "application/octet-stream")
            .body(body)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        // Any non-2xx (a served-but-broken peer / proxy) is a network-class
        // failure from openraft's perspective: it should retry.
        let resp = resp
            .error_for_status()
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let wire: WireResult =
            dec(&bytes).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        match wire {
            WireResult::Ok(ok) => {
                dec::<Resp>(&ok).map_err(|e| RPCError::Network(NetworkError::new(&e)))
            }
            WireResult::Err(err) => {
                let raft_err: RaftError<NodeId, E> =
                    dec(&err).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
                Err(RPCError::RemoteError(RemoteError::new(self.target, raft_err)))
            }
        }
    }
}

// VERIFIED 0.9.24: RaftNetwork is generated by add_async_trait → plain async fn
// with &mut self + RPCOption; install_snapshot is required (no
// generic-snapshot-data). Mirrors `net_mem`'s impl shape.
impl RaftNetwork<TypeConfig> for HttpRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, NetRpcError> {
        self.post::<_, AppendEntriesResponse<NodeId>, openraft::error::Infallible>("append", &rpc)
            .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, NetRpcError> {
        self.post::<_, VoteResponse<NodeId>, openraft::error::Infallible>("vote", &rpc)
            .await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, SnapRpcError> {
        self.post::<_, InstallSnapshotResponse<NodeId>, InstallSnapshotError>("snapshot", &rpc)
            .await
    }
}

// ── Object replication transport: HttpObjectPeer ─────────────────────────────

/// HTTP transport for the [`crate::object_store::ObjectPeer`] trait.
///
/// This is the production counterpart to [`crate::object_store::LocalObjectPeer`]
/// (which wraps a sibling in-process `DiskObjectStore`). Each method is a single
/// HTTP round-trip to the peer node's object endpoints, which are served by
/// `ledge-server`'s `object_routes`:
///
/// - `put` / `put_git` → `POST {base}/objects/{shard}/replicate` with the **raw
///   object content** as the octet-stream body. The git type is carried in the
///   `?type=<n>` query (`put` uses the blob default, type 3). The receiver writes
///   the bytes to its local `DiskObjectStore` via `write`/`write_git_object`,
///   which re-derives the BLAKE3 content address; the 200 body is the resulting
///   ObjectId hex. Because the store is content-addressed, the put is **idempotent**
///   and the response id is verified to equal the expected id (a mismatch is a
///   [`LedgeError::Corruption`], catching a tampering/buggy peer).
/// - `get` → `GET {base}/objects/{shard}/{id_hex}` returning the raw content bytes
///   (`200`) or `None` (`404`). The caller (`ReplicatedObjectStore::read`)
///   re-verifies the content address on store, so a corrupt fetch is rejected
///   there too.
/// - `has` → `GET` with the same id; `200` ⇒ present, `404` ⇒ absent.
///
/// The `reqwest::Client` is pooled (keep-alive) and cheap to clone, mirroring
/// [`HttpRaftNetwork`].
pub struct HttpObjectPeer {
    /// Base URL the peer serves `/objects/*` on (e.g. `http://127.0.0.1:8080`).
    base_url: String,
    /// Which shard's object replica this peer holds.
    shard: ShardId,
    /// Pooled HTTP client (keep-alive), shared across calls.
    client: reqwest::Client,
}

impl HttpObjectPeer {
    /// Build a peer client for `shard` against `base_url` over a fresh pooled
    /// client.
    pub fn new(base_url: impl Into<String>, shard: ShardId) -> Self {
        Self {
            base_url: base_url.into(),
            shard,
            client: reqwest::Client::new(),
        }
    }

    /// Build a peer client reusing an existing pooled `reqwest::Client`.
    pub fn with_client(base_url: impl Into<String>, shard: ShardId, client: reqwest::Client) -> Self {
        Self {
            base_url: base_url.into(),
            shard,
            client,
        }
    }

    /// `{base}/objects/{shard}/replicate` — the typed-write replicate endpoint.
    fn replicate_url(&self) -> String {
        format!("{}/objects/{}/replicate", self.base_url, self.shard.0)
    }

    /// `{base}/objects/{shard}/{id_hex}` — the by-id fetch/probe endpoint.
    fn object_url(&self, id: &ObjectId) -> String {
        format!("{}/objects/{}/{}", self.base_url, self.shard.0, id.to_hex())
    }

    /// Common put body: POST raw `content` with `?type=git_type`, expect the
    /// 200 body to be the ObjectId hex, and verify it equals `id`.
    async fn put_typed(&self, id: &ObjectId, git_type: u8, content: &[u8]) -> ledge_core::Result<()> {
        let url = format!("{}?type={}", self.replicate_url(), git_type);
        let resp = self
            .client
            .post(&url)
            .header("content-type", "application/octet-stream")
            .body(content.to_vec())
            .send()
            .await
            .map_err(|e| LedgeError::Unavailable(format!("replicate {} to peer failed: {e}", id.to_hex())))?;
        if !resp.status().is_success() {
            return Err(LedgeError::Unavailable(format!(
                "replicate {} to peer returned {}",
                id.to_hex(),
                resp.status()
            )));
        }
        let body = resp
            .text()
            .await
            .map_err(|e| LedgeError::Unavailable(format!("read replicate response: {e}")))?;
        let got = ObjectId::from_hex(body.trim())?;
        if &got != id {
            // Content addressing makes this verifiable: the peer must have
            // re-derived the identical id, or it stored bytes under a different
            // address (tampered/buggy peer).
            return Err(LedgeError::Corruption(format!(
                "peer replicate for {} produced address {}",
                id.to_hex(),
                got.to_hex()
            )));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::object_store::ObjectPeer for HttpObjectPeer {
    async fn put(&self, id: &ObjectId, content: &[u8]) -> ledge_core::Result<()> {
        // Plain `put` stores as a git blob (type 3), matching DiskObjectStore::write.
        self.put_typed(id, 3, content).await
    }

    async fn put_git(&self, id: &ObjectId, git_type: u8, content: &[u8]) -> ledge_core::Result<()> {
        self.put_typed(id, git_type, content).await
    }

    async fn get(&self, id: &ObjectId) -> ledge_core::Result<Option<Bytes>> {
        let resp = self
            .client
            .get(self.object_url(id))
            .send()
            .await
            .map_err(|e| LedgeError::Unavailable(format!("fetch {} from peer failed: {e}", id.to_hex())))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(LedgeError::Unavailable(format!(
                "fetch {} from peer returned {}",
                id.to_hex(),
                resp.status()
            )));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| LedgeError::Unavailable(format!("read fetch response: {e}")))?;
        Ok(Some(bytes))
    }

    async fn has(&self, id: &ObjectId) -> ledge_core::Result<bool> {
        let resp = self
            .client
            .get(self.object_url(id))
            .send()
            .await
            .map_err(|e| LedgeError::Unavailable(format!("probe {} on peer failed: {e}", id.to_hex())))?;
        match resp.status() {
            s if s.is_success() => Ok(true),
            reqwest::StatusCode::NOT_FOUND => Ok(false),
            other => Err(LedgeError::Unavailable(format!(
                "probe {} on peer returned {other}",
                id.to_hex()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    //! Transport tests. The full consensus-correctness proof lives in the
    //! in-memory network cluster tests (Task 3); here we prove (1) the bincode
    //! wire format round-trips for every RPC type, and (2) a single live RPC
    //! flows request → served `Raft` → response over the real handler entrypoint
    //! (and, when feasible, over a real localhost socket via Axum).

    use super::*;
    use ledge_raft::{LogStore, StateMachineStore, TypeConfig};
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    };
    use openraft::{Config, LogId, SnapshotMeta, StoredMembership, Vote};
    use std::sync::Arc;


    fn raft_config() -> Arc<Config> {
        Arc::new(
            Config {
                heartbeat_interval: 100,
                election_timeout_min: 300,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        )
    }

    /// Build a single-node `Raft<TypeConfig>` with the in-memory stores. Used as
    /// the served peer for the live-RPC tests.
    async fn one_node_raft(id: NodeId) -> openraft::Raft<TypeConfig> {
        let log = LogStore::default();
        let sm = StateMachineStore::new_temp().await;
        let net = crate::net_mem::MemNetworkFactory::new(ShardId(0), crate::net_mem::Registry::new());
        openraft::Raft::new(id, raft_config(), net, log, sm)
            .await
            .expect("Raft::new")
    }

    #[test]
    fn rpc_request_response_roundtrip_serde() {
        // Requests.
        let append = AppendEntriesRequest::<TypeConfig> {
            vote: Vote::new(1, 1),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };
        let back: AppendEntriesRequest<TypeConfig> = dec(&enc(&append).unwrap()).unwrap();
        assert_eq!(back.vote, append.vote);
        assert_eq!(back.entries.len(), 0);

        let vote = VoteRequest::<NodeId>::new(Vote::new(2, 1), Some(LogId::new(
            openraft::CommittedLeaderId::new(2, 1),
            7,
        )));
        let back: VoteRequest<NodeId> = dec(&enc(&vote).unwrap()).unwrap();
        assert_eq!(back, vote);

        let snap = InstallSnapshotRequest::<TypeConfig> {
            vote: Vote::new(3, 1),
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "snap-1".to_string(),
            },
            offset: 0,
            data: vec![1, 2, 3, 4],
            done: true,
        };
        let back: InstallSnapshotRequest<TypeConfig> = dec(&enc(&snap).unwrap()).unwrap();
        assert_eq!(back.vote, snap.vote);
        assert_eq!(back.data, snap.data);
        assert_eq!(back.done, snap.done);
        assert_eq!(back.meta.snapshot_id, "snap-1");

        // Responses.
        let ar = AppendEntriesResponse::<NodeId>::Success;
        let back: AppendEntriesResponse<NodeId> = dec(&enc(&ar).unwrap()).unwrap();
        assert_eq!(back, ar);

        let vr = VoteResponse::<NodeId>::new(Vote::new(2, 1), None, true);
        let back: VoteResponse<NodeId> = dec(&enc(&vr).unwrap()).unwrap();
        assert_eq!(back, vr);

        let sr = InstallSnapshotResponse::<NodeId> {
            vote: Vote::new(3, 1),
        };
        let back: InstallSnapshotResponse<NodeId> = dec(&enc(&sr).unwrap()).unwrap();
        assert_eq!(back, sr);
    }

    #[tokio::test]
    async fn handle_rpc_vote_against_real_raft() {
        // A fresh (uninitialized) single node grants a vote in term 5.
        let raft = one_node_raft(1).await;
        let req = VoteRequest::<NodeId>::new(Vote::new(5, 2), None);
        let body = enc(&req).unwrap();

        let out = handle_rpc(ShardId(0), RpcKind::Vote, &body, &raft)
            .await
            .expect("handler ok");
        let wire: WireResult = dec(&out).unwrap();
        let resp_bytes = match wire {
            WireResult::Ok(b) => b,
            WireResult::Err(_) => panic!("expected Ok vote response"),
        };
        let resp: VoteResponse<NodeId> = dec(&resp_bytes).unwrap();
        assert!(resp.vote_granted, "fresh node should grant a higher-term vote");

        // The served Raft observed the vote: its persisted vote is now term 5.
        let metrics = raft.metrics().borrow().clone();
        assert!(metrics.vote.leader_id().term >= 5);
        raft.shutdown().await.ok();
    }

    #[tokio::test]
    async fn handle_rpc_append_against_real_raft() {
        // An empty append-entries (heartbeat) from a leader at term 1 succeeds.
        let raft = one_node_raft(1).await;
        let req = AppendEntriesRequest::<TypeConfig> {
            vote: Vote::new_committed(1, 1),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };
        let body = enc(&req).unwrap();
        let out = handle_rpc(ShardId(0), RpcKind::Append, &body, &raft)
            .await
            .expect("handler ok");
        let wire: WireResult = dec(&out).unwrap();
        match wire {
            WireResult::Ok(b) => {
                let resp: AppendEntriesResponse<NodeId> = dec(&b).unwrap();
                assert!(
                    matches!(resp, AppendEntriesResponse::Success),
                    "heartbeat should succeed, got {resp:?}"
                );
            }
            WireResult::Err(b) => {
                let e: RaftError<NodeId> = dec(&b).unwrap();
                panic!("expected Ok append response, got RaftError: {e:?}");
            }
        }
        raft.shutdown().await.ok();
    }

    #[tokio::test]
    async fn single_rpc_against_served_endpoint() {
        // End-to-end over a real localhost socket: Axum route → handle_rpc →
        // Raft, with the client HttpRaftNetwork making the POST. Proves the HTTP
        // transport (URL shape, octet-stream body, WireResult envelope).
        use axum::extract::State;
        use axum::routing::post;
        use axum::Router;
        use std::collections::HashMap;

        let raft = Arc::new(one_node_raft(1).await);

        // Minimal server exposing POST /raft/{shard}/{kind}.
        async fn route(
            State(raft): State<Arc<openraft::Raft<TypeConfig>>>,
            path: axum::extract::Path<(u32, String)>,
            body: axum::body::Bytes,
        ) -> Result<Vec<u8>, (axum::http::StatusCode, String)> {
            let (shard, kind_seg) = path.0;
            let kind = RpcKind::from_path(&kind_seg)
                .ok_or((axum::http::StatusCode::NOT_FOUND, "bad kind".into()))?;
            handle_rpc(ShardId(shard), kind, &body, &raft)
                .await
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))
        }

        let app = Router::new()
            .route("/raft/{shard}/{kind}", post(route))
            .with_state(raft.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Client side: HttpRaftNetwork pointed at the ephemeral port.
        let mut peers = HashMap::new();
        peers.insert(1u64, format!("http://{addr}"));
        let mut factory = HttpRaftNetworkFactory::new(ShardId(0), peers);
        let mut conn = factory.new_client(1, &Node::new(format!("http://{addr}"))).await;

        let resp = conn
            .vote(VoteRequest::new(Vote::new(9, 1), None), RPCOption::new(std::time::Duration::from_secs(2)))
            .await
            .expect("vote RPC over HTTP");
        assert!(resp.vote_granted, "fresh node should grant a higher-term vote over HTTP");

        // The served Raft observed it.
        let metrics = raft.metrics().borrow().clone();
        assert!(metrics.vote.leader_id().term >= 9);

        raft.shutdown().await.ok();
        server.abort();
    }

    // ── Object replication transport tests ───────────────────────────────────

    use crate::object_store::{ObjectPeer, ReplicatedObjectStore};
    use bytes::Bytes;
    use ledge_core::{LedgeError, ObjectId, ObjectStore};
    use ledge_object_store::DiskObjectStore;

    /// Spin a real Axum server over a tempdir-backed `DiskObjectStore` exposing
    /// the object replicate/fetch endpoints, returning `(base_url, store, join)`.
    ///
    /// The route shape MUST match `ledge-server`'s `object_routes`:
    /// `POST /objects/{shard}/replicate?type=<n>` (raw body → ObjectId hex) and
    /// `GET /objects/{shard}/{id_hex}` (raw content or 404). This in-crate copy
    /// keeps the transport test self-contained (no dep on `ledge-server`).
    async fn serve_object_store() -> (String, Arc<DiskObjectStore>, tokio::task::JoinHandle<()>) {
        use axum::extract::{Path, Query, State};
        use axum::http::StatusCode;
        use axum::routing::{get, post};
        use axum::Router;

        let dir = tempfile::tempdir().unwrap();
        // Leak the tempdir so the served store outlives the test body (the dir is
        // reclaimed at process exit; acceptable for a transient test fixture).
        let path = dir.keep();
        let store = Arc::new(DiskObjectStore::new(path).unwrap());

        #[derive(serde::Deserialize)]
        struct TypeQuery {
            #[serde(default)]
            r#type: Option<u8>,
        }

        async fn replicate(
            State(store): State<Arc<DiskObjectStore>>,
            Path(_shard): Path<u32>,
            Query(q): Query<TypeQuery>,
            body: axum::body::Bytes,
        ) -> Result<String, (StatusCode, String)> {
            let git_type = q.r#type.unwrap_or(3);
            let id = store
                .write_git_object(git_type, body)
                .await
                .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
            Ok(id.to_hex())
        }

        async fn fetch(
            State(store): State<Arc<DiskObjectStore>>,
            Path((_shard, id_hex)): Path<(u32, String)>,
        ) -> Result<Vec<u8>, StatusCode> {
            let id = ObjectId::from_hex(&id_hex).map_err(|_| StatusCode::BAD_REQUEST)?;
            match store.read(id).await {
                Ok(bytes) => Ok(bytes.to_vec()),
                Err(LedgeError::NotFound(_)) => Err(StatusCode::NOT_FOUND),
                Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
            }
        }

        let app = Router::new()
            .route("/objects/{shard}/replicate", post(replicate))
            .route("/objects/{shard}/{id}", get(fetch))
            .with_state(store.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), store, join)
    }

    #[tokio::test]
    async fn http_object_peer_roundtrip() {
        let (base, store, server) = serve_object_store().await;
        let peer = HttpObjectPeer::new(base, ShardId(0));

        // put → returns the content address; GET fetches identical bytes.
        let content = Bytes::from_static(b"replicate me over HTTP");
        let id = ObjectId::from(blake3::hash(&content));
        peer.put(&id, &content).await.expect("put over HTTP");
        assert!(store.exists(id).await.unwrap(), "peer stored the object");
        assert!(peer.has(&id).await.unwrap(), "has() sees the object");

        let got = peer.get(&id).await.unwrap().expect("get returns Some");
        assert_eq!(&got[..], &content[..], "fetched bytes identical");

        // A missing id round-trips as None / false (404).
        let absent = ObjectId::from(blake3::hash(b"never written"));
        assert!(peer.get(&absent).await.unwrap().is_none());
        assert!(!peer.has(&absent).await.unwrap());

        server.abort();
    }

    #[tokio::test]
    async fn http_object_peer_put_git_preserves_type() {
        let (base, store, server) = serve_object_store().await;
        let peer = HttpObjectPeer::new(base, ShardId(0));

        // A tree object (type 2): the receiver must reproduce the type byte so
        // its content address + git header match.
        let content = Bytes::from_static(b"100644 file\0\x01\x02\x03");
        let id = ObjectId::from(blake3::hash(&content));
        peer.put_git(&id, 2, &content).await.expect("put_git over HTTP");
        assert_eq!(store.git_type_of(id).await.unwrap(), 2, "type byte preserved");

        server.abort();
    }

    #[tokio::test]
    async fn http_object_peer_rejects_mismatched_address() {
        // The receiver re-derives the id from the bytes; the client verifies the
        // returned id equals the expected id. Claim an id that does NOT match the
        // content → the put must be rejected as Corruption.
        let (base, _store, server) = serve_object_store().await;
        let peer = HttpObjectPeer::new(base, ShardId(0));

        let content = Bytes::from_static(b"honest bytes");
        let wrong_id = ObjectId::from(blake3::hash(b"a different object"));
        let err = peer
            .put(&wrong_id, &content)
            .await
            .expect_err("mismatched content address must be rejected");
        assert!(
            matches!(err, LedgeError::Corruption(_)),
            "expected Corruption, got {err:?}"
        );

        server.abort();
    }

    #[tokio::test]
    async fn replicated_store_over_http_writes_to_quorum() {
        // 3-replica shard: 1 local + 2 HTTP peers. quorum = 2. A write must land
        // on the local store AND at least one peer; best-effort drains the rest,
        // so both peers should converge. We assert both end up with the object.
        let local_dir = tempfile::tempdir().unwrap();
        let local = Arc::new(DiskObjectStore::new(local_dir.path().to_path_buf()).unwrap());

        let (base_a, store_a, server_a) = serve_object_store().await;
        let (base_b, store_b, server_b) = serve_object_store().await;
        let peer_a: Arc<dyn ObjectPeer> = Arc::new(HttpObjectPeer::new(base_a, ShardId(0)));
        let peer_b: Arc<dyn ObjectPeer> = Arc::new(HttpObjectPeer::new(base_b, ShardId(0)));

        let store = ReplicatedObjectStore::new(local.clone(), vec![peer_a, peer_b]);
        assert_eq!(store.quorum(), 2, "n=3 ⇒ quorum 2");

        let content = Bytes::from_static(b"quorum-durable object");
        let id = store.write(content.clone()).await.expect("quorum write");
        assert!(local.exists(id).await.unwrap(), "local has it");

        // The post-quorum drain runs in the background; poll until both converge.
        for _ in 0..100 {
            if store_a.exists(id).await.unwrap() && store_b.exists(id).await.unwrap() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(store_a.exists(id).await.unwrap(), "peer A converged");
        assert!(store_b.exists(id).await.unwrap(), "peer B converged");

        server_a.abort();
        server_b.abort();
    }

    #[tokio::test]
    async fn replicated_store_over_http_quorum_with_one_peer_down() {
        // 3-replica shard, but one peer points at a dead address (connection
        // refused). quorum = 2 = local + the one live peer, so the write still
        // succeeds; anti-entropy repairs the dead replica later.
        let local_dir = tempfile::tempdir().unwrap();
        let local = Arc::new(DiskObjectStore::new(local_dir.path().to_path_buf()).unwrap());

        let (base_live, store_live, server_live) = serve_object_store().await;
        let peer_live: Arc<dyn ObjectPeer> = Arc::new(HttpObjectPeer::new(base_live, ShardId(0)));
        // Reserved-port / closed socket: connection refused → put errors, but
        // quorum is still reached by local + the live peer.
        let peer_dead: Arc<dyn ObjectPeer> =
            Arc::new(HttpObjectPeer::new("http://127.0.0.1:1", ShardId(0)));

        let store = ReplicatedObjectStore::new(local.clone(), vec![peer_live, peer_dead]);
        assert_eq!(store.quorum(), 2);

        let content = Bytes::from_static(b"survives one down peer");
        let id = store.write(content).await.expect("quorum still reached with one peer down");
        assert!(local.exists(id).await.unwrap());

        for _ in 0..100 {
            if store_live.exists(id).await.unwrap() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(store_live.exists(id).await.unwrap(), "live peer got it");

        server_live.abort();
    }
}
