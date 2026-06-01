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
}
