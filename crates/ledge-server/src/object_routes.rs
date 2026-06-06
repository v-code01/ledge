//! Content-addressed object replication HTTP endpoints (Phase 3, spec §2.5).
//!
//! These are the server side of the [`ledge_cluster::net_http::HttpObjectPeer`]
//! transport: the endpoints a peer node POSTs object bytes to (and GETs by id
//! for anti-entropy fetch) so within-shard object writes replicate to a quorum.
//!
//! - `POST /objects/{shard}/replicate?type=<n>` — write the raw octet-stream
//!   body to **this node's local** [`DiskObjectStore`] via `write_git_object`
//!   (default `type=3`, a git blob). The store is BLAKE3 content-addressed, so
//!   the write is idempotent (a re-put of identical bytes is a no-op) and the
//!   200 body is the resulting [`ObjectId`] hex. The sender verifies that id
//!   equals the id it expected, so a tampered/buggy node is caught.
//! - `GET /objects/{shard}/{id}` — read the raw content from the local store,
//!   returning `200` with the bytes or `404` if absent.
//!
//! # Single-node safety
//! These routes operate purely on `AppState::objects_disk` — the node-local
//! concrete store that exists in BOTH modes. In single-node mode they simply
//! serve the local objects (harmless: no peer ever calls them). They do not
//! touch the `dyn ObjectStore` / `dyn RefStore` seams or any git/workspace path,
//! so existing single-node behavior and tests are unchanged. The `{shard}` path
//! segment is accepted for routing symmetry with the cluster transport; the
//! node-local store holds whatever shards this node hosts.

use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use tracing::warn;

use ledge_core::{LedgeError, ObjectId, ObjectStore};

use crate::routes::AppState;

/// `?type=<n>` query for the replicate endpoint: the git object type tag
/// (1=commit, 2=tree, 3=blob, 4=tag). Absent ⇒ blob (3), matching `write`.
#[derive(Debug, Deserialize)]
pub struct ReplicateQuery {
    #[serde(default)]
    pub r#type: Option<u8>,
}

/// `POST /objects/{shard}/replicate?type=<n>` — replicate one object to this
/// node's local store. Body is the raw object content. Returns `200` with the
/// content-addressed [`ObjectId`] hex, or `400` if the bytes cannot be stored
/// (e.g. an unknown git type tag).
pub async fn replicate_object(
    State(state): State<AppState>,
    Path(_shard): Path<u32>,
    Query(q): Query<ReplicateQuery>,
    body: Bytes,
) -> Response {
    let git_type = q.r#type.unwrap_or(3);
    // Content-addressed + idempotent: the store re-derives the id from the bytes
    // and the type tag; a re-put of identical content is a no-op success.
    match state.objects_disk.write_git_object(git_type, body).await {
        Ok(id) => (StatusCode::OK, id.to_hex()).into_response(),
        Err(e) => {
            warn!(error = %e, git_type, "object replicate failed");
            (StatusCode::BAD_REQUEST, e.to_string()).into_response()
        }
    }
}

/// `GET /objects/{shard}/{id}` — serve the raw content for `id` from this node's
/// local store. Returns `200` with the bytes, `404` if absent, `400` if the id
/// is not valid hex, or `500` on an unexpected store error.
pub async fn get_object(
    State(state): State<AppState>,
    Path((_shard, id_hex)): Path<(u32, String)>,
) -> Response {
    let id = match ObjectId::from_hex(&id_hex) {
        Ok(id) => id,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid object id hex").into_response(),
    };
    match state.objects_disk.read(id).await {
        Ok(bytes) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            bytes,
        )
            .into_response(),
        Err(LedgeError::NotFound(_)) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            warn!(error = %e, id = %id_hex, "object fetch failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Build a minimal `AppState` over a tempdir-backed store for handler tests.
    /// Only `objects_disk` is exercised by these routes; the other seams are the
    /// up-cast of the same local store (single-node shape).
    async fn test_state() -> (AppState, tempfile::TempDir, Arc<ledge_object_store::DiskObjectStore>)
    {
        use ledge_core::HLC;
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let objects =
            Arc::new(ledge_object_store::DiskObjectStore::new(data_dir.clone()).unwrap());
        let hlc = Arc::new(HLC::new());
        let refs = Arc::new(ledge_ref_store::RefStoreImpl::open(data_dir.clone(), hlc.clone()).unwrap());
        let (workspaces, leases, gc) = crate::build_workspace_stack(
            data_dir.clone(),
            objects.clone(),
            refs.clone(),
            hlc,
        )
        .unwrap();
        let state = AppState {
            objects: objects.clone() as Arc<dyn ObjectStore>,
            objects_disk: objects.clone(),
            refs: refs as Arc<dyn ledge_core::RefStore>,
            workspaces,
            leases,
            gc,
            default_ttl_secs: 3600,
            data_dir,
            raft_shards: None,
            cluster_refs: None,
            shard_map: None,
            cluster_gc: None,
        };
        (state, dir, objects)
    }

    #[tokio::test]
    async fn replicate_then_get_roundtrips() {
        let (state, _dir, store) = test_state().await;

        let body = Bytes::from_static(b"hello replication");
        let resp = replicate_object(
            State(state.clone()),
            Path(0),
            Query(ReplicateQuery { r#type: None }),
            body.clone(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let id_hex = String::from_utf8(
            axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        let id = ObjectId::from_hex(&id_hex).unwrap();
        assert!(store.exists(id).await.unwrap());

        let resp = get_object(State(state), Path((0, id_hex))).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let got = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&got[..], &body[..]);
    }

    #[tokio::test]
    async fn get_missing_returns_404() {
        let (state, _dir, _store) = test_state().await;
        let absent = ObjectId::from(blake3::hash(b"absent"));
        let resp = get_object(State(state), Path((0, absent.to_hex()))).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_bad_hex_returns_400() {
        let (state, _dir, _store) = test_state().await;
        let resp = get_object(State(state), Path((0, "not-hex".into()))).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
