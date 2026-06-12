//! Admin control-plane handlers (Phase 2d).
//!
//! `POST /admin/snapshot` CoW-clones the entire `data_dir` (objects + refs +
//! leases) to a fresh destination directory using the native copy-on-write
//! syscall via [`ledge_cow::clone_tree`]. On APFS/btrfs/XFS/ReFS this is an
//! O(metadata) operation that shares blocks until the copies diverge; on a
//! non-CoW filesystem it transparently falls back to a byte copy. The result
//! is a fully independent, valid Ledge data directory.

use std::path::PathBuf;
use std::time::Instant;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::{metrics, routes::AppState};

/// `POST /admin/snapshot` body.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SnapshotRequest {
    /// Absolute destination path. Must not already exist.
    pub dest: String,
}

/// `POST /admin/snapshot` response.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SnapshotResponse {
    /// Files satisfied by a CoW reflink.
    pub reflinked: usize,
    /// Files that fell back to a byte copy.
    pub copied: usize,
    /// Total regular files cloned.
    pub files: usize,
    /// Directories created in the snapshot.
    pub dirs: usize,
    /// Total logical bytes of cloned files.
    pub bytes: u64,
    /// True iff every file was reflinked (no byte copies) and at least one was.
    pub cow_used: bool,
    /// Wall-clock duration of the clone, in milliseconds.
    pub duration_ms: u64,
}

/// `POST /admin/snapshot` — CoW-clone the whole data dir to `dest`.
///
/// Validates `dest` is absolute and does not already exist (400 otherwise),
/// then runs the (blocking) recursive clone on a `spawn_blocking` worker so it
/// does not stall the async runtime. Returns 200 with clone statistics.
pub async fn admin_snapshot(
    State(state): State<AppState>,
    _principal: crate::auth::Principal,
    Json(req): Json<SnapshotRequest>,
) -> Response {
    let dest = PathBuf::from(&req.dest);
    if !dest.is_absolute() {
        return (StatusCode::BAD_REQUEST, "dest must be an absolute path").into_response();
    }
    if dest.exists() {
        return (StatusCode::BAD_REQUEST, "dest already exists").into_response();
    }

    let src = state.data_dir.clone();
    let started = Instant::now();
    // clone_tree is synchronous filesystem work; isolate it from the reactor.
    let result = tokio::task::spawn_blocking(move || ledge_cow::clone_tree(&src, &dest)).await;

    let stats = match result {
        Ok(Ok(stats)) => stats,
        Ok(Err(e)) => {
            warn!(error = %e, "snapshot clone failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("snapshot failed: {e}"))
                .into_response();
        }
        Err(e) => {
            warn!(error = %e, "snapshot task join failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let elapsed = started.elapsed();
    metrics::record_snapshot(&stats, elapsed);

    let cow_used = stats.copied == 0 && stats.reflinked > 0;
    let duration_ms = elapsed.as_millis() as u64;
    info!(
        files = stats.files,
        dirs = stats.dirs,
        bytes = stats.bytes,
        reflinked = stats.reflinked,
        copied = stats.copied,
        cow_used,
        duration_ms,
        "admin snapshot complete"
    );

    let body = SnapshotResponse {
        reflinked: stats.reflinked,
        copied: stats.copied,
        files: stats.files,
        dirs: stats.dirs,
        bytes: stats.bytes,
        cow_used,
        duration_ms,
    };
    (StatusCode::OK, Json(body)).into_response()
}

/// `POST /admin/repack` — deltify cold objects to reclaim disk; returns stats.
///
/// Runs the offline [`ledge_object_store::repack::repack`] pass over the
/// node-local concrete disk store. Each candidate is re-stored as a delta only
/// if the store's self-verifying `deltify` accepts it, so a repack can never
/// corrupt or grow the store. Returns 200 with the pass statistics and the
/// before/after byte ratio.
pub async fn admin_repack(State(state): State<AppState>) -> Response {
    match ledge_object_store::repack::repack(&state.objects_disk).await {
        Ok(s) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "objects_seen": s.objects_seen,
                "objects_deltified": s.objects_deltified,
                "objects_packed": s.objects_packed,
                "files_before": s.files_before,
                "files_after": s.files_after,
                "bytes_before": s.bytes_before,
                "bytes_after": s.bytes_after,
                "ratio": if s.bytes_after > 0 { s.bytes_before as f64 / s.bytes_after as f64 } else { 1.0 },
            })),
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// `POST /admin/tier` — spill cold pack bodies to the configured S3 object store.
///
/// Runs one [`ledge_object_store::DiskObjectStore::tier_packs`] pass over the
/// node-local disk store, uploading each not-yet-tiered `.pack` body to the cold
/// tier and recording the put-side counters. Outcomes:
/// - **200** with `{packs_tiered, bytes_uploaded}` on success.
/// - **503** when no cold tier is configured (`[s3].enabled = false`) — the pass
///   returns an `Unavailable("s3 cold tier disabled")` error, matched on the
///   `"disabled"` substring so it is reported as a soft "feature off", not a
///   server fault.
/// - **502** for any other failure (e.g. the S3 upload itself errored), since
///   the fault is in the upstream object store, not this node.
pub async fn admin_tier(State(state): State<AppState>) -> Response {
    match state.objects_disk.tier_packs().await {
        Ok(s) => {
            crate::metrics::record_s3_tier("put", s.packs_tiered as u64, s.bytes_uploaded);
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "packs_tiered": s.packs_tiered,
                    "bytes_uploaded": s.bytes_uploaded,
                })),
            )
                .into_response()
        }
        Err(e) => {
            // disabled (no cold tier) ⇒ 503; any other (upstream) error ⇒ 502.
            let msg = e.to_string();
            if msg.contains("disabled") {
                StatusCode::SERVICE_UNAVAILABLE.into_response()
            } else {
                (StatusCode::BAD_GATEWAY, msg).into_response()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_request_response_roundtrip() {
        let req: SnapshotRequest =
            serde_json::from_str(r#"{"dest":"/tmp/snap"}"#).unwrap();
        assert_eq!(req.dest, "/tmp/snap");

        let resp = SnapshotResponse {
            reflinked: 4,
            copied: 0,
            files: 4,
            dirs: 2,
            bytes: 4096,
            cow_used: true,
            duration_ms: 3,
        };
        let s = serde_json::to_string(&resp).unwrap();
        let back: SnapshotResponse = serde_json::from_str(&s).unwrap();
        assert_eq!(back, resp);
    }
}

#[cfg(test)]
mod route_tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use ledge_core::{HLC, ObjectStore, RefName, RefStore};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tower::ServiceExt; // oneshot

    fn state_over(dir: &TempDir) -> AppState {
        let p = dir.path().to_path_buf();
        let hlc = Arc::new(HLC::new());
        let objects = Arc::new(ledge_object_store::DiskObjectStore::new(p.clone()).unwrap());
        let refs = Arc::new(ledge_ref_store::RefStoreImpl::open(p.clone(), hlc.clone()).unwrap());
        let (workspaces, leases, gc) =
            crate::build_workspace_stack(p.clone(), objects.clone(), refs.clone(), hlc, ledge_workspace::QuotaLimits::default(), std::sync::Arc::new(ledge_workspace::UsageMap::default())).unwrap();
        AppState {
            objects: objects.clone() as std::sync::Arc<dyn ledge_core::ObjectStore>,
            objects_disk: objects.clone(),
            refs: refs.clone() as std::sync::Arc<dyn ledge_core::RefStore>,
            workspaces,
            leases,
            gc,
            default_ttl_secs: 3600,
            data_dir: p,
            raft_shards: None,
            cluster_refs: None,
            cluster_objects: None,
            webhooks: None,
            sync: None,
            shard_map: None,
            cluster_gc: None,
            auth: crate::auth::AuthCtx::disabled(),
            quota: crate::quota::QuotaCtx::disabled(),
        }
    }

    /// Seed a data dir with one object + one ref, snapshot it to a fresh
    /// destination, then open `DiskObjectStore` + `RefStoreImpl` from the
    /// destination and read both back — proving the snapshot is an independent,
    /// valid Ledge repo.
    #[tokio::test]
    async fn snapshot_produces_independent_valid_repo() {
        let src_dir = TempDir::new().unwrap();
        let state = state_over(&src_dir);

        // Seed: write a git blob object and a ref pointing at it.
        let content = bytes::Bytes::from_static(b"snapshot payload object");
        let oid = state
            .objects_disk
            .write_git_object(3, content.clone())
            .await
            .unwrap();
        let ref_name = RefName::new("refs/heads/main").unwrap();
        state.refs.update(&ref_name, oid, None).await.unwrap();

        // Destination: a child of a tempdir that does not yet exist (the handler
        // creates it; clone_tree refuses a pre-existing dest).
        let dest_parent = TempDir::new().unwrap();
        let dest = dest_parent.path().join("snap");
        let dest_str = dest.to_str().unwrap().to_string();

        let app = crate::build_app(state.clone());
        let body = serde_json::to_string(&SnapshotRequest { dest: dest_str }).unwrap();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/snapshot")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let b = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let out: SnapshotResponse = serde_json::from_slice(&b).unwrap();
        assert!(out.files > 0, "snapshot must have cloned files: {out:?}");

        // Open the snapshot as an independent repo and read back the seeded data.
        let hlc2 = Arc::new(HLC::new());
        let snap_objects = ledge_object_store::DiskObjectStore::new(dest.clone()).unwrap();
        let snap_refs = ledge_ref_store::RefStoreImpl::open(dest.clone(), hlc2).unwrap();

        let read_back = snap_objects.read(oid).await.unwrap();
        assert_eq!(read_back, content, "object content must survive the snapshot");

        let ref_entry = snap_refs
            .get(&ref_name)
            .await
            .unwrap()
            .expect("ref must exist in the snapshot");
        assert_eq!(ref_entry.target, oid, "ref target must survive the snapshot");
    }

    /// `POST /admin/repack` returns 200 and a JSON body carrying the pass stats.
    #[tokio::test]
    async fn repack_endpoint_returns_stats() {
        let dir = TempDir::new().unwrap();
        let state = state_over(&dir);

        // Seed a couple of objects so the pass has something to enumerate.
        for i in 0..3u32 {
            let c = bytes::Bytes::from(format!("repack object {i}"));
            state.objects_disk.write_git_object(3, c).await.unwrap();
        }

        let app = crate::build_app(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/repack")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let b = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let out: serde_json::Value = serde_json::from_slice(&b).unwrap();
        assert!(
            out.get("objects_seen").and_then(|v| v.as_u64()).is_some(),
            "body must carry objects_seen: {out:?}"
        );
    }

    /// `POST /admin/tier` on a store WITHOUT a configured cold tier returns 503
    /// (Service Unavailable) — the feature is off, not a server fault. The
    /// default `state_over` store never installs a cold tier, so `tier_packs`
    /// returns the `"s3 cold tier disabled"` error that maps to 503.
    #[tokio::test]
    async fn tier_endpoint_503_when_disabled() {
        let dir = TempDir::new().unwrap();
        let state = state_over(&dir);

        let app = crate::build_app(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/tier")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
