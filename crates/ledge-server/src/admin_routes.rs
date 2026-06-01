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
pub async fn admin_snapshot(State(state): State<AppState>, Json(req): Json<SnapshotRequest>) -> Response {
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
            crate::build_workspace_stack(p.clone(), objects.clone(), refs.clone(), hlc).unwrap();
        AppState {
            objects,
            refs,
            workspaces,
            leases,
            gc,
            default_ttl_secs: 3600,
            data_dir: p,
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
            .objects
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
}
