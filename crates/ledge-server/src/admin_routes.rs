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
