//! Git LFS (Large File Storage) — the Batch API + the "basic" transfer adapter.
//!
//! git-lfs keeps large files OUT of the packfile: the repo stores a small text
//! pointer, and the bytes move over a separate HTTP API. A client resolves
//! objects through `POST …/info/lfs/objects/batch`, then `PUT`/`GET`s each
//! object by its `oid` (a SHA-256 of the content). Ledge stores those objects
//! content-addressed under `<data_dir>/lfs/` and verifies the SHA-256 on upload,
//! so a corrupt/mismatched object can never be stored.
//!
//! v1 scope: the `basic` transfer adapter (the default) for the durable-repo
//! path (`/<repo>/info/lfs/…`). Objects are content-addressed and shared
//! (dedup); access requires auth but is not per-object ACL'd — same model as the
//! git object store. Locking, the SSH-discovery shortcut, and per-tenant LFS
//! namespaces are follow-ons.

use std::path::{Path, PathBuf};

use axum::{
    body::Bytes,
    extract::{Path as AxPath, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use sha2::{Digest, Sha256};

use crate::routes::AppState;

/// Content-addressed LFS object store rooted at `<data_dir>/lfs`. A thin handle:
/// the bytes live on disk, keyed by their lowercase-hex SHA-256 `oid`.
pub struct LfsStore {
    root: PathBuf,
}

impl LfsStore {
    pub fn at(data_dir: &Path) -> Self {
        Self {
            root: data_dir.join("lfs"),
        }
    }

    /// `<root>/<oid[0:2]>/<oid[2:4]>/<oid>` — two-level fanout to avoid huge dirs.
    fn object_path(&self, oid: &str) -> PathBuf {
        self.root.join(&oid[0..2]).join(&oid[2..4]).join(oid)
    }

    /// A valid LFS oid is 64 lowercase hex chars (a SHA-256).
    fn valid_oid(oid: &str) -> bool {
        oid.len() == 64 && oid.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    }

    pub fn has(&self, oid: &str) -> bool {
        Self::valid_oid(oid) && self.object_path(oid).is_file()
    }

    pub fn get(&self, oid: &str) -> Option<Vec<u8>> {
        if !Self::valid_oid(oid) {
            return None;
        }
        std::fs::read(self.object_path(oid)).ok()
    }

    /// Store `bytes` iff their SHA-256 equals `oid` (and `size`, when given). The
    /// content-address verification is the integrity guarantee — a mismatched or
    /// corrupt upload is rejected, never written.
    pub fn put(&self, oid: &str, bytes: &[u8], size: Option<u64>) -> Result<(), String> {
        if !Self::valid_oid(oid) {
            return Err("invalid oid".into());
        }
        if let Some(s) = size {
            if s != bytes.len() as u64 {
                return Err(format!("size mismatch: declared {s}, got {}", bytes.len()));
            }
        }
        let actual = hex::encode(Sha256::digest(bytes));
        if actual != oid {
            return Err(format!("oid mismatch: declared {oid}, computed {actual}"));
        }
        if self.has(oid) {
            return Ok(()); // already stored (idempotent)
        }
        let path = self.object_path(oid);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
        }
        // Atomic publish: write a temp sibling then rename.
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, bytes).map_err(|e| e.to_string())?;
        std::fs::rename(&tmp, &path).map_err(|e| e.to_string())?;
        Ok(())
    }
}

// ── Batch API wire types ──────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
pub struct BatchRequest {
    operation: String,
    #[serde(default)]
    objects: Vec<ObjectId>,
}
#[derive(serde::Deserialize, Clone)]
struct ObjectId {
    oid: String,
    #[serde(default)]
    size: u64,
}
#[derive(serde::Serialize)]
pub struct BatchResponse {
    transfer: &'static str,
    objects: Vec<ObjectResp>,
}
#[derive(serde::Serialize)]
struct ObjectResp {
    oid: String,
    size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    authenticated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    actions: Option<Actions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ObjError>,
}
#[derive(serde::Serialize)]
struct Actions {
    #[serde(skip_serializing_if = "Option::is_none")]
    download: Option<Act>,
    #[serde(skip_serializing_if = "Option::is_none")]
    upload: Option<Act>,
}
#[derive(serde::Serialize)]
struct Act {
    href: String,
}
#[derive(serde::Serialize)]
struct ObjError {
    code: u32,
    message: String,
}

/// Reconstruct the base URL the client reached us on, so batch `href`s point back
/// here. Honors `X-Forwarded-Proto`/`Host` (TLS-terminating proxy); defaults http.
fn base_url(headers: &HeaderMap, repo: &str) -> String {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");
    format!("{scheme}://{host}/{repo}/info/lfs/objects")
}

/// `POST /{repo}/info/lfs/objects/batch` — resolve objects for upload/download.
pub async fn lfs_batch(
    State(state): State<AppState>,
    AxPath(repo): AxPath<String>,
    headers: HeaderMap,
    _principal: crate::auth::Principal,
    Json(req): Json<BatchRequest>,
) -> Response {
    let store = LfsStore::at(&state.data_dir);
    let base = base_url(&headers, &repo);
    let download = req.operation == "download";

    let objects = req
        .objects
        .iter()
        .map(|o| {
            let href = format!("{base}/{}", o.oid);
            if download {
                if store.has(&o.oid) {
                    ObjectResp { oid: o.oid.clone(), size: o.size, authenticated: Some(true), actions: Some(Actions { download: Some(Act { href }), upload: None }), error: None }
                } else {
                    ObjectResp { oid: o.oid.clone(), size: o.size, authenticated: None, actions: None, error: Some(ObjError { code: 404, message: "object not found".into() }) }
                }
            } else {
                // upload: omit the action for objects we already hold (client skips them).
                let actions = if store.has(&o.oid) {
                    None
                } else {
                    Some(Actions { download: None, upload: Some(Act { href }) })
                };
                ObjectResp { oid: o.oid.clone(), size: o.size, authenticated: Some(true), actions, error: None }
            }
        })
        .collect();

    let body = BatchResponse { transfer: "basic", objects };
    (
        StatusCode::OK,
        [("content-type", "application/vnd.git-lfs+json")],
        Json(body),
    )
        .into_response()
}

/// `PUT /{repo}/info/lfs/objects/{oid}` — upload an object (verified by SHA-256).
pub async fn lfs_upload(
    State(state): State<AppState>,
    AxPath((_repo, oid)): AxPath<(String, String)>,
    _principal: crate::auth::Principal,
    body: Bytes,
) -> Response {
    let store = LfsStore::at(&state.data_dir);
    match store.put(&oid, &body, None) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => {
            tracing::warn!(oid = %oid, error = %e, "lfs upload rejected");
            (StatusCode::UNPROCESSABLE_ENTITY, e).into_response()
        }
    }
}

/// `GET /{repo}/info/lfs/objects/{oid}` — download an object's bytes.
pub async fn lfs_download(
    State(state): State<AppState>,
    AxPath((_repo, oid)): AxPath<(String, String)>,
    _principal: crate::auth::Principal,
) -> Response {
    let store = LfsStore::at(&state.data_dir);
    match store.get(&oid) {
        Some(bytes) => (
            StatusCode::OK,
            [("content-type", "application/octet-stream")],
            bytes,
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_verifies_content_address() {
        let dir = tempfile::tempdir().unwrap();
        let store = LfsStore::at(dir.path());
        let data = b"large file contents";
        let oid = hex::encode(Sha256::digest(data));
        assert!(!store.has(&oid));
        store.put(&oid, data, Some(data.len() as u64)).unwrap();
        assert!(store.has(&oid));
        assert_eq!(store.get(&oid).unwrap(), data);
        // wrong oid (claims a different hash) is rejected
        let bad = "0".repeat(64);
        assert!(store.put(&bad, data, None).is_err());
        // size mismatch rejected
        assert!(store.put(&oid, data, Some(999)).is_err());
        // malformed oid rejected
        assert!(!store.has("xyz"));
        assert!(store.get("nothex").is_none());
    }
}
