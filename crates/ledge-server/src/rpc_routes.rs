//! Binary control-plane endpoint: `POST /rpc` (Phase 2b, Tier 1).
//!
//! Reads a Cap'n Proto-serialized [`ledge_rpc::ledge_capnp::request`] from the
//! raw request body, dispatches it against an [`RpcCtx`] assembled from
//! [`AppState`], and returns the Cap'n Proto-serialized `Response` bytes with
//! Content-Type `application/x-ledge-capnp`.
//!
//! A *business* failure (unknown workspace, missing object, commit conflict) is
//! encoded inside the returned `Response` and still yields HTTP 200; only a
//! genuinely malformed request body (undecodable capnp) maps to HTTP 400.

use std::time::Instant;

use axum::{
    body::Bytes,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use tracing::{info, warn, Instrument};

use ledge_rpc::RpcCtx;

use crate::{metrics, routes::AppState};

/// Wire content type for capnp `/rpc` request and response bodies.
const CONTENT_TYPE: &str = "application/x-ledge-capnp";

/// `POST /rpc` — decode one capnp `Request`, dispatch, return the `Response`.
///
/// 4d-1: `principal` is the verified identity the auth middleware injected
/// (disabled mode injects the synthetic root). Extracting it here proves the
/// request reached this handler authenticated; the `FromRequestParts`
/// `Principal` extractor must precede the body-consuming `Bytes` extractor.
/// 4d-2 will thread `principal.tenant_id` into `RpcCtx` for per-tenant
/// resource scoping — 4d-1 only asserts presence (no `RpcCtx` change).
pub async fn rpc(
    State(state): State<AppState>,
    principal: crate::auth::Principal,
    body: Bytes,
) -> Response {
    let start = Instant::now();
    tracing::debug!(principal = %principal.principal_id, "rpc authenticated");
    // The metric/trace label is the request union tag, decoded once here. A
    // malformed body decodes to "unknown" (and dispatch will return Err -> 400).
    let method = ledge_rpc::method_name(&body);
    let span = tracing::info_span!("rpc", method);

    let ctx = RpcCtx {
        // RpcCtx needs the concrete DiskObjectStore (writeObject uses
        // write_git_object, not on the ObjectStore trait); refs is the dyn seam.
        objects: state.objects_disk.clone(),
        refs: state.refs.clone(),
        workspaces: state.workspaces.clone(),
        gc: state.gc.clone(),
        default_ttl_secs: state.default_ttl_secs,
    };

    let result = ledge_rpc::dispatch(&body, &ctx).instrument(span).await;
    metrics::record_rpc_request(method, start.elapsed());

    match result {
        Ok(bytes) => {
            info!(method, bytes = bytes.len(), "rpc dispatched");
            ([(header::CONTENT_TYPE, CONTENT_TYPE)], bytes).into_response()
        }
        Err(e) => {
            // Only a malformed/undecodable message reaches here; business errors
            // are encoded into a 200 Response above.
            warn!(method, error = %e, "rpc malformed request");
            (StatusCode::BAD_REQUEST, format!("malformed rpc request: {e}")).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use capnp::message::{Builder, ReaderOptions};
    use capnp::serialize;
    use ledge_core::HLC;
    use ledge_rpc::ledge_capnp::{request, response};
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
            shard_map: None,
            cluster_gc: None,
            auth: crate::auth::AuthCtx::disabled(),
        }
    }

    fn write_object_req(content: &[u8]) -> Vec<u8> {
        let mut msg = Builder::new_default();
        {
            let root = msg.init_root::<request::Builder>();
            let mut w = root.init_write_object();
            w.set_git_type(3);
            w.set_content(content);
        }
        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &msg).unwrap();
        buf
    }

    fn read_object_req(id_bytes: &[u8; 32]) -> Vec<u8> {
        let mut msg = Builder::new_default();
        {
            let root = msg.init_root::<request::Builder>();
            let r = root.init_read_object();
            r.init_id().set_bytes(&id_bytes[..]);
        }
        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &msg).unwrap();
        buf
    }

    async fn post_rpc(app: axum::Router, body: Vec<u8>) -> (StatusCode, Vec<u8>) {
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc")
                    .header("content-type", CONTENT_TYPE)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (status, bytes.to_vec())
    }

    /// Full round-trip over the HTTP boundary: POST writeObject, decode the
    /// returned objectId, POST readObject for it, decode the content.
    #[tokio::test]
    async fn rpc_write_then_read_roundtrips_over_http() {
        let dir = TempDir::new().unwrap();
        let state = state_over(&dir);
        let content = b"capnp over http payload";

        let (status, out) =
            post_rpc(crate::build_app(state.clone()), write_object_req(content)).await;
        assert_eq!(status, StatusCode::OK);

        let reader = serialize::read_message(&mut &out[..], ReaderOptions::new()).unwrap();
        let id_bytes: [u8; 32] =
            match reader.get_root::<response::Reader>().unwrap().which().unwrap() {
                response::Which::ObjectId(oid) => {
                    oid.unwrap().get_bytes().unwrap().try_into().unwrap()
                }
                _ => panic!("expected objectId"),
            };

        let (status2, out2) =
            post_rpc(crate::build_app(state), read_object_req(&id_bytes)).await;
        assert_eq!(status2, StatusCode::OK);
        let reader2 = serialize::read_message(&mut &out2[..], ReaderOptions::new()).unwrap();
        match reader2.get_root::<response::Reader>().unwrap().which().unwrap() {
            response::Which::ObjectContent(c) => assert_eq!(c.unwrap(), &content[..]),
            _ => panic!("expected objectContent"),
        }
    }

    /// A malformed body yields HTTP 400.
    #[tokio::test]
    async fn rpc_malformed_body_yields_400() {
        let dir = TempDir::new().unwrap();
        let state = state_over(&dir);
        let (status, _) = post_rpc(crate::build_app(state), vec![0xFFu8; 3]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
