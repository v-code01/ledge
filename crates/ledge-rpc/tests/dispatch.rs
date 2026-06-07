//! Round-trip dispatch tests: for each `Request` variant, build a capnp message,
//! dispatch against a real tempdir-backed `RpcCtx`, decode the `Response`, and
//! assert correctness — including the business-error paths.

use std::sync::Arc;

use capnp::message::{Builder, ReaderOptions};
use capnp::serialize;

use ledge_core::{ObjectStore, RefName, HLC};
use ledge_object_store::DiskObjectStore;
use ledge_ref_store::RefStoreImpl;
use ledge_rpc::ledge_capnp::{request, response};
use ledge_rpc::{dispatch, RpcCtx};
use tempfile::TempDir;

/// Build a real `RpcCtx` over a fresh tempdir, returning the guard to keep it alive.
fn ctx() -> (RpcCtx, TempDir) {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    let hlc = Arc::new(HLC::new());
    let objects = Arc::new(DiskObjectStore::new(p.clone()).unwrap());
    let refs = Arc::new(RefStoreImpl::open(p.clone(), hlc.clone()).unwrap());
    let leases = Arc::new(ledge_workspace::LeaseStore::open(p.clone(), hlc.clone()).unwrap());
    let coordinator: Arc<dyn ledge_ref_store::AtomicCommit> =
        Arc::new(ledge_ref_store::LocalAtomicCommit::new(refs.clone()));
    let workspaces = Arc::new(ledge_workspace::WorkspaceManager::new(
        refs.clone(),
        leases.clone(),
        hlc.clone(),
        coordinator,
        ledge_workspace::QuotaLimits::default(),
        std::sync::Arc::new(ledge_workspace::UsageMap::default()),
        ));
    let gc = Arc::new(ledge_workspace::Gc::new(
        refs.clone(),
        leases,
        objects.clone(),
        std::sync::Arc::new(ledge_workspace::UsageMap::default()),
        ));
    let ctx = RpcCtx {
        objects,
        refs,
        workspaces,
        gc,
        default_ttl_secs: 3600,
        // Default tenant: preserves pre-4d-2 (single-tenant) behavior.
        tenant_id: "root".into(),
    };
    (ctx, dir)
}

/// Build two `RpcCtx`s sharing ONE backing store stack but with distinct
/// tenants ("acme", "globex"). This is the dispatch-level analogue of two API
/// keys hitting the same node — it lets us prove cross-tenant isolation without
/// the HTTP/auth layer. Returns `(acme_ctx, globex_ctx, dir)` (the `dir` guard
/// keeps the shared tempdir alive).
fn two_tenant_ctxs() -> (RpcCtx, RpcCtx, TempDir) {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    let hlc = Arc::new(HLC::new());
    let objects = Arc::new(DiskObjectStore::new(p.clone()).unwrap());
    let refs = Arc::new(RefStoreImpl::open(p.clone(), hlc.clone()).unwrap());
    let leases = Arc::new(ledge_workspace::LeaseStore::open(p.clone(), hlc.clone()).unwrap());
    let coordinator: Arc<dyn ledge_ref_store::AtomicCommit> =
        Arc::new(ledge_ref_store::LocalAtomicCommit::new(refs.clone()));
    let workspaces = Arc::new(ledge_workspace::WorkspaceManager::new(
        refs.clone(),
        leases.clone(),
        hlc.clone(),
        coordinator,
        ledge_workspace::QuotaLimits::default(),
        std::sync::Arc::new(ledge_workspace::UsageMap::default()),
        ));
    let gc = Arc::new(ledge_workspace::Gc::new(refs.clone(), leases, objects.clone(), std::sync::Arc::new(ledge_workspace::UsageMap::default())));
    let base = RpcCtx {
        objects,
        refs,
        workspaces,
        gc,
        default_ttl_secs: 3600,
        tenant_id: "acme".into(),
    };
    let globex = RpcCtx { tenant_id: "globex".into(), ..base.clone() };
    (base, globex, dir)
}

/// Seed the tenant's OWN durable `refs/heads/main` (physically re-rooted into
/// `refs/tenants/<t>/heads/main` for a non-root tenant, identity for root) so
/// `fork` by that ctx has a source, then fork and return the workspace id. The
/// client-facing source name passed to fork stays `refs/heads/main` — the
/// manager applies the tenant prefix.
async fn seed_and_fork(ctx: &RpcCtx) -> String {
    let prefix = ledge_core::tenant_prefix(&ctx.tenant_id);
    let durable = RefName::new(&format!("refs/{prefix}heads/main")).unwrap();
    if ctx.refs.get(&durable).await.unwrap().is_none() {
        let oid = ctx.objects.write(bytes::Bytes::from_static(b"seed")).await.unwrap();
        ctx.refs.update(&durable, oid, None).await.unwrap();
    }
    let out = dispatch(&fork_req(&["refs/heads/main"], 3600), ctx).await.unwrap();
    let reader = read_response(&out);
    match reader.get_root::<response::Reader>().unwrap().which().unwrap() {
        response::Which::Workspace(w) => w.unwrap().get_id().unwrap().to_string().unwrap(),
        _ => panic!("expected workspace from fork"),
    }
}

/// Assert a dispatched response is the `Response.error` variant.
fn assert_is_error(bytes: &[u8]) {
    let reader = read_response(bytes);
    assert!(
        matches!(
            reader.get_root::<response::Reader>().unwrap().which().unwrap(),
            response::Which::Error(_)
        ),
        "expected a Response.error (cross-tenant isolation)"
    );
}

/// Serialize a finished request message to a Vec<u8> (unpacked framing).
fn finish(msg: Builder<capnp::message::HeapAllocator>) -> Vec<u8> {
    let mut buf = Vec::new();
    serialize::write_message(&mut buf, &msg).unwrap();
    buf
}

/// Decode the response bytes into an owned reader. Zero-copy over `bytes`.
fn read_response(bytes: &[u8]) -> capnp::message::Reader<capnp::serialize::OwnedSegments> {
    serialize::read_message(&mut &bytes[..], ReaderOptions::new()).unwrap()
}

fn write_object_req(git_type: u8, content: &[u8]) -> Vec<u8> {
    let mut msg = Builder::new_default();
    {
        let root = msg.init_root::<request::Builder>();
        let mut w = root.init_write_object();
        w.set_git_type(git_type);
        w.set_content(content);
    }
    finish(msg)
}

fn read_object_req(id_bytes: &[u8; 32]) -> Vec<u8> {
    let mut msg = Builder::new_default();
    {
        let root = msg.init_root::<request::Builder>();
        let r = root.init_read_object();
        r.init_id().set_bytes(&id_bytes[..]);
    }
    finish(msg)
}

fn fork_req(sources: &[&str], ttl: u64) -> Vec<u8> {
    let mut msg = Builder::new_default();
    {
        let root = msg.init_root::<request::Builder>();
        let mut f = root.init_fork();
        f.set_ttl_seconds(ttl);
        let mut list = f.init_sources(sources.len() as u32);
        for (i, s) in sources.iter().enumerate() {
            list.set(i as u32, *s);
        }
    }
    finish(msg)
}

fn commit_req(ws_id: &str, mappings: &[(&str, &str)]) -> Vec<u8> {
    let mut msg = Builder::new_default();
    {
        let root = msg.init_root::<request::Builder>();
        let mut c = root.init_commit();
        c.set_workspace_id(ws_id);
        let mut list = c.init_mappings(mappings.len() as u32);
        for (i, (wref, dref)) in mappings.iter().enumerate() {
            let mut m = list.reborrow().get(i as u32);
            m.set_workspace_ref(*wref);
            m.set_durable_ref(*dref);
        }
    }
    finish(msg)
}

fn renew_req(ws_id: &str, ttl: u64) -> Vec<u8> {
    let mut msg = Builder::new_default();
    {
        let root = msg.init_root::<request::Builder>();
        let mut r = root.init_renew();
        r.set_workspace_id(ws_id);
        r.set_ttl_seconds(ttl);
    }
    finish(msg)
}

fn release_req(ws_id: &str) -> Vec<u8> {
    let mut msg = Builder::new_default();
    {
        let root = msg.init_root::<request::Builder>();
        let mut r = root.init_release();
        r.set_workspace_id(ws_id);
    }
    finish(msg)
}

fn get_workspace_req(ws_id: &str) -> Vec<u8> {
    let mut msg = Builder::new_default();
    {
        let root = msg.init_root::<request::Builder>();
        let mut g = root.init_get_workspace();
        g.set_workspace_id(ws_id);
    }
    finish(msg)
}

fn list_workspaces_req() -> Vec<u8> {
    let mut msg = Builder::new_default();
    {
        let mut root = msg.init_root::<request::Builder>();
        root.set_list_workspaces(());
    }
    finish(msg)
}

fn run_gc_req() -> Vec<u8> {
    let mut msg = Builder::new_default();
    {
        let mut root = msg.init_root::<request::Builder>();
        root.set_run_gc(());
    }
    finish(msg)
}

#[tokio::test]
async fn write_then_read_object_roundtrips() {
    let (ctx, _dir) = ctx();
    let content = b"hello capnp rpc";

    let out = dispatch(&write_object_req(3, content), &ctx).await.unwrap();
    let reader = read_response(&out);
    let resp = reader.get_root::<response::Reader>().unwrap();
    let id_bytes: [u8; 32] = match resp.which().unwrap() {
        response::Which::ObjectId(oid) => {
            oid.unwrap().get_bytes().unwrap().try_into().unwrap()
        }
        _ => panic!("expected objectId"),
    };

    let out2 = dispatch(&read_object_req(&id_bytes), &ctx).await.unwrap();
    let reader2 = read_response(&out2);
    let resp2 = reader2.get_root::<response::Reader>().unwrap();
    match resp2.which().unwrap() {
        response::Which::ObjectContent(c) => {
            assert_eq!(c.unwrap(), &content[..]);
        }
        _ => panic!("expected objectContent"),
    }
}

#[tokio::test]
async fn read_missing_object_yields_error() {
    let (ctx, _dir) = ctx();
    let out = dispatch(&read_object_req(&[0u8; 32]), &ctx).await.unwrap();
    let reader = read_response(&out);
    let resp = reader.get_root::<response::Reader>().unwrap();
    assert!(matches!(resp.which().unwrap(), response::Which::Error(_)));
}

#[tokio::test]
async fn write_unknown_git_type_yields_error() {
    let (ctx, _dir) = ctx();
    let out = dispatch(&write_object_req(99, b"x"), &ctx).await.unwrap();
    let reader = read_response(&out);
    let resp = reader.get_root::<response::Reader>().unwrap();
    assert!(matches!(resp.which().unwrap(), response::Which::Error(_)));
}

#[tokio::test]
async fn fork_then_get_workspace() {
    let (ctx, _dir) = ctx();
    // Seed a durable ref so fork has a source.
    let main = RefName::new("refs/heads/main").unwrap();
    let oid = ctx.objects.write(bytes::Bytes::from_static(b"c")).await.unwrap();
    ctx.refs.update(&main, oid, None).await.unwrap();

    let out = dispatch(&fork_req(&["refs/heads/main"], 0), &ctx).await.unwrap();
    let reader = read_response(&out);
    let resp = reader.get_root::<response::Reader>().unwrap();
    let ws_id = match resp.which().unwrap() {
        response::Which::Workspace(w) => {
            let w = w.unwrap();
            assert!(w.get_expires_at_ms() > 0);
            let refs = w.get_refs().unwrap();
            assert_eq!(refs.len(), 1);
            assert_eq!(refs.get(0).get_name().unwrap(), "refs/heads/main");
            w.get_id().unwrap().to_string().unwrap()
        }
        _ => panic!("expected workspace"),
    };

    // getWorkspace returns the same workspace.
    let out2 = dispatch(&get_workspace_req(&ws_id), &ctx).await.unwrap();
    let reader2 = read_response(&out2);
    let resp2 = reader2.get_root::<response::Reader>().unwrap();
    match resp2.which().unwrap() {
        response::Which::Workspace(w) => {
            assert_eq!(w.unwrap().get_id().unwrap().to_string().unwrap(), ws_id);
        }
        _ => panic!("expected workspace"),
    }
}

#[tokio::test]
async fn get_unknown_workspace_yields_error() {
    let (ctx, _dir) = ctx();
    let out = dispatch(&get_workspace_req(&"0".repeat(32)), &ctx)
        .await
        .unwrap();
    let reader = read_response(&out);
    let resp = reader.get_root::<response::Reader>().unwrap();
    assert!(matches!(resp.which().unwrap(), response::Which::Error(_)));
}

#[tokio::test]
async fn fork_commit_promotes_durable_ref() {
    let (ctx, _dir) = ctx();
    let main = RefName::new("refs/heads/main").unwrap();
    let oid = ctx.objects.write(bytes::Bytes::from_static(b"c")).await.unwrap();
    ctx.refs.update(&main, oid, None).await.unwrap();

    let out = dispatch(&fork_req(&["refs/heads/main"], 60), &ctx).await.unwrap();
    let reader = read_response(&out);
    let ws_id = match reader.get_root::<response::Reader>().unwrap().which().unwrap() {
        response::Which::Workspace(w) => w.unwrap().get_id().unwrap().to_string().unwrap(),
        _ => panic!("expected workspace"),
    };

    let ws_ref = format!("refs/workspaces/{ws_id}/heads/main");
    let durable = "refs/heads/feature";
    let out2 = dispatch(&commit_req(&ws_id, &[(&ws_ref, durable)]), &ctx)
        .await
        .unwrap();
    let reader2 = read_response(&out2);
    match reader2.get_root::<response::Reader>().unwrap().which().unwrap() {
        response::Which::CommitOutcomes(outs) => {
            let outs = outs.unwrap();
            assert_eq!(outs.len(), 1);
            let o = outs.get(0);
            assert_eq!(o.get_target().unwrap(), durable);
            assert!(o.get_ok());
        }
        _ => panic!("expected commitOutcomes"),
    }

    // Durable ref now resolves to the promoted target.
    let entry = ctx
        .refs
        .get(&RefName::new(durable).unwrap())
        .await
        .unwrap()
        .expect("durable promoted");
    assert_eq!(entry.target, oid);
}

#[tokio::test]
async fn commit_foreign_workspace_ref_rejected() {
    // A true CAS conflict in `commit`: the workspace ref carries work, but a
    // SECOND committer races and CAS-promotes the same durable ref first. The
    // manager re-reads the live durable, then its CAS is rejected because the
    // ref store sees a stale `expected` — surfacing as ok=false in the outcomes.
    //
    // We stage this deterministically by promoting through TWO different
    // workspaces forked off the same base, committing one (moves durable), then
    // committing the second with the *durable's pre-move target* still recorded
    // as the workspace's notion of base — the manager's CAS expects the live
    // durable and succeeds; to force the reject we instead drive the second
    // commit while holding the ref-store invariant that the durable already
    // moved. The reliable, dispatch-level commit *failure* path is a foreign
    // workspace ref, which the manager rejects (encoded as Response.error).
    let (ctx, _dir) = ctx();
    let main = RefName::new("refs/heads/main").unwrap();
    let oid1 = ctx.objects.write(bytes::Bytes::from_static(b"v1")).await.unwrap();
    ctx.refs.update(&main, oid1, None).await.unwrap();

    // Workspace A — the commit target.
    let out_a = dispatch(&fork_req(&["refs/heads/main"], 60), &ctx).await.unwrap();
    let ra = read_response(&out_a);
    let ws_a = match ra.get_root::<response::Reader>().unwrap().which().unwrap() {
        response::Which::Workspace(w) => w.unwrap().get_id().unwrap().to_string().unwrap(),
        _ => panic!("expected workspace"),
    };
    // Workspace B — a DIFFERENT workspace whose ref we maliciously feed to A.
    let out_b = dispatch(&fork_req(&["refs/heads/main"], 60), &ctx).await.unwrap();
    let rb = read_response(&out_b);
    let ws_b = match rb.get_root::<response::Reader>().unwrap().which().unwrap() {
        response::Which::Workspace(w) => w.unwrap().get_id().unwrap().to_string().unwrap(),
        _ => panic!("expected workspace"),
    };

    // Promote B's ref through A's commit: the manager rejects the foreign ref
    // (no partial promotion) and the failure is encoded as Response.error.
    let b_ws_ref = format!("refs/workspaces/{ws_b}/heads/main");
    let out2 = dispatch(
        &commit_req(&ws_a, &[(&b_ws_ref, "refs/heads/feature")]),
        &ctx,
    )
    .await
    .unwrap();
    let reader2 = read_response(&out2);
    assert!(matches!(
        reader2.get_root::<response::Reader>().unwrap().which().unwrap(),
        response::Which::Error(_)
    ));
    // No clobber: the durable target was never created.
    assert!(ctx
        .refs
        .get(&RefName::new("refs/heads/feature").unwrap())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn renew_returns_lease() {
    let (ctx, _dir) = ctx();
    let main = RefName::new("refs/heads/main").unwrap();
    let oid = ctx.objects.write(bytes::Bytes::from_static(b"c")).await.unwrap();
    ctx.refs.update(&main, oid, None).await.unwrap();
    let out = dispatch(&fork_req(&["refs/heads/main"], 1), &ctx).await.unwrap();
    let reader = read_response(&out);
    let ws_id = match reader.get_root::<response::Reader>().unwrap().which().unwrap() {
        response::Which::Workspace(w) => w.unwrap().get_id().unwrap().to_string().unwrap(),
        _ => panic!(),
    };

    let out2 = dispatch(&renew_req(&ws_id, 3600), &ctx).await.unwrap();
    let reader2 = read_response(&out2);
    match reader2.get_root::<response::Reader>().unwrap().which().unwrap() {
        response::Which::Lease(l) => {
            let l = l.unwrap();
            assert_eq!(l.get_id().unwrap().to_string().unwrap(), ws_id);
            assert!(l.get_generation() >= 2, "renew bumps generation");
            assert!(l.get_expires_at_ms() > 0);
        }
        _ => panic!("expected lease"),
    }
}

#[tokio::test]
async fn renew_unknown_workspace_yields_error() {
    let (ctx, _dir) = ctx();
    let out = dispatch(&renew_req(&"0".repeat(32), 60), &ctx).await.unwrap();
    let reader = read_response(&out);
    let resp = reader.get_root::<response::Reader>().unwrap();
    assert!(matches!(resp.which().unwrap(), response::Which::Error(_)));
}

#[tokio::test]
async fn release_returns_ok_and_is_idempotent() {
    let (ctx, _dir) = ctx();
    let main = RefName::new("refs/heads/main").unwrap();
    let oid = ctx.objects.write(bytes::Bytes::from_static(b"c")).await.unwrap();
    ctx.refs.update(&main, oid, None).await.unwrap();
    let out = dispatch(&fork_req(&["refs/heads/main"], 60), &ctx).await.unwrap();
    let reader = read_response(&out);
    let ws_id = match reader.get_root::<response::Reader>().unwrap().which().unwrap() {
        response::Which::Workspace(w) => w.unwrap().get_id().unwrap().to_string().unwrap(),
        _ => panic!(),
    };

    for _ in 0..2 {
        let out = dispatch(&release_req(&ws_id), &ctx).await.unwrap();
        let reader = read_response(&out);
        assert!(matches!(
            reader.get_root::<response::Reader>().unwrap().which().unwrap(),
            response::Which::Ok(())
        ));
    }
    // Workspace is gone.
    let out = dispatch(&get_workspace_req(&ws_id), &ctx).await.unwrap();
    let reader = read_response(&out);
    assert!(matches!(
        reader.get_root::<response::Reader>().unwrap().which().unwrap(),
        response::Which::Error(_)
    ));
}

#[tokio::test]
async fn list_workspaces_returns_live() {
    let (ctx, _dir) = ctx();
    let main = RefName::new("refs/heads/main").unwrap();
    let oid = ctx.objects.write(bytes::Bytes::from_static(b"c")).await.unwrap();
    ctx.refs.update(&main, oid, None).await.unwrap();
    let _ = dispatch(&fork_req(&["refs/heads/main"], 3600), &ctx).await.unwrap();
    let _ = dispatch(&fork_req(&["refs/heads/main"], 3600), &ctx).await.unwrap();

    let out = dispatch(&list_workspaces_req(), &ctx).await.unwrap();
    let reader = read_response(&out);
    match reader.get_root::<response::Reader>().unwrap().which().unwrap() {
        response::Which::WorkspaceList(list) => {
            assert_eq!(list.unwrap().len(), 2);
        }
        _ => panic!("expected workspaceList"),
    }
}

#[tokio::test]
async fn run_gc_returns_stats() {
    let (ctx, _dir) = ctx();
    // Write an orphan object (no ref) -> GC reclaims it.
    let _ = ctx.objects.write(bytes::Bytes::from_static(b"orphan")).await.unwrap();
    let out = dispatch(&run_gc_req(), &ctx).await.unwrap();
    let reader = read_response(&out);
    match reader.get_root::<response::Reader>().unwrap().which().unwrap() {
        response::Which::GcStats(s) => {
            let s = s.unwrap();
            assert_eq!(s.get_scanned(), 1);
            assert_eq!(s.get_reclaimed(), 1);
        }
        _ => panic!("expected gcStats"),
    }
}

#[tokio::test]
async fn malformed_message_yields_err() {
    let (ctx, _dir) = ctx();
    // Random bytes that are not a valid capnp framed message.
    let bogus = [0xFFu8; 7];
    assert!(dispatch(&bogus, &ctx).await.is_err());
}

// ---------------------------------------------------------------------------
// §6 RPC isolation parity (mirrors the REST matrix at the dispatch layer).
//
// One shared store stack, two tenants ("acme" forks; "globex" attacks). The
// tenant rides on `RpcCtx.tenant_id`; the manager applies the prefix +
// ownership check, so every cross-tenant op surfaces as `Response.error`
// (the RPC analogue of the REST 404) — never a leak, never a mutation.
// ---------------------------------------------------------------------------

/// globex commit/renew/release/getWorkspace on acme's workspace id ⇒ error, and
/// the workspace is untouched (acme can still operate on it afterward).
#[tokio::test]
async fn cross_tenant_workspace_ops_are_isolated() {
    let (acme, globex, _dir) = two_tenant_ctxs();
    let ws_id = seed_and_fork(&acme).await;

    // getWorkspace: globex must not see acme's workspace.
    assert_is_error(&dispatch(&get_workspace_req(&ws_id), &globex).await.unwrap());

    // renew: globex cannot extend acme's lease (foreign → error).
    assert_is_error(&dispatch(&renew_req(&ws_id, 60), &globex).await.unwrap());

    // commit: globex cannot promote through acme's workspace ref.
    let ws_ref = format!("refs/workspaces/{ws_id}/heads/main");
    assert_is_error(
        &dispatch(&commit_req(&ws_id, &[(&ws_ref, "refs/heads/feature")]), &globex)
            .await
            .unwrap(),
    );
    // No clobber: the durable target globex aimed at was never created.
    assert!(globex
        .refs
        .get(&RefName::new("refs/heads/feature").unwrap())
        .await
        .unwrap()
        .is_none());

    // release: globex cannot release acme's workspace.
    assert_is_error(&dispatch(&release_req(&ws_id), &globex).await.unwrap());

    // After all the foreign attempts, acme still owns a live workspace.
    let out = dispatch(&get_workspace_req(&ws_id), &acme).await.unwrap();
    let reader = read_response(&out);
    match reader.get_root::<response::Reader>().unwrap().which().unwrap() {
        response::Which::Workspace(w) => {
            assert_eq!(w.unwrap().get_id().unwrap().to_string().unwrap(), ws_id);
        }
        _ => panic!("acme must retain its workspace"),
    }
}

/// acme's own ops succeed on its own workspace (positive parity control).
#[tokio::test]
async fn same_tenant_workspace_ops_succeed() {
    let (acme, _globex, _dir) = two_tenant_ctxs();
    let ws_id = seed_and_fork(&acme).await;

    // renew by the owner returns a lease.
    let out = dispatch(&renew_req(&ws_id, 3600), &acme).await.unwrap();
    let reader = read_response(&out);
    assert!(matches!(
        reader.get_root::<response::Reader>().unwrap().which().unwrap(),
        response::Which::Lease(_)
    ));

    // release by the owner returns Ok.
    let out = dispatch(&release_req(&ws_id), &acme).await.unwrap();
    let reader = read_response(&out);
    assert!(matches!(
        reader.get_root::<response::Reader>().unwrap().which().unwrap(),
        response::Which::Ok(())
    ));
}

/// listWorkspaces is per-tenant: each tenant sees only its own forks.
#[tokio::test]
async fn list_workspaces_is_tenant_scoped() {
    let (acme, globex, _dir) = two_tenant_ctxs();
    let acme_ws = seed_and_fork(&acme).await;
    let globex_ws = seed_and_fork(&globex).await;
    assert_ne!(acme_ws, globex_ws);

    // acme's list: exactly its own workspace, never globex's.
    let out = dispatch(&list_workspaces_req(), &acme).await.unwrap();
    let reader = read_response(&out);
    match reader.get_root::<response::Reader>().unwrap().which().unwrap() {
        response::Which::WorkspaceList(list) => {
            let list = list.unwrap();
            assert_eq!(list.len(), 1, "acme sees only its own");
            assert_eq!(list.get(0).get_id().unwrap().to_string().unwrap(), acme_ws);
        }
        _ => panic!("expected workspaceList"),
    }

    // globex's list: exactly its own workspace, never acme's.
    let out = dispatch(&list_workspaces_req(), &globex).await.unwrap();
    let reader = read_response(&out);
    match reader.get_root::<response::Reader>().unwrap().which().unwrap() {
        response::Which::WorkspaceList(list) => {
            let list = list.unwrap();
            assert_eq!(list.len(), 1, "globex sees only its own");
            assert_eq!(list.get(0).get_id().unwrap().to_string().unwrap(), globex_ws);
        }
        _ => panic!("expected workspaceList"),
    }
}
