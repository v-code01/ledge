//! Request dispatch in three phases so no `!Send` Cap'n Proto reader/builder is
//! ever held across an `.await`, keeping the returned future `Send` (required by
//! the Axum HTTP handler that wraps it):
//!
//! 1. **decode**  — read the capnp `Request` into an owned, plain-Rust
//!    [`DecodedRequest`]; the capnp reader is dropped before any await.
//! 2. **execute** — `.await` the matching store / manager / GC op, producing an
//!    owned [`DispatchResult`]; no capnp types are alive here.
//! 3. **encode**  — synchronously serialize the result into a fresh capnp
//!    `Response`; no awaits during encode.
//!
//! Business failures (unknown workspace, missing object, bad ref) are carried as
//! [`DispatchResult::Error`] and encoded into `Response.error`; commit conflicts
//! ride in [`DispatchResult::CommitOutcomes`] with `ok = false`. Only a
//! genuinely malformed message returns `Err` (→ HTTP 400).

use bytes::Bytes;
use capnp::message::{Builder, ReaderOptions};
use capnp::serialize;

use ledge_core::{LedgeError, ObjectId, ObjectStore, RefEntry, RefName, Result};
use ledge_workspace::{CommitOutcome, WorkspaceId, WorkspaceView};

use crate::ledge_capnp::{request, response};
use crate::RpcCtx;

/// The method label for metrics/tracing, derived from the request union tag.
/// Decoded independently of [`dispatch`] so the HTTP layer can label its metrics
/// without threading a value out of the dispatch future.
pub fn method_name(request_bytes: &[u8]) -> &'static str {
    let reader = match serialize::read_message(&mut &request_bytes[..], ReaderOptions::new()) {
        Ok(r) => r,
        Err(_) => return "unknown",
    };
    let root = match reader.get_root::<request::Reader>() {
        Ok(r) => r,
        Err(_) => return "unknown",
    };
    match root.which() {
        Ok(request::Which::WriteObject(_)) => "writeObject",
        Ok(request::Which::ReadObject(_)) => "readObject",
        Ok(request::Which::Fork(_)) => "fork",
        Ok(request::Which::Commit(_)) => "commit",
        Ok(request::Which::Renew(_)) => "renew",
        Ok(request::Which::Release(_)) => "release",
        Ok(request::Which::GetWorkspace(_)) => "getWorkspace",
        Ok(request::Which::ListWorkspaces(())) => "listWorkspaces",
        Ok(request::Which::RunGc(())) => "runGc",
        Err(_) => "unknown",
    }
}

/// An owned, plain-Rust decoding of a capnp `Request` (no capnp types retained).
enum DecodedRequest {
    WriteObject { git_type: u8, content: Bytes },
    ReadObject { id: ObjectId },
    Fork { sources: Vec<String>, ttl_seconds: u64 },
    Commit { workspace_id: String, mappings: Vec<(String, String)> },
    Renew { workspace_id: String, ttl_seconds: u64 },
    Release { workspace_id: String },
    GetWorkspace { workspace_id: String },
    ListWorkspaces,
    RunGc,
}

/// An owned, plain-Rust result, ready to encode into a capnp `Response`.
enum DispatchResult {
    /// A business error — encoded into `Response.error`.
    Error(String),
    ObjectId(ObjectId),
    ObjectContent(Bytes),
    Workspace(WorkspaceView),
    CommitOutcomes(Vec<CommitOutcome>),
    Lease(ledge_workspace::Lease),
    Ok,
    WorkspaceList(Vec<WorkspaceView>),
    GcStats(ledge_workspace::GcStats),
}

/// Decode a capnp `Request` from `request_bytes`, service it against `ctx`, and
/// return the serialized capnp `Response` bytes.
///
/// Returns `Err` only when `request_bytes` is not a decodable capnp message or
/// the union is uninitialized — every *business* failure is encoded into the
/// returned `Response`.
pub async fn dispatch(request_bytes: &[u8], ctx: &RpcCtx) -> Result<Vec<u8>> {
    // Phase 1: decode (capnp reader lives only inside this call, dropped here).
    let decoded = decode_request(request_bytes)?;
    // Phase 2: execute (no capnp types alive across this await).
    let result = execute(decoded, ctx).await;
    // Phase 3: encode (synchronous; no awaits).
    encode_response(&result)
}

/// Phase 1 — decode the capnp `Request` into a [`DecodedRequest`].
fn decode_request(request_bytes: &[u8]) -> Result<DecodedRequest> {
    let reader = serialize::read_message(&mut &request_bytes[..], ReaderOptions::new())
        .map_err(malformed)?;
    let req = reader.get_root::<request::Reader>().map_err(malformed)?;
    match req.which().map_err(|e| malformed(e.into()))? {
        request::Which::WriteObject(r) => Ok(DecodedRequest::WriteObject {
            git_type: r.get_git_type(),
            content: Bytes::copy_from_slice(r.get_content().map_err(malformed)?),
        }),
        request::Which::ReadObject(r) => Ok(DecodedRequest::ReadObject {
            id: read_object_id(r.get_id().map_err(malformed)?)?,
        }),
        request::Which::Fork(r) => {
            let ttl_seconds = r.get_ttl_seconds();
            let sources_reader = r.get_sources().map_err(malformed)?;
            let mut sources = Vec::with_capacity(sources_reader.len() as usize);
            for s in sources_reader.iter() {
                sources.push(s.map_err(malformed)?.to_str().map_err(bad_utf8)?.to_string());
            }
            Ok(DecodedRequest::Fork { sources, ttl_seconds })
        }
        request::Which::Commit(r) => {
            let workspace_id = r
                .get_workspace_id()
                .map_err(malformed)?
                .to_str()
                .map_err(bad_utf8)?
                .to_string();
            let mappings_reader = r.get_mappings().map_err(malformed)?;
            let mut mappings = Vec::with_capacity(mappings_reader.len() as usize);
            for m in mappings_reader.iter() {
                let wref = m
                    .get_workspace_ref()
                    .map_err(malformed)?
                    .to_str()
                    .map_err(bad_utf8)?
                    .to_string();
                let dref = m
                    .get_durable_ref()
                    .map_err(malformed)?
                    .to_str()
                    .map_err(bad_utf8)?
                    .to_string();
                mappings.push((wref, dref));
            }
            Ok(DecodedRequest::Commit { workspace_id, mappings })
        }
        request::Which::Renew(r) => Ok(DecodedRequest::Renew {
            workspace_id: r
                .get_workspace_id()
                .map_err(malformed)?
                .to_str()
                .map_err(bad_utf8)?
                .to_string(),
            ttl_seconds: r.get_ttl_seconds(),
        }),
        request::Which::Release(r) => Ok(DecodedRequest::Release {
            workspace_id: r
                .get_workspace_id()
                .map_err(malformed)?
                .to_str()
                .map_err(bad_utf8)?
                .to_string(),
        }),
        request::Which::GetWorkspace(r) => Ok(DecodedRequest::GetWorkspace {
            workspace_id: r
                .get_workspace_id()
                .map_err(malformed)?
                .to_str()
                .map_err(bad_utf8)?
                .to_string(),
        }),
        request::Which::ListWorkspaces(()) => Ok(DecodedRequest::ListWorkspaces),
        request::Which::RunGc(()) => Ok(DecodedRequest::RunGc),
    }
}

/// Phase 2 — execute one decoded request, mapping every business failure to
/// [`DispatchResult::Error`] (never a returned `Err`).
async fn execute(req: DecodedRequest, ctx: &RpcCtx) -> DispatchResult {
    match req {
        DecodedRequest::WriteObject { git_type, content } => {
            match ctx.objects.write_git_object(git_type, content).await {
                Ok(id) => DispatchResult::ObjectId(id),
                Err(e) => DispatchResult::Error(e.to_string()),
            }
        }
        DecodedRequest::ReadObject { id } => match ctx.objects.read(id).await {
            Ok(content) => DispatchResult::ObjectContent(content),
            Err(e) => DispatchResult::Error(e.to_string()),
        },
        DecodedRequest::Fork { sources, ttl_seconds } => {
            let names = match parse_ref_names(&sources) {
                Ok(n) => n,
                Err(e) => return DispatchResult::Error(e),
            };
            match ctx.workspaces.fork(&names, ctx.resolve_ttl(ttl_seconds), &ctx.tenant_id).await {
                Ok(view) => DispatchResult::Workspace(view),
                Err(e) => DispatchResult::Error(e.to_string()),
            }
        }
        DecodedRequest::Commit { workspace_id, mappings } => {
            let id = match WorkspaceId::from_hex(&workspace_id) {
                Ok(id) => id,
                Err(e) => return DispatchResult::Error(e.to_string()),
            };
            let mut parsed = Vec::with_capacity(mappings.len());
            for (wref, dref) in &mappings {
                let w = match RefName::new(wref) {
                    Ok(n) => n,
                    Err(e) => return DispatchResult::Error(e.to_string()),
                };
                let d = match RefName::new(dref) {
                    Ok(n) => n,
                    Err(e) => return DispatchResult::Error(e.to_string()),
                };
                parsed.push((w, d));
            }
            match ctx.workspaces.commit(id, &parsed, &ctx.tenant_id).await {
                Ok(outcomes) => DispatchResult::CommitOutcomes(outcomes),
                Err(e) => DispatchResult::Error(e.to_string()),
            }
        }
        DecodedRequest::Renew { workspace_id, ttl_seconds } => {
            let id = match WorkspaceId::from_hex(&workspace_id) {
                Ok(id) => id,
                Err(e) => return DispatchResult::Error(e.to_string()),
            };
            match ctx.workspaces.renew(id, ctx.resolve_ttl(ttl_seconds), &ctx.tenant_id).await {
                Ok(lease) => DispatchResult::Lease(lease),
                Err(e) => DispatchResult::Error(e.to_string()),
            }
        }
        DecodedRequest::Release { workspace_id } => {
            let id = match WorkspaceId::from_hex(&workspace_id) {
                Ok(id) => id,
                Err(e) => return DispatchResult::Error(e.to_string()),
            };
            match ctx.workspaces.release(id, &ctx.tenant_id).await {
                Ok(()) => DispatchResult::Ok,
                Err(e) => DispatchResult::Error(e.to_string()),
            }
        }
        DecodedRequest::GetWorkspace { workspace_id } => {
            let id = match WorkspaceId::from_hex(&workspace_id) {
                Ok(id) => id,
                Err(e) => return DispatchResult::Error(e.to_string()),
            };
            match ctx.workspaces.get(id, &ctx.tenant_id).await {
                Ok(Some(view)) => DispatchResult::Workspace(view),
                Ok(None) => DispatchResult::Error(format!("unknown workspace {workspace_id}")),
                Err(e) => DispatchResult::Error(e.to_string()),
            }
        }
        DecodedRequest::ListWorkspaces => match ctx.workspaces.list(&ctx.tenant_id).await {
            Ok(views) => DispatchResult::WorkspaceList(views),
            Err(e) => DispatchResult::Error(e.to_string()),
        },
        DecodedRequest::RunGc => match ctx.gc.run().await {
            Ok(stats) => DispatchResult::GcStats(stats),
            Err(e) => DispatchResult::Error(e.to_string()),
        },
    }
}

/// Phase 3 — encode a [`DispatchResult`] into a serialized capnp `Response`.
fn encode_response(result: &DispatchResult) -> Result<Vec<u8>> {
    let mut out = Builder::new_default();
    {
        let resp = out.init_root::<response::Builder>();
        match result {
            DispatchResult::Error(msg) => {
                let mut resp = resp;
                resp.set_error(msg.as_str());
            }
            DispatchResult::ObjectId(id) => {
                resp.init_object_id().set_bytes(id.as_bytes());
            }
            DispatchResult::ObjectContent(content) => {
                let mut resp = resp;
                resp.set_object_content(content);
            }
            DispatchResult::Workspace(view) => {
                write_workspace_info(resp.init_workspace(), view);
            }
            DispatchResult::CommitOutcomes(outcomes) => {
                let mut list = resp.init_commit_outcomes(outcomes.len() as u32);
                for (i, outcome) in outcomes.iter().enumerate() {
                    encode_commit_outcome(list.reborrow().get(i as u32), outcome);
                }
            }
            DispatchResult::Lease(lease) => {
                let mut l = resp.init_lease();
                l.set_id(lease.id.to_hex().as_str());
                l.set_expires_at_ms(lease.expires_at_ms);
                l.set_created_at_ms(lease.created_at_ms);
                l.set_generation(lease.generation);
            }
            DispatchResult::Ok => {
                let mut resp = resp;
                resp.set_ok(());
            }
            DispatchResult::WorkspaceList(views) => {
                let mut list = resp.init_workspace_list(views.len() as u32);
                for (i, view) in views.iter().enumerate() {
                    write_workspace_info(list.reborrow().get(i as u32), view);
                }
            }
            DispatchResult::GcStats(stats) => {
                let mut g = resp.init_gc_stats();
                g.set_scanned(stats.scanned as u64);
                g.set_reachable(stats.reachable as u64);
                g.set_reclaimed(stats.reclaimed as u64);
                g.set_bytes_freed(stats.bytes_freed);
            }
        }
    }
    let mut buf = Vec::new();
    serialize::write_message(&mut buf, &out)
        .map_err(|e| LedgeError::Io(std::io::Error::other(e.to_string())))?;
    Ok(buf)
}

/// Validate a list of source ref strings into [`RefName`]s; first failure wins.
fn parse_ref_names(sources: &[String]) -> std::result::Result<Vec<RefName>, String> {
    let mut names = Vec::with_capacity(sources.len());
    for s in sources {
        names.push(RefName::new(s).map_err(|e| e.to_string())?);
    }
    Ok(names)
}

/// Map a capnp decode error to a `LedgeError` for the malformed-message path.
fn malformed(e: capnp::Error) -> LedgeError {
    LedgeError::Corruption(format!("malformed capnp request: {e}"))
}

/// Map a UTF-8 decode error on a capnp `Text` field to the malformed path.
fn bad_utf8(e: std::str::Utf8Error) -> LedgeError {
    LedgeError::Corruption(format!("malformed capnp request: non-utf8 text: {e}"))
}

/// Read a 32-byte `ObjectId` from a capnp `ObjectId` struct. A wrong length is
/// a malformed message (the field is fixed-width by contract).
fn read_object_id(reader: crate::ledge_capnp::object_id::Reader<'_>) -> Result<ObjectId> {
    let bytes = reader.get_bytes().map_err(malformed)?;
    let len = bytes.len();
    let arr: [u8; 32] = bytes.try_into().map_err(|_| {
        LedgeError::Corruption(format!("ObjectId must be 32 bytes, got {len}"))
    })?;
    Ok(ObjectId::from_bytes(arr))
}

/// Encode a [`WorkspaceView`] into a capnp `WorkspaceInfo` builder.
fn write_workspace_info(
    mut info: crate::ledge_capnp::workspace_info::Builder<'_>,
    view: &WorkspaceView,
) {
    info.set_id(view.id.to_hex().as_str());
    info.set_expires_at_ms(view.lease.expires_at_ms);
    let mut refs = info.init_refs(view.refs.len() as u32);
    for (i, (name, entry)) in view.refs.iter().enumerate() {
        let mut nr = refs.reborrow().get(i as u32);
        nr.set_name(name.as_str());
        write_ref_entry(nr.init_entry(), entry);
    }
}

/// Encode a [`RefEntry`] into a capnp `RefEntry` builder.
fn write_ref_entry(mut re: crate::ledge_capnp::ref_entry::Builder<'_>, entry: &RefEntry) {
    re.reborrow().init_target().set_bytes(entry.target.as_bytes());
    re.set_hlc(entry.hlc);
    re.set_version(entry.version);
}

/// Encode one [`CommitOutcome`] into a capnp `CommitOutcome` builder.
///
/// `Ok` → `ok = true`, `version` is the promoted entry's version. `Conflict`
/// → `ok = false`, `version` is the *live* durable entry's version the caller
/// must reconcile against. The durable ref is never clobbered on conflict.
fn encode_commit_outcome(
    mut o: crate::ledge_capnp::commit_outcome::Builder<'_>,
    outcome: &CommitOutcome,
) {
    match outcome {
        CommitOutcome::Ok { target, entry } => {
            o.set_target(target.as_str());
            o.set_ok(true);
            o.set_version(entry.version);
        }
        CommitOutcome::Conflict { target, current } => {
            o.set_target(target.as_str());
            o.set_ok(false);
            o.set_version(current.version);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledge_capnp::commit_outcome;
    use capnp::message::Builder;
    use ledge_core::RefEntry;

    /// The `CommitOutcome::Conflict` arm must encode `ok = false` and carry the
    /// live durable entry's version — the dispatch-level proof of the spec's
    /// "commit conflict → commitOutcomes ok=false" contract. (The end-to-end
    /// race is non-deterministic to drive through the reconciling manager, so we
    /// assert the encoder directly.)
    #[test]
    fn conflict_outcome_encodes_ok_false() {
        let mut msg = Builder::new_default();
        let builder = msg.init_root::<commit_outcome::Builder>();
        let current = RefEntry {
            target: ObjectId::from_bytes([7u8; 32]),
            hlc: 1,
            version: 9,
        };
        encode_commit_outcome(
            builder,
            &CommitOutcome::Conflict {
                target: "refs/heads/main".to_string(),
                current,
            },
        );
        let reader = msg.get_root_as_reader::<commit_outcome::Reader>().unwrap();
        assert_eq!(reader.get_target().unwrap(), "refs/heads/main");
        assert!(!reader.get_ok());
        assert_eq!(reader.get_version(), 9);
    }

    /// The `Ok` arm encodes `ok = true` and the promoted entry's version.
    #[test]
    fn ok_outcome_encodes_ok_true() {
        let mut msg = Builder::new_default();
        let builder = msg.init_root::<commit_outcome::Builder>();
        let entry = RefEntry {
            target: ObjectId::from_bytes([3u8; 32]),
            hlc: 2,
            version: 4,
        };
        encode_commit_outcome(
            builder,
            &CommitOutcome::Ok {
                target: "refs/heads/feature".to_string(),
                entry,
            },
        );
        let reader = msg.get_root_as_reader::<commit_outcome::Reader>().unwrap();
        assert_eq!(reader.get_target().unwrap(), "refs/heads/feature");
        assert!(reader.get_ok());
        assert_eq!(reader.get_version(), 4);
    }

    /// `method_name` extracts the union tag without a full dispatch.
    #[test]
    fn method_name_reads_union_tag() {
        let mut msg = Builder::new_default();
        {
            let mut root = msg.init_root::<request::Builder>();
            root.set_run_gc(());
        }
        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &msg).unwrap();
        assert_eq!(method_name(&buf), "runGc");
        assert_eq!(method_name(&[0xFFu8; 3]), "unknown");
    }

    /// The dispatch future must be `Send` — the Axum handler that wraps it
    /// requires it. The three-phase decode/execute/encode split (no capnp type
    /// held across an await) is what makes this hold; assert it at compile time.
    #[allow(dead_code)]
    fn dispatch_future_is_send(bytes: &'static [u8], ctx: &'static RpcCtx) {
        fn assert_send<T: Send>(_: T) {}
        assert_send(dispatch(bytes, ctx));
    }
}
