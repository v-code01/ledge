pub mod git_cli;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ledge_core::{LedgeError, ObjectId, ObjectStore, RefName, RefStore, Result};
use ledge_object_store::DiskObjectStore;
use ledge_workspace::{WorkspaceId, WorkspaceManager};

/// One upstream ref that was mirrored into the workspace namespace.
pub struct ImportedRef {
    /// Upstream ref name verbatim, e.g. `refs/heads/main` or `refs/tags/v1`.
    pub name: String,
    /// Hex SHA-1 of the git object the ref points at (the upstream object id).
    pub target_sha1: String,
}

/// Outcome of [`SyncEngine::import`].
pub struct ImportResult {
    /// Hex workspace id the upstream was mirrored into.
    pub workspace_id: String,
    /// Upstream's default branch short name (e.g. `main`), if discoverable.
    pub default_branch: Option<String>,
    /// Every `refs/heads/*` and `refs/tags/*` that was mirrored.
    pub refs: Vec<ImportedRef>,
}

/// One workspace head successfully pushed upstream by [`SyncEngine::export`].
#[derive(Debug)]
pub struct PushedRef {
    /// Upstream ref name written, e.g. `refs/heads/main`.
    pub reference: String,
    /// Hex SHA-1 of the git object the head points at (the canonical commit id).
    pub sha1: String,
}

/// One workspace head the upstream rejected (e.g. non-fast-forward, no `force`).
#[derive(Debug)]
pub struct RejectedRef {
    /// Upstream ref name that was rejected.
    pub reference: String,
    /// Git's porcelain summary for the rejection (e.g. `[rejected] (non-fast-forward)`).
    pub reason: String,
}

/// Outcome of [`SyncEngine::export`].
#[derive(Debug)]
pub struct ExportResult {
    /// Heads accepted upstream.
    pub pushed: Vec<PushedRef>,
    /// Heads the upstream rejected.
    pub rejected: Vec<RejectedRef>,
}

/// Orchestrates upstream git remote sync into Ledge workspaces.
///
/// IMPORT clones a bare mirror of an upstream repo, ingests every reachable git
/// object into the object store (preserving canonical git SHA-1s), then mirrors
/// each upstream ref into the freshly-forked workspace's ref namespace.
pub struct SyncEngine {
    objects: Arc<DiskObjectStore>,
    refs: Arc<dyn RefStore>,
    workspaces: Arc<WorkspaceManager>,
    allowed_hosts: Vec<String>,
}

impl SyncEngine {
    pub fn new(
        objects: Arc<DiskObjectStore>,
        refs: Arc<dyn RefStore>,
        workspaces: Arc<WorkspaceManager>,
        allowed_hosts: Vec<String>,
    ) -> Self {
        Self {
            objects,
            refs,
            workspaces,
            allowed_hosts,
        }
    }

    /// Reject upstreams whose host isn't allow-listed (empty list ⇒ allow any).
    ///
    /// Parses the host out of `scheme://[user@]host[:port]/path` without pulling
    /// in a URL dependency: take the authority, drop any userinfo, then cut at the
    /// first `/` or `:` to isolate the bare host.
    fn host_allowed(&self, url: &str) -> bool {
        if self.allowed_hosts.is_empty() {
            return true;
        }
        let after = url.split_once("://").map(|x| x.1).unwrap_or(url);
        let hostport = after.rsplit('@').next().unwrap_or(after);
        let host = hostport.split(['/', ':']).next().unwrap_or("");
        self.allowed_hosts.iter().any(|h| h == host)
    }

    /// Clone `upstream_url` into a fresh workspace for `tenant`: ingest all objects
    /// + mirror refs/heads + refs/tags. Returns the workspace id + imported refs.
    ///
    /// Round-trip fidelity: each ingested object keeps its canonical git SHA-1
    /// (computed by the object store from `type len\0content`), so the workspace
    /// ref target's SHA-1 equals the upstream commit SHA-1 exactly.
    pub async fn import(
        &self,
        tenant: &str,
        upstream_url: &str,
        auth: Option<&str>,
        ttl_secs: u64,
    ) -> Result<ImportResult> {
        let start = std::time::Instant::now();
        let res = self
            .import_inner(tenant, upstream_url, auth, ttl_secs)
            .await;
        let result = if res.is_ok() { "ok" } else { "failed" };
        crate::metrics::record_sync("import", result);
        crate::metrics::record_sync_duration("import", start.elapsed());
        res
    }

    async fn import_inner(
        &self,
        tenant: &str,
        upstream_url: &str,
        auth: Option<&str>,
        ttl_secs: u64,
    ) -> Result<ImportResult> {
        if !self.host_allowed(upstream_url) {
            return Err(LedgeError::Unavailable("upstream host not allowed".into()));
        }

        // Fork an empty workspace for this tenant; the upstream refs land in its
        // private `refs/workspaces/<id>/...` namespace.
        let view = self
            .workspaces
            .fork(&[], Duration::from_secs(ttl_secs), tenant)
            .await?;
        let ws_hex = view.id.to_hex();

        // Mirror the upstream into a throwaway bare repo, then drain it.
        let tmp = tempfile::TempDir::new()
            .map_err(|e| LedgeError::Io(std::io::Error::other(e.to_string())))?;
        let repo = tmp.path().join("up.git");
        git_cli::clone_bare(upstream_url, auth, &repo).await?;

        // Ingest every object; remember sha1 → store id so refs can be resolved.
        let objs = git_cli::cat_all_objects(&repo).await?;
        let mut sha1_to_oid = std::collections::HashMap::with_capacity(objs.len());
        let mut n = 0u64;
        for (sha1, ty, content) in objs {
            let oid = self.objects.write_git_object(ty, Bytes::from(content)).await?;
            sha1_to_oid.insert(sha1, oid);
            n += 1;
        }
        crate::metrics::record_sync_objects("import", n);

        // Mirror each upstream ref into the workspace namespace, retargeting the
        // upstream SHA-1 to the corresponding store object id.
        let mut out_refs = Vec::new();
        for (name, sha1) in git_cli::for_each_ref(&repo).await? {
            let rest = name.strip_prefix("refs/").unwrap_or(&name);
            let ws_ref = format!("refs/workspaces/{ws_hex}/{rest}");
            let Some(oid) = sha1_to_oid.get(&sha1).copied() else {
                continue;
            };
            let rn = RefName::new(&ws_ref).map_err(|e| LedgeError::Corruption(e.to_string()))?;
            self.refs.update(&rn, oid, None).await?;
            out_refs.push(ImportedRef {
                name,
                target_sha1: hex20(&sha1),
            });
        }

        let default_branch = git_cli::default_branch(&repo).await;
        Ok(ImportResult {
            workspace_id: ws_hex,
            default_branch,
            refs: out_refs,
        })
    }

    /// Push a workspace's heads back to `upstream_url`. Ownership-checked (foreign
    /// workspace ⇒ NotFound). Materializes reachable objects as loose git objects
    /// in a temp bare repo, then `git push`. `refs` (client `refs/heads/<b>` or
    /// `<b>` names) selects a subset; None ⇒ all the workspace's heads. `force`
    /// allows non-fast-forward.
    pub async fn export(
        &self,
        tenant: &str,
        workspace_id: &str,
        upstream_url: &str,
        auth: Option<&str>,
        refs: Option<Vec<String>>,
        force: bool,
    ) -> Result<ExportResult> {
        if !self.host_allowed(upstream_url) {
            return Err(LedgeError::Unavailable("upstream host not allowed".into()));
        }
        // A malformed id, or a foreign/absent workspace, is a uniform NotFound —
        // no existence leak across tenants. NotFound carries an ObjectId, so we
        // use the project's zero-sentinel (cf. workspace_routes), which maps to a
        // 404 at the HTTP layer.
        let wid = WorkspaceId::from_hex(workspace_id)
            .map_err(|_| LedgeError::NotFound(ObjectId::from_bytes([0u8; 32])))?;
        if self.workspaces.get(wid, tenant).await?.is_none() {
            return Err(LedgeError::NotFound(ObjectId::from_bytes([0u8; 32])));
        }
        let prefix = format!("refs/workspaces/{workspace_id}/heads/");
        let all = self.refs.list(&prefix).await?;
        let want: Option<std::collections::HashSet<String>> = refs.map(|v| {
            v.into_iter()
                .map(|r| r.strip_prefix("refs/heads/").unwrap_or(&r).to_string())
                .collect()
        });
        let mut tips: Vec<(String, ObjectId)> = Vec::new();
        for (name, entry) in all {
            let branch = name.as_str().strip_prefix(&prefix).unwrap_or("").to_string();
            if branch.is_empty() {
                continue;
            }
            if want.as_ref().is_some_and(|w| !w.contains(&branch)) {
                continue;
            }
            tips.push((branch, entry.target));
        }
        if tips.is_empty() {
            // No selected branch resolved to a head in this workspace.
            return Err(LedgeError::NotFound(ObjectId::from_bytes([0u8; 32])));
        }
        let tmp = tempfile::TempDir::new()
            .map_err(|e| LedgeError::Io(std::io::Error::other(e.to_string())))?;
        let repo = tmp.path().join("out.git");
        git_cli::init_bare(&repo).await?;
        let roots: Vec<ObjectId> = tips.iter().map(|(_, oid)| *oid).collect();
        let reachable = ledge_object_store::graph::reachable_from(&self.objects, roots).await?;
        let mut n = 0u64;
        for oid in &reachable {
            let sha1 = self.objects.sha1_of(*oid).await?;
            let ty = self.objects.git_type_of(*oid).await?;
            let content = self.objects.read(*oid).await?;
            git_cli::write_loose_object(&repo, &sha1, ty, &content)?;
            n += 1;
        }
        crate::metrics::record_sync_objects("export", n);
        let mut refspecs = Vec::new();
        let mut tip_sha: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        for (branch, oid) in &tips {
            let sha = hex20(&self.objects.sha1_of(*oid).await?);
            git_cli::update_ref(&repo, &format!("refs/heads/{branch}"), &sha).await?;
            refspecs.push(format!("refs/heads/{branch}:refs/heads/{branch}"));
            tip_sha.insert(branch.clone(), sha);
        }
        let results = git_cli::push(&repo, upstream_url, auth, &refspecs, force).await?;
        let (mut pushed, mut rejected) = (Vec::new(), Vec::new());
        for r in results {
            let branch = r
                .reference
                .strip_prefix("refs/heads/")
                .unwrap_or(&r.reference)
                .to_string();
            if r.status == "rejected" {
                rejected.push(RejectedRef {
                    reference: r.reference,
                    reason: r.summary,
                });
            } else {
                pushed.push(PushedRef {
                    reference: r.reference.clone(),
                    sha1: tip_sha.get(&branch).cloned().unwrap_or_default(),
                });
            }
        }
        Ok(ExportResult { pushed, rejected })
    }
}

/// Lowercase hex-encode a 20-byte git SHA-1.
fn hex20(b: &[u8; 20]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(40);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::process::Command as Cmd;

    async fn run(args: &[&str], cwd: &std::path::Path) {
        let out = Cmd::new("git")
            .args(args)
            .current_dir(cwd)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .await
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    async fn make_upstream() -> (tempfile::TempDir, String /*main sha1 hex*/) {
        let work = tempfile::TempDir::new().unwrap();
        run(&["init", "--initial-branch=main", "."], work.path()).await;
        run(&["config", "user.email", "t@l"], work.path()).await;
        run(&["config", "user.name", "t"], work.path()).await;
        std::fs::write(work.path().join("a.txt"), b"hello\n").unwrap();
        run(&["add", "a.txt"], work.path()).await;
        run(&["commit", "-m", "c1"], work.path()).await;
        let sha = String::from_utf8(
            Cmd::new("git")
                .args(["rev-parse", "main"])
                .current_dir(work.path())
                .output()
                .await
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();
        let bare = tempfile::TempDir::new().unwrap();
        run(
            &[
                "clone",
                "--bare",
                work.path().to_str().unwrap(),
                bare.path().to_str().unwrap(),
            ],
            std::path::Path::new("/"),
        )
        .await;
        (bare, sha)
    }

    /// Same as `make_upstream` — a single-commit `main` in a bare repo, returning
    /// the bare TempDir plus the hex SHA-1 of `main` for round-trip assertions.
    async fn make_upstream_with_sha() -> (tempfile::TempDir, String) {
        make_upstream().await
    }

    #[tokio::test]
    async fn export_roundtrips_sha1() {
        let dir = tempfile::TempDir::new().unwrap();
        let hlc = std::sync::Arc::new(ledge_core::HLC::new());
        let objects = std::sync::Arc::new(
            ledge_object_store::DiskObjectStore::new(dir.path().to_path_buf()).unwrap(),
        );
        let refs = std::sync::Arc::new(
            ledge_ref_store::RefStoreImpl::open(dir.path().to_path_buf(), hlc.clone()).unwrap(),
        );
        let (workspaces, _l, _g) = crate::build_workspace_stack(
            dir.path().to_path_buf(),
            objects.clone(),
            refs.clone(),
            hlc.clone(),
            ledge_workspace::QuotaLimits::default(),
            std::sync::Arc::new(ledge_workspace::UsageMap::default()),
        )
        .unwrap();
        let refs_dyn: std::sync::Arc<dyn ledge_core::RefStore> = refs.clone();
        let engine = SyncEngine::new(objects.clone(), refs_dyn, workspaces, vec![]);

        let (bare_a, main_a) = make_upstream_with_sha().await;
        let url_a = format!("file://{}", bare_a.path().display());
        let imp = engine.import("root", &url_a, None, 3600).await.unwrap();

        let bare_b = tempfile::TempDir::new().unwrap();
        super::git_cli::init_bare(bare_b.path()).await.unwrap();
        let url_b = format!("file://{}", bare_b.path().display());
        let res = engine
            .export("root", &imp.workspace_id, &url_b, None, None, false)
            .await
            .unwrap();
        assert!(
            res.pushed.iter().any(|r| r.reference == "refs/heads/main"),
            "pushed main: {res:?}"
        );

        let out = tempfile::TempDir::new().unwrap();
        let g = tokio::process::Command::new("git")
            .args(["clone", "--quiet", &url_b, out.path().to_str().unwrap()])
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .await
            .unwrap();
        assert!(
            g.status.success(),
            "clone B: {}",
            String::from_utf8_lossy(&g.stderr)
        );
        let got = String::from_utf8(
            tokio::process::Command::new("git")
                .args(["rev-parse", "main"])
                .current_dir(out.path())
                .output()
                .await
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();
        assert_eq!(got, main_a, "round-trip A→Ledge→B preserves the commit SHA-1");
    }

    #[tokio::test]
    async fn export_foreign_workspace_is_not_found() {
        let dir = tempfile::TempDir::new().unwrap();
        let hlc = std::sync::Arc::new(ledge_core::HLC::new());
        let objects = std::sync::Arc::new(
            ledge_object_store::DiskObjectStore::new(dir.path().to_path_buf()).unwrap(),
        );
        let refs = std::sync::Arc::new(
            ledge_ref_store::RefStoreImpl::open(dir.path().to_path_buf(), hlc.clone()).unwrap(),
        );
        let (workspaces, _l, _g) = crate::build_workspace_stack(
            dir.path().to_path_buf(),
            objects.clone(),
            refs.clone(),
            hlc.clone(),
            ledge_workspace::QuotaLimits::default(),
            std::sync::Arc::new(ledge_workspace::UsageMap::default()),
        )
        .unwrap();
        let engine = SyncEngine::new(
            objects.clone(),
            refs.clone() as std::sync::Arc<dyn ledge_core::RefStore>,
            workspaces,
            vec![],
        );
        let (bare_a, _) = make_upstream_with_sha().await;
        let imp = engine
            .import("acme", &format!("file://{}", bare_a.path().display()), None, 3600)
            .await
            .unwrap();
        let bare_b = tempfile::TempDir::new().unwrap();
        super::git_cli::init_bare(bare_b.path()).await.unwrap();
        // export as a DIFFERENT tenant ⇒ NotFound
        let r = engine
            .export(
                "globex",
                &imp.workspace_id,
                &format!("file://{}", bare_b.path().display()),
                None,
                None,
                false,
            )
            .await;
        assert!(
            matches!(r, Err(ledge_core::LedgeError::NotFound(_))),
            "foreign export ⇒ NotFound, got {r:?}"
        );
    }

    #[tokio::test]
    async fn import_mirrors_upstream_into_workspace() {
        let dir = tempfile::TempDir::new().unwrap();
        let hlc = Arc::new(ledge_core::HLC::new());
        let objects =
            Arc::new(ledge_object_store::DiskObjectStore::new(dir.path().to_path_buf()).unwrap());
        let refs = Arc::new(
            ledge_ref_store::RefStoreImpl::open(dir.path().to_path_buf(), hlc.clone()).unwrap(),
        );
        let (workspaces, _leases, _gc) = crate::build_workspace_stack(
            dir.path().to_path_buf(),
            objects.clone(),
            refs.clone(),
            hlc.clone(),
            ledge_workspace::QuotaLimits::default(),
            Arc::new(ledge_workspace::UsageMap::default()),
        )
        .unwrap();
        let refs_dyn: Arc<dyn ledge_core::RefStore> = refs.clone();
        let engine = SyncEngine::new(objects.clone(), refs_dyn.clone(), workspaces, vec![]);

        let (bare, main_sha) = make_upstream().await;
        let url = format!("file://{}", bare.path().display());
        let res = engine.import("root", &url, None, 3600).await.unwrap();

        assert_eq!(res.default_branch.as_deref(), Some("main"));
        assert!(res.refs.iter().any(|r| r.name == "refs/heads/main"));
        // the workspace ref exists + its target's git SHA-1 == upstream main
        let ws_ref = ledge_core::RefName::new(&format!(
            "refs/workspaces/{}/heads/main",
            res.workspace_id
        ))
        .unwrap();
        let entry = refs_dyn.get(&ws_ref).await.unwrap().expect("ws ref set");
        let got_sha = objects.sha1_of(entry.target).await.unwrap();
        let got_hex: String = got_sha.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            got_hex, main_sha,
            "imported main points at the upstream commit"
        );
    }
}
