//! Tenant → physical ref-namespace mapping (Phase 4d-2 spec §3.1).
//!
//! [`tenant_prefix`] is the single, shared definition of how a tenant's DURABLE
//! refs are physically namespaced. It lives in `ledge-core` (the leaf crate) so
//! `ledge-workspace`, `ledge-rpc`, and `ledge-server` all use ONE copy without a
//! dependency cycle.
//!
//! The synthetic `root` tenant (auth-disabled / admin) keeps the LEGACY global
//! namespace (empty prefix) so existing data and every pre-4d-2 test are
//! byte-identical. The empty string is treated as `root` (a legacy lease decoded
//! without a `tenant_id` defaults to `""` — see `ledge_workspace::lease`).

/// The physical ref-namespace prefix for a tenant's DURABLE refs.
///
/// - `"root"` (and `""`, the legacy default) → `""` (the global namespace;
///   `refs/heads/main` stays `refs/heads/main`).
/// - any other `t` → `"tenants/<t>/"` (so `refs/heads/main` is physically
///   `refs/tenants/<t>/heads/main`).
///
/// Applied to durable ref names only (fork sources, commit targets, the
/// default-repo git segment). Workspace refs stay at `refs/workspaces/<wid>/…`
/// (spec §3.2). The returned string is a `segment` in the `ledge-git`
/// `present_ref`/`store_ref` sense: inserted immediately after `refs/`.
///
/// Pure, total, no allocation for the root path.
pub fn tenant_prefix(tenant_id: &str) -> String {
    if tenant_id.is_empty() || tenant_id == "root" {
        String::new()
    } else {
        format!("tenants/{tenant_id}/")
    }
}

/// The owning tenant of a DURABLE ref name — the inverse of [`tenant_prefix`]
/// (Phase 4d-3 spec §3.6).
///
/// - `refs/tenants/<t>/…` → `<t>` (a real tenant's durable namespace).
/// - `refs/heads/*`, `refs/tags/*`, and anything else (malformed, or the
///   workspace namespace that callers must not pass here) → `"root"` (the global
///   namespace, matching `tenant_prefix("root") == ""`).
///
/// Borrows from the input (zero allocation). Used by the GC to group durable
/// roots per tenant for usage measurement (`ledge_workspace::Gc::run`,
/// `ledge_cluster::gc::ClusterGc::run`).
pub fn tenant_of_ref(name: &str) -> &str {
    // refs/tenants/<t>/… : the tenant is the segment after `refs/tenants/`.
    if let Some(rest) = name.strip_prefix("refs/tenants/") {
        if let Some((tenant, _suffix)) = rest.split_once('/') {
            if !tenant.is_empty() {
                return tenant;
            }
        }
    }
    "root"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_empty_prefix() {
        assert_eq!(tenant_prefix("root"), "");
    }

    #[test]
    fn empty_is_treated_as_root() {
        // A legacy lease decoded without tenant_id defaults to "" ⇒ root/global.
        assert_eq!(tenant_prefix(""), "");
    }

    #[test]
    fn named_tenant_is_prefixed() {
        assert_eq!(tenant_prefix("acme"), "tenants/acme/");
        assert_eq!(tenant_prefix("globex"), "tenants/globex/");
    }

    #[test]
    fn prefix_composes_as_a_git_segment() {
        // The segment is inserted after `refs/`: refs/<prefix>heads/main.
        let p = tenant_prefix("acme");
        assert_eq!(
            format!("refs/{p}heads/main"),
            "refs/tenants/acme/heads/main"
        );
        // Root collapses to the legacy form.
        let r = tenant_prefix("root");
        assert_eq!(format!("refs/{r}heads/main"), "refs/heads/main");
    }

    #[test]
    fn tenant_of_ref_extracts_named_tenant() {
        assert_eq!(tenant_of_ref("refs/tenants/acme/heads/main"), "acme");
        assert_eq!(tenant_of_ref("refs/tenants/globex/tags/v1"), "globex");
    }

    #[test]
    fn tenant_of_ref_root_for_global_and_malformed() {
        assert_eq!(tenant_of_ref("refs/heads/main"), "root");
        assert_eq!(tenant_of_ref("refs/tags/v1"), "root");
        // The workspace namespace is ephemeral, never a durable tenant ⇒ root.
        assert_eq!(tenant_of_ref("refs/workspaces/abcd/heads/main"), "root");
        assert_eq!(tenant_of_ref("refs/tenants/"), "root"); // no tenant segment
        assert_eq!(tenant_of_ref("refs/tenants/acme"), "root"); // no trailing slash ⇒ no suffix
        assert_eq!(tenant_of_ref("garbage"), "root");
    }

    #[test]
    fn tenant_of_ref_inverts_tenant_prefix() {
        // For any named tenant, of_ref(refs/<prefix>heads/main) == tenant.
        for t in ["acme", "globex"] {
            let p = tenant_prefix(t);
            let name = format!("refs/{p}heads/main");
            assert_eq!(tenant_of_ref(&name), t);
        }
        // Root collapses both directions.
        assert_eq!(
            tenant_of_ref(&format!("refs/{}heads/main", tenant_prefix("root"))),
            "root"
        );
    }
}
