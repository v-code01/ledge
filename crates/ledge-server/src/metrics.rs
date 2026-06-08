use ledge_core::LedgeError;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::sync::OnceLock;

static HANDLE: OnceLock<metrics_exporter_prometheus::PrometheusHandle> = OnceLock::new();

pub const OBJECT_WRITES_TOTAL: &str = "ledge_object_writes_total";
pub const OBJECT_WRITE_BYTES_TOTAL: &str = "ledge_object_write_bytes_total";
pub const OBJECT_WRITE_DURATION: &str = "ledge_object_write_duration_seconds";
pub const OBJECT_READS_TOTAL: &str = "ledge_object_reads_total";
pub const REF_UPDATES_TOTAL: &str = "ledge_ref_updates_total";
pub const REF_CAS_RETRIES_TOTAL: &str = "ledge_ref_cas_retries_total";
pub const GIT_REQUESTS_TOTAL: &str = "ledge_git_requests_total";
pub const GIT_REQUEST_DURATION: &str = "ledge_git_request_duration_seconds";
pub const WORKSPACES_ACTIVE: &str = "ledge_workspaces_active";
pub const WORKSPACE_FORKS_TOTAL: &str = "ledge_workspace_forks_total";
pub const WORKSPACE_COMMITS_TOTAL: &str = "ledge_workspace_commits_total";
pub const WORKSPACE_RELEASES_TOTAL: &str = "ledge_workspace_releases_total";
pub const LEASES_EXPIRED_TOTAL: &str = "ledge_leases_expired_total";
pub const GC_RUNS_TOTAL: &str = "ledge_gc_runs_total";
pub const GC_OBJECTS_RECLAIMED_TOTAL: &str = "ledge_gc_objects_reclaimed_total";
pub const GC_BYTES_FREED_TOTAL: &str = "ledge_gc_bytes_freed_total";
pub const GC_DURATION: &str = "ledge_gc_duration_seconds";
pub const GC_GRACE_RETAINED: &str = "ledge_gc_grace_retained";
pub const GC_ROOTS: &str = "ledge_gc_roots";
pub const SNAPSHOTS_TOTAL: &str = "ledge_snapshots_total";
pub const SNAPSHOT_FILES_TOTAL: &str = "ledge_snapshot_files_total";
pub const SNAPSHOT_BYTES_TOTAL: &str = "ledge_snapshot_bytes_total";
pub const SNAPSHOT_REFLINKED_TOTAL: &str = "ledge_snapshot_reflinked_total";
pub const SNAPSHOT_COPIED_TOTAL: &str = "ledge_snapshot_copied_total";
pub const SNAPSHOT_DURATION: &str = "ledge_snapshot_duration_seconds";
pub const RPC_REQUESTS_TOTAL: &str = "ledge_rpc_requests_total";
pub const RPC_REQUEST_DURATION: &str = "ledge_rpc_request_duration_seconds";

// ── Auth metrics (Phase 4d-1, spec §7/§8). Emitted by the auth middleware on
//    every classified request; the gauge tracks the live key count. Labels are
//    low-cardinality (`result` ∈ ok/unauthenticated/forbidden) and NEVER include
//    the path or any credential, so no secret leaks into the metrics surface. ───
pub const AUTH_REQUESTS_TOTAL: &str = "ledge_auth_requests_total";
pub const AUTH_KEYS: &str = "ledge_auth_keys";

// ── Tenant-isolation metric (Phase 4d-2, spec §7). Bumped whenever a workspace
//    op is denied by a cross-tenant ownership mismatch (a 404 from the manager's
//    NotFound, or a foreign-id `get` → None). Unlabeled — it carries NO tenant id
//    (no cross-tenant info on the metrics surface). ─────────────────────────────
pub const TENANT_DENIED_TOTAL: &str = "ledge_tenant_denied_total";

// ── Per-shard Raft gauges/counters (spec §7). Populated ONLY in cluster mode by
//    `record_raft_metrics`; single-node never emits these series, so `/metrics`
//    output for a single-node server is unchanged. ─────────────────────────────
pub const RAFT_LEADER: &str = "ledge_raft_leader";
pub const RAFT_TERM: &str = "ledge_raft_term";
pub const RAFT_LAST_APPLIED: &str = "ledge_raft_last_applied";
pub const RAFT_COMMIT_INDEX: &str = "ledge_raft_commit_index";
pub const RAFT_ELECTIONS_TOTAL: &str = "ledge_raft_elections_total";

// ── Per-shard placement metrics (Phase 4a §5). `ledge_shard_hosted` is set once
//    per shard at `build_cluster_stack` time; the applied/forwarded counters are
//    bumped on the `/cluster/ref-op` apply path and the forwarder's POST path
//    respectively. Single-node never emits any of these series. ────────────────
pub const SHARD_HOSTED: &str = "ledge_shard_hosted";
pub const REF_OP_APPLIED_TOTAL: &str = "ledge_ref_op_applied_total";
/// Name of the forward counter. Defined here for documentation/parity; the
/// counter is INCREMENTED in `ledge-cluster`'s `HttpForwarder::forward` at the
/// true forward site (see [`ledge_cluster::forward::REF_OP_FORWARDED_TOTAL`]),
/// which uses this identical name so both crates agree on the series.
pub const REF_OP_FORWARDED_TOTAL: &str = "ledge_ref_op_forwarded_total";

// ── Cross-shard 2PC transaction metrics (Phase 4b, spec §7). Like the forward
//    counter above, the `_TOTAL`/`_DURATION` series are EMITTED at their true
//    site in `ledge-cluster`'s `TxnCoordinator`/`TxnResolver` (which re-declare
//    these identical names — see `ledge_cluster::txn`), so both crates agree on
//    the series. The names + record helpers are also declared here for
//    documentation parity and for any server-side emission. Single-node never
//    runs the coordinator, so single-node `/metrics` is unchanged. ────────────
pub const TXN_STARTED_TOTAL: &str = "ledge_txn_started_total";
pub const TXN_COMMITTED_TOTAL: &str = "ledge_txn_committed_total";
pub const TXN_ABORTED_TOTAL: &str = "ledge_txn_aborted_total";
pub const TXN_RECOVERED_TOTAL: &str = "ledge_txn_recovered_total";
pub const TXN_PREPARE_VOTES_TOTAL: &str = "ledge_txn_prepare_votes_total";
pub const PREPARED_LOCKS: &str = "ledge_prepared_locks";
pub const TXN_DURATION: &str = "ledge_txn_duration_seconds";

pub fn install_recorder() -> ledge_core::Result<()> {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| LedgeError::Io(std::io::Error::other(e.to_string())))?;
    let _ = HANDLE.set(handle);
    Ok(())
}

pub fn render() -> String {
    HANDLE.get().map(|h| h.render()).unwrap_or_default()
}

pub fn record_object_write() { metrics::counter!(OBJECT_WRITES_TOTAL).increment(1); }
pub fn record_object_write_bytes(bytes: u64) { metrics::counter!(OBJECT_WRITE_BYTES_TOTAL).increment(bytes); }
pub fn record_object_write_duration(d: std::time::Duration) { metrics::histogram!(OBJECT_WRITE_DURATION).record(d.as_secs_f64()); }
pub fn record_object_read() { metrics::counter!(OBJECT_READS_TOTAL).increment(1); }
pub fn record_ref_update() { metrics::counter!(REF_UPDATES_TOTAL).increment(1); }
pub fn record_ref_cas_retries(n: u64) { metrics::counter!(REF_CAS_RETRIES_TOTAL).increment(n); }
pub fn record_git_request(svc: &'static str) { metrics::counter!(GIT_REQUESTS_TOTAL, "service" => svc).increment(1); }
pub fn record_git_request_duration(svc: &'static str, d: std::time::Duration) { metrics::histogram!(GIT_REQUEST_DURATION, "service" => svc).record(d.as_secs_f64()); }

/// Gauge: live (unexpired, non-tombstoned) workspace count.
pub fn set_workspaces_active(n: f64) { metrics::gauge!(WORKSPACES_ACTIVE).set(n); }
pub fn record_workspace_fork() { metrics::counter!(WORKSPACE_FORKS_TOTAL).increment(1); }
pub fn record_workspace_commit(n: u64) { metrics::counter!(WORKSPACE_COMMITS_TOTAL).increment(n); }
pub fn record_workspace_release() { metrics::counter!(WORKSPACE_RELEASES_TOTAL).increment(1); }
pub fn record_lease_expired(n: u64) { metrics::counter!(LEASES_EXPIRED_TOTAL).increment(n); }

/// Record one GC pass: bump the run counter, reclaimed/bytes counters, the
/// duration histogram, AND the last-pass grace-retained gauge (0 for single-node).
///
/// Used by the single-node `Gc::run` path (`admin_gc`); the cluster path emits the
/// same series at its true site in [`ledge_cluster::gc::ClusterGc::run`] (which
/// re-declares these identical names), so the route does NOT call this for the
/// cluster branch — calling it there would double-count `GC_RUNS_TOTAL`.
pub fn record_gc_run(stats: &ledge_workspace::GcStats, d: std::time::Duration) {
    metrics::counter!(GC_RUNS_TOTAL).increment(1);
    metrics::counter!(GC_OBJECTS_RECLAIMED_TOTAL).increment(stats.reclaimed as u64);
    metrics::counter!(GC_BYTES_FREED_TOTAL).increment(stats.bytes_freed);
    metrics::histogram!(GC_DURATION).record(d.as_secs_f64());
    metrics::gauge!(GC_GRACE_RETAINED).set(stats.skipped_grace as f64);
}

/// Set the last-pass root-count gauge for one root source (`kind` ∈
/// `committed`/`prepared`/`lease`). Emitted by `ClusterGc::run` (spec §7); the
/// single-node `Gc` has no cross-shard/prepared root split and does not call it.
pub fn set_gc_roots(kind: &'static str, n: u64) {
    metrics::gauge!(GC_ROOTS, "kind" => kind).set(n as f64);
}

/// Record one CoW snapshot: bump the run counter, files/bytes/reflinked/copied
/// counters, and the duration histogram (Phase 2d, `POST /admin/snapshot`).
pub fn record_snapshot(stats: &ledge_cow::CloneStats, d: std::time::Duration) {
    metrics::counter!(SNAPSHOTS_TOTAL).increment(1);
    metrics::counter!(SNAPSHOT_FILES_TOTAL).increment(stats.files as u64);
    metrics::counter!(SNAPSHOT_BYTES_TOTAL).increment(stats.bytes);
    metrics::counter!(SNAPSHOT_REFLINKED_TOTAL).increment(stats.reflinked as u64);
    metrics::counter!(SNAPSHOT_COPIED_TOTAL).increment(stats.copied as u64);
    metrics::histogram!(SNAPSHOT_DURATION).record(d.as_secs_f64());
}

/// Record one `POST /rpc` call: bump the per-method counter and the per-method
/// duration histogram. The label is the decoded request union tag (e.g.
/// "writeObject"), or "unknown" for an undecodable / malformed body.
pub fn record_rpc_request(method: &'static str, d: std::time::Duration) {
    metrics::counter!(RPC_REQUESTS_TOTAL, "method" => method).increment(1);
    metrics::histogram!(RPC_REQUEST_DURATION, "method" => method).record(d.as_secs_f64());
}

/// Record one auth decision. `result` ∈ `ok` / `unauthenticated` / `forbidden`.
/// Never includes the path or any credential as a label (low cardinality + no
/// secret leakage — spec §8).
pub fn record_auth_request(result: &'static str) {
    metrics::counter!(AUTH_REQUESTS_TOTAL, "result" => result).increment(1);
}

/// Gauge: count of live (non-revoked, non-expired) API keys.
pub fn set_auth_keys(n: f64) {
    metrics::gauge!(AUTH_KEYS).set(n);
}

/// Count a cross-tenant access denied (an ownership-mismatch 404). Helps
/// operators spot misconfigured clients / probing. Never labeled with the other
/// tenant's id (no cross-tenant info in metrics — spec §7).
pub fn record_tenant_denied() {
    metrics::counter!(TENANT_DENIED_TOTAL).increment(1);
}

/// Metric name for quota denials (pull-forward from Task 7; Task 7 adds the
/// usage gauges + the metric-name test alongside this).
pub const QUOTA_DENIED_TOTAL: &str = "ledge_quota_denied_total";

/// Count a quota denial. `resource` ∈ workspaces|durable_bytes|object_count|requests.
/// Never labeled with a tenant id (spec §5).
pub fn record_quota_denied(resource: &'static str) {
    metrics::counter!(QUOTA_DENIED_TOTAL, "resource" => resource).increment(1);
}

// ── Quota usage gauges (Phase 4d-3, spec §5). `denied_total{resource}` (above)
//    bumps on every denial; the usage gauges below reflect the latest GC
//    measurement, labeled by tenant (cardinality bounded by tenant count —
//    spec §5). NEVER labeled with another tenant's secret; the resource/tenant
//    labels are low-cardinality. ──────────────────────────────────────────────
pub const QUOTA_USAGE_BYTES: &str = "ledge_quota_usage_bytes";
pub const QUOTA_USAGE_OBJECTS: &str = "ledge_quota_usage_objects";
pub const QUOTA_WORKSPACES: &str = "ledge_quota_workspaces";

/// Set the per-tenant usage gauges from the latest GC measurement. Called by the
/// GC scheduler after each pass refreshes the `UsageMap` (Phase 4d-3, spec §5).
pub fn set_quota_usage(tenant: &str, bytes: u64, objects: u64) {
    metrics::gauge!(QUOTA_USAGE_BYTES, "tenant" => tenant.to_string()).set(bytes as f64);
    metrics::gauge!(QUOTA_USAGE_OBJECTS, "tenant" => tenant.to_string()).set(objects as f64);
}

/// Set the per-tenant live-workspace gauge (Phase 4d-3, spec §5).
pub fn set_quota_workspaces(tenant: &str, n: u64) {
    metrics::gauge!(QUOTA_WORKSPACES, "tenant" => tenant.to_string()).set(n as f64);
}

// ── Webhook delivery metrics (Phase: webhooks, Task 4). The deliveries counter
//    is labeled only by `result` ∈ ok|failed (low cardinality; never the tenant,
//    url, or secret — no leakage on the metrics surface). The duration histogram
//    measures end-to-end delivery wall time; the gauge tracks the registered
//    webhook count, set from the store after register/delete. ──────────────────
pub const WEBHOOK_DELIVERIES_TOTAL: &str = "ledge_webhook_deliveries_total";
pub const WEBHOOK_DELIVERY_DURATION: &str = "ledge_webhook_delivery_duration_seconds";
pub const WEBHOOKS_REGISTERED: &str = "ledge_webhooks_registered";

/// Count a webhook delivery outcome. `result` ∈ ok|failed. No tenant/url/secret label.
pub fn record_webhook_delivery(result: &'static str) {
    metrics::counter!(WEBHOOK_DELIVERIES_TOTAL, "result" => result).increment(1);
}
pub fn record_webhook_delivery_duration(d: std::time::Duration) {
    metrics::histogram!(WEBHOOK_DELIVERY_DURATION).record(d.as_secs_f64());
}
/// Gauge: count of registered webhooks (set from the store after register/delete).
pub fn set_webhooks_registered(n: f64) {
    metrics::gauge!(WEBHOOKS_REGISTERED).set(n);
}

/// Transport-posture gauges (Phase 4d-4): set ONCE at boot so /metrics reflects
/// whether TLS / mTLS is active. Zero label cardinality.
pub const TLS_ENABLED: &str = "ledge_tls_enabled";
pub const TLS_MTLS_ENABLED: &str = "ledge_tls_mtls_enabled";

/// Publish the boot transport posture (1.0 = on, 0.0 = off).
pub fn set_tls_posture(tls_enabled: bool, mtls_enabled: bool) {
    metrics::gauge!(TLS_ENABLED).set(if tls_enabled { 1.0 } else { 0.0 });
    metrics::gauge!(TLS_MTLS_ENABLED).set(if mtls_enabled { 1.0 } else { 0.0 });
}

/// Update the per-shard Raft gauges/counters from a `RaftMetrics` snapshot.
///
/// Called from the cluster-mode metrics poller (one task per shard, started in
/// `main.rs` only when `cluster.enabled`). The `shard` is the label value; all
/// series are tagged `shard="<n>"`. The election counter is recorded as the
/// current term (term monotonically increases by at least one per election, so
/// the term is a faithful lower bound / proxy for cumulative elections).
///
/// `leader` carries the leader's node id as a gauge when this shard has a
/// leader, else `0` (with `current_leader == None`); callers distinguishing
/// "no leader" from "leader is node 0" should consult `current_leader` directly,
/// but in Ledge node ids start at 1 so `0` unambiguously means "no leader".
pub fn record_raft_metrics(
    shard: u32,
    current_leader: Option<u64>,
    current_term: u64,
    last_applied: Option<u64>,
    commit_index: Option<u64>,
) {
    let shard_label = shard.to_string();
    metrics::gauge!(RAFT_LEADER, "shard" => shard_label.clone())
        .set(current_leader.unwrap_or(0) as f64);
    metrics::gauge!(RAFT_TERM, "shard" => shard_label.clone()).set(current_term as f64);
    metrics::gauge!(RAFT_LAST_APPLIED, "shard" => shard_label.clone())
        .set(last_applied.unwrap_or(0) as f64);
    metrics::gauge!(RAFT_COMMIT_INDEX, "shard" => shard_label.clone())
        .set(commit_index.unwrap_or(0) as f64);
    // Term as the cumulative-elections proxy (absolute gauge, not delta): a fresh
    // election strictly raises the term, so this is monotone and re-derivable.
    metrics::gauge!(RAFT_ELECTIONS_TOTAL, "shard" => shard_label).set(current_term as f64);
}

/// Gauge: `1` if this node hosts `shard`, else `0`. Set once at host-build time
/// in `build_cluster_stack` (cluster only); single-node never emits this series.
pub fn set_shard_hosted(shard: u32, hosted: bool) {
    metrics::gauge!(SHARD_HOSTED, "shard" => shard.to_string())
        .set(if hosted { 1.0 } else { 0.0 });
}

/// Counter: a shard-targeted ref op was APPLIED locally via `/cluster/ref-op`.
pub fn record_ref_op_applied(shard: u32) {
    metrics::counter!(REF_OP_APPLIED_TOTAL, "shard" => shard.to_string()).increment(1);
}

// ── 2PC transaction-lifecycle record helpers (spec §7). Documentation-parity
//    wrappers over the same series the coordinator/resolver emit at the true
//    site; kept here so the names round-trip and `/metrics` consumers can find a
//    single helper module. ─────────────────────────────────────────────────────

/// Counter: a multi-shard 2PC transaction entered the prepare phase.
pub fn record_txn_started() { metrics::counter!(TXN_STARTED_TOTAL).increment(1); }
/// Counter: a transaction reached a durable `TxnDecide{commit}` (the commit point).
pub fn record_txn_committed() { metrics::counter!(TXN_COMMITTED_TOTAL).increment(1); }
/// Counter: a transaction aborted. `reason` is the abort cause (e.g. `prepare_no`).
pub fn record_txn_aborted(reason: &'static str) {
    metrics::counter!(TXN_ABORTED_TOTAL, "reason" => reason).increment(1);
}
/// Counter: a prepared lock was resolved by the crash-recovery `TxnResolver`
/// (rolled forward after a Commit decision or released on presumed-abort).
pub fn record_txn_recovered() { metrics::counter!(TXN_RECOVERED_TOTAL).increment(1); }
/// Counter: one prepare vote; `vote` is `"yes"` or `"no"`.
pub fn record_txn_prepare_vote(vote: &'static str) {
    metrics::counter!(TXN_PREPARE_VOTES_TOTAL, "vote" => vote).increment(1);
}
/// Histogram: wall time of a multi-shard 2PC transaction, in seconds.
pub fn record_txn_duration(d: std::time::Duration) {
    metrics::histogram!(TXN_DURATION).record(d.as_secs_f64());
}
/// Gauge: number of currently-held prepared locks (in-doubt 2PC participants).
pub fn set_prepared_locks(n: u64) { metrics::gauge!(PREPARED_LOCKS).set(n as f64); }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_metric_constants_correct() {
        assert_eq!(TLS_ENABLED, "ledge_tls_enabled");
        assert_eq!(TLS_MTLS_ENABLED, "ledge_tls_mtls_enabled");
    }
    #[test]
    fn tls_record_helpers_no_panic_without_recorder() {
        set_tls_posture(true, false);
        set_tls_posture(true, true);
        set_tls_posture(false, false);
    }

    #[test]
    fn txn_metric_names_match_spec() {
        assert_eq!(TXN_STARTED_TOTAL, "ledge_txn_started_total");
        assert_eq!(TXN_COMMITTED_TOTAL, "ledge_txn_committed_total");
        assert_eq!(TXN_ABORTED_TOTAL, "ledge_txn_aborted_total");
        assert_eq!(TXN_RECOVERED_TOTAL, "ledge_txn_recovered_total");
        assert_eq!(TXN_PREPARE_VOTES_TOTAL, "ledge_txn_prepare_votes_total");
        assert_eq!(PREPARED_LOCKS, "ledge_prepared_locks");
        assert_eq!(TXN_DURATION, "ledge_txn_duration_seconds");
    }

    #[test]
    fn txn_record_helpers_no_panic_without_recorder() {
        record_txn_started();
        record_txn_committed();
        record_txn_aborted("prepare_no");
        record_txn_recovered();
        record_txn_prepare_vote("yes");
        record_txn_prepare_vote("no");
        record_txn_duration(std::time::Duration::from_millis(3));
        set_prepared_locks(2);
    }

    #[test]
    fn raft_metric_name_constants_correct() {
        assert_eq!(RAFT_LEADER, "ledge_raft_leader");
        assert_eq!(RAFT_TERM, "ledge_raft_term");
        assert_eq!(RAFT_LAST_APPLIED, "ledge_raft_last_applied");
    }

    #[test]
    fn record_raft_metrics_no_panic() {
        // Safe to call without a recorder installed (mirrors the other helpers).
        record_raft_metrics(0, Some(1), 3, Some(7), Some(7));
        record_raft_metrics(1, None, 0, None, None);
    }

    #[test]
    fn shard_placement_metric_constants_correct() {
        assert_eq!(SHARD_HOSTED, "ledge_shard_hosted");
        assert_eq!(REF_OP_APPLIED_TOTAL, "ledge_ref_op_applied_total");
        assert_eq!(REF_OP_FORWARDED_TOTAL, "ledge_ref_op_forwarded_total");
    }

    #[test]
    fn shard_placement_metric_helpers_no_panic() {
        // Safe to call without a recorder installed (mirrors the other helpers).
        set_shard_hosted(0, true);
        set_shard_hosted(1, false);
        record_ref_op_applied(0);
    }

    #[test]
    fn metric_name_constants_correct() {
        assert_eq!(OBJECT_WRITES_TOTAL, "ledge_object_writes_total");
        assert_eq!(GIT_REQUEST_DURATION, "ledge_git_request_duration_seconds");
    }

    #[test]
    fn record_helpers_no_panic_without_recorder() {
        record_object_write();
        record_object_write_bytes(1024);
        record_object_write_duration(std::time::Duration::from_millis(5));
        record_object_read();
        record_ref_update();
        record_ref_cas_retries(3);
        record_git_request("upload-pack");
        record_git_request_duration("receive-pack", std::time::Duration::from_millis(12));
    }

    #[test]
    fn workspace_metric_constants_correct() {
        assert_eq!(WORKSPACES_ACTIVE, "ledge_workspaces_active");
        assert_eq!(GC_DURATION, "ledge_gc_duration_seconds");
    }

    #[test]
    fn gc_metric_names_match_spec() {
        assert_eq!(GC_RUNS_TOTAL, "ledge_gc_runs_total");
        assert_eq!(GC_OBJECTS_RECLAIMED_TOTAL, "ledge_gc_objects_reclaimed_total");
        assert_eq!(GC_BYTES_FREED_TOTAL, "ledge_gc_bytes_freed_total");
        assert_eq!(GC_DURATION, "ledge_gc_duration_seconds");
        assert_eq!(GC_GRACE_RETAINED, "ledge_gc_grace_retained");
        assert_eq!(GC_ROOTS, "ledge_gc_roots");
    }

    #[test]
    fn snapshot_metric_constants_correct() {
        assert_eq!(SNAPSHOTS_TOTAL, "ledge_snapshots_total");
        assert_eq!(SNAPSHOT_DURATION, "ledge_snapshot_duration_seconds");
    }

    #[test]
    fn rpc_metric_constants_correct() {
        assert_eq!(RPC_REQUESTS_TOTAL, "ledge_rpc_requests_total");
        assert_eq!(RPC_REQUEST_DURATION, "ledge_rpc_request_duration_seconds");
    }

    #[test]
    fn rpc_record_helper_no_panic_without_recorder() {
        record_rpc_request("writeObject", std::time::Duration::from_millis(1));
        record_rpc_request("unknown", std::time::Duration::from_micros(50));
    }

    #[test]
    fn auth_metric_constants_correct() {
        assert_eq!(AUTH_REQUESTS_TOTAL, "ledge_auth_requests_total");
        assert_eq!(AUTH_KEYS, "ledge_auth_keys");
    }

    #[test]
    fn auth_record_helpers_no_panic_without_recorder() {
        record_auth_request("ok");
        record_auth_request("unauthenticated");
        record_auth_request("forbidden");
        set_auth_keys(7.0);
    }

    #[test]
    fn tenant_denied_metric_constant_correct() {
        assert_eq!(TENANT_DENIED_TOTAL, "ledge_tenant_denied_total");
    }

    #[test]
    fn tenant_denied_record_helper_no_panic_without_recorder() {
        record_tenant_denied();
    }

    #[test]
    fn quota_metric_constants_correct() {
        assert_eq!(QUOTA_DENIED_TOTAL, "ledge_quota_denied_total");
        assert_eq!(QUOTA_USAGE_BYTES, "ledge_quota_usage_bytes");
        assert_eq!(QUOTA_USAGE_OBJECTS, "ledge_quota_usage_objects");
        assert_eq!(QUOTA_WORKSPACES, "ledge_quota_workspaces");
    }

    #[test]
    fn quota_record_helpers_no_panic_without_recorder() {
        record_quota_denied("workspaces");
        record_quota_denied("durable_bytes");
        record_quota_denied("object_count");
        record_quota_denied("requests");
        set_quota_usage("acme", 4096, 7);
        set_quota_workspaces("acme", 3);
    }

    #[test]
    fn webhook_metric_constants_correct() {
        assert_eq!(WEBHOOK_DELIVERIES_TOTAL, "ledge_webhook_deliveries_total");
        assert_eq!(WEBHOOK_DELIVERY_DURATION, "ledge_webhook_delivery_duration_seconds");
        assert_eq!(WEBHOOKS_REGISTERED, "ledge_webhooks_registered");
    }
    #[test]
    fn webhook_record_helpers_no_panic_without_recorder() {
        record_webhook_delivery("ok");
        record_webhook_delivery("failed");
        record_webhook_delivery_duration(std::time::Duration::from_millis(5));
        set_webhooks_registered(3.0);
    }

    #[test]
    fn snapshot_record_helper_no_panic_without_recorder() {
        let stats = ledge_cow::CloneStats {
            files: 4,
            dirs: 2,
            reflinked: 4,
            copied: 0,
            bytes: 4096,
        };
        record_snapshot(&stats, std::time::Duration::from_millis(2));
    }

    #[test]
    fn workspace_record_helpers_no_panic_without_recorder() {
        set_workspaces_active(3.0);
        record_workspace_fork();
        record_workspace_commit(2);
        record_workspace_release();
        record_lease_expired(5);
        let stats = ledge_workspace::GcStats {
            scanned: 10,
            reachable: 7,
            reclaimed: 3,
            bytes_freed: 4096,
            skipped_grace: 0,
        };
        record_gc_run(&stats, std::time::Duration::from_millis(8));
        set_gc_roots("committed", 4);
        set_gc_roots("prepared", 1);
        set_gc_roots("lease", 2);
    }
}
