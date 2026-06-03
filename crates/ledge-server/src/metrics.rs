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
pub const SNAPSHOTS_TOTAL: &str = "ledge_snapshots_total";
pub const SNAPSHOT_FILES_TOTAL: &str = "ledge_snapshot_files_total";
pub const SNAPSHOT_BYTES_TOTAL: &str = "ledge_snapshot_bytes_total";
pub const SNAPSHOT_REFLINKED_TOTAL: &str = "ledge_snapshot_reflinked_total";
pub const SNAPSHOT_COPIED_TOTAL: &str = "ledge_snapshot_copied_total";
pub const SNAPSHOT_DURATION: &str = "ledge_snapshot_duration_seconds";
pub const RPC_REQUESTS_TOTAL: &str = "ledge_rpc_requests_total";
pub const RPC_REQUEST_DURATION: &str = "ledge_rpc_request_duration_seconds";

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

/// Record one GC pass: bump the run counter, reclaimed/bytes counters, duration histogram.
pub fn record_gc_run(stats: &ledge_workspace::GcStats, d: std::time::Duration) {
    metrics::counter!(GC_RUNS_TOTAL).increment(1);
    metrics::counter!(GC_OBJECTS_RECLAIMED_TOTAL).increment(stats.reclaimed as u64);
    metrics::counter!(GC_BYTES_FREED_TOTAL).increment(stats.bytes_freed);
    metrics::histogram!(GC_DURATION).record(d.as_secs_f64());
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

#[cfg(test)]
mod tests {
    use super::*;

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
        };
        record_gc_run(&stats, std::time::Duration::from_millis(8));
    }
}
