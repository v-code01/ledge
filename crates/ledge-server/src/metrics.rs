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

#[cfg(test)]
mod tests {
    use super::*;

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
