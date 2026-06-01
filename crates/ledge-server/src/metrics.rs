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
}
