//! Ties the alerting rules to the code. Every `ledge_*` time series referenced by
//! the canonical rules file (deploy/helm/ledge/files/ledge-alerts.yml) MUST appear
//! in the server's rendered /metrics output. If a metric is renamed or removed,
//! this test fails the build instead of silently disabling an alert in prod.
//!
//! It also pins the exporter format: histograms render as Prometheus SUMMARIES
//! (the recorder configures no buckets), so the latency alerts use
//! rate(_sum)/rate(_count). If that ever flips to bucketed histograms, the
//! alerts must change too — this test fails loudly so we notice.

use ledge_server::metrics;

const ALERTS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../deploy/helm/ledge/files/ledge-alerts.yml"
));

/// Pull every `ledge_<ident>` token out of the `expr:` lines of the rules file.
/// Only `expr:` lines are scanned so annotation prose can't introduce phantom
/// tokens; recording-rule names (which use `:`) are not produced by this grammar.
fn referenced_series(alerts: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in alerts.lines() {
        let t = line.trim_start();
        if !t.starts_with("expr:") {
            continue;
        }
        let bytes = t.as_bytes();
        let mut i = 0;
        while let Some(pos) = t[i..].find("ledge_") {
            let start = i + pos;
            let mut end = start;
            while end < bytes.len() {
                let c = bytes[end];
                if c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' {
                    end += 1;
                } else {
                    break;
                }
            }
            out.push(t[start..end].to_string());
            i = end;
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Materialise one observation of every series the alerts depend on so the
/// recorder renders them. In production these are emitted across many call sites
/// (and the cluster series only in cluster mode); here we just need each to exist.
fn emit_all() {
    metrics::record_git_request_duration("upload-pack", std::time::Duration::from_millis(5));
    metrics::record_object_write_duration(std::time::Duration::from_millis(2));
    metrics::record_rpc_request("writeObject", std::time::Duration::from_millis(1));
    metrics::record_auth_request("unauthenticated");
    metrics::record_auth_request("forbidden");
    metrics::record_tenant_denied();
    metrics::record_webhook_delivery("ok");
    metrics::record_webhook_delivery("failed");
    metrics::record_sync("import", "failed");
    metrics::record_quota_denied("workspaces");
    metrics::record_ref_cas_retries(1);
    metrics::record_ref_update();
    // Cluster-only series (emitted unconditionally here; gated by cluster mode in prod).
    metrics::record_raft_metrics(0, Some(1), 1, Some(1), Some(2));
    metrics::set_prepared_locks(1);
    metrics::record_txn_aborted("prepare_no");
}

#[test]
fn alert_rules_reference_only_real_metrics() {
    metrics::install_recorder().expect("install prometheus recorder");
    emit_all();
    let rendered = metrics::render();
    assert!(!rendered.is_empty(), "render() produced no output");

    // Format pin: durations are summaries (_sum/_count), never bucketed.
    assert!(
        rendered.contains("ledge_git_request_duration_seconds_sum"),
        "expected summary _sum series"
    );
    assert!(
        rendered.contains("ledge_git_request_duration_seconds_count"),
        "expected summary _count series"
    );
    assert!(
        !rendered.contains("ledge_git_request_duration_seconds_bucket"),
        "histogram buckets appeared — latency alerts use _sum/_count and must be revisited"
    );

    let referenced = referenced_series(ALERTS);
    assert!(
        referenced.len() >= 12,
        "extractor found too few series ({}): {referenced:?}",
        referenced.len()
    );
    let missing: Vec<&String> = referenced
        .iter()
        .filter(|name| !rendered.contains(name.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "alert rules reference metrics absent from /metrics: {missing:?}"
    );
}
