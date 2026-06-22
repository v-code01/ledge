# Monitoring Ledge (Prometheus + Grafana)

Ledge exports Prometheus metrics on the dedicated, plain-HTTP, TLS-agnostic
metrics port (`[metrics].addr`, default `:9090`) at `/metrics` — scrape and probe
there. This directory ships ready-to-run alerting, a scrape config, and a Grafana
dashboard.

## Single source of truth for alerts

The alert rules live in **one** file:

    deploy/helm/ledge/files/ledge-alerts.yml

Everything else references it, so the rules can never drift:

| Consumer | How it loads the rules |
|---|---|
| Helm / Prometheus Operator | `templates/prometheusrule.yaml` embeds it via `.Files.Get` |
| Standalone Prometheus (below) | `prometheus.yml` `rule_files`, bind-mounted into the container |
| CI | `crates/ledge-server/tests/metrics_alerts.rs` asserts every metric the rules reference actually appears in `/metrics` |

That last point matters: if a metric is renamed or removed in the code, the build
fails instead of silently disabling an alert in production.

## Quick start (Docker Compose)

Merge the monitoring stack with the single-node compose so Prometheus can reach
the Ledge container on the shared network:

```sh
docker compose \
  -f deploy/compose/docker-compose.yml \
  -f deploy/monitoring/docker-compose.monitoring.yml up -d
```

- Prometheus → http://localhost:9091 (check **Status → Targets** and **Alerts**)
- Grafana → http://localhost:3001 (login `admin` / `admin`) → dashboard **Ledge → Ledge Overview**

The Grafana datasource and dashboard are auto-provisioned from
`grafana/provisioning/` and `grafana/dashboards/`.

## Kubernetes (Prometheus Operator)

The Helm chart renders a `ServiceMonitor` (scrape) and a `PrometheusRule`
(alerts). Enable both, adding whatever label your operator's selectors require:

```yaml
metrics:
  serviceMonitor:
    enabled: true
  prometheusRule:
    enabled: true
    labels:
      release: kube-prometheus-stack
```

## Alert reference

All thresholds are starting points — tune them to your traffic. Latency uses
`rate(_sum)/rate(_count)` because histograms are exported as summaries (the
recorder configures no buckets, so there are no `_bucket` series).

| Alert | Severity | Fires when |
|---|---|---|
| `LedgeInstanceDown` | critical | target unreachable >2m (`up{job="ledge"}==0`) |
| `LedgeGitLatencyHigh` | warning | mean git-wire latency >2s for 10m under traffic |
| `LedgeObjectWriteLatencyHigh` | warning | mean object write >500ms for 10m under load |
| `LedgeRpcLatencyHigh` | warning | mean RPC latency >1s for 10m under load |
| `LedgeAuthFailureSpike` | warning | >5/s unauthenticated/forbidden for 10m |
| `LedgeTenantDeniedSpike` | warning | >1/s cross-tenant denials for 10m |
| `LedgeWebhookDeliveryFailureRate` | warning | >20% webhook deliveries failing for 10m |
| `LedgeSyncFailing` | warning | a GitHub import/export op failing for 15m |
| `LedgeQuotaDenials` | info | a quota rejecting requests for 15m |
| `LedgeRefCasContentionHigh` | warning | >1 CAS retry per ref update for 15m |
| `LedgeRaftNoLeader` | critical | a shard has no leader for 1m (cluster) |
| `LedgeRaftElectionsFlapping` | warning | term advances >3× in 15m (cluster) |
| `LedgeRaftApplyLag` | warning | commit−applied >1000 for 5m (cluster) |
| `LedgePreparedLocksStuck` | warning | in-doubt 2PC locks held >15m (cluster) |
| `LedgeTxnAbortSpike` | warning | >1/s 2PC aborts for 10m (cluster) |

The `ledge-cluster` group references series that exist only in cluster mode, so
those alerts never fire on a single-node deployment — correct by construction.

## Notes

- `/metrics` is **unauthenticated**. Scrape it from inside your trust boundary;
  don't expose it publicly if your series are sensitive (see `deploy/README.md`).
- This standalone stack is for local eval / small deployments. For production,
  prefer the Helm chart with your existing Prometheus Operator.
