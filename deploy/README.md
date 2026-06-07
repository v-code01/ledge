# Deploying Ledge

Ledge is a single self-contained binary (`ledge`) with **no external dependencies**
(no database, cache, or object store). All state is on local disk; in cluster mode
it is replicated via Raft. This directory ships verified deployment artifacts.

## The single-port model (read this first)

The server serves **everything on one port** — `[server].addr` (default `:3000`):
git wire / REST / RPC / admin **and** `/healthz`, `/metrics`, `/raft/*`,
`/cluster/*`. `[metrics].addr` and `[cluster].raft_bind` are parsed but are **not**
currently separate listeners. So:

- Prometheus scrapes `/metrics` on the **client port**.
- Cluster peers reach each other's `/raft` on the **client port** (or the
  `[tls].peer_addr` port under mTLS).
- `/healthz` and `/metrics` are **public** (no auth), even when `[auth]` is on.

The only second listener is the **mTLS peer listener** on `[tls].peer_addr`, bound
only when `[tls].mtls = true`.

## Quickstart

### Docker (single node)
```sh
docker build -t ledge:latest .
docker run -d -p 3000:3000 -v ledge-data:/var/lib/ledge ledge:latest
curl -fsS http://localhost:3000/healthz        # {"status":"ok"}
```

### docker-compose (single node)
```sh
docker compose -f deploy/compose/docker-compose.yml up -d
```

### docker-compose (3-node cluster)
```sh
docker compose -f deploy/compose/docker-compose.cluster.yml up -d
# bootstrap the shard ONCE (cluster_secret matches the compose file):
curl -X POST http://localhost:3000/cluster/init \
  -H "Authorization: Bearer dev-cluster-secret-change-me" \
  -H "content-type: application/json" \
  -d '{"shard":0,"members":{"1":"http://ledge-1:3000","2":"http://ledge-2:3000","3":"http://ledge-3:3000"}}'
curl -fsS http://localhost:3000/cluster/status -H "Authorization: Bearer dev-cluster-secret-change-me"
```

### Helm (Kubernetes)
```sh
# single node
helm install ledge deploy/helm/ledge
# 3-node authenticated cluster
helm install ledge deploy/helm/ledge \
  --set cluster.enabled=true,replicaCount=3,auth.enabled=true,auth.clusterSecret=<strong-secret>
# then bootstrap shard 0 once (port-forward a pod, POST /cluster/init with the
# headless DNS members — see NOTES.txt).
```
The StatefulSet derives each pod's Raft `node_id` from its ordinal
(`ledge-0` → 1). Each pod gets its own PVC for `/var/lib/ledge`.

### systemd (bare metal / VM)
```sh
install -m0755 target/release/ledge /usr/local/bin/ledge
useradd --system --home-dir /var/lib/ledge ledge
install -d -o ledge -g ledge -m0700 /etc/ledge
cp deploy/config.toml.sample /etc/ledge/config.toml   # then edit
cp deploy/systemd/ledge.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now ledge
```

## Ports

| Port | Purpose | Notes |
|------|---------|-------|
| 3000 | client: git/REST/RPC/admin + `/healthz` + `/metrics` + `/raft` + `/cluster` | HTTP, or HTTPS when `[tls].enabled` |
| `[tls].peer_addr` (e.g. 4443) | mTLS peer listener | only when `[tls].mtls=true`; put on a private network |

(`9090` and `4001` from `[metrics].addr` / `[cluster].raft_bind` are **not** bound.)

## Configuration (env / TOML)

Every key is `LEDGE__<SECTION>__<KEY>` (double underscore) or a TOML key. See
`deploy/config.toml.sample`.

| Env | Default | Meaning |
|-----|---------|---------|
| `LEDGE__SERVER__ADDR` | `0.0.0.0:3000` | client listener |
| `LEDGE__SERVER__DATA_DIR` | `/var/lib/ledge` | on-disk state root |
| `LEDGE__METRICS__ENABLED` | `true` | (`/metrics` is on the client port regardless) |
| `LEDGE__WORKSPACE__GC_INTERVAL_SECS` | `300` | GC pass interval |
| `LEDGE__AUTH__ENABLED` | `false` | **enable in prod**; API-key auth |
| `LEDGE__AUTH__CLUSTER_SECRET` | — | node↔node bearer; **required when auth+clustered** |
| `LEDGE__AUTH__BOOTSTRAP_ADMIN_TOKEN` | — | first-boot root admin key (empty store only) |
| `LEDGE__QUOTAS__ENABLED` | `false` | per-tenant quotas (root exempt) |
| `LEDGE__QUOTAS__MAX_{WORKSPACES,DURABLE_BYTES,OBJECT_COUNT,REQUESTS_PER_SEC}` | unlimited | per-tenant limits |
| `LEDGE__TLS__ENABLED` | `false` | **enable in prod**; server TLS (encrypts tokens) |
| `LEDGE__TLS__{CERT_PATH,KEY_PATH}` | — | required when TLS enabled |
| `LEDGE__TLS__MTLS` | `false` | mutual TLS peer auth |
| `LEDGE__TLS__{CA_PATH,PEER_ADDR,CLIENT_CERT_PATH,CLIENT_KEY_PATH}` | — | required when mtls |
| `LEDGE__CLUSTER__ENABLED` | `false` | sharded Raft |
| `LEDGE__CLUSTER__NODE_ID` | `1` | **unique per node** (Helm derives from ordinal) |
| `LEDGE__CLUSTER__SHARDS__*` | — | shard map (identical on every node) |

## State layout (`data_dir`)

```
objects/   content-addressed object store (BLAKE3)
refs/      durable ref WAL (+ checkpoints)
leases/    workspace lease WAL
auth/      API-key WAL (when [auth] enabled)
shard-N/   per-shard Raft log + state machine (cluster mode)
```
Back up `data_dir` (or use `POST /admin/snapshot` for a CoW snapshot). For
Kubernetes, the per-pod PVC holds this.

## Production security checklist

- [ ] `[auth].enabled = true` with a **strong** `cluster_secret` (clustered) and a
      bootstrap admin token; mint per-tenant keys via `ledge auth create-key`.
- [ ] `[tls].enabled = true` (encrypts API tokens in transit); `[tls].mtls = true`
      for clustered node authentication.
- [ ] Put the mTLS `peer_addr` (and all node↔node traffic) on a **private network**.
- [ ] Remember `/healthz` and `/metrics` are **unauthenticated** — don't expose
      `/metrics` publicly if your series are sensitive; scrape it from inside.
- [ ] Don't issue **root-tenant** keys to untrusted clients (root is a superuser
      namespace, exempt from tenancy + quotas).
- [ ] Rotate certs by rolling restart (no hot reload yet).

## Honest limitations

- **systemd unit is authored but not machine-verified here** (built on macOS, no
  systemd) — run `systemd-analyze verify /etc/systemd/system/ledge.service` on
  Linux before relying on it.
- **Single-shard Helm placement** — every replica is a member of one shard
  (replication factor = `replicaCount`); multi-shard placement is a follow-on.
- **Local-arch image only** — no registry push / signing / multi-arch in these
  artifacts (the Dockerfile is buildx-ready for a later multi-arch flip).
- **Cluster bootstrap is a one-time manual `POST /cluster/init`** (standard Raft) —
  not auto-bootstrapped on `up`.
