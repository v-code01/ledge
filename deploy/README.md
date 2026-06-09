# Deploying Ledge

Ledge is a single self-contained binary (`ledge`) with **no external dependencies**
(no database, cache, or object store). All state is on local disk; in cluster mode
it is replicated via Raft. This directory ships verified deployment artifacts.

## The listener model (read this first)

- **Client port** (`[server].addr`, default `:3000`): git wire / REST / RPC / admin
  + `/raft/*` + `/cluster/*` (+ `/healthz` + `/metrics` for back-compat). HTTP, or
  HTTPS when `[tls].enabled`.
- **Metrics/health port** (`[metrics].addr`, default `:9090`): a dedicated
  **plain-HTTP** listener serving `/metrics` + `/healthz`, bound when
  `[metrics].enabled` (default true). Scrape Prometheus + run health probes here —
  it is TLS-agnostic, so it works even when the client port is HTTPS.
- **mTLS peer port** (`[tls].peer_addr`): a mutual-TLS listener bound only when
  `[tls].mtls = true` (cluster peer traffic).

Notes:
- `/healthz` and `/metrics` are **public** (no auth) on both ports, even when
  `[auth]` is on.
- Cluster peers reach each other's `/raft` on the **client port** (or the
  `[tls].peer_addr` port under mTLS). `[cluster].raft_bind` is parsed but is **not**
  a separate listener (`/raft` rides the client port — documented).

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
| 3000 | client: git/REST/RPC/admin + `/raft` + `/cluster` (+ `/healthz`/`/metrics` back-compat) | HTTP, or HTTPS when `[tls].enabled` |
| 9090 | dedicated metrics/health: `/metrics` + `/healthz` | plain HTTP, bound when `[metrics].enabled` (default); scrape + probe here (TLS-agnostic) |
| `[tls].peer_addr` (e.g. 4443) | mTLS peer listener | only when `[tls].mtls=true`; put on a private network |

(`4001` from `[cluster].raft_bind` is **not** bound — `/raft` rides the client port.)

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

## Live cluster reconfiguration (Phase 4g)

Change a shard's replica set on a running cluster — grow the replication factor,
decommission a node, or replace a dead one — with no downtime, via
`POST /cluster/{shard}/reconfigure` (send it to the shard **leader**; it carries
the `cluster_secret` bearer like other `/cluster/*` routes). `num_shards` is
unchanged (no key reshuffle); openraft handles the joint-consensus voter change
and streams the Raft log+snapshot to any newly-added node.

```sh
# add node 4 to shard 0 (grow / stage a replacement) — POST to the current leader:
curl -X POST http://<leader>:3000/cluster/0/reconfigure \
  -H "Authorization: Bearer <cluster_secret>" -H "content-type: application/json" \
  -d '{"members":{"1":"http://node-1:3000","2":"http://node-2:3000","3":"http://node-3:3000","4":"http://node-4:3000"}}'
# watch convergence: voters + last_applied per shard
curl -fsS http://<leader>:3000/cluster/status -H "Authorization: Bearer <cluster_secret>"
```

**Replace a dead node (node 3 → node 5):**
1. Start node 5 with the target shard listed in its `[[cluster.shards]]` config (so
   it boots an empty Raft group ready to receive the snapshot — pre-provisioning).
2. POST `/cluster/0/reconfigure` to the leader with the new member set (node 5 in,
   node 3 out). openraft adds node 5 as a learner (catches up), then promotes it and
   drops node 3 in one transition.
3. Persist the new member set into every node's `[[cluster.shards]]` config for the
   next boot (the runtime change lives in openraft's log meanwhile; it is not written
   back to your config file automatically).

Caveats: removing the **current leader** triggers a leadership transfer (open­raft
handles it; prefer reconfiguring from a node that stays a voter). Changing the
**number of shards** (keyspace split/merge) is NOT supported — that needs a routing
redesign (see the design spec). Reconfigure-rebuilt object peers carry the bearer
but not per-node TLS config; a rolling restart re-derives full TLS peers.

## Webhooks (event surface)

Ledge can POST a signed event to a tenant-registered URL whenever a durable ref
advances. Disabled by default; enable with:

```toml
[webhooks]
enabled = true
```

When enabled, each tenant manages its own webhooks (tenant-scoped via the caller's
principal; disabled ⇒ all `/webhooks` routes return 503):

```sh
# register — the secret is returned ONCE (store it now; it is never shown again)
curl -X POST http://<node>:3000/webhooks \
  -H "Authorization: Bearer <token>" -H "content-type: application/json" \
  -d '{"url":"https://my-sink.example/hook"}'        # optional: "events":["ref_committed"] (empty ⇒ all)
# → 201 {"id":"<hex>","secret":"<64-hex>","url":...,"events":[...],"created_at_ms":...}

curl http://<node>:3000/webhooks   -H "Authorization: Bearer <token>"   # list (NO secret)
curl -X DELETE http://<node>:3000/webhooks/<id> -H "Authorization: Bearer <token>"   # → 204
```

**Delivery & verification.** On a successful commit, Ledge POSTs a JSON body
(`{"event":"ref.committed","tenant":...,"workspace_id":...,"ref":...,"new_target":...,"ts_ms":...}`)
with headers `X-Ledge-Event: ref.committed`, `X-Ledge-Delivery: <id>`, and
`X-Ledge-Signature: blake3=<hex>`. Verify authenticity by recomputing the keyed
hash over the **raw request body** and constant-time comparing:

```
expected = "blake3=" + hex(blake3_keyed_hash(secret_bytes, raw_body))
assert expected == X-Ledge-Signature
```

where `secret_bytes` is the 32 bytes decoded from the hex `secret` returned at
registration.

**Caveats.**
- **Best-effort, not a durable outbox.** Delivery is an in-process spawned task
  with bounded retry (3 attempts, exponential backoff). If the node crashes
  mid-delivery, that event is lost — there is no persistent replay queue. The
  commit itself is unaffected: a dead or slow sink never fails or blocks a commit.
- **SSRF.** Webhook target URLs are tenant-controlled. A malicious tenant can aim
  a webhook at internal addresses (link-local, cloud metadata, RFC1918). For
  untrusted multi-tenant deployments, gate target hosts (allowlist/denylist,
  egress proxy) before enabling.
- **Only `ref.committed` is emitted today** (other event kinds are reserved in the
  type but not yet produced).

## Git remote sync (import)

On-demand IMPORT pulls an upstream git repo into a fresh Ledge workspace,
preserving canonical git SHA-1s (a re-clone of the workspace produces commits
byte-identical to the upstream's). It is **disabled by default** and requires the
`git` binary on `PATH` — the container image ships it.

Enable it in config:

```toml
[sync]
enabled = true
# allowed_upstream_hosts = ["github.com"]   # SSRF allow-list; omit ⇒ any host
```

Import an upstream, then clone the resulting workspace from Ledge:

```bash
# POST returns 201 {workspace_id, default_branch, refs:[{name,target_sha1}, …]}
curl -X POST http://<host>:3000/sync/import \
  -H "content-type: application/json" \
  -d '{"upstream_url":"https://github.com/owner/repo.git"}'
# → {"workspace_id":"<id>","default_branch":"main","refs":[…]}

git clone http://<host>:3000/ws/<workspace_id>
```

Optional fields: `upstream_auth` (a credential for private upstreams) and
`ttl_seconds` (overrides the workspace TTL). For GitHub over HTTPS, pass a PAT as
`x-access-token:<PAT>`:

```bash
curl -X POST http://<host>:3000/sync/import \
  -H "content-type: application/json" \
  -d '{"upstream_url":"https://github.com/owner/private.git","upstream_auth":"x-access-token:<PAT>"}'
```

**Caveats.**
- **Import-only.** Export / push-back (Ledge → upstream) is a follow-on; today the
  flow is upstream → Ledge workspace only.
- **On-demand only.** Each `POST /sync/import` is a one-shot clone into a new
  workspace; there is no scheduled / continuous mirror.
- **SSRF.** `upstream_url` is caller-controlled and the import shells out to `git`.
  For untrusted multi-tenant deployments set `allowed_upstream_hosts` (empty ⇒ any
  host is permitted). Tokens are only meaningful over `https://` URLs.
- Returns **503** when `[sync].enabled` is false and **502** when the upstream
  clone/ingest fails (e.g. bad URL, auth, or unreachable host).

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
