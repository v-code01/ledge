# Backup & restore runbook

Ledge keeps **all** durable state under one directory (`[server].data_dir`, e.g.
`/var/lib/ledge`):

```
<data_dir>/
  objects/    content-addressed objects (immutable) + packs
  refs/       ref store + write-ahead log
  leases/     workspace lease write-ahead log
  lfs/        Git-LFS objects (sha256-addressed)
  auth/       API-key WAL (when [auth] is enabled)
  shard-N/    per-shard Raft log + state machine (cluster mode)
```

On boot Ledge replays the WALs and opens `objects/`, so a faithful copy of
`data_dir` **is** a complete, restorable backup. The scripts here archive the
whole directory, so `auth/` and any `shard-N/` are included automatically. This
is filesystem-level backup, complementary to the S3 cold tier
(`POST /admin/tier`), which offloads cold packs but is not a point-in-time backup
of refs/leases/LFS.

**Scope: single node.** In cluster mode each node has its own `data_dir` (its own
`shard-N/` Raft state); this runbook backs up and restores one node. Restoring a
whole cluster from per-node backups is a separate procedure — for routine
durability in a cluster, rely on replication + per-node backups, and restore a
lost node by re-joining it, not by transplanting another node's `shard-N/`.

Scripts in this directory — all verified end-to-end by
[`verify-roundtrip.sh`](verify-roundtrip.sh) (push → backup → wipe → restore →
assert the ref returns byte-identical, for both methods):

| Script | Purpose |
|---|---|
| `backup.sh`  | data dir → one `.tar.gz` (cold or hot) |
| `restore.sh` | `.tar.gz` → data dir |
| `verify-roundtrip.sh` | the automated proof the above work |

## Choose a method

| | Cold (default) | Hot (`--hot`) |
|---|---|---|
| Server state | **stopped** | **running** (no downtime) |
| Consistency | exact | valid + recent; may miss a write in flight at the snapshot instant |
| Mechanism | `tar` of the data dir | `POST /admin/snapshot` (CoW reflink) → `tar` |
| Use when | maintenance window OK | can't take downtime |

Why hot is safe: objects are content-addressed and write-once, and the ref/lease
WALs are append-only and replay-tolerant — so a CoW clone taken mid-flight is
always a valid Ledge dir. The only thing it can lose is the single write still
in flight at the snapshot instant (a sub-second RPO). For an exact point-in-time,
use cold, or briefly quiesce writes before the hot snapshot.

## Back up

```sh
# Cold — stop the server first (systemd: `systemctl stop ledge`; compose: `docker compose stop`)
deploy/backup/backup.sh --data-dir /var/lib/ledge --out /backups/ledge-$(date +%F).tar.gz

# Hot — server stays up; admin must be reachable (the snapshot dest is written server-side)
deploy/backup/backup.sh --hot --url http://127.0.0.1:3000 \
  --data-dir /var/lib/ledge --out /backups/ledge-$(date +%F).tar.gz
```

> `/admin/snapshot` is an admin endpoint. If auth is enabled, gate it and pass the
> admin token via your reverse proxy / `curl` as usual; if exposed, restrict it.

Then copy the tarball off-box (S3, another host) — a backup on the same disk does
not survive disk loss.

## Restore

```sh
# 1. Stop the server.
# 2. Restore into the (empty) data dir; --force overwrites a non-empty dir.
deploy/backup/restore.sh --in /backups/ledge-2026-06-23.tar.gz \
  --data-dir /var/lib/ledge --force
# 3. Start the server, then verify:
curl -fsS http://127.0.0.1:9090/healthz
git ls-remote http://127.0.0.1:3000/ws/<id>        # refs come back
```

Restoring onto a fresh machine is the disaster-recovery path: install Ledge,
restore the tarball into `data_dir`, start. No rebuild/reindex step — the WALs and
content-addressed store are self-describing.

## Verify the scripts on your build

```sh
cargo build -p ledge-server
bash deploy/backup/verify-roundtrip.sh   # exits non-zero if any round-trip fails
```

## Suggested schedule

- Nightly hot backup (cron), retained N days, shipped off-box.
- A weekly cold backup during a quiet window for an exact-consistency anchor.
- Periodically run `restore.sh` into a scratch dir and boot it — an untested
  backup is not a backup.
