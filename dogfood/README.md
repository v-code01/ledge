# Dogfood: Ledge hosting Ledge

The system that replaces git, hosting its own source code — deployed, verified, running.

```sh
bash dogfood/selfhost.sh
```

This (1) deploys a **persistent self-hosted Ledge** instance, (2) makes Ledge **import its
own source** (via the git-sync feature), (3) **clones that source back out of Ledge** and
asserts the HEAD commit SHA-1 is byte-identical, then leaves the instance running.

Latest verified run — **6 PASS / 0 FAIL** ([`results/2026-06-09-delta.txt`](results/2026-06-09-delta.txt)):
a raw `git push` of the full source into Ledge succeeded (delta-capable receive-pack) and the
cloned-back HEAD equaled the host source HEAD exactly. Ledge is serving its own source over the
git wire protocol.

## What it dogfoods

- **Deploy artifacts (Phase 4e):** the `ledge:latest` Docker image, a compose service with a
  named volume + `restart: unless-stopped` + auth + the metrics/health port.
- **Git remote sync:** `POST /sync/import` ingests the Ledge source (`file:///srv/ledge-src`,
  the host repo mounted read-only) into a workspace — delta-safe (`git cat-file
  --batch-all-objects`), preserving commit SHA-1s.
- **The git server (Phase 1):** `git clone http://localhost:3030/ws/<id>` serves the source
  back, byte-identical.

## How "persistent" works (and its honest ceiling)

State lives in the named volume `ledge-selfhost-data`; the container is `restart:
unless-stopped`. So the instance — and the hosted source — **survive container restart,
Docker-daemon restart, and machine reboot**.

**It is NOT off-machine durable.** Everything is on this one SSD. If the disk dies, it's
gone. True durability needs a deploy on a *separate* host (the artifacts support it; out of
scope here). This dogfood proves "Ledge can host its own source persistently," not "your code
survives this laptop dying."

## A gap this surfaced, now fixed: raw `git push` works

This dogfood originally surfaced a real limitation — a raw `git push` of the source into Ledge
**failed with HTTP 500**, because the `receive-pack` decoder handled only non-delta objects
while a real multi-commit push sends a *delta-compressed* pack. **That is now fixed.**
`ledge_git::push::decode_pack_objects` resolves OFS_DELTA and REF_DELTA (including thin-pack
bases looked up from the object store), so the delta-capable `receive-pack` accepts real packs.
Step 2 of this script now pushes the full Ledge source straight into the instance as a hard
**PASS** assertion. `sync-import` remains the in-process bridge for mirroring upstreams, but
`git push http://localhost:3030/ws/<id>/ HEAD:refs/heads/main` is now a first-class path —
which makes **continuous self-hosting via push** possible.

## Disk footprint (measured, honest)

Same ~2,100-object Ledge source, three ways:

| Representation | Size |
|---|---|
| git (delta + zlib, hard repack) | **1.4 MB** |
| Ledge — **zlib per-object** (current) | **20 MB** |
| Ledge — raw, uncompressed (before) | 30 MB |

Per-object compression shipped (`DiskObjectStore`, zlib, transparent, zero migration, identity
unchanged) and cut the store **30 MB → 20 MB (~33%)** — verified end-to-end here (push +
clone-back stay byte-identical). The honest read: that's a modest down payment. Per-object zlib
can't dedup *across* similar objects, and this corpus is dominated by many small objects, so the
ratio is ~1.5×, not the 3–4× a naive estimate suggests. The measurement makes the real picture
clear — **almost the entire remaining gap (20 MB → 1.4 MB, ~14×) is delta**, i.e. git storing
diffs between similar object versions. **Delta retention is the dominant remaining lever** (and
reuses the `apply_delta` already shipped for receive-pack). Compression was cut 1 of 2.

## Other honest notes

- **Snapshot today, continuous now possible:** the sync-import step holds the source as of the
  import. With delta-capable `receive-pack` shipped, a `git push` post-commit hook (or the
  webhook surface) can now drive continuous self-hosting straight into Ledge.
- **Workspace-hosted:** the source lives in a workspace with a 1-year lease (Ledge has no root
  durable git surface; the workspace is the unit). Renew/long-TTL keeps it alive.
- The bootstrap auth token in `docker-compose.yml` is a dev token — fine for a local dogfood,
  not for anything exposed.

## Manage the instance

```sh
docker compose -f dogfood/docker-compose.yml ps        # status
docker compose -f dogfood/docker-compose.yml logs -f   # logs
docker compose -f dogfood/docker-compose.yml down       # stop (keeps the volume/data)
docker compose -f dogfood/docker-compose.yml down -v    # stop + wipe the hosted source
```
