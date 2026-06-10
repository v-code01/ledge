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

## Disk footprint (measured, honest — and a finding that surprised us)

Same ~2,170-object Ledge source. Two things shipped: per-object **compression** (zlib) and
**delta retention** (offline repack, `POST /admin/repack`). Both are transparent end-to-end
(push + clone-back stay byte-identical, verified 6/6 here). What the measurement then revealed:

| Metric | Value |
|---|---|
| git (delta + zlib pack) | **1.4 MB** |
| Ledge — **actual content bytes** (compressed + deltified) | **4.18 MB** |
| Ledge — **`du` block-allocated** | **~20 MB** |
| of which: filesystem block overhead | **~16 MB** |
| objects < 1 KB | **1,595 of 2,170 (74%)** |

Compression cut raw content ~30 MB → ~8 MB; **delta retention** then deltified 1,277 objects
(content `4.18 MB → 2.19 MB`, ~1.9× on the touched bytes) — every rewrite **self-verified**
(`apply_delta` round-trip + BLAKE3 match, so a bad delta can never corrupt) and GC **retains the
base of every kept delta** (no data loss). The actual *content* footprint, **4.18 MB, is now
within ~3× of git's 1.4 MB** — the content-level gap is largely closed.

**But `du` barely moved (21 → 19 MB), and the measurement says why:** Ledge stores one file per
object, 74% of objects are <1 KB, and each costs a full ~8 KB filesystem block — so **~16 MB of
the on-disk size is per-file block overhead, not content.** Our earlier "delta is the ~14×
lever" framing was measuring `du`, which is dominated by block rounding, not bytes.

**The true remaining lever is packing** — many objects into a few files (git's actual model),
which eliminates the per-object block overhead. Delta retention is its necessary precursor
(packing deltified content → near-git size); packing alone would kill the block overhead but
leave content un-deltified. Pack-file storage is the next architectural step; delta + compression
are done and correct.

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
