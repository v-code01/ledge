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

## Disk footprint (measured, honest — the full journey to near-git)

Same ~2,200-object Ledge source. Three storage cuts shipped — **compression** (zlib per object),
**delta retention** (offline `POST /admin/repack`), and **packing** (consolidate to one pack
file). All transparent end-to-end (push + clone-back byte-identical, verified **6/6** at each
stage). The on-disk (`du`) journey:

| Stage | Store on disk (`du`) | Files |
|---|---|---|
| git (delta + zlib pack) | **1.4 MB** | ~3 |
| Ledge — raw, uncompressed | 30 MB | 2,200 |
| Ledge — + compression (zlib) | 20 MB | 2,200 |
| Ledge — + delta retention | ~20 MB* | 2,200 |
| **Ledge — + packing (current)** | **2.3 MB** | **2** |

*Delta retention deltified 1,313 objects (content `4.3 MB → 2.2 MB`, ~1.9×) but `du` barely moved
— because the per-object-file model meant **74% of objects (<1 KB) each cost a full ~8 KB block**,
so ~16 MB of the 20 MB was filesystem block overhead, not content. That measurement is what
pointed at packing.

**Packing closed it:** consolidating 2,218 files → **one 2.2 MB pack + one 88 KB index** drops
`du` from 21 MB to **2.3 MB — within ~1.6× of git's 1.4 MB.** The disk gap is essentially closed.
A `.pack` record is the exact bytes of a loose object file, so reads parse packed/loose
identically; the repack writes a new pack, swaps it in, **verifies every object by read-back, then**
deletes the loose files (and prunes the emptied `objects/XX/YY` dirs — which themselves cost
~10 MB of empty-directory blocks if left behind). Content-addressing (BLAKE3) + delta self-verify
+ GC base-retention remain the correctness nets.

**Two honest follow-ons the measurement surfaced:**
- `write_git_object` doesn't dedup against packs — re-importing an already-packed object writes a
  duplicate *loose* copy (correct on read; loose shadows pack), re-inflating disk until the next
  repack. A cheap `exists()`-against-packs check on write fixes it.
- Base selection is still a size-window-16 heuristic (git uses ~250) → ~59% deltified vs git's
  ~95%; a bigger window narrows the last ~1.6× toward parity. New writes stay loose until repack
  (no background scheduler yet).

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

## Clone speed (measured, honest — Ledge now beats git on repeat clones)

Same ~2,200-object repo, same machine, network transport on both sides (`git daemon`
git:// for git, HTTP for Ledge — a fair same-transport comparison, not git's local `file://`):

`bash dogfood/clone-speed.sh` (latest run —
[`results/2026-06-12-cold-clone-warm.txt`](results/2026-06-12-cold-clone-warm.txt), **3 PASS / 0
FAIL**, all three clone byte-identical HEAD):

| Clone | Time |
|---|---|
| **Ledge — first clone, eager-warmed** | **0.13 s** |
| Ledge — repeat / warm cache | 0.13 s |
| git over the network (`git daemon`) | 0.31 s |
| Ledge — first / cold (warming off, builds the pack) | 0.79 s |

The journey: 1.08 s → **0.83 s** (cached two-tier `sha1_index` — also fixed a correctness
bug where clone-from-a-purely-packed-store failed) → **0.13 s** warm (upload-pack response
cache: the encoded pack is memoized by want-set, so a clone of an unchanged repo streams
precomputed bytes — git's own model).

**The cold-clone axis is now flipped too — Ledge beats git on the *first* clone.** Previously the
first clone of a never-seen tip built the pack on demand (~0.79 s) while git served a pack
precomputed at `gc` time, so git won the *cold* case. **Eager warming closes it:** the upload-pack
response is now precomputed and cached **at write time** — after every push (background, off the
hot path), once on boot (`warm_all_segments`), and on the synchronous `POST /admin/warm` ops
trigger. So the *first* clone is a cache hit (**0.13 s**), not a build — measured **below git's
cold clone (0.31 s)**. The proof above isolates the variable: import via `sync-import` (which does
*not* warm) gives the 0.79 s cold baseline; a server **restart** (boot-warm repopulates from the
packed store) makes the very first clone of the fresh process — never cloned before — land at
0.13 s.

**Honest note:** warming doesn't make the work vanish, it *moves* it — the graph-walk + encode now
happens at push/boot/admin time instead of on the first clone, which is the right trade for the
agent/CI re-clone workload (and for git's own gc-precompute model). The cache is want-set-keyed (a
tip sha uniquely determines its closure ⇒ self-invalidating, never stale), bounded LRU (32 entries
/ 256 MiB), per-node in-memory. A clone racing a push before the background warm finishes still
builds (then caches); `POST /admin/warm` / boot-warm are the synchronous guarantees.


## Native git packs (the storage IS a git packfile now)

Phases A–D: Ledge's cold tier is a **real git v2 packfile** — written from scratch in Rust,
**certified by `git verify-pack` running inside the container** (delta chains to length 41), and
read back by Ledge's BLAKE3 `ObjectId` via a `.lidx` sidecar. One artifact: git-valid (clone-ready)
and blake3-addressable (Ledge reads).

| Disk (`du`, same source) | Size |
|---|---|
| git pack | **1.4 MB** |
| **Ledge — native git pack, window=64** | **1.9 MB (pack 1.67 MB)** |
| Ledge — internal pack (pre-git-pack) | 2.3 MB |
| Ledge — raw, uncompressed | 30 MB |

86% of objects deltify (REF_DELTA, self-verified). The disk gap to git closed from ~1.6× to
**~1.35×**. Honest residual: git still edges disk (1.4 vs 1.9 MB) via OFS_DELTA + window-250 +
path-aware base selection — closing it fully (sub-1.4 MB) needs base-index caching to afford a
250-wide window + OFS deltas (diminishing returns). Repack at window=64 is ~54 s (offline
maintenance; the raw-length delta ranking made a wide window affordable at all).

## Off-machine durability: S3 cold tier (the "survives the laptop dying" answer)

`bash dogfood/s3/tier.sh` (MinIO standing in for S3). Latest run — **8 PASS / 0 FAIL**
([`results/2026-06-11-s3-tier.txt`](results/2026-06-11-s3-tier.txt)):

import the source → repack to a git pack → **`POST /admin/tier`** spills the **1.7 MB pack body
to MinIO** and **removes the local `.pack`** (the Ledge volume keeps only the 68 KB `.idx` +
146 KB `.lidx` + a 75-byte `.s3` marker) → `git clone` the workspace back **succeeds
byte-identical** because the server **restores the pack body from MinIO** to serve it.

Two real S3 round-trips prove it: a `HEAD` verifies the upload *before* the local body is
deleted (the marker is written only on success), and a `GET` restores the body on the cold read.
A native git pack is an immutable, content-addressed blob — the ideal tiering unit; the indexes
stay local so lookups never hit the network, only fetching a cold pack's bytes does. **Cold pack
bodies now live in object storage, off the Ledge volume** — the original "what survives the SSD
dying" concern, answered. (`[s3]` is default-off; enable via `LEDGE__S3__*`.)

Honest v1 residuals: whole-pack restore (not byte-range); only `.pack` tiers (indexes stay local —
full-node DR needs them in S3 too, but they're regenerable via `git index-pack`/blake3); explicit
tiering (no auto age/size policy); no warm-cache eviction.


### Full-node disaster recovery (lose the whole disk, rebuild from S3)

`bash dogfood/s3/tier.sh` now also proves **full-node DR** — latest run **11 PASS / 0 FAIL**
([`results/2026-06-11-s3-dr.txt`](results/2026-06-11-s3-dr.txt)):

tier (pack body **and** `.idx`/`.lidx` now go to S3) → **`rm -f` the ENTIRE local
`objects/pack/` dir** (simulate the SSD dying — `.pack`, `.idx`, `.lidx`, marker, all gone) →
**`POST /admin/recover`** pulls the small indexes back from S3 and writes markers
(`packs_recovered: 1`) → **`git clone` the workspace back succeeds, rebuilt entirely from object
storage** (indexes recovered eagerly, the pack body restores on the read).

A node also runs `recover_from_s3` on boot, so a freshly-provisioned/wiped instance self-heals
from S3. **This is the complete answer to "what survives the laptop dying": tier, lose
everything local, recover from S3.** (Un-repacked loose objects aren't tiered — repack first;
recovery pulls indexes eagerly + bodies lazily; byte-range restore is a v3 perf follow-on.)
