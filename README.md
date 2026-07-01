<h1 align="center">Ledge</h1>

<p align="center">
  <b>Git-compatible storage infrastructure, rebuilt for agent-scale workloads.</b><br>
  Speaks the git wire protocol on the surface. Content-addressed, replicated,
  and formally verified underneath.
</p>

<p align="center">
  <a href="LICENSE"><img alt="License: BSL 1.1" src="https://img.shields.io/badge/license-BSL%201.1-blue.svg"></a>
  <img alt="Rust" src="https://img.shields.io/badge/rust-1.89%2B-orange.svg">
  <img alt="Status" src="https://img.shields.io/badge/status-early%2Fsource--available-yellow.svg">
</p>

---

> **Status: early, source-available.** The core works and is tested (667 tests,
> TLA+-verified consensus), and a stock `git` client clones/pushes/fetches
> against it today. It is **not yet production-ready for untrusted multi-tenant
> use** — see [Status & limitations](#status--honest-limitations). This is a
> "source-available / fair-source" project (BSL 1.1, converts to Apache-2.0 in
> 2030), **not** OSI "open source." See [License](#license).

## What it is

`git` is a content-addressed filesystem plus a porcelain (the `git` CLI). Ledge
is the other half: a **server and storage engine** that speaks git's wire
protocol, so you keep using the `git` client — but what's behind the remote is
purpose-built for how machines use repos: write-heavy, parallel, ephemeral,
many-tenant.

You don't replace `git`. You point it at Ledge.

```sh
git clone http://localhost:3000/ws/<id>      # a stock git client. no plugins.
```

## Why it beats git's storage layer (measured)

Same source, same machine, same transport — reproducible via the scripts in
[`dogfood/`](dogfood/):

| Metric | Ledge | git | Notes |
|---|---|---|---|
| **Warm / repeat clone** | **0.13 s** | 0.31 s | upload-pack response memoized by want-set |
| **First / cold clone** | **0.13 s** | 0.31 s | precomputed at push/boot ("eager warming") |
| **Pack size (bytes)** | **1.50 MB** | 1.58 MB | OFS_DELTA + name-hash sort + window-250 |

The pack Ledge writes is a **real git v2 packfile** — `git verify-pack` and
`git unpack-objects` accept it — that is *also* addressable by BLAKE3 `ObjectId`
via a sidecar index. One artifact, both namespaces.

Honest caveat on disk: total on-disk `du` is ~3% larger than git's, because Ledge
keeps that extra BLAKE3↔offset bridge index next to the pack. The pack itself is
smaller; the bridge is the content-addressing tax. (Details in
[`dogfood/README.md`](dogfood/README.md).)

## What's actually in the box

- **Git smart-HTTP**: clone / push / fetch, including delta-compressed packs.
- **Git over SSH**: native embedded SSH server (no external `sshd`) serving
  `git clone` / `git fetch` / `git push` over `ssh://`.
- **Shallow clone** (`git clone --depth N`) over both HTTP and SSH — depth-bounded
  history for fast CI checkouts.
- **Partial clone** (`git clone --filter=blob:none` / `blob:limit=N`) — blobless
  checkouts that lazily fetch objects on demand, for big-repo / monorepo workflows.
- **Git LFS** (Batch API + basic transfer) — large files move over the LFS API,
  content-addressed (SHA-256) and verified on upload; `git lfs push/pull` round-trip.
- **BLAKE3 content addressing** (`ObjectId = blake3(content)`), not SHA-1.
- **Sharded Raft replication** (openraft): linearizable compare-and-swap on refs,
  leader-failover with no committed-data loss.
- **Workspaces**: ephemeral, lease-backed forks with mark-and-sweep GC.
- **Multi-tenancy**: per-tenant ref namespaces + ownership-gated access.
- **Auth / quotas / TLS+mTLS** (all default-off; opt in via config).
- **S3 cold tier + full-node disaster recovery**: tier packs off-machine, lose
  the whole local disk, recover from object storage.
- **Webhooks** and **bidirectional GitHub sync** (import + export, SHA-1-faithful).
- **Native SDK** over Cap'n Proto (Rust / TypeScript / Python / Go).
- **TLA+ formal verification** of the ref store, cross-shard 2PC, distributed GC,
  sharding, and reachability ([`formal/`](formal/)).

## Quickstart

```sh
# Run a single node — prebuilt multi-arch image (amd64 / arm64), or build locally
docker run -p 3000:3000 ghcr.io/v-code01/ledge:latest      # or: docker build -t ledge . && docker run -p 3000:3000 ledge

# Create a workspace and use it with a normal git client
curl -X POST http://localhost:3000/workspaces
git clone http://localhost:3000/ws/<id> myrepo
cd myrepo && echo hi > a.txt && git add . && git commit -m wip
git push origin HEAD:refs/heads/main
```

Deploy artifacts (Compose, Helm, systemd) live in [`deploy/`](deploy/).

### Container images

Signed, multi-arch (`linux/amd64` + `linux/arm64`) images are published to GHCR on
each `v*` release, with SBOM + SLSA build-provenance attestations:

```sh
docker pull ghcr.io/v-code01/ledge:latest         # or a pinned tag, e.g. :v0.1.0

# Verify the cosign signature (keyless — GitHub OIDC via Fulcio/Rekor, no keys):
cosign verify ghcr.io/v-code01/ledge:latest \
  --certificate-identity-regexp 'https://github.com/v-code01/ledge/.github/workflows/release.yml@.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Architecture

```
   git client  (clone / fetch / push)          native SDK
   ┌────────────────────────────┐         ┌──────────────────┐
   │  smart-HTTP   │   SSH       │         │ Cap'n Proto      │
   │  (axum)       │  (russh)    │         │ rs · ts · py · go│
   └───────┬───────────┬────────┘         └────────┬─────────┘
           │           │  git wire protocol         │ POST /rpc
   ┌───────▼───────────▼────────────────────────────▼─────────┐
   │                    ledge-server  (axum)                   │
   │  upload/receive-pack · shallow · partial · negotiation    │
   │  auth · tenancy · quotas · TLS/mTLS · webhooks · sync     │
   └───────┬───────────────────────────────────────────┬──────┘
           │   dyn ObjectStore  /  dyn RefStore  (the seams)   │
           │   single-node ↦ local stores · cluster ↦ Raft     │
   ┌───────▼────────────────┐               ┌──────────────────▼───┐
   │   ledge-object-store   │               │    ledge-ref-store    │
   │ BLAKE3 content-address │               │ lock-free ART + WAL   │
   │ native git packs       │               │ atomic compare-and-set│
   │ delta · S3 cold tier   │               └───────────┬───────────┘
   └────────────────────────┘                           │
           ▲                                  ┌──────────▼───────────┐
           └──────────────────────────────────│  ledge-cluster       │
                 sharded Raft replication      │  openraft · 2PC · GC │
                 (linearizable CAS on refs)    │  (TLA+ verified)     │
                                               └──────────────────────┘
```

The load-bearing idea: every read/write goes through the **`dyn ObjectStore` /
`dyn RefStore` seams**. Single-node binds the local stores; a cluster binds the
Raft-replicated ones — the git surface above is byte-identical either way.

A Rust workspace of focused crates:

| Crate | Responsibility |
|---|---|
| `ledge-core` | `ObjectId`, HLC clocks, the git delta codec, core traits |
| `ledge-object-store` | content-addressed store, native git-pack reader/writer, S3 tier |
| `ledge-ref-store` | lock-free ART ref store, WAL, atomic-commit seam |
| `ledge-git` | git smart-HTTP: upload-pack / receive-pack, pack encode/decode |
| `ledge-workspace` | workspaces, leases, GC, quotas |
| `ledge-raft` / `ledge-cluster` | openraft state machine, sharding, replication, 2PC |
| `ledge-rpc` | Cap'n Proto native protocol |
| `ledge-server` | the Axum binary that wires it all together |

Benchmark methodology and reproduction scripts are in [`dogfood/`](dogfood/).

## Status & honest limitations

Ledge is weeks old. It is a strong artifact and a real engine, but here's exactly
what is **not** ready — so you can decide where it fits:

- **Multi-host is validated under emulated WAN + clock skew, but not yet on
  separate physical machines.** The 3-node cluster passes a chaos suite
  ([`soak/wan-chaos.sh`](soak/wan-chaos.sh), **16/0**) under injected latency,
  jitter, packet loss, reordering, and an asymmetric partition, with the nodes on
  genuinely skewed wall clocks (+5s / −7s via libfaketime) — including a real
  `git push` that replicates byte-identically while the clocks disagree, plus
  leader-stability and no-split-brain / no-commit-regression assertions. Residual:
  it still runs on **one host** (shared kernel + monotonic clock), so this
  emulates WAN conditions rather than proving real geographically-separate
  hardware. That last step needs actual machines.
- **Incremental `git fetch` is now negotiated** (`have`-line support): a fetch
  transfers only the objects the client lacks, not the full closure — verified
  end-to-end against a real `git` client (clone a 25-commit repo, push one commit,
  fetch → exactly the new commit/tree/blob move, not the history). Basic single-ACK
  negotiation; `multi_ack_detailed` and shallow/partial clone are still follow-ons.
- **SSH transport does clone + fetch + push.** Shallow (`--depth N`) and partial
  (`--filter=blob:none` / `blob:limit=N`) clone are supported over HTTP and SSH.
  **No sparse / tree-filter clone**; shallow *deepen-with-history* and LFS file
  locking are follow-ons. SSH auth is an authorized-keys allowlist where each key's **comment
  is its tenant** (`ssh-ed25519 … acme` ⇒ tenant `acme`; no comment ⇒ root), and
  SSH access to a workspace is gated by tenant ownership — same isolation as HTTP.
  Empty/absent allowlist accepts any key as root (dev only).
- **No external security audit**; tenant isolation has documented sharp edges
  (see [`SECURITY.md`](SECURITY.md)).
- **No multi-day soak**; long-run memory behavior is unproven.

**Good for today:** a technical reference / showcase, and single-tenant,
single-node use where you control the client and the repos. **Not yet for:**
hosting strangers' code as a managed multi-tenant service.

A first-party **security & production-readiness review** — threat model,
severity-rated findings, ranked residual risks, and a scoped verdict — is in
[`AUDIT.md`](AUDIT.md).

## License

Ledge is **source-available** under the **Business Source License 1.1** (see
[`LICENSE`](LICENSE)). In short: read it, run it, modify it, and use it in
production freely — the one thing you can't do is offer Ledge to third parties as
a competing hosted/managed service. Each release **converts to Apache-2.0** on
its Change Date (2030-06-13). This is *not* OSI "open source"; it's "fair source."
Commercial licensing: vanshverma.dev@gmail.com.

Contributions: see [`CONTRIBUTING.md`](CONTRIBUTING.md) (DCO sign-off, no CLA).
