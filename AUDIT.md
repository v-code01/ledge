# Ledge — Security & Production-Readiness Review

**Reviewed commit:** `b418d28` · **Date:** 2026-06-16 · **Reviewer:** first-party (maintainer) engineering review, conducted with audit rigor.

> **Disclaimer — read this first.** This is a **first-party** review, not an
> independent third-party audit or penetration test. It is honest about what was
> verified and how, and it documents the open gaps as findings (it does not paper
> over them). An independent external audit remains an open item — see **R-2**.
> "S-tier" here means rigor and honesty, not a clean bill of health.

---

## 1. Scope & methodology

**In scope:** the Rust workspace at `b418d28` — the object store, ref store, git
wire protocol (HTTP + SSH), LFS, auth/tenancy/quotas/TLS, webhooks/sync, and the
sharded-Raft cluster layer.

**Methods used (with evidence cited inline):**
- **Source review** of the trust-boundary code paths.
- **Property-based fuzzing** (`proptest`) of every parser that consumes
  attacker-controlled bytes.
- **Formal verification** — 5 TLA+ modules, model-checked (`formal/`).
- **Real-client end-to-end tests** against the actual `git` and `git-lfs`
  binaries (not mocks).
- **Tooling gates** — `cargo clippy --workspace -- -D warnings`, `cargo fmt
  --check`, and the full test suite, all green in CI.

**Explicitly NOT in scope (and why):** no independent pentest (R-2); no
validation on real multi-host hardware — the cluster was exercised only under
*emulated* WAN on a single host (R-1); no multi-day soak (R-3). These are the
load-bearing caveats; the verdict in §6 is scoped around them.

## 2. System & trust boundaries

Ledge speaks the git wire protocol on the surface and is content-addressed
(BLAKE3) underneath. Trust boundaries crossed by untrusted input:

| Boundary | Untrusted input | Entry point |
|---|---|---|
| git fetch/clone | `want`/`have`/`deepen`/`filter` lines | `ledge-git::fetch` |
| git push | a packfile (objects + deltas) | `ledge-git::push::decode_pack_objects` |
| LFS | object bytes + Batch API JSON | `ledge-server::lfs` |
| webhooks | tenant-supplied target URL | `ledge-server::ssrf` + `webhook::dispatch` |
| auth | API keys / SSH public keys | `ledge-server::auth`, `::ssh` |
| cluster | peer RPC | `ledge-cluster` (mTLS + shared secret) |

## 3. Threat model (actors)

- **Untrusted git client** — can push arbitrary (malformed/malicious) packfiles
  and request arbitrary objects.
- **Co-tenant** — an authenticated tenant attempting to read/write another
  tenant's data.
- **Network attacker** — observes/injects on the wire (mitigated by TLS/mTLS).
- **Malicious webhook registrant** — tries to turn the server into an SSRF pivot.

---

## 4. Findings

Severity: **Critical / High / Medium / Low / Info**. Status: **Mitigated** (with
evidence) or **Residual** (tracked in §5).

### F-1 — Untrusted packfile / delta decoding · **High** · Mitigated
A pushed pack is attacker-controlled; naive decoding is an OOM/DoS and panic
surface. Mitigations, all with bounded resources:
- Per-object decompression is bounded to the header-declared size and **capped at
  1 GiB** (`MAX_PACK_OBJECT`, `crates/ledge-git/src/push.rs`); the inflated length
  is verified, so a zlib bomb is rejected, never materialized. Read-path inflate
  is likewise bounded (`git_pack_file.rs`).
- Delta output is capped at 2 GiB (`MAX_OBJECT_SIZE`, `ledge-core/src/delta.rs`);
  delta-chain recursion is capped at depth 50 (`MAX_DELTA_DEPTH`).
- The pack header parser, `git_pack_len`, the `.lidx` parser, and `apply_delta`
  are **proptest-verified to never panic/hang/over-allocate on arbitrary bytes**;
  a concrete zlib-bomb regression test (`decode_rejects_zlib_bomb`) is included.

### F-2 — Authentication · **High** · Mitigated
- API keys are **hashed at rest (BLAKE3)**, compared in **constant time**
  (`subtle`), and instantly revocable (`ledge-server::auth`).
- Cluster peer RPC authenticates via mutual TLS **and** a shared secret
  (defense-in-depth).
- **Residual:** tokens are cleartext unless TLS is enabled — terminate TLS in
  production (documented in `SECURITY.md`).

### F-3 — Multi-tenancy isolation · **High** · Mitigated (with a noted residual)
Enforced on **all three planes** — REST, git smart-HTTP, and RPC — via a single
ownership check: a foreign/unknown workspace returns 404 (no existence leak).
Durable refs are physically partitioned per tenant (`tenant_prefix`); SSH
connections act as their key's tenant and workspace access is lease-gated
(`ssh::resolve_segment`/`workspace_owned`).
- **Residual (R-4):** object/LFS confidentiality rests on ObjectId
  unguessability + ref-reachability, **not** per-object ACLs — an out-of-band
  ObjectId leak permits a cross-tenant read of *that* object.

### F-4 — SSRF via tenant-controlled webhook URLs · **Medium** · Mitigated
`ssrf::guard_outbound` resolves each target and **blocks non-public addresses**
(loopback, RFC-1918, link-local incl. cloud metadata `169.254.169.254`, CGNAT,
IPv6 ULA/link-local). The connection is **pinned to the validated IP**, closing
the DNS-rebinding window. On by default; `[webhooks].allow_private_targets`
opts out for single-tenant/dev. Address classification is unit-tested.

### F-5 — Memory safety / undefined behaviour · **High** · Mitigated
- Exactly **one hand-written `unsafe`** in the workspace: `unsafe impl Send/Sync
  for HLC` (`ledge-core/src/hlc.rs`), with a written soundness justification (the
  type is an atomic `u64`).
- `ledge-rpc` is `#![deny(unsafe_code)]`; the only other `unsafe` is Cap'n
  Proto **generated** code (reviewed-by-construction).
- No raw FFI, no manual memory management; CoW uses the `reflink-copy` crate (no
  hand-written syscalls).

### F-6 — Consensus correctness · **High** · Verified (model-checked) · single-host only
The replication core is **model-checked in TLA+** (`formal/`): `RefStore.tla`,
`CrossShardTxn.tla`, `DistributedGc.tla`, `GcReachability.tla`, `Sharding.tla`,
each with working negative controls. Runtime tests prove leader election,
replication, linearizable CAS through Raft, **leader-failover with no
committed-data loss**, and snapshot-install convergence.
- **Residual (R-1):** all of this ran on a **single host** (real processes +
  container network + *emulated* WAN/clock-skew). It is **not** validated on real
  separate machines. This is the single biggest gap for multi-node production.

### F-7 — Integrity & cryptography · **Medium** · Mitigated
- Objects are content-addressed (`ObjectId = blake3(content)`); a wrong-content
  object cannot masquerade as another.
- LFS uploads are **SHA-256-verified on write** (`lfs::LfsStore::put`) — a corrupt
  object is never stored.
- Stored deltas **self-verify** (`apply_delta` round-trip + hash check) before
  commit; the native git pack is validated by `git verify-pack`.
- TLS/mTLS via rustls + aws-lc-rs; the SSH host key is a persisted Ed25519 key.

### F-8 — Denial of service / resource exhaustion · **Medium** · Mitigated
Beyond the F-1 bounds: HTTP requests carry a timeout layer; per-tenant **quotas**
(workspace count, durable bytes, object count) and a **request-rate limiter** are
available (`[quotas]`). Repack/GC are offline/bounded passes.

### F-9 — Panic surface · **Low** · Partially mitigated · Residual
~1,600 `unwrap`/`expect`/`panic!` call sites exist in non-test code. The
**attacker-reachable parsers are proptest-verified panic-free** (F-1), and the
bulk of the rest are internal invariants (e.g. ~73 `Mutex::lock().unwrap()`,
which only panic on poisoning). A panic in an async task aborts that request, not
the process. **Residual:** a systematic `unwrap → Result` sweep of the
request-handling paths is a hardening follow-on (R-5).

---

## 5. Residual risks / open items (ranked)

| ID | Sev | Item | What would close it |
|---|---|---|---|
| **R-1** | High | Cluster validated only under *emulated* WAN on one host; never on real separate machines. | Deploy across real instances (multi-AZ/region) and run the chaos/skew suite there. |
| **R-2** | High | No independent third-party security audit / pentest. | Engage an external auditor; this document is the starting threat model. |
| **R-3** | Medium | No multi-day soak; long-run memory behaviour unproven. | A multi-day production-shaped soak with RSS tracking. |
| **R-4** | Medium | Object/LFS confidentiality = unguessability + reachability, not per-object ACLs. | Per-object/owner ACLs if hosting mutually-distrusting tenants' *secrets*. |
| **R-5** | Low | Operational maturity: manual cluster bootstrap, no cert hot-rotation, no backup/restore runbook beyond S3, unpinned toolchain, broad `unwrap` surface. | Ops runbooks + a toolchain pin + an `unwrap` sweep. |

## 6. Verdict (scoped)

**Production-ready** for a **single-node, single-tenant or trusted-tenant
deployment that you operate**, with TLS + auth enabled: the storage engine is
content-addressed and integrity-checked, the ref store is formally verified, the
untrusted-input paths are bounded and fuzzed, memory safety is effectively total,
and a stock `git`/`git-lfs` client works over HTTP and SSH (incl. shallow/partial
clone and LFS). CI is green; there is exactly one (justified) `unsafe`.

**Not yet production-ready** for **hosting untrusted multi-tenant code as a
managed service, or for a multi-node cluster depended upon for durability**, until
**R-1** (real multi-host validation), **R-2** (independent audit), and **R-3**
(soak) are closed. These are environmental/process gaps, not code defects — they
cannot be discharged by writing more features.

## Appendix — reproduce the evidence

```sh
cargo test --workspace                         # full suite (green in CI)
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
make -C formal check                           # TLA+ model checking
bash soak/wan-chaos.sh                          # emulated-WAN + clock-skew chaos (16/0)
bash dogfood/transport-features.sh             # incremental/shallow/partial savings
# real-client transport + LFS e2e:
cargo test -p ledge-server --test git_ssh --test git_fetch_incremental --test git_lfs
```
