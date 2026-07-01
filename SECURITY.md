# Security Policy

## Status

A **first-party security & production-readiness review** (threat model, findings,
ranked residual risks, scoped verdict) is in [`AUDIT.md`](AUDIT.md). Ledge has
**not** had an *independent* external audit. Run it
behind your own trust boundary; do not expose an instance to untrusted tenants
on the public internet yet. See "Known limitations" below before deploying.

## Reporting a vulnerability

Please report security issues privately. **Do not open a public issue.**

- Email: **vanshverma.dev@gmail.com** with subject `SECURITY: <short summary>`.
- Include: affected version/commit, a description, and a reproduction if possible.
- You'll get an acknowledgement within 72 hours and a remediation timeline after triage.

Please give a reasonable disclosure window before publishing. Credit is given to
reporters who want it.

## Supported versions

Pre-1.0: only the latest `main` is supported. Fixes land on `main`.

## Security model (what Ledge does today)

- **Authentication:** opaque API keys, hashed at rest (BLAKE3), constant-time
  compare, instantly revocable. Default-off; enable `[auth]`.
- **Transport:** optional TLS, and mutual TLS for node-to-node cluster traffic
  (`[tls]`). **Tokens are cleartext unless TLS is enabled** — terminate TLS.
- **Tenant isolation:** per-tenant ref namespaces; workspace access is gated by
  an ownership check; a foreign/unknown workspace returns 404 (no existence leak).
- **Quotas:** per-tenant workspace-count, request-rate, durable-bytes, and
  object-count limits (`[quotas]`, default-off).

## Untrusted-input hardening

The receive-pack path ingests attacker-controlled packfiles. The pack decoder
bounds each object's decompression to its header-declared size (capped at 1 GiB)
and verifies the inflated length, so a **zlib decompression bomb cannot exhaust
memory**; the read-path inflate is likewise bounded. The pack header parser, the
pack-length probe, the `.lidx` parser, and the delta applier are **property-tested
(proptest) to never panic, hang, or over-allocate on arbitrary bytes**, and the
delta applier has a 2 GiB output guard.

## Known limitations (read before deploying)

These are honest, documented gaps — not undisclosed bugs:

- **No external audit.** The isolation and crypto choices are unaudited.
- **Git object confidentiality is enforced by reachability, not per-object ACLs.**
  Objects are content-addressed and deduplicated across tenants, but the
  upload-pack path now **authorizes every `want`**: a client may only receive an
  object that is an advertised tip of its segment or reachable from those tips
  (over HTTP *and* SSH). A `want` for a leaked/guessed id outside the caller's
  segment is refused with `ERR ... not our ref` — the same response as a
  nonexistent object, so there is no existence oracle. Isolation therefore no
  longer rests on ObjectId unguessability for git reads.
  - **Remaining gap (LFS):** the Git-LFS download path is not yet segment-scoped —
    an authenticated caller who knows an LFS oid can fetch it regardless of tenant.
    Namespacing LFS by segment is the open follow-on; until then, treat LFS
    objects as shared across tenants of one instance.
- **`root` is a superuser namespace.** Do not issue root-tenant keys to
  untrusted clients.
- **Webhook SSRF is guarded by default.** Webhook URLs are tenant-controlled, so
  the dispatcher resolves each target and **blocks non-public destinations**
  (loopback, RFC-1918 private, link-local incl. cloud metadata `169.254.169.254`,
  CGNAT, IPv6 ULA/link-local) unless `[webhooks].allow_private_targets=true` (dev /
  single-tenant). The connection is **pinned to the validated IP**, so the check
  is not open to DNS rebinding (the request connects to the address that was
  vetted, not a re-resolved one).
- **Single-host testing only.** Cluster/replication has not run on real
  multi-host networks; treat multi-node as experimental.
