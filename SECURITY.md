# Security Policy

## Status

Ledge is early-stage and has **not** had an external security audit. Run it
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
- **Object confidentiality is reachability-based, not per-object ACLs.** Objects
  are content-addressed and deduplicated across tenants; isolation rests on
  ObjectId unguessability + ref reachability. An out-of-band ObjectId leak can
  permit a cross-tenant read of that object.
- **`root` is a superuser namespace.** Do not issue root-tenant keys to
  untrusted clients.
- **Webhook SSRF is guarded by default.** Webhook URLs are tenant-controlled, so
  the dispatcher resolves each target and **blocks non-public destinations**
  (loopback, RFC-1918 private, link-local incl. cloud metadata `169.254.169.254`,
  CGNAT, IPv6 ULA/link-local) unless `[webhooks].allow_private_targets=true` (dev /
  single-tenant). Residual: a resolve-then-connect check is open to DNS rebinding
  (a pinned-IP connector is the follow-on); literal private IPs and hostnames that
  resolve to them are blocked.
- **Single-host testing only.** Cluster/replication has not run on real
  multi-host networks; treat multi-node as experimental.
