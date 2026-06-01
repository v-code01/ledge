# Ledge TypeScript SDK

The reference client for the Ledge control plane, talking to the Rust server's
binary `POST /rpc` endpoint over Cap'n Proto (Phase 2b, Tier 2). One typed
`LedgeClient` mirrors the `Request`/`Response` union from the shared schema
`sdk/schema/ledge.capnp` — the same contract the Rust server is generated from.

## Codegen approach: real Cap'n Proto codegen (`capnp-es`)

Bindings are generated, not hand-written. The toolchain is
[`capnp-es`](https://www.npmjs.com/package/capnp-es) (modern, ESM, zero-copy
reader/builder runtime + a `capnpc-*` plugin chain). It installs cleanly on
node 24 + npm 11 and drives the system `capnp` compiler (1.4.0) under the hood.

`npm run gen` regenerates `src/generated/ledge.ts` from `../schema/ledge.capnp`.
The hand-written codec fallback described in the design spec was **not** needed —
real codegen worked on the first probe.

### Wire compatibility with the Rust server

The Rust server encodes/decodes with `capnp::serialize` — standard **unpacked**,
framed messages (not packed). This SDK matches that exactly:

- **encode**: `Message.toArrayBuffer()` emits unpacked framed segments.
- **decode**: `new Message(buffer, /* packed */ false)` reads them.

Bodies are POSTed with Content-Type `application/x-ledge-capnp`.

## API

```ts
import { LedgeClient } from "@ledge/sdk";

const client = new LedgeClient("http://127.0.0.1:8080");

const id = await client.writeObject(3, new TextEncoder().encode("blob"));
const bytes = await client.readObject(id); // identical bytes back

const ws = await client.fork([], 120);          // empty sources allowed
await client.renew(ws.id, 300);
await client.commit(ws.id, [{ workspaceRef, durableRef }]);
const all = await client.listWorkspaces();
const stats = await client.runGc();
await client.release(ws.id);
```

A server-side business error (missing object, unknown workspace, commit conflict)
is surfaced as a thrown `LedgeRpcError`. A non-2xx HTTP status (e.g. a malformed
body -> 400) throws `LedgeTransportError`. `bigint` is used for all `UInt64`
fields (timestamps, versions, generations, GC counts) to avoid precision loss.

## Build / test

```sh
npm install        # installs capnp-es + vitest + typescript
npm run gen        # (re)generate src/generated/ledge.ts from the schema
npm run typecheck  # tsc --noEmit, strict
npm test           # vitest: end-to-end against the live Rust server
```

### What `npm test` does

It is a true end-to-end suite — no mocks. `test/server.ts`:

1. `cargo build --bin ledge` from the repo root (once; incremental).
2. Spawns `target/debug/ledge start --addr 127.0.0.1:<port> --data-dir <tmp>` on
   a random ephemeral port (retries on a bind/startup race), each over a fresh
   temp data dir.
3. Polls `GET /healthz` until ready, then runs the suite, then `SIGKILL`s the
   process and removes the temp dir.

`test/e2e.test.ts` drives the real server through the `LedgeClient` over HTTP +
capnp. Coverage (8 tests, all green):

- `writeObject` → `readObject` round-trips identical bytes.
- `writeObject` is content-addressed (same content → same 32-byte id).
- `readObject` on a missing id → `LedgeRpcError`.
- `listWorkspaces` empty on a fresh server.
- `runGc` returns well-formed stats (`reachable <= scanned`).
- `fork([])` → `getWorkspace` → `renew` (generation bump) → `release` full
  lifecycle, asserting list membership before/after.
- `commit` with no mappings → empty outcome list.
- `getWorkspace` on an unknown id → `LedgeRpcError`.

`fork` is tested with empty sources because the server allows it (a workspace
with no seeded refs), so the full workspace lifecycle is exercised end-to-end
without needing a pre-seeded durable ref (which only the Git push path creates).
