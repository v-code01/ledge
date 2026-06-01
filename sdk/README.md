# Ledge SDKs

Native, typed clients for the Ledge control plane over **Cap'n Proto**, all
generated from one cross-language schema and talking to the Rust server's binary
`POST /rpc` endpoint. The named benefit is zero-copy deserialization — a
property of the Cap'n Proto message format — over the proven Axum HTTP
transport. (Phase 2b; see `docs/superpowers/specs/2026-06-01-ledge-phase2b-sdk-design.md`.)

## The contract: `schema/ledge.capnp`

A single schema file is the source of truth all languages bind to. A request is
one arm of the `Request` union; a response is one arm of the `Response` union:

| Method           | Request arm                          | Success response arm |
|------------------|--------------------------------------|----------------------|
| `writeObject`    | `gitType: u8`, `content: Data`       | `objectId: ObjectId` |
| `readObject`     | `id: ObjectId`                       | `objectContent: Data`|
| `fork`           | `sources: List(Text)`, `ttlSeconds`  | `workspace: WorkspaceInfo` |
| `commit`         | `workspaceId`, `mappings`            | `commitOutcomes: List(CommitOutcome)` |
| `renew`          | `workspaceId`, `ttlSeconds`          | `lease: Lease` |
| `release`        | `workspaceId`                        | `ok: Void` |
| `getWorkspace`   | `workspaceId`                        | `workspace: WorkspaceInfo` |
| `listWorkspaces` | `Void`                               | `workspaceList: List(WorkspaceInfo)` |
| `runGc`          | `Void`                               | `gcStats: GcStats` |

Any operation can instead return the `error: Text` arm (a human-readable
business error, e.g. unknown workspace or missing object). Every SDK raises on
that arm.

### Wire format (all languages)

The Rust server uses `capnp::serialize` — standard **unpacked, framed** capnp
messages (not packed). Every client matches this exactly on both encode and
decode. Bodies are POSTed with Content-Type `application/x-ledge-capnp`.

The schema is the same across languages, so a message built by any client
decodes byte-for-byte on the server and vice versa. The TS/Python copies use the
canonical schema directly; the Go copy (`go/ledge_go.capnp`) adds go-capnp
package annotations but keeps the identical layout and file id
(`@0xbd4b7bd278003348`).

## Tier status

Set honestly from what actually ran in this environment (capnp 1.4.0, node 24,
python 3.13.5, go 1.24.1):

| Tier | Language   | Status            | Verification |
|------|------------|-------------------|--------------|
| 1    | Rust core  | **fully tested**  | `cargo test -p ledge-rpc -p ledge-server` — per-variant dispatch round-trips + error paths |
| 2    | TypeScript | **fully tested (e2e)** | `npm test` (vitest) — full agent flow vs the live server, real `capnp-es` codegen |
| 3    | Python     | **smoke-tested**  | `pytest` / `python -m ledge_sdk.smoke` vs the live server, dynamic `pycapnp` schema load |
| 4    | Go         | **smoke-tested**  | `go test ./...` vs the live server, real `capnpc-go` generated bindings |

All four tiers run against the same Rust server binary. The Go capnp toolchain
installed and codegen succeeded here, so Tier 4 ships generated bindings (not a
documented-only skeleton); the regeneration steps are recorded in
[`go/README.md`](./go/README.md) for reproducibility.

## Per-language usage

### TypeScript ([`ts/`](./ts/README.md))

```ts
import { LedgeClient } from "./ts/src/client.ts";
const client = new LedgeClient("http://127.0.0.1:8080");
const id = await client.writeObject(3, new TextEncoder().encode("hello"));
const content = await client.readObject(id); // Uint8Array "hello"
```

### Python ([`python/`](./python/README.md))

```python
from ledge_sdk import LedgeClient
client = LedgeClient("http://127.0.0.1:8080")
obj_id = client.write_object(3, b"hello")
assert client.read_object(obj_id) == b"hello"
```

```bash
cd python && python3 -m venv .venv && .venv/bin/pip install -e '.[test]'
.venv/bin/python -m pytest -q
```

### Go ([`go/`](./go/README.md))

```go
import ledge "github.com/vanshverma/ledge/sdk/go"
client := ledge.NewClient("http://127.0.0.1:8080")
id, _ := client.WriteObject(3, []byte("hello"))
content, _ := client.ReadObject(id) // []byte "hello"
```

```bash
cd go && go test ./...
```

## Running every tier's tests

Each suite builds the Rust `ledge` binary (cargo, incremental), spawns it on an
ephemeral port over a tmp data dir, polls `/healthz` until ready, drives the
flow, then tears it down.

```bash
cargo test -p ledge-rpc -p ledge-server   # Tier 1 (Rust)
cd sdk/ts     && npm test                  # Tier 2 (TypeScript)
cd sdk/python && .venv/bin/python -m pytest -q   # Tier 3 (Python)
cd sdk/go     && go test ./...             # Tier 4 (Go)
```
