# Ledge Go SDK (Tier 4)

A typed client for the Ledge control plane over the binary Cap'n Proto
`POST /rpc` endpoint. Generated bindings + a `Client`, smoke-tested against the
live Rust server in this environment.

## Status: bindings generated and smoke-tested

The Go capnp toolchain installed and codegen succeeded here, so this SDK ships
with **real generated bindings** (`ledgepb/ledge_go.capnp.go`) and a passing
`go test` smoke suite — not a skeleton.

## Usage

```go
import ledge "github.com/v-code01/ledge/sdk/go"

client := ledge.NewClient("http://127.0.0.1:8080") // no trailing /rpc

id, _ := client.WriteObject(3, []byte("hello")) // git blob type = 3
content, _ := client.ReadObject(id)             // == "hello"

ws, _ := client.Fork(nil, 120)
_ = client.Release(ws.ID)

stats, _ := client.RunGc() // *GcStats
```

Methods mirror the `Request` union: `WriteObject`, `ReadObject`, `Fork`,
`Commit`, `Renew`, `Release`, `GetWorkspace`, `ListWorkspaces`, `RunGc`.
Server business errors are returned as `*RPCError`; non-2xx HTTP as
`*TransportError`.

## Wire format

The Rust server uses `capnp::serialize` (unpacked, framed segments). go-capnp
`Message.Marshal` emits exactly that framing and `capnp.Unmarshal` reads it —
no packing on either side.

## Regenerating bindings

The canonical schema [`sdk/schema/ledge.capnp`](../schema/ledge.capnp) is kept
language-neutral. This package generates from [`ledge_go.capnp`](./ledge_go.capnp),
a copy with identical struct/union layout and file id that adds the go-capnp
`$Go.package` / `$Go.import` annotations.

Toolchain (versions confirmed in this environment):

```bash
# 1. Cap'n Proto compiler (Homebrew: `brew install capnp`), capnp 1.4.0 here.
# 2. The Go plugin and runtime (go 1.24, go-capnp v3.1.0-alpha.2 here):
go install capnproto.org/go/capnp/v3/capnpc-go@latest

# 3. Locate the go-capnp std dir that contains the `/go.capnp` annotations file:
STD="$(go env GOPATH)/pkg/mod/capnproto.org/go/capnp/v3@v3.1.0-alpha.2/std"

# 4. Generate into ./ledgepb:
PATH="$PATH:$(go env GOPATH)/bin" \
  capnp compile -I "$STD" \
    -o "$(go env GOPATH)/bin/capnpc-go:ledgepb" \
    --src-prefix . ledge_go.capnp
```

## Test

```bash
go test ./...   # builds the Rust ledge binary, spawns it, drives the full flow
```
