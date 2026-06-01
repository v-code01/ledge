# Ledge Python SDK (Tier 3)

A typed client for the Ledge control plane over the binary Cap'n Proto
`POST /rpc` endpoint. The wire contract is the shared
[`sdk/schema/ledge.capnp`](../schema/ledge.capnp) schema, loaded dynamically by
[`pycapnp`](https://pypi.org/project/pycapnp/) — there is **no codegen step**.

## Install

```bash
python3 -m venv .venv
.venv/bin/pip install pycapnp        # runtime
.venv/bin/pip install pytest          # to run the smoke tests
# or, editable install with the test extra:
.venv/bin/pip install -e '.[test]'
```

## Usage

```python
from ledge_sdk import LedgeClient

client = LedgeClient("http://127.0.0.1:8080")  # no trailing /rpc

obj_id = client.write_object(3, b"hello")       # git blob type = 3
assert client.read_object(obj_id) == b"hello"

ws = client.fork([], ttl_seconds=120)
print(ws.id, ws.expires_at_ms)
client.release(ws.id)

print(client.run_gc())                           # GcStats(scanned=..., ...)
```

Methods mirror the `Request` union: `write_object`, `read_object`, `fork`,
`commit`, `renew`, `release`, `get_workspace`, `list_workspaces`, `run_gc`.
Server business errors raise `LedgeRpcError`; non-2xx HTTP raises
`LedgeTransportError`.

## Wire format

The Rust server uses `capnp::serialize` (unpacked, framed segments). pycapnp
`Message.to_bytes()` / `Schema.from_bytes()` use exactly that framing. The
packed variants (`to_bytes_packed` / `from_bytes_packed`) are intentionally
**not** used — they would not interoperate.

## Test

The smoke suite builds the Rust `ledge` binary, spawns it on an ephemeral port
over a tmp data dir, polls `/healthz`, and drives the full agent flow through
the real server:

```bash
.venv/bin/python -m pytest -q        # pytest suite (tests/test_smoke.py)
.venv/bin/python -m ledge_sdk.smoke  # standalone runner (no pytest required)
```

## Type checking

`pyrightconfig.json` points Pyright at the project `.venv` (so `capnp` resolves)
and adds `tests/` to the import path (so the in-tree `server` harness resolves).
Install the deps into `.venv` first (see [Install](#install)), then:

```bash
pyright .                            # 0 errors
```

Every field access on a pycapnp builder/reader is a *runtime* attribute on a
class generated from the dynamically loaded `ledge.capnp` schema, invisible to a
static checker. `client.py` carries a scoped `# pyright: reportAttributeAccessIssue=false`
pragma for exactly that category; all other diagnostics remain enabled.
