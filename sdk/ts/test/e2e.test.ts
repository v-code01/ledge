// End-to-end tests: the real Rust `ledge` server, spawned as a child process,
// driven entirely through the TypeScript `LedgeClient` over capnp `POST /rpc`.
// Every assertion crosses the HTTP boundary and the capnp encode/decode path.

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { LedgeClient, LedgeRpcError } from "../src/client.ts";
import { buildServer, startServer, type RunningServer } from "./server.ts";

let server: RunningServer;
let client: LedgeClient;

beforeAll(async () => {
  buildServer();
  server = await startServer();
  client = new LedgeClient(server.baseUrl);
}, 180_000);

afterAll(async () => {
  await server?.stop();
});

describe("LedgeClient e2e against the live Rust server", () => {
  it("writeObject -> readObject round-trips identical bytes", async () => {
    // git blob type = 3; arbitrary payload.
    const content = new TextEncoder().encode("hello ledge over capnp/http");
    const id = await client.writeObject(3, content);

    expect(id).toBeInstanceOf(Uint8Array);
    expect(id.length).toBe(32);

    const got = await client.readObject(id);
    expect(got).toEqual(content);
  });

  it("writeObject is content-addressed: same content -> same id", async () => {
    const content = new TextEncoder().encode("deterministic payload");
    const a = await client.writeObject(3, content);
    const b = await client.writeObject(3, content);
    expect(b).toEqual(a);
  });

  it("readObject on a missing id surfaces a server error", async () => {
    const missing = new Uint8Array(32).fill(0xab);
    await expect(client.readObject(missing)).rejects.toBeInstanceOf(LedgeRpcError);
  });

  it("listWorkspaces is empty on a fresh server", async () => {
    const list = await client.listWorkspaces();
    expect(Array.isArray(list)).toBe(true);
    expect(list.length).toBe(0);
  });

  it("runGc returns well-formed stats", async () => {
    const stats = await client.runGc();
    expect(typeof stats.scanned).toBe("bigint");
    expect(typeof stats.reachable).toBe("bigint");
    expect(typeof stats.reclaimed).toBe("bigint");
    expect(typeof stats.bytesFreed).toBe("bigint");
    expect(stats.reachable).toBeLessThanOrEqual(stats.scanned);
  });

  it("fork (empty sources) -> getWorkspace -> renew -> release full lifecycle", async () => {
    // Empty sources is allowed: a workspace with no seeded refs is created.
    const ws = await client.fork([], 120);
    expect(ws.id).toMatch(/^[0-9a-f]+$/);
    expect(ws.refs).toEqual([]);
    expect(ws.expiresAtMs).toBeGreaterThan(0n);

    // It now shows up in the list.
    const list = await client.listWorkspaces();
    expect(list.map((w) => w.id)).toContain(ws.id);

    // getWorkspace returns the same id.
    const fetched = await client.getWorkspace(ws.id);
    expect(fetched.id).toBe(ws.id);

    // renew extends the lease and bumps generation.
    const lease = await client.renew(ws.id, 300);
    expect(lease.id).toBe(ws.id);
    expect(lease.generation).toBeGreaterThanOrEqual(2n);
    expect(lease.expiresAtMs).toBeGreaterThanOrEqual(ws.expiresAtMs);

    // release tears it down; it disappears from the list.
    await client.release(ws.id);
    const after = await client.listWorkspaces();
    expect(after.map((w) => w.id)).not.toContain(ws.id);
  });

  it("commit with no mappings returns an empty outcome list", async () => {
    const ws = await client.fork([], 60);
    const outcomes = await client.commit(ws.id, []);
    expect(outcomes).toEqual([]);
    await client.release(ws.id);
  });

  it("getWorkspace on an unknown id surfaces a server error", async () => {
    // 32-byte hex that is structurally valid but not a live workspace.
    const fakeId = "ab".repeat(16);
    await expect(client.getWorkspace(fakeId)).rejects.toBeInstanceOf(LedgeRpcError);
  });
});
