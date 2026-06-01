//! Ledge TypeScript reference SDK (Phase 2b, Tier 2).
//
// A `LedgeClient` wraps `fetch` to the Rust control plane's binary
// `POST /rpc` endpoint. Each method encodes a Cap'n Proto `Request` (the same
// `sdk/schema/ledge.capnp` contract the server is generated from), POSTs it as
// a standard framed (unpacked) capnp message with Content-Type
// `application/x-ledge-capnp`, then decodes the framed capnp `Response`.
//
// Wire format note: the Rust server uses `capnp::serialize` (unpacked framed
// segments). capnp-es `Message.toArrayBuffer()` emits exactly that framing, and
// `new Message(buffer, /* packed */ false)` reads it — no packing on either side.

import { Message, type Data } from "capnp-es";
import {
  Request,
  Response,
  Response_Which,
  type WorkspaceInfo as WireWorkspaceInfo,
  type GcStats as WireGcStats,
  type CommitOutcome as WireCommitOutcome,
} from "./generated/ledge.ts";

/** Wire content type for capnp `/rpc` request and response bodies. */
const CONTENT_TYPE = "application/x-ledge-capnp";

/** A single versioned ref state (decoded from the capnp `RefEntry`). */
export interface RefEntry {
  /** 32-byte BLAKE3 content address the ref points at. */
  target: Uint8Array;
  /** Hybrid logical clock stamp of the write. */
  hlc: bigint;
  /** Monotonic per-ref version for optimistic concurrency. */
  version: bigint;
}

/** A named ref carried by a workspace view (e.g. `refs/heads/main`). */
export interface NamedRef {
  name: string;
  entry: RefEntry;
}

/** A point-in-time view of a workspace returned by `fork` / `getWorkspace`. */
export interface WorkspaceInfo {
  id: string;
  expiresAtMs: bigint;
  refs: NamedRef[];
}

/** A workspace lease returned by `renew`. */
export interface Lease {
  id: string;
  expiresAtMs: bigint;
  createdAtMs: bigint;
  generation: bigint;
}

/** Per-pass garbage-collection accounting returned by `runGc`. */
export interface GcStats {
  scanned: bigint;
  reachable: bigint;
  reclaimed: bigint;
  bytesFreed: bigint;
}

/** One (workspace ref -> durable ref) promotion request for `commit`. */
export interface CommitMapping {
  workspaceRef: string;
  durableRef: string;
}

/** The result of promoting one workspace ref to a durable ref during `commit`. */
export interface CommitOutcome {
  target: string;
  /** `true` if the promotion landed; `false` on a concurrent-write conflict. */
  ok: boolean;
  /** On `ok`, the promoted version; on conflict, the live durable version. */
  version: bigint;
}

/** Thrown when the server encodes a business error in the `Response.error` variant. */
export class LedgeRpcError extends Error {
  override readonly name = "LedgeRpcError";
}

/** Thrown when the HTTP layer rejects the request (e.g. 400 malformed body). */
export class LedgeTransportError extends Error {
  override readonly name = "LedgeTransportError";
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

/**
 * A typed client for the Ledge control plane over Cap'n Proto `POST /rpc`.
 *
 * Construct with the server base URL (no trailing `/rpc`):
 * `new LedgeClient("http://127.0.0.1:8080")`.
 */
export class LedgeClient {
  private readonly rpcUrl: string;
  private readonly fetchImpl: typeof fetch;

  constructor(baseUrl: string, fetchImpl: typeof fetch = fetch) {
    // Normalize: strip a trailing slash so `${base}/rpc` is well-formed.
    const base = baseUrl.replace(/\/+$/, "");
    this.rpcUrl = `${base}/rpc`;
    this.fetchImpl = fetchImpl;
  }

  /** Store a git object (loose-object body, not yet wrapped). Returns its 32-byte id. */
  async writeObject(gitType: number, content: Uint8Array): Promise<Uint8Array> {
    const resp = await this.call((req) => {
      const w = req._initWriteObject();
      w.gitType = gitType;
      const data = w._initContent(content.length);
      data.copyBuffer(content);
    });
    this.expect(resp, Response_Which.OBJECT_ID);
    return copyData(resp.objectId.bytes);
  }

  /** Read an object's content by its 32-byte id. */
  async readObject(id: Uint8Array): Promise<Uint8Array> {
    requireLen(id, 32, "object id");
    const resp = await this.call((req) => {
      const r = req._initReadObject();
      const oid = r._initId();
      const data = oid._initBytes(id.length);
      data.copyBuffer(id);
    });
    this.expect(resp, Response_Which.OBJECT_CONTENT);
    return copyData(resp.objectContent);
  }

  /**
   * Fork a fresh workspace seeded from `sources` (durable ref names), leased for
   * `ttlSeconds` (0 = server default). Returns the new workspace view.
   */
  async fork(sources: string[], ttlSeconds: number | bigint): Promise<WorkspaceInfo> {
    const resp = await this.call((req) => {
      const f = req._initFork();
      const list = f._initSources(sources.length);
      sources.forEach((s, i) => list.set(i, s));
      f.ttlSeconds = BigInt(ttlSeconds);
    });
    this.expect(resp, Response_Which.WORKSPACE);
    return decodeWorkspace(resp.workspace);
  }

  /** Promote workspace refs to durable refs. One outcome per mapping, in order. */
  async commit(workspaceId: string, mappings: CommitMapping[]): Promise<CommitOutcome[]> {
    const resp = await this.call((req) => {
      const c = req._initCommit();
      c.workspaceId = workspaceId;
      const list = c._initMappings(mappings.length);
      mappings.forEach((m, i) => {
        const cm = list.get(i);
        cm.workspaceRef = m.workspaceRef;
        cm.durableRef = m.durableRef;
      });
    });
    this.expect(resp, Response_Which.COMMIT_OUTCOMES);
    return Array.from(resp.commitOutcomes, decodeCommitOutcome);
  }

  /** Extend a workspace's lease by `ttlSeconds` (0 = server default). */
  async renew(workspaceId: string, ttlSeconds: number | bigint): Promise<Lease> {
    const resp = await this.call((req) => {
      const r = req._initRenew();
      r.workspaceId = workspaceId;
      r.ttlSeconds = BigInt(ttlSeconds);
    });
    this.expect(resp, Response_Which.LEASE);
    const l = resp.lease;
    return {
      id: l.id,
      expiresAtMs: l.expiresAtMs,
      createdAtMs: l.createdAtMs,
      generation: l.generation,
    };
  }

  /** Release a workspace and its lease. */
  async release(workspaceId: string): Promise<void> {
    const resp = await this.call((req) => {
      const r = req._initRelease();
      r.workspaceId = workspaceId;
    });
    this.expect(resp, Response_Which.OK);
  }

  /** Fetch a point-in-time view of a workspace by id. */
  async getWorkspace(workspaceId: string): Promise<WorkspaceInfo> {
    const resp = await this.call((req) => {
      const g = req._initGetWorkspace();
      g.workspaceId = workspaceId;
    });
    this.expect(resp, Response_Which.WORKSPACE);
    return decodeWorkspace(resp.workspace);
  }

  /** List every live workspace. */
  async listWorkspaces(): Promise<WorkspaceInfo[]> {
    const resp = await this.call((req) => {
      req.listWorkspaces = true;
    });
    this.expect(resp, Response_Which.WORKSPACE_LIST);
    return Array.from(resp.workspaceList, decodeWorkspace);
  }

  /** Run one mark-and-sweep garbage-collection pass; returns its accounting. */
  async runGc(): Promise<GcStats> {
    const resp = await this.call((req) => {
      req.runGc = true;
    });
    this.expect(resp, Response_Which.GC_STATS);
    return decodeGcStats(resp.gcStats);
  }

  // ── transport ───────────────────────────────────────────────────────────

  /**
   * Encode a `Request` via `build`, POST it, and return the decoded `Response`.
   * Throws [`LedgeRpcError`] on the `error` variant and [`LedgeTransportError`]
   * on a non-2xx HTTP status.
   */
  private async call(build: (req: Request) => void): Promise<Response> {
    const msg = new Message();
    const req = msg.initRoot(Request);
    build(req);
    const body = msg.toArrayBuffer();

    const httpResp = await this.fetchImpl(this.rpcUrl, {
      method: "POST",
      headers: { "content-type": CONTENT_TYPE },
      body,
    });

    if (!httpResp.ok) {
      const text = await httpResp.text().catch(() => "");
      throw new LedgeTransportError(
        `POST ${this.rpcUrl} -> HTTP ${httpResp.status}${text ? `: ${text}` : ""}`,
        httpResp.status,
      );
    }

    const buf = await httpResp.arrayBuffer();
    // `false` => unpacked framing, matching the server's `capnp::serialize`.
    const respMsg = new Message(buf, false);
    const resp = respMsg.getRoot(Response);

    if (resp.which() === Response_Which.ERROR) {
      throw new LedgeRpcError(resp.error);
    }
    return resp;
  }

  /** Assert the response union tag, surfacing a server `error` if present. */
  private expect(resp: Response, want: Response_Which): void {
    const got = resp.which();
    if (got === want) return;
    if (got === Response_Which.ERROR) {
      throw new LedgeRpcError(resp.error);
    }
    throw new LedgeRpcError(
      `unexpected response variant ${got} (wanted ${want})`,
    );
  }
}

// ── decode helpers (capnp readers -> plain owned values) ────────────────────

/** Copy a capnp `Data` field into a standalone `Uint8Array`. `Data.toArrayBuffer`
 *  returns a fresh copy, so the result is decoupled from the message segment
 *  (whose buffer is reused on the next call). */
function copyData(data: Data): Uint8Array {
  return new Uint8Array(data.toArrayBuffer());
}

function decodeWorkspace(w: WireWorkspaceInfo): WorkspaceInfo {
  return {
    id: w.id,
    expiresAtMs: w.expiresAtMs,
    refs: Array.from(w.refs, (nr) => ({
      name: nr.name,
      entry: {
        target: copyData(nr.entry.target.bytes),
        hlc: nr.entry.hlc,
        version: nr.entry.version,
      },
    })),
  };
}

function decodeCommitOutcome(o: WireCommitOutcome): CommitOutcome {
  return { target: o.target, ok: o.ok, version: o.version };
}

function decodeGcStats(g: WireGcStats): GcStats {
  return {
    scanned: g.scanned,
    reachable: g.reachable,
    reclaimed: g.reclaimed,
    bytesFreed: g.bytesFreed,
  };
}

function requireLen(buf: Uint8Array, len: number, what: string): void {
  if (buf.length !== len) {
    throw new TypeError(`${what} must be ${len} bytes, got ${buf.length}`);
  }
}
