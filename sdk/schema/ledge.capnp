@0xbd4b7bd278003348;

# Ledge wire schema — the cross-language Cap'n Proto contract for POST /rpc.
# Phase 2b. Generated file id via `capnp id`. This single schema is the source
# of truth all SDK languages (Rust, TypeScript, Python, Go) generate from.

# A 32-byte BLAKE3 content address.
struct ObjectId {
  bytes @0 :Data;
}

# A single versioned ref state (mirrors ledge_core::RefEntry).
struct RefEntry {
  target  @0 :ObjectId;
  hlc     @1 :UInt64;
  version @2 :UInt64;
}

# A named ref carried by a workspace view, client-facing name (refs/heads/...).
struct NamedRef {
  name  @0 :Text;
  entry @1 :RefEntry;
}

# A point-in-time view of a workspace: its id, lease expiry, and its refs.
struct WorkspaceInfo {
  id          @0 :Text;
  expiresAtMs @1 :UInt64;
  refs        @2 :List(NamedRef);
}

# A workspace lease (returned by renew).
struct Lease {
  id          @0 :Text;
  expiresAtMs @1 :UInt64;
  createdAtMs @2 :UInt64;
  generation  @3 :UInt64;
}

# Per-pass GC accounting (mirrors ledge_workspace::GcStats).
struct GcStats {
  scanned    @0 :UInt64;
  reachable  @1 :UInt64;
  reclaimed  @2 :UInt64;
  bytesFreed @3 :UInt64;
}

# One (workspace ref -> durable ref) promotion request for commit.
struct CommitMapping {
  workspaceRef @0 :Text;
  durableRef   @1 :Text;
}

# The result of promoting one workspace ref to a durable ref during commit.
struct CommitOutcome {
  target  @0 :Text;
  ok      @1 :Bool;
  version @2 :UInt64;
}

# RPC envelope — a request is exactly one of these operations.
struct Request {
  union {
    writeObject :group {
      gitType @0 :UInt8;
      content @1 :Data;
    }
    readObject :group {
      id @2 :ObjectId;
    }
    fork :group {
      sources    @3 :List(Text);
      ttlSeconds @4 :UInt64;
    }
    commit :group {
      workspaceId @5 :Text;
      mappings    @6 :List(CommitMapping);
    }
    renew :group {
      workspaceId @7 :Text;
      ttlSeconds  @8 :UInt64;
    }
    release :group {
      workspaceId @9 :Text;
    }
    getWorkspace :group {
      workspaceId @10 :Text;
    }
    listWorkspaces @11 :Void;
    runGc          @12 :Void;
  }
}

# RPC envelope — a response is exactly one of these results.
struct Response {
  union {
    error          @0 :Text;                 # human-readable business error
    objectId       @1 :ObjectId;             # writeObject result
    objectContent  @2 :Data;                 # readObject result
    workspace      @3 :WorkspaceInfo;        # fork / getWorkspace
    commitOutcomes @4 :List(CommitOutcome);  # commit result
    lease          @5 :Lease;                # renew result
    ok             @6 :Void;                 # release result
    workspaceList  @7 :List(WorkspaceInfo);  # listWorkspaces result
    gcStats        @8 :GcStats;              # runGc result
  }
}
