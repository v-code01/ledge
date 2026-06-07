# Soak + Chaos harness (Phase 4f)

`chaos.sh` stands up the real 3-node docker-compose cluster (separate processes,
separate container network namespaces, per-node durable volumes) and subjects it
to injected chaos — process crash, restart, and **real network partitions** via
`docker network disconnect` — asserting the consensus-layer invariants over the
wire. It closes the "in-process test harness" caveat that 4b/4c/4d carried, as far
as a single physical host allows.

## Run

```sh
# requires: a running docker daemon, jq, the ledge image built (docker build -t ledge:latest .)
bash soak/chaos.sh                 # full run (~3-4 min)
SOAK_SECS=120 bash soak/chaos.sh   # longer soak window
```
Every wait is a bounded poll on observable state (`/cluster/status`), never a
fixed sleep-as-synchronization. The harness exits non-zero if any assertion fails.
Latest captured run: [`results/2026-06-07.txt`](results/2026-06-07.txt) — **15 PASS, 0 FAIL**.

## Scenarios → invariants

| # | Chaos | Invariant proven | Protocol |
|---|-------|------------------|----------|
| 1 | none (baseline) | membership entry committed + applied on all 3 nodes | Raft replication over real sockets |
| 2 | `docker kill` the leader | a new leader is elected; `last_applied` does not regress | leader-failover, committed-log durability |
| 3 | kill + `docker start` a follower | quorum (2/3) keeps serving; the restarted node's `last_applied` reconverges **from disk** | WAL + snapshot crash-recovery |
| 4 | `network disconnect` one node | majority keeps a leader; the isolated node has **no leader** (no split-brain); heal → reconverge | linearizability under partition |
| 5 | isolate two of three | the lone node's `commit_index` **does not advance** (no committed progress without quorum); reconnect → recover, no corruption | safety over liveness (CAP: C) |
| soak | sustained host-port load | zero errors over the window; cluster healthy after load + GC | steady-state stability |

## Notes on the assertions (honest)

- **Scenario 5 asserts no *committed progress*, not `leader=null`.** openraft (without
  check-quorum) keeps the stale leader *label* on an isolated leader. That is **safe**:
  without a quorum it commits nothing, and the other (also-isolated) nodes cannot
  elect a competing leader, so there is no split-brain. The real safety property is
  "`commit_index` does not advance without a quorum" — which is what the harness checks.
  (Scenario 4's isolated node *does* show `leader=null` because there it was a
  follower → a vote-less candidate; the asymmetry is expected.)
- **Soak memory:** the captured run shows ledge-1 RSS growing during the create-heavy
  soak (e.g. ~4→112 MiB over ~630 workspace creates). This is consistent with
  **accumulated live workspaces** (TTL 60s — none expired during a 45s window) plus
  allocator retention, **not proven** to be a leak. A longer *create-then-idle*
  reclamation soak (create load → stop → wait past TTL + GC → confirm RSS drops) is
  a documented follow-on; the harness reports the delta rather than claiming
  leak-free.

## `reconfig.sh` — live shard-membership reconfiguration (Phase 4g)

`reconfig.sh` stands up a real **4-node** cluster (`docker-compose.reconfig.yml`;
node 4 is pre-provisioned for shard 0 but NOT an initial voter) and exercises live
replica add/remove via `POST /cluster/{shard}/reconfigure`:

```sh
bash soak/reconfig.sh   # ~2-3 min; captured run: results/reconfig-2026-06-07.txt (10 PASS / 0 FAIL)
```

| Step | Action | Invariant proven |
|------|--------|------------------|
| 0 | init shard 0 = {1,2,3} | leader elected, baseline voters = {1,2,3} |
| 1 GROW | reconfigure → {1,2,3,4} | node 4 becomes a **voter** AND **catches up from the Raft log** (`last_applied` converges) — live add, data-complete |
| 2 SHRINK | reconfigure → {1,2,3} | node 4 dropped; committed log does **not** regress; cluster still serves — live remove |

Reconfigure is driven by openraft `add_learner` (blocking catch-up) → `change_membership(ReplaceAllVoters)`.
**`num_shards` is unchanged** — only replica sets move, so no key is rehashed.

Notes (honest):
- **Reconfigure must be POSTed to the shard LEADER** (openraft membership change is
  leader-only; a follower returns 503). The harness reads the leader from
  `/cluster/status` and targets it.
- The harness does **grow + shrink** (deterministic — node 4 is never the leader).
  *Replacing a different node* is the same mechanism; removing the **current leader**
  triggers an openraft leadership transfer (handled by openraft, avoided here only
  to keep the test deterministic).
- **Keyspace resharding (changing `num_shards`) is NOT done** — the routing is
  modulo (`hash % num_shards`), so a count change rehashes everything; that needs a
  consistent-hash routing redesign (documented deferral, spec §6). 4g changes
  *placement*, not *shard count*.
- New voters obtain existing objects via on-demand anti-entropy pull; the route also
  rebuilds this node's object push-peer set (bearer-authed). TLS clusters re-derive
  full TLS object peers on restart (the route's rebuilt peers carry the bearer but
  not the per-node TLS client config — a v1 limitation).

## Honest limitations (the single-host ceiling)

- **One physical host.** Real processes, real container network, real clean
  partitions — but NOT real multi-machine: no NIC/hardware faults, no geographic
  latency, no genuine inter-host clock skew (the clock is one host's clock). 4f
  closes the *in-process* gap, not the *geo* gap; a true geo-distributed soak needs
  real hosts (cloud/lab).
- **Clean partitions only** (`disconnect`/`connect`). Lossy / asymmetric / flapping /
  latency degradation (`tc`/`pumba`, needs NET_ADMIN) is a follow-on; the clean cut
  already proves the safety-critical no-split-brain property.
- **Consensus-layer focus.** The harness asserts on replicated Raft state
  (leader / term / commit_index / last_applied). Per-key data-write durability under
  chaos remains covered by the (proven) in-process 2PC/Raft tests — wiring a
  git-push data probe into the chaos loop is a follow-on.
- **Bounded soak** (minutes), **crash-fault model only** (no byzantine / disk
  corruption), **local/manual** (not in CI — docker-network chaos needs privileged
  runners).
