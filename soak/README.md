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

## Emulated WAN + clock skew (`soak/wan-chaos.sh`)

`bash soak/wan-chaos.sh` extends the clean-partition suite above with the two
conditions 4f deferred: **degraded networks** and **clock skew**. It runs the
3-node cluster on the `ledge:soak` image (libfaketime + iproute2) where node-2's
clock is **+5s** and node-3's is **−7s** (skewing `CLOCK_REALTIME`/HLC only —
`CLOCK_MONOTONIC`, and thus the Raft/tokio timers, stays real), and uses
`tc netem` to inject latency, jitter, loss, reordering, and a one-way partition.

Latest run — **16 PASS / 0 FAIL**
([`results/2026-06-14-wan-chaos-skew.txt`](results/2026-06-14-wan-chaos-skew.txt)):

1. **Clock skew is real + baseline holds** — asserts the nodes actually run on
   wall clocks that differ by seconds, and membership still commits+applies on all 3.
2. **Data durability under skew + 80ms latency** — a real `git push` (2 commits)
   replicates and clones back **byte-identical**, `commit_index` advances, and all
   3 replicas converge — while the clocks disagree.
3. **Latency + jitter** (120ms ±40ms) — leader does not churn, no commit regression.
4. **Packet loss** (12%) — cluster holds exactly one leader, recovers, no regression.
5. **Reordering** (30%) — stays consistent (single leader).
6. **Asymmetric partition** (leader egress 100% loss) — the majority elects a *new*
   leader, the reachable majority agrees on one leader (no split-brain), heals and
   reconverges, and the committed log never regresses.

## Longevity / memory soak (`soak/longevity.sh`) — R-3

Drives one Ledge node under steady, production-shaped churn — create a short-TTL
workspace, clone, commit, push, drop it; let the expiry sweeper + GC reclaim it —
while sampling process RSS, then reports the steady-state trend (warmup skipped).

```sh
bash soak/longevity.sh                              # bounded default (600s)
SOAK_SECONDS=$((3*24*3600)) bash soak/longevity.sh  # R-3 proper: multi-day
```

**What a BOUNDED run can and cannot tell you.** Per-workspace *in-memory* state is
freed on reclaim — the lease map entry is removed on tombstone
(`ledge-workspace/src/lease.rs`), and workspace refs are deleted from the ART on
release (`manager.rs`), which shrinks copy-on-write. But the lease/ref **WALs are
append-only and only compact at 64 MiB**. A run that never reaches that threshold
(e.g. the ~2,675-workspace / 6-min baseline in `results/2026-07-02-longevity.txt`,
which grew ~12% then would keep climbing) captures the **pre-compaction rising
edge** — that growth is expected and is *not* a leak. So the bounded run is a
harness + baseline with a runaway safety-net (fails only on >50% growth); it
cannot by itself prove no-leak.

**R-3 proper** is the same script run for days: it crosses the 64 MiB compaction
threshold repeatedly, so a healthy process shows a **sawtooth** (grow → compact →
grow), not monotonic growth. Pair it with a heap profiler (RSS ≠ live heap;
allocator retention/fragmentation inflates RSS without a leak) on a real host.

## Honest limitations (the single-host ceiling)

- **One physical host.** Real processes, real container network, real clean
  partitions — but NOT real multi-machine: no NIC/hardware faults, no geographic
  latency, no genuine inter-host clock skew (the clock is one host's clock). 4f
  closes the *in-process* gap, not the *geo* gap; a true geo-distributed soak needs
  real hosts (cloud/lab).
- ~~Clean partitions only~~ **Addressed by `wan-chaos.sh`:** latency, jitter, loss,
  reordering, and an asymmetric (one-way) partition via `tc netem`. Flapping is the
  remaining follow-on.
- ~~No clock skew~~ **Addressed by `wan-chaos.sh`:** per-node CLOCK_REALTIME offsets
  via libfaketime (+5s/−7s). Caveat: it's process-level faked time on one host, not
  two machines' independent hardware clocks.
- ~~Consensus-layer only~~ **Addressed by `wan-chaos.sh`:** scenario 2 drives a real
  `git push` through Raft under skew+latency and asserts a byte-identical clone-back
  plus replica convergence — a genuine per-key data-durability probe in the chaos loop.
- **Bounded soak** (minutes), **crash-fault model only** (no byzantine / disk
  corruption), **local/manual** (not in CI — docker-network chaos needs privileged
  runners).
