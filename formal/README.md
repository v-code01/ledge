# Ledge — Model-Checked TLA+ Specifications

A runnable, **model-checked** TLA+ formalization of the Ledge ref store's
concurrency and the Phase 2a garbage-collector safety guard. These are not
merely *published* specs — `make check` runs TLC and reports **zero invariant
violations** on the configured finite instances, and each headline invariant
has been validated against a deliberately-broken model (the "negative
control") to prove it is not vacuously true.

```
formal/
├── RefStore.tla          # ref store concurrency (CAS retry loop) + safety invariants
├── RefStore.cfg          # TLC config: constants, INVARIANTs, state constraint
├── GcReachability.tla    # mark-and-sweep candidate-set guard + GC safety
├── GcReachability.cfg
├── Makefile              # `make check` runs TLC on both modules
└── README.md             # this file
```

## How to run

```sh
make check        # default target: TLC on both modules, fail on any violation
make sany         # syntax-check both modules
make clean        # remove TLC scratch state / trace files
```

Toolchain (override via env if needed):
- `JAVA ?= /opt/homebrew/opt/openjdk/bin/java` (openjdk 26)
- `TLA  ?= $(HOME)/.tla/tla2tools.jar` (TLC 2026.05.26)

---

## What is modeled (and what is abstracted away)

**Modeled — the actual concurrency mechanism:**
- The ref store as an abstract partial map `RefName → RefEntry`.
- The ArcSwap root as that map plus a generation token `gen`; a write is a
  compare-and-swap of the whole root. Pointer equality in the implementation
  (`ArcSwap::compare_and_swap`) = generation equality in the model.
- N concurrent writer threads, each cycling: read root (capture `gen`) →
  compute new map → attempt CAS → on failure (root changed) retry. This is the
  Phase 1 `RefStoreImpl::update`/`delete` loop verbatim at protocol altitude.
- A monotonic Hybrid Logical Clock as a strictly-increasing global counter;
  every committed `RefEntry` is stamped with a fresh, unique, strictly-greater
  `hlc`.
- Snapshots: capturing the current map as a frozen value.
- The GC mark-and-sweep candidate-set guard, interleaved arbitrarily with
  concurrent object writes and lease (root) lifecycle.

**Abstracted away (deliberately — irrelevant to safety, would explode state):**
- The ART node structure (Node4/16/48/256, prefix compression, COW path
  cloning). A *representation* of the map; safety is about the map's logical
  state and the CAS protocol, not the tree shape.
- WAL byte framing, disk I/O, the Git wire protocol.
- Object content / BLAKE3 hashing — objects are opaque ids related by an
  abstract reachability function `reach`.

This is the correct altitude: model the protocol that can race, abstract the
data structure that cannot violate safety on its own.

---

## Module `RefStore.tla`

**Instance** (`RefStore.cfg`): `RefNames = {r1, r2}`, `ObjectIds = {o1, o2}`,
`Writers = {w1, w2}`, `MaxVersion = 2`, `NONE`. The state constraint bounds
per-ref version ≤ `MaxVersion`, the global `hlc`, the `committed` log length,
and snapshot count, so TLC's reachable graph is finite and the exhaustive BFS
completes in ~36 s.

This instance fully exercises the CAS protocol: create-if-absent, conflicting
CAS (expected mismatch → `ConflictAbort`), generation races between the two
writers (`RetryCAS`), and the +1 version progression per ref. Larger object /
version bounds add only target-choice and log-length multiplicity, not new
protocol behaviours, while exploding the graph past the sub-minute budget.

### Invariants

| Invariant | One-line meaning |
|---|---|
| `TypeOK` | Every variable is well-typed (refs, hlc, gen, pc, local, committed, snapshots). |
| `MonotonicVersion` | The k-th commit of a ref (in commit order) has version exactly k — +1 per commit, never decreasing, create = 1. |
| `HLCMonotonic` | Committed hlcs are strictly increasing in commit order and pairwise unique; the global hlc only increases (the linearizability witness: a total order on writes consistent with real-time commit order). |
| `NoLostUpdate` | No two commits win against the same root generation (`NoTwoCommitsSameReadGen`); adjacent same-ref commits differ by exactly 1 version (no skipped/duplicated version); commit stamps are unique. |
| `SnapshotIsolation` | A captured snapshot is a frozen value — its reads never change regardless of intervening commits. |

### TLC output (clean run, `make check`)

```
Model checking completed. No error has been found.
4172879 states generated, 1138254 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 12.
```

- States generated: **4,172,879**
- Distinct states: **1,138,254**
- Search depth: **12**
- Invariant violations: **0**

---

## Module `GcReachability.tla`

Models the Phase 2a mark-and-sweep candidate-set guard.

**Instance** (`GcReachability.cfg`): `Objects = {o1, o2, o3, o4}`, reachability
chain `o1 → o2 → o3` plus isolated `o4` (`reach <- ReachDef`). Naturally finite
(state graph bounded by `2^|Objects|` × phases); the exhaustive check is
sub-second.

The GC interleaves arbitrarily with `WriteObject` (concurrent push/fork) and
`AddRoot`/`RemoveRoot` (lease lifecycle). Phases: `GcFreeze` snapshots the store
into `candidates` and roots into `frozenRoots`; `GcMark` computes the
reachability closure of `frozenRoots`; `GcSweep` deletes exactly
`candidates \ reachable`.

### Invariants

| Invariant | One-line meaning |
|---|---|
| `TypeOK` | Every variable is a subset of `Objects` / valid phase. |
| `GCSafety` | The sweep set (`candidates \ reachable`) is disjoint from the frozen-root reachable closure — no reachable object is ever swept. |
| `MarkCoversLiveClosure` | After marking, every candidate reachable from the frozen roots has been marked — the precondition that makes `GCSafety` hold. |
| `NoLiveRootDangling` | No object reachable from a root live at freeze time, and present at freeze, is ever a deletion target — a live ref never dangles because of GC. Objects written after the freeze are outside `candidates`, so are never deleted either. |

### TLC output (clean run, `make check`)

```
Model checking completed. No error has been found.
158595 states generated, 29538 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 22.
```

- States generated: **158,595**
- Distinct states: **29,538**
- Search depth: **22**
- Invariant violations: **0**

---

## Negative control (proving the invariants have teeth)

TLA+ has no unit tests — the invariants *are* the tests, and TLC is the runner.
The TDD analogue is: introduce the known bug and confirm TLC produces the
expected counterexample, proving the invariant actually constrains the model
(is not vacuously true). This is the formal-methods equivalent of "watch the
test fail first."

### RefStore — headline invariant `NoLostUpdate`

**The break:** remove the CAS generation guard from `CommitCAS`:

```diff
 CommitCAS(w) ==
     ...
     IN  /\ pc[w] = "trying"
-        /\ gen = lv.readGen                 \* CAS guard: root unchanged
         /\ cur = lv.expected                \* precondition still holds
```

This lets a writer commit even after another writer has already advanced the
root generation since it read — exactly the lost-update race the lock-free CAS
loop exists to prevent.

> **First attempt — a real finding.** Removing *only* the `gen` guard while the
> `NoLostUpdate` invariant was stated purely over *version* progression did
> **not** produce a violation (TLC: "No error has been found", same 1,138,254
> distinct states). Reason: `CommitCAS` still derives `version = currentVersion
> + 1` from the live state and still enforces the `expected` precondition, so
> the version sequence stayed gap-free even with stale commits. The version-only
> invariant was partly redundant with the precondition and did **not** depend on
> the `gen` guard. That is precisely what a negative control is meant to expose.
>
> **The fix (model strengthening, not weakening):** record the winning
> `readGen` in each committed record and add `NoTwoCommitsSameReadGen` — no two
> commits may win against the same observed root generation. This clause depends
> directly on the `gen` guard, so it genuinely tests it. The corrected model
> still checks clean (4,172,879 states / 1,138,254 distinct), and the negative
> control now fires.

**Counterexample TLC produced** (`Error: Invariant NoLostUpdate is violated`,
exit 12, depth-5 trace):

```
State 3: <BeginUpdate(w2,r1,o1,NONE)>
  /\ refs = (r1 :> NONE @@ r2 :> NONE)
  /\ pc   = (w1 :> "trying" @@ w2 :> "trying")
  /\ gen  = 0
  /\ local = ( w1 :> [name|->r1, newTarget|->o1, expected|->o1,   readGen|->0]
            @@ w2 :> [name|->r1, newTarget|->o1, expected|->NONE, readGen|->0] )

State 4: <CommitCAS(w2)>                  \* w2 wins against readGen 0
  /\ refs = (r1 :> [target|->o1, hlc|->1, version|->1] @@ r2 :> NONE)
  /\ gen  = 1
  /\ committed = << [name|->r1, hlc|->1, readGen|->0, entry|->[..version|->1]] >>

State 5: <CommitCAS(w1)>                  \* w1 ALSO commits against stale readGen 0
  /\ refs = (r1 :> [target|->o1, hlc|->2, version|->2] @@ r2 :> NONE)
  /\ gen  = 2
  /\ committed = << [name|->r1, hlc|->1, readGen|->0, entry|->[..version|->1]],
                    [name|->r1, hlc|->2, readGen|->0, entry|->[..version|->2]] >>
                  \* two commits both carry readGen 0  ->  NoTwoCommitsSameReadGen FALSE
```

Both writers read the root at `gen = 0`; w2 commits (gen → 1); w1 then commits
against the now-stale `readGen = 0` because the guard is gone. Two commits carry
`readGen = 0` → a lost update. With the guard restored, every commit consumes a
distinct generation (0, 1, 2, …) and the violation is unreachable.

The broken model is **not** committed; restoring the one deleted line returns
the clean module in this directory.

### GcReachability — `GCSafety` (sanity negative control)

As an additional teeth-check, breaking `GcMark` to mark only the roots
themselves instead of their closure (`reachable' = frozenRoots`) makes a
reachable child (e.g. `o2`, reachable from rooted `o1`) fall into the sweep set.
TLC reports `Error: Invariant GCSafety is violated` (exit 12), confirming
`GCSafety` is non-vacuous. Restoring `reachable' = ReachableClosure(frozenRoots)`
returns the clean module.

---

## Out of scope

WAL crash-recovery modeling, the Git wire protocol, object content/hashing,
liveness/termination (safety only), and the ART node structure. Phase 3 will
extend `RefStore.tla` with the Raft replication layer.
