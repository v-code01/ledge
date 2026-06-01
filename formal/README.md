# Ledge — Model-Checked TLA+ Specifications

A runnable, **model-checked** TLA+ formalization of the Ledge ref store's
concurrency, the Phase 2a garbage-collector safety guard, and the Phase 3
sharding layer (routing totality + apply determinism). These are not merely
*published* specs — `make check` runs TLC and reports **zero invariant
violations** on the configured finite instances, and each headline invariant
has been validated against a deliberately-broken model (a "negative control")
to prove it is not vacuously true — four are documented below
(`NoLostUpdate`, `SnapshotIsolation`, `GCSafety`, `RoutingTotality`).

```
formal/
├── RefStore.tla          # ref store concurrency (CAS retry loop) + safety invariants
├── RefStore.cfg          # TLC config: constants, INVARIANTs, state constraint
├── GcReachability.tla    # mark-and-sweep candidate-set guard + GC safety
├── GcReachability.cfg
├── Sharding.tla          # Phase 3 routing totality + apply determinism
├── Sharding.cfg          # TLC config (clean instance)
├── Sharding_neg.cfg      # negative-control config (BadRouting = TRUE; must fail)
├── Makefile              # `make check` runs TLC on all modules; `make neg` runs the negative control
└── README.md             # this file
```

## How to run

```sh
make check        # default target: TLC on all modules, fail on any violation
make sany         # syntax-check all modules
make neg          # run the Sharding negative control; PASS iff TLC reports a violation
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
  Both flows are modeled: `BeginUpdate`/`BeginDelete` capture the read
  generation; `CommitCAS` commits either a new entry (update) or `NONE`
  (delete) under the same gen-guard + precondition + HLC stamp.
- The precise abort taxonomy of `RefStoreImpl::update`/`delete`:
  `ConflictAbort` (ref present, wrong target → `LedgeError::Conflict`),
  `NotFoundAbort` (ref absent but a concrete target expected →
  `LedgeError::NotFound`), and `RetryCAS` (root generation advanced → retry).
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

**Instance** (`RefStore.cfg`): `RefNames = {r1}`, `ObjectIds = {o1, o2}`,
`Writers = {w1, w2}`, `MaxVersion = 2`, `NONE`. The state constraint bounds
per-ref version ≤ `MaxVersion`, the global `hlc`, the `committed` log length
(`MaxCommits = MaxVersion·|RefNames| + |RefNames|`, accounting for deletes that
let a ref be recreated), and snapshot count, so TLC's reachable graph is finite
and the exhaustive BFS completes in ~1 s.

**Why a single ref name.** The original instance used `RefNames = {r1, r2}`.
Adding the `Delete` flow (a ref can go absent then be recreated, restarting its
version at 1), the `NotFoundAbort` branch, and the richer snapshot record
(frozen map + independent live witnesses) expands the per-ref state graph
substantially; with two refs the exhaustive check exceeds the sub-minute budget
(>6.5M distinct states and still climbing past 5 minutes). Reduced to one ref
name, the check finishes in ~1 s. A single ref with two concurrent writers
still fully exercises *every* modeled behaviour:

- create-if-absent (`expected = NONE` on an absent ref);
- update CAS (`expected =` matching `ObjectId`), +1 version progression;
- conflicting CAS → `ConflictAbort` (present, wrong target → `Conflict`);
- absent ref + concrete `expected` → `NotFoundAbort` (the `NotFound` branch,
  distinct from `Conflict`);
- delete (`expected =` current target) → `refs[name] := NONE`;
- recreate after delete (version restarts at 1, clock keeps advancing);
- generation races between the two writers → `RetryCAS`;
- snapshot isolation across both update *and* delete commits.

A second ref name adds only independent-namespace multiplicity (CAS is on the
whole root, but per-ref safety is identical), not new protocol behaviour, while
multiplying the state graph past budget. Coverage statistics from a
`-coverage 60` run confirm every action — including `BeginDelete`, the delete
case of `CommitCAS`, and `NotFoundAbort` — fires with non-zero distinct-state
contribution (see "Delete coverage" below).

### Invariants

| Invariant | One-line meaning |
|---|---|
| `TypeOK` | Every variable is well-typed (refs, hlc, gen, pc, local, committed, snapshots), including the `op` tag (`update`/`delete`) on each commit and the snapshot's frozen-map + live-witness fields. |
| `MonotonicVersion` | Each *update* commit of a ref has version equal to its 1-based position counted **since the most recent delete** of that ref (or since the start) — +1 per update, and a delete **resets** the count so a recreate starts at version 1 again. |
| `HLCMonotonic` | Committed hlcs are strictly increasing in commit order and pairwise unique (deletes stamp the clock too); the global hlc only increases (the linearizability witness: a total order on writes consistent with real-time commit order). |
| `NoLostUpdate` | No two commits win against the same root generation (`NoTwoCommitsSameReadGen`); adjacent same-ref **update** commits with no commit of that ref between them differ by exactly 1 version (no skipped/duplicated version); commit stamps are unique. |
| `SnapshotIsolation` | A captured snapshot is a **frozen value**: reading its frozen map always yields exactly what was observed live at capture time (recorded in independent `liveTargets`/`liveVersions` witnesses), regardless of any later commit — update *or* delete — that moves live `refs` on. Falsifiable; see negative control below. |

> **On `SnapshotIsolation` (was vacuous, now has teeth).** The earlier
> formulation asserted `SnapshotGet(s, name) = s.map[name]`, where `SnapshotGet`
> was *defined* as `s.map[name]` — a tautology testing nothing. The current
> invariant captures, atomically with the frozen map, an independent witness of
> each ref's live target/version at snapshot time (`liveTargets`/`liveVersions`,
> computed via the `TargetOf`/`VersionOf` paths, not the raw map copy) and
> asserts the frozen map *still reads that witness* at every later state. This
> can only hold if the snapshot is a frozen value rather than a live re-read —
> proven by the `BrokenSnapshot` negative control below.

### TLC output (clean run, `make check`)

```
Model checking completed. No error has been found.
121949 states generated, 30888 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 10.
```

- States generated: **121,949**
- Distinct states: **30,888**
- Search depth: **10**
- Invariant violations: **0**
- Wall time: **~1 s**

### Delete coverage (confirmation the delete commit fires)

A `-coverage 60` run reports non-zero distinct-state contribution for every
action, including the delete path: `BeginDelete` (6,112 distinct states),
`CommitCAS` (which folds both update and delete commits), and `NotFoundAbort`
(10,536 transitions). To prove the **delete commit itself** fires (not merely
`BeginDelete`), a temporary probe invariant `committed[i].op # "delete"` was
checked and TLC produced the expected counterexample — a create at version 1
followed by a `delete` commit recording `target |-> NONE, version |-> 0`:

```
State 4: <CommitCAS(w1)>   \* the DELETE commit
  committed = << [name|->r1, op|->"update", entry|->[target|->o1, hlc|->1, version|->1]],
                 [name|->r1, op|->"delete", entry|->[target|->NONE, hlc|->2, version|->0]] >>
```

A second probe (`recreate after delete must not produce version 1`) likewise
fired, exhibiting create → delete → recreate with the version **reset to 1**
(`hlc |-> 3, version |-> 1`), confirming `MonotonicVersion`'s delete-reset
clause is exercised and remains non-vacuous. Both probes were removed after
confirmation; the committed module contains no probe invariants.

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

## Module `Sharding.tla`

Models the **Phase 3 Ledge-specific** additions around the Raft consensus
core. **Raft's own safety is inherited, not re-derived here.** Election
safety, log matching, leader completeness, and state-machine safety come from
openraft, whose protocol is the Ongaro/Diego Raft TLA+ lineage (the canonical
`raft.tla`). This module deliberately does **not** re-model consensus — it
verifies only the two properties that sit *above* (routing) and *below*
(deterministic apply) the consensus core, assuming the committed-log
abstraction Raft provides (a single agreed total order of entries per shard).

**Instance** (`Sharding.cfg`): `Refs = {r1, r2, r3}`, `NumShards = 2`,
`Objects = {o1, o2}`, `NONE` (a model value), `BadRouting = FALSE`,
`Hash <- HashDef` (`r1→4, r2→7, r3→9`, so `shard_for` spreads `r1→0, r2→1,
r3→1`), and `OpLog <- OpLogDef` (a 4-op committed shard log: create → update →
stale-conflict → delete). The operational determinism model has a tiny state
graph (two `applied` counters in `0..4` plus the derived per-replica state);
the exhaustive check is sub-second.

### What is modeled

- **Routing (static).** `shard_for(r) == Hash[r] % NumShards` — the
  `ShardRouter::shard_for` at protocol altitude (the concrete hash is BLAKE3;
  here `Hash` is an abstract total `CONSTANT` function `Refs → Nat`, the only
  property routing safety depends on). `ROUTES(r)` is the *set* of shards a
  ref maps to; for a correct total function it is always a singleton.
- **Deterministic apply.** A shard state machine is a ref-map
  `Refs → (entry ∪ {NONE})`; `Apply(state, op) == ⟨state', resp⟩` is a **pure
  function** of `(state, op)` — the explicit-hlc apply path
  (`RefStoreImpl::apply_op`). Ops carry their `hlc` explicitly (leader-stamped
  before replication, per the §4 data flow), so apply never reads a clock or
  any replica-local nondeterministic source. TLA+ functions are deterministic
  by construction, so making `Apply` a function *is* the determinism
  guarantee; the operational invariant proves two replicas converge under it.
- **Operational confluence.** Two replicas (`sm1`/`resps1`, `sm2`/`resps2`)
  consume the **same** committed log `OpLog` in index order; `applied1` /
  `applied2` track each replica's progress. A nondeterministic scheduler
  (`Step1` / `Step2`) advances either replica by one entry at a time, so TLC
  explores **every interleaving**. Each replica always applies
  `OpLog[applied+1]` next — the same ordered prefix, possibly at different
  speeds.

### Invariants

| Invariant | One-line meaning |
|---|---|
| `TypeOK` | Every variable is well-typed: both replica states are total ref-maps with NONE-or-`[target,hlc,version]` entries (checked structurally, since `Entry`'s `hlc: Nat` is infinite and not enumerable); `applied` counters within log bounds; responses drawn from `{Updated, Deleted, Conflict}`. |
| `RoutingTotality` | `shard_for` is a total function into the shard index space, and **each ref maps to exactly one shard** (`Cardinality(ROUTES(r)) = 1`). Falsifiable; see negative control. |
| `Partition` | The per-shard owned sets **partition** the ref-name space: their union is all of `Refs` (none orphaned) and they are pairwise disjoint (none double-owned). |
| `DeterminismInv` | **Apply determinism (operational).** Whenever the two replicas have applied the same committed-prefix length (`applied1 = applied2`), their state machines **and** response sequences are identical — confluence over every scheduler interleaving TLC explores. This is the property §4 ("followers apply the same entry → identical state") depends on. |
| `PrefixFunctional` | Each replica's `(state, resps)` equals folding `Apply` over `OpLog[1..applied]` from `InitState` — the applied state is a pure **function of the first `applied` entries only**, pinning each replica to the canonical fold so determinism is structural, not a scheduling coincidence. |

> **On `RoutingTotality` / `Partition` being constant-level.** Both are
> functions of the `CONSTANT`s only (no spec variables), so TLC prints an
> informational warning ("constant-level formula … evaluates to TRUE") and
> checks them once at initialization. They are kept as `INVARIANT`s (rather
> than `ASSUME`s) so the negative control fires as a normal invariant
> violation and `make check` exercises them on every run. The warning is
> benign; `make check` exits `0`.

### TLC output (clean run, `make check`)

```
Model checking completed. No error has been found.
42 states generated, 25 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 9.
```

- States generated: **42**
- Distinct states: **25**
- Search depth: **9**
- Invariant violations: **0**
- Wall time: **sub-second**

---

## Negative controls (proving the invariants have teeth)

TLA+ has no unit tests — the invariants *are* the tests, and TLC is the runner.
The TDD analogue is: introduce the known bug and confirm TLC produces the
expected counterexample, proving the invariant actually constrains the model
(is not vacuously true). This is the formal-methods equivalent of "watch the
test fail first."

Four negative controls are documented below, one per headline invariant:
`NoLostUpdate` (RefStore), `SnapshotIsolation` (RefStore), `GCSafety`
(GcReachability), and `RoutingTotality` (Sharding). The RefStore/GC ones are
built as throwaway copies under `/tmp` and are **not** committed; the Sharding
one ships as a committed config (`Sharding_neg.cfg`, `BadRouting = TRUE`) and a
`make neg` target, because a `CONSTANT` toggle makes it a clean, repeatable,
zero-edit control. The `.tla` modules in this directory are all the clean
ones.

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
> still checks clean, and the negative control fires.

**Counterexample TLC produced** (`Error: Invariant NoLostUpdate is violated`,
depth-5 trace, single-ref instance):

```
State 2: <BeginUpdate(w1,r1,o1,o1)>      \* w1 reads root at gen 0
State 3: <BeginUpdate(w2,r1,o1,NONE)>    \* w2 also reads root at gen 0
  /\ gen  = 0
  /\ local = ( w1 :> [name|->r1, newTarget|->o1, expected|->o1,   readGen|->0]
            @@ w2 :> [name|->r1, newTarget|->o1, expected|->NONE, readGen|->0] )

State 4: <CommitCAS(w2)>                  \* w2 wins against readGen 0
  /\ gen  = 1
  /\ committed = << [name|->r1, op|->"update", hlc|->1, readGen|->0,
                     entry|->[target|->o1, hlc|->1, version|->1]] >>

State 5: <CommitCAS(w1)>                  \* w1 ALSO commits against stale readGen 0
  /\ gen  = 2
  /\ committed = << [..readGen|->0..], [..readGen|->0..] >>
                  \* two commits both carry readGen 0 -> NoTwoCommitsSameReadGen FALSE
```

Both writers read the root at `gen = 0`; w2 commits (gen → 1); w1 then commits
against the now-stale `readGen = 0` because the guard is gone. Two commits carry
`readGen = 0` → a lost update. With the guard restored, every commit consumes a
distinct generation (0, 1, 2, …) and the violation is unreachable.

### RefStore — headline invariant `SnapshotIsolation`

**The break:** make the snapshot store a **live reference** instead of a frozen
value — i.e. have the snapshot-read helpers re-read live `refs` rather than the
frozen `s.map` captured at snapshot time:

```diff
 (* Frozen read against a captured snapshot: read the FROZEN map by value. *)
 SnapshotTargetOf(s, name) ==
-    IF s.map[name] = NONE THEN NONE ELSE s.map[name].target
+    IF refs[name] = NONE THEN NONE ELSE refs[name].target   \* BROKEN: live re-read
 SnapshotVersionOf(s, name) ==
-    IF s.map[name] = NONE THEN 0 ELSE s.map[name].version
+    IF refs[name] = NONE THEN 0 ELSE refs[name].version     \* BROKEN: live re-read
```

This is exactly the bug `O(1)` snapshot must not have: returning a view that
tracks live writes instead of the point-in-time root captured by
`ArcSwap::load_full`. The `liveTargets`/`liveVersions` witnesses recorded at
capture stay fixed, so once any later commit moves `refs` on, the broken live
re-read diverges from the witness.

**Counterexample TLC produced** (`Error: Invariant SnapshotIsolation is
violated`, depth-3 trace):

```
State 2: <BeginUpdate(w1,r1,o1,NONE)>    \* r1 still absent
State 3: <Snapshot>                      \* capture while r1 = NONE
  /\ refs = (r1 :> NONE)
  /\ snapshots = << [ map|->(r1 :> NONE), atGen|->0,
                      liveTargets|->(r1 :> NONE), liveVersions|->(r1 :> 0) ] >>

State 4: <CommitCAS(w1)>                  \* commit makes r1 = o1 (live moves on)
  /\ refs = (r1 :> [target|->o1, hlc|->1, version|->1])
  \* broken SnapshotTargetOf re-reads live refs -> o1, but liveTargets witness = NONE
  \* o1 # NONE -> SnapshotIsolation FALSE
```

The snapshot was captured with `r1` absent (`liveTargets = NONE`). After the
commit sets `r1 = o1`, the broken live re-read yields `o1`, which no longer
matches the frozen witness `NONE` → violation. The frozen-by-value reader (the
committed module) keeps reading `NONE` and the invariant holds. This also fires
across a *delete* commit (snapshot of a present ref, then delete → live read
becomes `NONE` while the witness is the old `ObjectId`).

### GcReachability — `GCSafety`

Breaking `GcMark` to mark only the roots themselves instead of their closure
(`reachable' = frozenRoots`) makes a reachable child (`o2`, reachable from
rooted `o1`) fall into the sweep set. TLC reports `Error: Invariant GCSafety is
violated`, confirming `GCSafety` is non-vacuous:

```
State 5: <GcFreeze>   /\ candidates = {o1, o2}  /\ frozenRoots = {o1}
State 6: <GcMark>     /\ reachable  = {o1}      \* BROKEN: should be {o1, o2}
  \* sweep set candidates \ reachable = {o2}, but o2 is reachable from root o1
  \* {o2} \cap ReachableClosure({o1}) = {o2} # {} -> GCSafety FALSE
```

Restoring `reachable' = ReachableClosure(frozenRoots)` returns the clean module.

### Sharding — headline invariant `RoutingTotality`

Unlike the controls above, this one ships as a committed `CONSTANT` toggle so
it is a single repeatable command — no source edit. `Sharding.tla` exposes a
`BadRouting` boolean: when `TRUE`, the routing relation double-assigns the
first ref (by hash) to **two** shards instead of one:

```tla
ROUTES(r) ==
    IF BadRouting /\ r = FirstRef /\ NumShards >= 2
    THEN { shard_for(r), OtherShard(r) }   \* BROKEN: two shards own r
    ELSE { shard_for(r) }                  \* correct: exactly one shard
```

This is exactly the partition bug routing must not have: a ref owned by two
shards (`Cardinality(ROUTES(r)) = 2`, and the ref lands in two shards' owned
sets). `Sharding_neg.cfg` sets `BadRouting = TRUE`; everything else matches
the clean instance.

**Run it:**

```sh
make neg
# or directly:
java -cp ~/.tla/tla2tools.jar tlc2.TLC -config Sharding_neg.cfg Sharding.tla
```

**Counterexample TLC produces** (TLC exits non-zero; `make neg` PASSES on that
non-zero exit):

```
Error: The invariant of RoutingTotality is equal to FALSE
```

`RoutingTotality` is constant-level, so TLC evaluates it at initialization and
reports it `equal to FALSE` (no state trace needed — the violation is in the
constants, not a reachable state): `r1` (the first ref by hash) now routes to
both shard `0` and shard `1`, so `Cardinality(ROUTES("r1")) = 2 ≠ 1`. With
`BadRouting = FALSE` (the default `Sharding.cfg`) every ref's routing image is
a singleton and the invariant holds. The `make neg` target asserts the
non-zero exit, so it is part of the suite's definition of "green."

> **`ApplyDeterminism` was also validated against a broken apply.** A throwaway
> `/tmp` copy gave replica 2 a perturbed apply (`version + 99` on update). At
> `applied1 = applied2 = 1` the two replicas diverged (`version 1` vs `version
> 99`), firing **both** `DeterminismInv` and `PrefixFunctional`. This confirms
> the determinism invariants are non-vacuous — they genuinely depend on apply
> being a pure function of `(state, op)`. The probe was removed after
> confirmation; the committed module is the clean one.

---

## Out of scope

WAL crash-recovery modeling, the Git wire protocol, object content/hashing,
liveness/termination (safety only), and the ART node structure. Raft consensus
itself (election safety, log matching, leader completeness, state-machine
safety) is **not** re-derived — it is inherited from openraft's Ongaro/Diego
TLA+ lineage; `Sharding.tla` models only the Ledge-specific routing-totality
and apply-determinism properties layered above and below that consensus core.
