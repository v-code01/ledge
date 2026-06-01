-------------------------------- MODULE RefStore --------------------------------
(***************************************************************************)
(* Model-checked TLA+ specification of the Ledge ref store (Phase 1).      *)
(*                                                                         *)
(* The ref store is an ArcSwap-backed root: a partial map                  *)
(*   RefNames -> (RefEntry \cup {NONE})                                     *)
(* plus a generation token `gen` that models ArcSwap pointer identity.     *)
(* A write is a compare-and-swap of the whole root: a writer reads the     *)
(* current root (capturing `gen`), computes a new map, and commits only    *)
(* if `gen` is unchanged at commit time (no other writer raced ahead).     *)
(*                                                                         *)
(* This is the Phase 1 RefStoreImpl::update / ::delete retry loop verbatim *)
(* at the protocol altitude: model the CAS that can race, abstract the ART *)
(* node structure (a mere representation of the map) that cannot violate   *)
(* safety on its own.                                                       *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    RefNames,    \* finite set of ref names, e.g. {r1, r2}
    ObjectIds,   \* finite set of object ids, e.g. {o1, o2, o3}
    Writers,     \* finite set of writer-thread ids, e.g. {w1, w2}
    MaxVersion,  \* natural bound on per-ref version (state-space finiteness)
    NONE         \* sentinel for "ref absent" and "create-if-absent" expectation

ASSUME NONE \notin ObjectIds
ASSUME MaxVersion \in Nat /\ MaxVersion >= 1

VARIABLES
    refs,        \* RefNames -> (RefEntry \cup {NONE})
    hlc,         \* Nat, global Hybrid Logical Clock (monotonic, only increases)
    gen,         \* Nat, root generation counter (ArcSwap pointer identity)
    pc,          \* Writers -> {"idle","trying"}
    local,       \* Writers -> captured local snapshot of an in-flight update
    committed,   \* Seq of all committed writes (linearizability order witness)
    snapshots    \* Seq of captured map snapshots (frozen-view check)

vars == <<refs, hlc, gen, pc, local, committed, snapshots>>

(***************************************************************************)
(* A committed RefEntry.  version is per-ref and increments by exactly 1   *)
(* on every successful commit; hlc is the globally-unique commit stamp.     *)
(* A commit may also set a ref to NONE (a delete); see Delete/CommitCAS.     *)
(***************************************************************************)
RefEntry == [target: ObjectIds, hlc: Nat, version: Nat]

Targets == ObjectIds \cup {NONE}      \* observable target: an id or absent

(* Current observable target of a ref: its object id, or NONE if absent. *)
TargetOf(name) == IF refs[name] = NONE THEN NONE ELSE refs[name].target

(* Current version of a ref (0 when absent). *)
VersionOf(name) == IF refs[name] = NONE THEN 0 ELSE refs[name].version

----------------------------------------------------------------------------
(***************************************************************************)
(* Initial state: every ref absent, clock at 0, generation 0, all writers  *)
(* idle, no commits, no snapshots.                                          *)
(***************************************************************************)
Init ==
    /\ refs = [n \in RefNames |-> NONE]
    /\ hlc = 0
    /\ gen = 0
    /\ pc = [w \in Writers |-> "idle"]
    /\ local = [w \in Writers |-> NONE]
    /\ committed = << >>
    /\ snapshots = << >>

----------------------------------------------------------------------------
(***************************************************************************)
(* BeginUpdate(w, name, newTarget, expected):                              *)
(* idle writer w reads the current root, capturing `gen` as readGen and    *)
(* recording its intended op.  Moves to "trying".  op = "update".          *)
(*   expected \in ObjectIds      -> compare-and-swap: succeed iff current   *)
(*                                  target = expected                       *)
(*   expected = NONE             -> create-if-absent: succeed iff ref absent*)
(***************************************************************************)
BeginUpdate(w, name, newTarget, expected) ==
    /\ pc[w] = "idle"
    /\ local' = [local EXCEPT ![w] =
                    [ op        |-> "update",
                      readGen   |-> gen,
                      name      |-> name,
                      newTarget |-> newTarget,
                      expected  |-> expected ]]
    /\ pc' = [pc EXCEPT ![w] = "trying"]
    /\ UNCHANGED <<refs, hlc, gen, committed, snapshots>>

(***************************************************************************)
(* BeginDelete(w, name, expected):                                          *)
(* idle writer w reads the current root, capturing `gen` as readGen, and    *)
(* records a delete op.  Moves to "trying".  op = "delete".                 *)
(* Mirrors RefStoreImpl::delete: `expected` must be a concrete ObjectId     *)
(* (Rust takes `expected: ObjectId`, not Option), and the commit clears the *)
(* ref to NONE iff the current target still equals `expected`.              *)
(* `newTarget` is set to NONE (delete carries no new target).               *)
(***************************************************************************)
BeginDelete(w, name, expected) ==
    /\ pc[w] = "idle"
    /\ expected \in ObjectIds            \* delete always names a concrete target
    /\ local' = [local EXCEPT ![w] =
                    [ op        |-> "delete",
                      readGen   |-> gen,
                      name      |-> name,
                      newTarget |-> NONE,
                      expected  |-> expected ]]
    /\ pc' = [pc EXCEPT ![w] = "trying"]
    /\ UNCHANGED <<refs, hlc, gen, committed, snapshots>>

(***************************************************************************)
(* CommitCAS(w): the lock-free commit point, a single atomic action.       *)
(* Succeeds iff:                                                            *)
(*   (a) gen = captured readGen  (no other writer committed since the read);*)
(*   (b) the precondition still holds: current target = expected.           *)
(* Effect (update): refs[name] := [newTarget, hlc+1, oldVersion+1].         *)
(* Effect (delete): refs[name] := NONE.                                     *)
(* In both cases hlc++; gen++; append to committed; back to idle.           *)
(*                                                                          *)
(* The committed record carries `target` (an ObjectId or NONE) so the log   *)
(* witnesses deletes too.  For deletes we record version 0 (the absent      *)
(* version) so the per-ref version progression resets correctly on recreate.*)
(***************************************************************************)
CommitCAS(w) ==
    LET lv    == local[w]
        name  == lv.name
        cur   == TargetOf(name)
        ov    == VersionOf(name)
        isDel == lv.op = "delete"
        nv    == IF isDel THEN 0 ELSE ov + 1
        nhlc  == hlc + 1
        entry == [target |-> lv.newTarget, hlc |-> nhlc, version |-> nv]
        newRef == IF isDel THEN NONE ELSE entry
    IN  /\ pc[w] = "trying"
        /\ gen = lv.readGen                 \* CAS guard: root unchanged
        /\ cur = lv.expected                \* precondition still holds
        /\ (isDel \/ nv <= MaxVersion)      \* update bounded; delete needs no bound
        /\ refs' = [refs EXCEPT ![name] = newRef]
        /\ hlc' = nhlc
        /\ gen' = gen + 1
        /\ committed' = Append(committed,
                            [name |-> name, op |-> lv.op, target |-> lv.newTarget,
                             entry |-> entry, hlc |-> nhlc, readGen |-> lv.readGen])
        /\ pc' = [pc EXCEPT ![w] = "idle"]
        /\ local' = [local EXCEPT ![w] = NONE]
        /\ UNCHANGED <<snapshots>>

(***************************************************************************)
(* RetryCAS(w): another writer committed (gen advanced past readGen).      *)
(* The CAS would fail; return to idle to re-read.                          *)
(***************************************************************************)
RetryCAS(w) ==
    /\ pc[w] = "trying"
    /\ gen # local[w].readGen
    /\ pc' = [pc EXCEPT ![w] = "idle"]
    /\ local' = [local EXCEPT ![w] = NONE]
    /\ UNCHANGED <<refs, hlc, gen, committed, snapshots>>

(***************************************************************************)
(* ConflictAbort(w): root unchanged (gen = readGen) but the precondition   *)
(* no longer holds and the ref is *present* with the wrong target          *)
(* (current target # expected, current target # NONE).                     *)
(*   - update with expected=Some, ref present, wrong target  -> Conflict     *)
(*   - delete with expected=id,   ref present, wrong target  -> Conflict     *)
(* The (None, Some) "absent but expected concrete" case is handled by       *)
(* NotFoundAbort below, matching the Rust LedgeError::NotFound branch.       *)
(***************************************************************************)
ConflictAbort(w) ==
    /\ pc[w] = "trying"
    /\ gen = local[w].readGen
    /\ TargetOf(local[w].name) # local[w].expected
    /\ TargetOf(local[w].name) # NONE        \* ref present -> Conflict, not NotFound
    /\ pc' = [pc EXCEPT ![w] = "idle"]
    /\ local' = [local EXCEPT ![w] = NONE]
    /\ UNCHANGED <<refs, hlc, gen, committed, snapshots>>

(***************************************************************************)
(* NotFoundAbort(w): root unchanged, the ref is absent (TargetOf = NONE),   *)
(* but the writer expected a concrete ObjectId (expected # NONE).           *)
(* This models the Rust `(None, Some(_)) -> LedgeError::NotFound` branch,    *)
(* distinct from Conflict.  Applies to:                                     *)
(*   - update with expected=Some(id) against an absent ref  -> NotFound      *)
(*   - delete (expected always concrete) against an absent ref -> NotFound   *)
(* A create (update with expected=NONE) against an absent ref is NOT a       *)
(* NotFound: it is the legal create path handled by CommitCAS.               *)
(***************************************************************************)
NotFoundAbort(w) ==
    /\ pc[w] = "trying"
    /\ gen = local[w].readGen
    /\ TargetOf(local[w].name) = NONE        \* ref absent
    /\ local[w].expected # NONE              \* but a concrete target was expected
    /\ pc' = [pc EXCEPT ![w] = "idle"]
    /\ local' = [local EXCEPT ![w] = NONE]
    /\ UNCHANGED <<refs, hlc, gen, committed, snapshots>>

(***************************************************************************)
(* Snapshot: capture the current map as a frozen value.  We record:         *)
(*   - map:   the whole `refs` function captured *by value* (the ArcSwap     *)
(*            load_full: a frozen Arc to the root that existed at capture);  *)
(*   - atGen: the generation at capture (witness of "when");                 *)
(*   - liveTargets: an *independently captured* witness of each ref's        *)
(*            observable target at the instant of capture, computed via      *)
(*            TargetOf (a different code path than the raw `map` copy).      *)
(*   - liveVersions: likewise for versions.                                  *)
(* The two captures are taken atomically from the same state, so at capture  *)
(* time they necessarily agree.  SnapshotIsolation asserts they STILL agree  *)
(* at every later state -- which can only hold if `map` is a frozen value,   *)
(* not a live re-read.  See BrokenSnapshot (negative control) which omits     *)
(* `map` and forces the invariant to re-read live `refs`, breaking it.       *)
(***************************************************************************)
Snapshot ==
    /\ snapshots' = Append(snapshots,
                       [ map          |-> refs,
                         atGen        |-> gen,
                         liveTargets  |-> [n \in RefNames |-> TargetOf(n)],
                         liveVersions |-> [n \in RefNames |-> VersionOf(n)] ])
    /\ UNCHANGED <<refs, hlc, gen, pc, local, committed>>

(* Frozen read against a captured snapshot: read the FROZEN map by value. *)
SnapshotTargetOf(s, name) ==
    IF s.map[name] = NONE THEN NONE ELSE s.map[name].target
SnapshotVersionOf(s, name) ==
    IF s.map[name] = NONE THEN 0 ELSE s.map[name].version

----------------------------------------------------------------------------
Next ==
    \/ \E w \in Writers, name \in RefNames, t \in ObjectIds, e \in Targets :
            BeginUpdate(w, name, t, e)
    \/ \E w \in Writers, name \in RefNames, e \in ObjectIds :
            BeginDelete(w, name, e)
    \/ \E w \in Writers : CommitCAS(w)
    \/ \E w \in Writers : RetryCAS(w)
    \/ \E w \in Writers : ConflictAbort(w)
    \/ \E w \in Writers : NotFoundAbort(w)
    \/ Snapshot

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(*                              INVARIANTS                                  *)
----------------------------------------------------------------------------

(* All variables well-typed. *)
TypeOK ==
    /\ refs \in [RefNames -> (RefEntry \cup {NONE})]
    /\ hlc \in Nat
    /\ gen \in Nat
    /\ pc \in [Writers -> {"idle", "trying"}]
    /\ \A w \in Writers :
          \/ local[w] = NONE
          \/ /\ local[w].op \in {"update", "delete"}
             /\ local[w].readGen \in Nat
             /\ local[w].name \in RefNames
             /\ local[w].newTarget \in Targets
             /\ local[w].expected \in Targets
    /\ \A i \in DOMAIN committed :
          /\ committed[i].name \in RefNames
          /\ committed[i].op \in {"update", "delete"}
          /\ committed[i].target \in Targets
          /\ committed[i].entry \in [target: Targets, hlc: Nat, version: Nat]
          /\ committed[i].hlc \in Nat
          /\ committed[i].readGen \in Nat
    /\ \A i \in DOMAIN snapshots :
          /\ snapshots[i].map \in [RefNames -> (RefEntry \cup {NONE})]
          /\ snapshots[i].atGen \in Nat
          /\ snapshots[i].liveTargets \in [RefNames -> Targets]
          /\ snapshots[i].liveVersions \in [RefNames -> Nat]

(***************************************************************************)
(* MonotonicVersion: across the commit history, for each ref the version    *)
(* of its k-th *update* commit (in commit order) is k counted SINCE the      *)
(* most recent delete of that ref (or since the start).  A delete resets the *)
(* count: the next create after a delete is version 1 again.                 *)
(*                                                                          *)
(* Formally: for each update commit i of `name`, its version equals the      *)
(* number of update commits of `name` at indices j (j <= i) that occur after *)
(* the last delete commit of `name` strictly before i (or after 0).          *)
(***************************************************************************)
\* Index of the most recent delete commit of `name` strictly before index i
\* (0 if none).
LastDeleteBefore(name, i) ==
    LET dels == { j \in DOMAIN committed :
                    j < i /\ committed[j].name = name /\ committed[j].op = "delete" }
    IN  IF dels = {} THEN 0 ELSE CHOOSE m \in dels : \A j \in dels : j <= m

MonotonicVersion ==
    \A name \in RefNames :
        \A i \in DOMAIN committed :
            (committed[i].name = name /\ committed[i].op = "update") =>
                committed[i].entry.version
                    = Cardinality({ j \in DOMAIN committed :
                                      /\ j <= i
                                      /\ j > LastDeleteBefore(name, i)
                                      /\ committed[j].name = name
                                      /\ committed[j].op = "update" })

(***************************************************************************)
(* HLCMonotonic: committed hlcs are strictly increasing in commit order    *)
(* and pairwise unique (a total order on successful writes consistent with *)
(* real-time commit order -- the linearizability witness).  The global hlc  *)
(* equals the largest committed hlc (only ever increases).  Deletes are     *)
(* full commits too, so they advance and stamp the clock identically.        *)
(***************************************************************************)
HLCMonotonic ==
    /\ \A i, j \in DOMAIN committed :
          i < j => committed[i].hlc < committed[j].hlc
    /\ \A i, j \in DOMAIN committed :
          committed[i].hlc = committed[j].hlc => i = j
    /\ \A i \in DOMAIN committed : committed[i].hlc <= hlc

(***************************************************************************)
(* NoLostUpdate / LinearizableCAS:                                         *)
(*  - No two commits share the same (name, readGen): each successful CAS    *)
(*    consumed a distinct root generation, so no two writers both "won"     *)
(*    against the same observed root (no lost update).                      *)
(*  - Adjacent UPDATE commits to the same ref with no delete between them    *)
(*    differ in version by exactly 1 (no skipped/duplicated version); an     *)
(*    update immediately following a delete (or starting fresh) is version 1.*)
(* We record the winning readGen (the root generation the CAS consumed)     *)
(* alongside each commit.  The gen-equality guard in CommitCAS forces every  *)
(* successful CAS to consume a *distinct* generation (gen increments on each *)
(* commit and a commit requires gen = readGen), so no two commits can win    *)
(* against the same observed root -- a direct lost-update impossibility.     *)
(* This is the clause the negative control breaks: drop the gen guard and    *)
(* two writers can both commit against the same readGen.                     *)
(***************************************************************************)
NoTwoCommitsSameReadGen ==
    \A i, j \in DOMAIN committed :
        committed[i].readGen = committed[j].readGen => i = j

\* Consecutive same-ref UPDATE commits with no intervening commit of that ref
\* (delete or update) must differ in version by exactly 1.  If the immediately
\* preceding same-ref commit was a delete, the next update restarts at 1, which
\* MonotonicVersion already pins; here we only constrain update->update adjacency.
AdjacentSameRefDifferByOne ==
    \A name \in RefNames :
        \A i, j \in DOMAIN committed :
            /\ committed[i].name = name /\ committed[i].op = "update"
            /\ committed[j].name = name /\ committed[j].op = "update"
            /\ i < j
            /\ ~ (\E k \in DOMAIN committed :
                    i < k /\ k < j /\ committed[k].name = name)
            => committed[j].entry.version = committed[i].entry.version + 1

\* No two distinct commits carry the same global hlc stamp == no two CAS
\* operations linearized at the same point (a direct "no lost update"
\* witness: each winning CAS advanced the global clock by a unique amount).
DistinctCommitStamps ==
    \A i, j \in DOMAIN committed :
        committed[i].hlc = committed[j].hlc => i = j

NoLostUpdate ==
    /\ NoTwoCommitsSameReadGen
    /\ AdjacentSameRefDifferByOne
    /\ DistinctCommitStamps

(***************************************************************************)
(* SnapshotIsolation: a captured snapshot is a FROZEN value.  Reading the   *)
(* snapshot's frozen `map` must always yield exactly what was observed live  *)
(* at the instant of capture -- regardless of any commits (update OR delete) *)
(* that happened afterward and moved live `refs` on.                         *)
(*                                                                          *)
(* `liveTargets`/`liveVersions` were captured atomically with `map` and are  *)
(* never mutated, so they are the immutable witness of "what this snapshot    *)
(* should read forever."  The invariant asserts the frozen `map` still reads  *)
(* that witness.  This is FALSIFIABLE: BrokenSnapshot (negative control)      *)
(* drops `map` and reads live `refs` instead, so once any later commit        *)
(* changes `refs[name]`, the frozen witness and the live read diverge and TLC *)
(* reports a SnapshotIsolation violation.                                    *)
(***************************************************************************)
SnapshotIsolation ==
    \A i \in DOMAIN snapshots :
        \A name \in RefNames :
            /\ SnapshotTargetOf(snapshots[i], name)  = snapshots[i].liveTargets[name]
            /\ SnapshotVersionOf(snapshots[i], name) = snapshots[i].liveVersions[name]

----------------------------------------------------------------------------
(*                          STATE CONSTRAINT                               *)
(***************************************************************************)
(* Bound the otherwise-unbounded counters so TLC's reachable state graph    *)
(* is finite.  Per-ref version <= MaxVersion; the global hlc and committed   *)
(* log are bounded by the max possible number of commits.  With delete in    *)
(* the model a ref can be recreated, so the number of commits per ref is no  *)
(* longer capped by MaxVersion alone; we cap committed-log length directly.  *)
(***************************************************************************)
MaxCommits == MaxVersion * Cardinality(RefNames) + Cardinality(RefNames)

StateConstraint ==
    /\ \A name \in RefNames : VersionOf(name) <= MaxVersion
    /\ hlc <= MaxCommits
    /\ Len(committed) <= MaxCommits
    /\ Len(snapshots) <= 1

=============================================================================
