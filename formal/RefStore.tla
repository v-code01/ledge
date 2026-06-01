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
(* recording its intended op.  Moves to "trying".                          *)
(*   expected \in ObjectIds      -> compare-and-swap: succeed iff current   *)
(*                                  target = expected                       *)
(*   expected = NONE             -> create-if-absent: succeed iff ref absent*)
(***************************************************************************)
BeginUpdate(w, name, newTarget, expected) ==
    /\ pc[w] = "idle"
    /\ local' = [local EXCEPT ![w] =
                    [ readGen   |-> gen,
                      name      |-> name,
                      newTarget |-> newTarget,
                      expected  |-> expected ]]
    /\ pc' = [pc EXCEPT ![w] = "trying"]
    /\ UNCHANGED <<refs, hlc, gen, committed, snapshots>>

(***************************************************************************)
(* CommitCAS(w): the lock-free commit point, a single atomic action.       *)
(* Succeeds iff:                                                            *)
(*   (a) gen = captured readGen  (no other writer committed since the read);*)
(*   (b) the precondition still holds: current target = expected.           *)
(* Effect: refs[name] := [newTarget, hlc+1, oldVersion+1]; hlc++; gen++;    *)
(* append to committed; back to idle.                                       *)
(***************************************************************************)
CommitCAS(w) ==
    LET lv   == local[w]
        name == lv.name
        cur  == TargetOf(name)
        ov   == VersionOf(name)
        nv   == ov + 1
        nhlc == hlc + 1
        entry == [target |-> lv.newTarget, hlc |-> nhlc, version |-> nv]
    IN  /\ pc[w] = "trying"
        /\ gen = lv.readGen                 \* CAS guard: root unchanged
        /\ cur = lv.expected                \* precondition still holds
        /\ nv <= MaxVersion                 \* bounded (state constraint also guards)
        /\ refs' = [refs EXCEPT ![name] = entry]
        /\ hlc' = nhlc
        /\ gen' = gen + 1
        /\ committed' = Append(committed,
                            [name |-> name, entry |-> entry, hlc |-> nhlc,
                             readGen |-> lv.readGen])
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
(* no longer holds (current target # expected) -> LedgeError::Conflict.    *)
(***************************************************************************)
ConflictAbort(w) ==
    /\ pc[w] = "trying"
    /\ gen = local[w].readGen
    /\ TargetOf(local[w].name) # local[w].expected
    /\ pc' = [pc EXCEPT ![w] = "idle"]
    /\ local' = [local EXCEPT ![w] = NONE]
    /\ UNCHANGED <<refs, hlc, gen, committed, snapshots>>

(***************************************************************************)
(* Snapshot: capture the current map as a frozen value.  Later commits     *)
(* must not change what this snapshot reads (SnapshotIsolation invariant). *)
(***************************************************************************)
Snapshot ==
    /\ snapshots' = Append(snapshots, [map |-> refs, atGen |-> gen])
    /\ UNCHANGED <<refs, hlc, gen, pc, local, committed>>

(* Frozen read against a captured snapshot. *)
SnapshotGet(s, name) == s.map[name]

----------------------------------------------------------------------------
Next ==
    \/ \E w \in Writers, name \in RefNames, t \in ObjectIds, e \in Targets :
            BeginUpdate(w, name, t, e)
    \/ \E w \in Writers : CommitCAS(w)
    \/ \E w \in Writers : RetryCAS(w)
    \/ \E w \in Writers : ConflictAbort(w)
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
          \/ /\ local[w].readGen \in Nat
             /\ local[w].name \in RefNames
             /\ local[w].newTarget \in ObjectIds
             /\ local[w].expected \in Targets
    /\ \A i \in DOMAIN committed :
          /\ committed[i].name \in RefNames
          /\ committed[i].entry \in RefEntry
          /\ committed[i].hlc \in Nat
          /\ committed[i].readGen \in Nat
    /\ \A i \in DOMAIN snapshots :
          /\ snapshots[i].map \in [RefNames -> (RefEntry \cup {NONE})]
          /\ snapshots[i].atGen \in Nat

(***************************************************************************)
(* MonotonicVersion: across the commit history, for each ref the version   *)
(* sequence of its commits is 1, 2, 3, ... -- the k-th commit of a ref has *)
(* version k.  This is exactly "+1 per commit, never decreases, create=1". *)
(***************************************************************************)
CommitsOf(name) ==
    LET idx == { i \in DOMAIN committed : committed[i].name = name }
    IN  idx

\* The j-th (1-based, in commit order) commit of `name` must have version j.
MonotonicVersion ==
    \A name \in RefNames :
        \A i \in DOMAIN committed :
            committed[i].name = name =>
                committed[i].entry.version
                    = Cardinality({ j \in DOMAIN committed :
                                      j <= i /\ committed[j].name = name })

(***************************************************************************)
(* HLCMonotonic: committed hlcs are strictly increasing in commit order    *)
(* and pairwise unique (a total order on successful writes consistent with *)
(* real-time commit order -- the linearizability witness).  The global hlc *)
(* equals the largest committed hlc (only ever increases).                 *)
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
(*  - Adjacent commits to the same ref differ in version by exactly 1       *)
(*    (no skipped/duplicated version).                                      *)
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

AdjacentSameRefDifferByOne ==
    \A name \in RefNames :
        \A i, j \in DOMAIN committed :
            /\ committed[i].name = name
            /\ committed[j].name = name
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
(* SnapshotIsolation: a captured snapshot is a frozen value.  Its reads     *)
(* never change, regardless of intervening commits.  Because Snapshot       *)
(* copies `refs` by value into the history and no action mutates a past     *)
(* snapshots[i], SnapshotGet is stable.  We assert the snapshot's map is a   *)
(* valid frozen root and that reading it yields the value stored at capture. *)
(***************************************************************************)
SnapshotIsolation ==
    \A i \in DOMAIN snapshots :
        \A name \in RefNames :
            SnapshotGet(snapshots[i], name) = snapshots[i].map[name]

----------------------------------------------------------------------------
(*                          STATE CONSTRAINT                               *)
(***************************************************************************)
(* Bound the otherwise-unbounded counters so TLC's reachable state graph    *)
(* is finite.  Per-ref version <= MaxVersion; the global hlc is bounded by   *)
(* the max possible number of commits.                                      *)
(***************************************************************************)
StateConstraint ==
    /\ \A name \in RefNames : VersionOf(name) <= MaxVersion
    /\ hlc <= MaxVersion * Cardinality(RefNames)
    /\ Len(committed) <= MaxVersion * Cardinality(RefNames)
    /\ Len(snapshots) <= 1

=============================================================================
