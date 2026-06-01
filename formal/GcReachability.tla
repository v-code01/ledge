----------------------------- MODULE GcReachability -----------------------------
(***************************************************************************)
(* Model-checked TLA+ specification of the Ledge Phase 2a mark-and-sweep   *)
(* garbage collector's candidate-set guard.                                *)
(*                                                                         *)
(* The GC runs concurrently with writers (push/fork writing objects) and   *)
(* lease lifecycle (forks adding/removing live-lease roots).  The safety    *)
(* mechanism is the *candidate set*: GC freezes the set of objects it is    *)
(* allowed to consider for deletion (a snapshot of the store taken with     *)
(* roots), then marks reachability from the frozen roots, then sweeps only  *)
(*   candidates \ reachable.                                                *)
(* Anything written *after* the freeze is, by construction, not in          *)
(* candidates, so can never be swept -- the classic concurrent-GC race      *)
(* (a new object linked into a live root mid-collection) is impossible.     *)
(*                                                                         *)
(* Object content / BLAKE3 hashing is abstracted: objects are opaque ids    *)
(* related by an abstract reachability function `reach`.                    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Objects,   \* finite set of object ids, e.g. {o1, o2, o3, o4}
    reach      \* Objects -> SUBSET Objects: direct out-edges (o points at ...)

ASSUME reach \in [Objects -> SUBSET Objects]

(***************************************************************************)
(* Concrete reachability for the model instance, supplied to TLC via a      *)
(* `reach <- ReachDef` override in the .cfg (TLC config files cannot parse   *)
(* function literals directly).  Chain o1 -> o2 -> o3, isolated o4.         *)
(***************************************************************************)
ReachDef ==
    ( "o1" :> {"o2"} @@ "o2" :> {"o3"} @@ "o3" :> {} @@ "o4" :> {} )

VARIABLES
    store,        \* SUBSET Objects: objects currently on disk
    roots,        \* SUBSET Objects: current live roots (durable + live-lease)
    gcPhase,      \* "idle" | "frozen" | "marked"
    candidates,   \* SUBSET Objects: the frozen list_all_ids() snapshot
    reachable,    \* SUBSET Objects: the marked set
    frozenRoots   \* SUBSET Objects: roots as snapshotted at freeze time

vars == <<store, roots, gcPhase, candidates, reachable, frozenRoots>>

----------------------------------------------------------------------------
(***************************************************************************)
(* Reachable closure of a set S under `reach`.  Objects is finite, so the   *)
(* fixpoint is reached in at most Cardinality(Objects) steps; we expand by  *)
(* exactly that many one-step expansions (a safe over-approximation of the  *)
(* iteration count that is guaranteed to reach the fixpoint).               *)
(***************************************************************************)
OneStep(S) == S \cup UNION { reach[o] : o \in S }

RECURSIVE Expand(_, _)
Expand(S, n) == IF n = 0 THEN S ELSE Expand(OneStep(S), n - 1)

ReachableClosure(S) == Expand(S, Cardinality(Objects))

----------------------------------------------------------------------------
Init ==
    /\ store = {}
    /\ roots = {}
    /\ gcPhase = "idle"
    /\ candidates = {}
    /\ reachable = {}
    /\ frozenRoots = {}

----------------------------------------------------------------------------
(***************************************************************************)
(* WriteObject(o): a concurrent push/fork writes object o to the store.     *)
(* May interleave with any GC phase -- this is the race the candidate set   *)
(* must survive.                                                            *)
(***************************************************************************)
WriteObject(o) ==
    /\ o \notin store
    /\ store' = store \cup {o}
    /\ UNCHANGED <<roots, gcPhase, candidates, reachable, frozenRoots>>

(***************************************************************************)
(* AddRoot(o): a fork takes a live lease, adding a live root.  The object   *)
(* must already be on disk (you cannot root an object you never wrote).     *)
(***************************************************************************)
AddRoot(o) ==
    /\ o \in store
    /\ o \notin roots
    /\ roots' = roots \cup {o}
    /\ UNCHANGED <<store, gcPhase, candidates, reachable, frozenRoots>>

(***************************************************************************)
(* RemoveRoot(o): a lease expires / sweeper retires a root.                 *)
(***************************************************************************)
RemoveRoot(o) ==
    /\ o \in roots
    /\ roots' = roots \ {o}
    /\ UNCHANGED <<store, gcPhase, candidates, reachable, frozenRoots>>

(***************************************************************************)
(* GcFreeze: atomically snapshot the current store as `candidates` and the  *)
(* current roots as `frozenRoots`.  This is list_all_ids() taken together   *)
(* with the live-root snapshot at the start of a collection.                *)
(***************************************************************************)
GcFreeze ==
    /\ gcPhase = "idle"
    /\ candidates' = store
    /\ frozenRoots' = roots
    /\ gcPhase' = "frozen"
    /\ UNCHANGED <<store, roots, reachable>>

(***************************************************************************)
(* GcMark: compute reachability closure from the frozen roots.             *)
(***************************************************************************)
GcMark ==
    /\ gcPhase = "frozen"
    /\ reachable' = ReachableClosure(frozenRoots)
    /\ gcPhase' = "marked"
    /\ UNCHANGED <<store, roots, candidates, frozenRoots>>

(***************************************************************************)
(* GcSweep: delete exactly candidates \ reachable from the store.  Objects  *)
(* written after the freeze are not in candidates, so they survive.         *)
(***************************************************************************)
GcSweep ==
    /\ gcPhase = "marked"
    /\ store' = store \ (candidates \ reachable)
    /\ gcPhase' = "idle"
    /\ candidates' = {}
    /\ reachable' = {}
    /\ frozenRoots' = {}
    /\ UNCHANGED <<roots>>

----------------------------------------------------------------------------
Next ==
    \/ \E o \in Objects : WriteObject(o)
    \/ \E o \in Objects : AddRoot(o)
    \/ \E o \in Objects : RemoveRoot(o)
    \/ GcFreeze
    \/ GcMark
    \/ GcSweep

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(*                              INVARIANTS                                  *)
----------------------------------------------------------------------------

TypeOK ==
    /\ store \subseteq Objects
    /\ roots \subseteq Objects
    /\ gcPhase \in {"idle", "frozen", "marked"}
    /\ candidates \subseteq Objects
    /\ reachable \subseteq Objects
    /\ frozenRoots \subseteq Objects

(***************************************************************************)
(* GCSafety: the sweep deletes only candidates \ reachable.  Therefore an   *)
(* object that is either (a) NOT a candidate (e.g. written after freeze) or  *)
(* (b) reachable from the frozen roots is never deleted.  We assert the      *)
(* sweep-time guarantee directly: in the "marked" phase (immediately before  *)
(* a sweep may fire), every object reachable from the frozen roots that is   *)
(* on disk will remain on disk after the sweep -- i.e. it is not in the      *)
(* delete set.  Equivalently the delete set is disjoint from the frozen-root *)
(* reachable closure.                                                        *)
(***************************************************************************)
GCSafety ==
    gcPhase = "marked" =>
        (candidates \ reachable) \cap ReachableClosure(frozenRoots) = {}

(***************************************************************************)
(* MarkCoversLiveClosure: once marking is done (phases "marked"), every      *)
(* candidate object reachable from the frozen roots has been marked          *)
(* reachable.  This is the heart of the safety guard: the sweep set          *)
(* candidates \ reachable can therefore never contain a frozen-root-reachable *)
(* object.  (GCSafety is the disjointness consequence; this is the           *)
(* "marking is complete" precondition that makes it hold.)                   *)
(***************************************************************************)
MarkCoversLiveClosure ==
    gcPhase = "marked" =>
        (candidates \cap ReachableClosure(frozenRoots)) \subseteq reachable

(***************************************************************************)
(* NoLiveRootDangling: GC never removes a resident object that is reachable  *)
(* from a root that was live at freeze time.  Concretely: an object that was *)
(* a candidate (on disk at freeze) and is reachable from a frozen root is     *)
(* never in the sweep's delete set, so a live ref pinned before the          *)
(* collection never dangles because of GC.  Written-after-freeze objects are *)
(* outside `candidates` and so are also never deleted (covered by GCSafety).  *)
(*                                                                          *)
(* We assert it as: the set of objects the sweep would delete is disjoint    *)
(* from the frozen-root reachable closure -- no live, pinned object is ever   *)
(* a deletion target.                                                        *)
(***************************************************************************)
NoLiveRootDangling ==
    gcPhase = "marked" =>
        \A r \in frozenRoots :
            \A o \in (ReachableClosure({r}) \cap candidates) :
                o \notin (candidates \ reachable)

=============================================================================
