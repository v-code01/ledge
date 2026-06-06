----------------------------- MODULE DistributedGc -----------------------------
(***************************************************************************)
(* Ledge Phase 4c: decentralized cross-node garbage collection.            *)
(*                                                                         *)
(* Each NODE hosts a subset of SHARDS (Hosts subseteq Nodes x Shards).     *)
(* Each shard carries committed roots (committedRoots[s]) and prepared 2PC *)
(* staged roots (preparedRoots[s]). By the write-locality invariant (WL),  *)
(* an object enters a node's store only for a shard the node hosts. A       *)
(* node's GC freezes its store as candidates (the freeze guard), then       *)
(* collects the roots of its hosted shards, marks the reachable closure,    *)
(* and sweeps candidates \ reachable.                                       *)
(*                                                                         *)
(* PASS ORDERING (spec 4.4): freeze candidates -> collect roots -> mark ->   *)
(* sweep.  Critically the ROOTS are read AFTER the candidate freeze, as a    *)
(* current (leader-linearized) snapshot: "freezing before the root read      *)
(* ensures any ref committed before the (later) root read that points to a   *)
(* frozen candidate is observed by the mark."  We model that faithfully:     *)
(*   - GcFreeze(n) snapshots ONLY candidates[n] = store[n] (the freeze       *)
(*     guard); it does NOT yet read roots.                                   *)
(*   - GcMark(n) reads the CURRENT hosted roots, records them as frozen      *)
(*     snapshots, and computes their reachable closure.                      *)
(* Collapsing the root read into GcFreeze (as a naive model would) creates   *)
(* an ARTIFICIAL race -- a Commit interleaving between freeze and root read   *)
(* that the real ordered protocol does not have -- so the root snapshot lives *)
(* in GcMark, matching the spec's freeze-then-collect-roots ordering.        *)
(*                                                                         *)
(* The grace fence is an implementation-level real-time device (spec 4.4)   *)
(* that closes the post-mark object-resurrection race (a ref committed       *)
(* AFTER the mark, pointing at an object the mark already deemed dead).      *)
(* This model ABSTRACTS the grace fence away and proves safety RELATIVE TO   *)
(* THE ROOTS SNAPSHOTTED AT MARK TIME, exactly as GcReachability.tla proves  *)
(* single-node safety relative to its frozen roots.  The formal heart is     *)
(* that the snapshot captures committed UNION prepared over every hosted      *)
(* shard -- the cross-shard, prepared-intent-aware root set.                  *)
(*                                                                         *)
(* SNAPSHOTS recorded at GcMark (the root-read point):                      *)
(*   frozenRoots[n]    -- the roots the GC actually MARKS from.  Under a      *)
(*                        correct GC this is committed UNION prepared; the    *)
(*                        negative control (BadGc) drops the prepared union,  *)
(*                        so the mark wrongly omits in-flight intents.        *)
(*   frozenLive[n]     -- the TRUE live roots (committed UNION prepared) over *)
(*                        hosted shards, BadGc-INSENSITIVE: the ground truth  *)
(*                        of what was live at root-read time.                 *)
(*   frozenPrepared[n] -- the TRUE prepared roots over hosted shards.         *)
(* frozenLive/frozenPrepared are the GROUND TRUTH against which the safety    *)
(* invariants judge the sweep.  The invariants therefore have teeth: under   *)
(* BadGc the mark misses a prepared object (frozenRoots omits it) but it is   *)
(* still in frozenLive/frozenPrepared, so the sweep deleting it violates the  *)
(* safety.                                                                   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Objects,    \* finite set of object ids, e.g. {o1, o2, o3}
    Nodes,      \* finite set of node ids, e.g. {n1, n2}
    Shards,     \* finite set of shard ids, e.g. {s1, s2}
    Hosts,      \* subset of [node: Nodes, shard: Shards] : placement
    reach,      \* Objects -> SUBSET Objects : direct out-edges
    BadGc       \* FALSE = correct GC; TRUE = negative control

ASSUME reach \in [Objects -> SUBSET Objects]
ASSUME Hosts \subseteq [node: Nodes, shard: Shards]
ASSUME BadGc \in BOOLEAN

(***************************************************************************)
(* Concrete reachability + placement for the model instance (the .cfg      *)
(* cannot parse function/record-set literals, so they are defined here and  *)
(* overridden via `reach <- ReachDef` / `Hosts <- HostsDef`).              *)
(* Chain o1 -> o2 (a non-trivial reachability closure: pinning o1 must keep  *)
(* o2 too).  Both nodes host both shards (co-hosting), so a staged object on  *)
(* either shard is sweepable by either node -- the exact condition the         *)
(* prepared-pinning safety must survive.                                     *)
(***************************************************************************)
ReachDef == ( "o1" :> {"o2"} @@ "o2" :> {} )
HostsDef ==
    { [node |-> n, shard |-> s] : n \in Nodes, s \in Shards }

ShardsHostedBy(n) == { h.shard : h \in { x \in Hosts : x.node = n } }

VARIABLES
    committedRoots,   \* [Shards -> SUBSET Objects]
    preparedRoots,    \* [Shards -> SUBSET Objects]
    store,            \* [Nodes -> SUBSET Objects]
    gcPhase,          \* [Nodes -> {"idle","frozen","marked"}]
    candidates,       \* [Nodes -> SUBSET Objects] : frozen store snapshot
    reachable,        \* [Nodes -> SUBSET Objects] : marked set
    frozenRoots,      \* [Nodes -> SUBSET Objects] : roots the GC marks from
    frozenLive,       \* [Nodes -> SUBSET Objects] : true live roots @ mark
    frozenPrepared    \* [Nodes -> SUBSET Objects] : true prepared roots @ mark

vars ==
    << committedRoots, preparedRoots, store, gcPhase, candidates, reachable,
       frozenRoots, frozenLive, frozenPrepared >>

----------------------------------------------------------------------------
OneStep(S) == S \cup UNION { reach[o] : o \in S }
RECURSIVE Expand(_, _)
Expand(S, n) == IF n = 0 THEN S ELSE Expand(OneStep(S), n - 1)
ReachableClosure(S) == Expand(S, Cardinality(Objects))

(* Committed / prepared roots over the shards node n hosts, read at the      *)
(* mark point.  CommittedHosted / PreparedHosted are the GROUND TRUTH of     *)
(* what is live for n's store; HostedRoots is what the GC actually marks      *)
(* from -- which the negative control corrupts by dropping the prepared set.  *)
CommittedHosted(n) ==
    UNION { committedRoots[s] : s \in ShardsHostedBy(n) }
PreparedHosted(n) ==
    UNION { preparedRoots[s] : s \in ShardsHostedBy(n) }

(* The roots a correct GC on node n marks from: committed UNION prepared.    *)
(* The negative control (BadGc) drops the prepared union, modelling a GC     *)
(* that ignores in-flight 2PC staged intents.                               *)
HostedRoots(n) ==
    IF BadGc THEN CommittedHosted(n) ELSE CommittedHosted(n) \cup PreparedHosted(n)

----------------------------------------------------------------------------
Init ==
    /\ committedRoots = [s \in Shards |-> {}]
    /\ preparedRoots  = [s \in Shards |-> {}]
    /\ store = [n \in Nodes |-> {}]
    /\ gcPhase = [n \in Nodes |-> "idle"]
    /\ candidates = [n \in Nodes |-> {}]
    /\ reachable = [n \in Nodes |-> {}]
    /\ frozenRoots = [n \in Nodes |-> {}]
    /\ frozenLive = [n \in Nodes |-> {}]
    /\ frozenPrepared = [n \in Nodes |-> {}]

----------------------------------------------------------------------------
(* WriteObject(n,o): node n writes o for a hosted shard (write-locality).   *)
(* Any node hosts some shard in this model, so n may write any object.      *)
WriteObject(n, o) ==
    /\ o \notin store[n]
    /\ store' = [store EXCEPT ![n] = @ \cup {o}]
    /\ UNCHANGED << committedRoots, preparedRoots, gcPhase, candidates,
                    reachable, frozenRoots, frozenLive, frozenPrepared >>

(* Commit(s,o): a committed ref on shard s points at o. *)
Commit(s, o) ==
    /\ o \notin committedRoots[s]
    /\ committedRoots' = [committedRoots EXCEPT ![s] = @ \cup {o}]
    /\ UNCHANGED << preparedRoots, store, gcPhase, candidates, reachable,
                    frozenRoots, frozenLive, frozenPrepared >>

(* Prepare(s,o): a 2PC intent stages o on shard s (no committed referrer yet). *)
Prepare(s, o) ==
    /\ o \notin preparedRoots[s]
    /\ o \notin committedRoots[s]
    /\ preparedRoots' = [preparedRoots EXCEPT ![s] = @ \cup {o}]
    /\ UNCHANGED << committedRoots, store, gcPhase, candidates, reachable,
                    frozenRoots, frozenLive, frozenPrepared >>

(* CommitPrepared(s,o): the staged intent commits -- o moves prepared -> committed. *)
CommitPrepared(s, o) ==
    /\ o \in preparedRoots[s]
    /\ preparedRoots' = [preparedRoots EXCEPT ![s] = @ \ {o}]
    /\ committedRoots' = [committedRoots EXCEPT ![s] = @ \cup {o}]
    /\ UNCHANGED << store, gcPhase, candidates, reachable,
                    frozenRoots, frozenLive, frozenPrepared >>

(* AbortPrepared(s,o): the intent aborts -- o leaves prepared, no committed ref. *)
AbortPrepared(s, o) ==
    /\ o \in preparedRoots[s]
    /\ preparedRoots' = [preparedRoots EXCEPT ![s] = @ \ {o}]
    /\ UNCHANGED << committedRoots, store, gcPhase, candidates, reachable,
                    frozenRoots, frozenLive, frozenPrepared >>

(* GcFreeze(n): the freeze guard -- snapshot store[n] as candidates.  Objects  *)
(* written after the freeze are not candidates and survive this pass.  Roots  *)
(* are NOT read here; the (later) GcMark reads them (spec 4.4 ordering).      *)
GcFreeze(n) ==
    /\ gcPhase[n] = "idle"
    /\ candidates' = [candidates EXCEPT ![n] = store[n]]
    /\ gcPhase' = [gcPhase EXCEPT ![n] = "frozen"]
    /\ UNCHANGED << committedRoots, preparedRoots, store, reachable,
                    frozenRoots, frozenLive, frozenPrepared >>

(* GcMark(n): collect the CURRENT hosted roots (the linearized root read,     *)
(* AFTER the freeze), record the ground-truth committed/prepared snapshots    *)
(* and the marked-from root set, then compute the reachable closure of what   *)
(* the GC marks from.                                                         *)
GcMark(n) ==
    /\ gcPhase[n] = "frozen"
    /\ frozenRoots' = [frozenRoots EXCEPT ![n] = HostedRoots(n)]
    /\ frozenLive' = [frozenLive EXCEPT ![n] = CommittedHosted(n) \cup PreparedHosted(n)]
    /\ frozenPrepared' = [frozenPrepared EXCEPT ![n] = PreparedHosted(n)]
    /\ reachable' = [reachable EXCEPT ![n] = ReachableClosure(HostedRoots(n))]
    /\ gcPhase' = [gcPhase EXCEPT ![n] = "marked"]
    /\ UNCHANGED << committedRoots, preparedRoots, store, candidates >>

(* GcSweep(n): delete candidates[n] \ reachable[n] from store[n]. *)
GcSweep(n) ==
    /\ gcPhase[n] = "marked"
    /\ store' = [store EXCEPT ![n] = @ \ (candidates[n] \ reachable[n])]
    /\ gcPhase' = [gcPhase EXCEPT ![n] = "idle"]
    /\ candidates' = [candidates EXCEPT ![n] = {}]
    /\ reachable' = [reachable EXCEPT ![n] = {}]
    /\ frozenRoots' = [frozenRoots EXCEPT ![n] = {}]
    /\ frozenLive' = [frozenLive EXCEPT ![n] = {}]
    /\ frozenPrepared' = [frozenPrepared EXCEPT ![n] = {}]
    /\ UNCHANGED << committedRoots, preparedRoots >>

----------------------------------------------------------------------------
Next ==
    \/ \E n \in Nodes, o \in Objects : WriteObject(n, o)
    \/ \E s \in Shards, o \in Objects : Commit(s, o)
    \/ \E s \in Shards, o \in Objects : Prepare(s, o)
    \/ \E s \in Shards, o \in Objects : CommitPrepared(s, o)
    \/ \E s \in Shards, o \in Objects : AbortPrepared(s, o)
    \/ \E n \in Nodes : GcFreeze(n)
    \/ \E n \in Nodes : GcMark(n)
    \/ \E n \in Nodes : GcSweep(n)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(*                              INVARIANTS                                  *)
----------------------------------------------------------------------------

TypeOK ==
    /\ committedRoots \in [Shards -> SUBSET Objects]
    /\ preparedRoots \in [Shards -> SUBSET Objects]
    /\ store \in [Nodes -> SUBSET Objects]
    /\ gcPhase \in [Nodes -> {"idle","frozen","marked"}]
    /\ candidates \in [Nodes -> SUBSET Objects]
    /\ reachable \in [Nodes -> SUBSET Objects]
    /\ frozenRoots \in [Nodes -> SUBSET Objects]
    /\ frozenLive \in [Nodes -> SUBSET Objects]
    /\ frozenPrepared \in [Nodes -> SUBSET Objects]

(* The live root set that MATTERS for node n's safety: the closure of the    *)
(* committed AND prepared roots of every hosted shard, as snapshotted at the  *)
(* mark point (the ground truth, INDEPENDENT of BadGc).  Stated relative to   *)
(* the mark-time snapshot, exactly as GcReachability.tla states GCSafety      *)
(* relative to its frozen roots (the grace fence -- abstracted here -- is what *)
(* extends this to roots committed AFTER the mark).                          *)
LiveClosure(n) == ReachableClosure(frozenLive[n])

(* NoLiveObjectDeleted (distributed GCSafety): a node in "marked" never        *)
(* sweeps an object in the reachable closure of the committed-and-prepared     *)
(* roots its hosted shards held at mark time.  Falsified by the negative       *)
(* control: BadGc drops the prepared union from what the GC marks, so a        *)
(* staged object is unmarked yet still in LiveClosure -> swept -> violation.    *)
NoLiveObjectDeleted ==
    \A n \in Nodes :
        gcPhase[n] = "marked" =>
            (candidates[n] \ reachable[n]) \cap LiveClosure(n) = {}

(* MarkCoversHostedClosure (completeness): once marked, every candidate        *)
(* reachable from the roots the GC MARKED FROM is in reachable[n].  This holds  *)
(* even under BadGc (the mark is internally complete w.r.t. its own -- possibly *)
(* deficient -- root set); it is the marking-correctness precondition.          *)
MarkCoversHostedClosure ==
    \A n \in Nodes :
        gcPhase[n] = "marked" =>
            (candidates[n] \cap ReachableClosure(frozenRoots[n])) \subseteq reachable[n]

(* PreparedPinned (the explicit 4b interaction): no object in the reachable     *)
(* closure of the prepared roots a hosting node held at mark time is ever in    *)
(* that node's sweep set.  In-flight cross-shard 2PC staged objects are pinned. *)
(* Falsified by the negative control: BadGc never marks the prepared closure,   *)
(* so a staged object lands in the sweep set -> violation.                       *)
PreparedPinned ==
    \A n \in Nodes :
        gcPhase[n] = "marked" =>
            (candidates[n] \ reachable[n]) \cap ReachableClosure(frozenPrepared[n]) = {}

=============================================================================
