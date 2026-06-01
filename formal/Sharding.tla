-------------------------------- MODULE Sharding --------------------------------
(***************************************************************************)
(* Model-checked TLA+ specification of the Ledge Phase 3 sharding layer.   *)
(*                                                                         *)
(* This module verifies the two properties that are SPECIFIC to Ledge's    *)
(* sharded-Raft design and are NOT covered by Raft itself:                 *)
(*                                                                         *)
(*   1. RoutingTotality / Partition -- the `shard_for` router is a total   *)
(*      function and the per-shard owned ref-sets partition the ref-name   *)
(*      space (every ref maps to exactly one shard; none to two; none      *)
(*      orphaned).                                                         *)
(*                                                                         *)
(*   2. ApplyDeterminism (DeterminismInv) -- two replicas of a shard,      *)
(*      consuming the SAME committed log prefix in index order under a     *)
(*      nondeterministic scheduler, reach IDENTICAL state machine state    *)
(*      AND identical response sequences.  This is confluence over         *)
(*      interleavings: the per-shard apply is a pure function of           *)
(*      (state, op), so two replicas that have applied the same prefix     *)
(*      length necessarily agree.                                         *)
(*                                                                         *)
(* RAFT'S OWN SAFETY IS INHERITED, NOT RE-DERIVED HERE.  Election safety,  *)
(* log matching, leader completeness, and state-machine safety come from   *)
(* openraft, whose protocol is the Ongaro/Diego TLA+ Raft lineage          *)
(* (the canonical `raft.tla`).  We deliberately do NOT re-model Raft       *)
(* consensus -- we model only the two Ledge-specific additions that sit    *)
(* above (routing) and below (deterministic apply) the consensus core,     *)
(* assuming the committed-log abstraction Raft provides (a single agreed   *)
(* total order of entries per shard).                                      *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Refs,        \* finite set of model ref-name tokens, e.g. {r1, r2, r3}
    NumShards,   \* Nat \ {0}: number of shard groups
    Hash,        \* Refs -> Nat: a deterministic content-hash abstraction
    Objects,     \* finite set of object ids that ops can target, e.g. {o1, o2}
    OpLog,       \* the committed log: a Seq of ops both replicas must apply
    BadRouting,  \* BOOLEAN: negative-control toggle (double-assigns one ref)
    NONE         \* sentinel model value for "ref absent" / create-if-absent

ASSUME NumShards \in Nat /\ NumShards >= 1
\* Hash is a total function from Refs into the naturals.  We check totality
\* (DOMAIN = Refs) and that every image is a natural; we cannot write
\* `Hash \in [Refs -> Nat]` because Nat is infinite and TLC cannot enumerate
\* the (infinite) function set to test membership.
ASSUME DOMAIN Hash = Refs /\ \A r \in Refs : Hash[r] \in Nat
ASSUME BadRouting \in BOOLEAN

ASSUME NONE \notin Objects

Shards == 0 .. (NumShards - 1)

(***************************************************************************)
(* Concrete instances supplied to TLC via `<- ...Def` overrides in the     *)
(* .cfg (TLC config files cannot parse function / record literals          *)
(* directly), mirroring `reach <- ReachDef` in GcReachability.tla.         *)
(*                                                                         *)
(* HashDef: a deterministic hash giving a non-trivial routing spread over  *)
(* the shards (with NumShards = 2: r1->0, r2->1, r3->1).                    *)
(***************************************************************************)
HashDef == ( "r1" :> 4 @@ "r2" :> 7 @@ "r3" :> 9 )

(***************************************************************************)
(* OpLogDef: the shared committed log both replicas apply.  A 4-op shard   *)
(* log exercising create / update-CAS / conflict / delete:                 *)
(*   1. create r1 -> o1            (expected NONE)        => Updated        *)
(*   2. update r1 o1 -> o2         (expected o1)          => Updated        *)
(*   3. update r1 expecting o1     (stale; current o2)    => Conflict       *)
(*   4. delete r1 expecting o2     (current o2)           => Deleted        *)
(* hlcs are assigned explicitly (leader-stamped before replication), so    *)
(* Apply is a pure function of the op -- the determinism crux.             *)
(***************************************************************************)
OpLogDef == <<
    [kind |-> "update", name |-> "r1", target |-> "o1", expected |-> NONE, hlc |-> 1],
    [kind |-> "update", name |-> "r1", target |-> "o2", expected |-> "o1", hlc |-> 2],
    [kind |-> "update", name |-> "r1", target |-> "o1", expected |-> "o1", hlc |-> 3],
    [kind |-> "delete", name |-> "r1",                  expected |-> "o2", hlc |-> 4]
>>

----------------------------------------------------------------------------
(*                          ROUTING (static)                               *)
(***************************************************************************)
(* shard_for(r) routes a ref to its owning shard by hashing modulo the     *)
(* shard count -- the Phase 3 `ShardRouter::shard_for` at protocol         *)
(* altitude (the concrete hash is BLAKE3; here it is an abstract total     *)
(* CONSTANT function Refs -> Nat, which is the only property routing        *)
(* safety depends on).                                                     *)
(*                                                                         *)
(* The model exposes ROUTES(r): the SET of shards a ref maps to.  For a    *)
(* correct total function this set is always a singleton.  The negative    *)
(* control BadRouting makes ROUTES double-assign one ref to TWO shards,    *)
(* breaking the single-assignment clause of RoutingTotality.               *)
(***************************************************************************)
shard_for(r) == Hash[r] % NumShards

\* The (possibly broken) routing relation, as the set of shards each ref
\* maps to.  Correct routing: a singleton {shard_for(r)}.  Broken routing
\* (BadRouting = TRUE): the lexicographically-first ref is double-assigned
\* to shard_for(r) AND a second, distinct shard, so its image is not a
\* singleton -- exactly the "one ref owned by two shards" partition bug.
FirstRef == CHOOSE r \in Refs : \A r2 \in Refs : Hash[r] <= Hash[r2]

OtherShard(r) == CHOOSE s \in Shards : s # shard_for(r)

ROUTES(r) ==
    IF BadRouting /\ r = FirstRef /\ NumShards >= 2
    THEN { shard_for(r), OtherShard(r) }   \* BROKEN: two shards own r
    ELSE { shard_for(r) }                  \* correct: exactly one shard

\* Owned set of a shard under the (possibly broken) routing relation.
OwnedBy(s) == { r \in Refs : s \in ROUTES(r) }

----------------------------------------------------------------------------
(*                   DETERMINISTIC STATE-MACHINE APPLY                      *)
(***************************************************************************)
(* A shard's state machine is a ref-map: RefNames -> (entry \cup {NONE}).  *)
(* An entry is [target, hlc, version].  Apply is the deterministic,        *)
(* explicit-hlc apply path (`RefStoreImpl::apply_op`): a PURE FUNCTION of   *)
(* (state, op).  TLA+ functions are deterministic by construction, so      *)
(* making Apply a function is itself the determinism guarantee; the        *)
(* operational invariant below proves two replicas converge under it.      *)
(*                                                                         *)
(* Ops carry their hlc explicitly (assigned by the leader before           *)
(* replication, per the design data-flow), so apply is a pure function of  *)
(* the op -- it never reads a clock or any replica-local nondeterministic  *)
(* source.  That is the crux: were apply to mint an hlc itself, two        *)
(* replicas would diverge; the negative-control discussion covers this.    *)
(***************************************************************************)
Entry == [target: Objects, hlc: Nat, version: Nat]

TargetOf(state, name) == IF state[name] = NONE THEN NONE ELSE state[name].target
VersionOf(state, name) == IF state[name] = NONE THEN 0 ELSE state[name].version

(***************************************************************************)
(* Apply(state, op) == << state', resp >>.  Ops:                           *)
(*   [kind|->"update", name, target, expected, hlc]                        *)
(*       CAS: succeed iff current target = expected (expected = NONE means  *)
(*       create-if-absent).  On success set entry, version+1; resp =        *)
(*       "Updated".  On mismatch: state unchanged; resp = "Conflict".      *)
(*   [kind|->"delete", name, expected, hlc]                                *)
(*       succeed iff current target = expected; clear to NONE; resp =       *)
(*       "Deleted".  On mismatch: unchanged; resp = "Conflict".            *)
(* Pure function of (state, op): no clock read, no randomness.             *)
(***************************************************************************)
Apply(state, op) ==
    IF op.kind = "update"
    THEN IF TargetOf(state, op.name) = op.expected
         THEN << [state EXCEPT ![op.name] =
                    [target |-> op.target, hlc |-> op.hlc,
                     version |-> VersionOf(state, op.name) + 1]],
                 "Updated" >>
         ELSE << state, "Conflict" >>
    ELSE \* delete
         IF TargetOf(state, op.name) = op.expected
         THEN << [state EXCEPT ![op.name] = NONE], "Deleted" >>
         ELSE << state, "Conflict" >>

\* Fold the apply over a log prefix, accumulating state and the response
\* sequence.  Pure: a function of the prefix alone, so two replicas that
\* have applied the same prefix length share the same (state, resps).
RECURSIVE ApplyLog(_, _, _)
ApplyLog(state, resps, log) ==
    IF log = << >>
    THEN << state, resps >>
    ELSE LET sr == Apply(state, Head(log))
         IN  ApplyLog(sr[1], Append(resps, sr[2]), Tail(log))

InitState == [n \in Refs |-> NONE]

----------------------------------------------------------------------------
(*                      OPERATIONAL DETERMINISM MODEL                       *)
(***************************************************************************)
(* Two replicas (sm1/resps1 and sm2/resps2) of ONE shard consume the SAME  *)
(* committed log `OpLog` in index order.  applied1/applied2 track how many *)
(* entries each has applied.  A nondeterministic scheduler advances either *)
(* replica by one entry at a time -- TLC explores every interleaving.      *)
(* The next entry a replica applies is always OpLog[applied+1], so each    *)
(* replica consumes the SAME ordered prefix (Raft's single agreed order),  *)
(* just possibly at different speeds.                                      *)
(*                                                                         *)
(* DeterminismInv asserts: whenever the two replicas have applied the same *)
(* number of entries (same committed prefix length), their state machines  *)
(* AND response sequences are identical -- regardless of the interleaving  *)
(* that got them there.                                                    *)
(***************************************************************************)
VARIABLES sm1, sm2, resps1, resps2, applied1, applied2

vars == <<sm1, sm2, resps1, resps2, applied1, applied2>>

LogLen == Len(OpLog)

Init ==
    /\ sm1 = InitState
    /\ sm2 = InitState
    /\ resps1 = << >>
    /\ resps2 = << >>
    /\ applied1 = 0
    /\ applied2 = 0

\* Replica 1 applies its next committed entry (OpLog[applied1 + 1]).
Step1 ==
    /\ applied1 < LogLen
    /\ LET op == OpLog[applied1 + 1]
           sr == Apply(sm1, op)
       IN  /\ sm1' = sr[1]
           /\ resps1' = Append(resps1, sr[2])
           /\ applied1' = applied1 + 1
    /\ UNCHANGED <<sm2, resps2, applied2>>

\* Replica 2 applies its next committed entry (OpLog[applied2 + 1]).
Step2 ==
    /\ applied2 < LogLen
    /\ LET op == OpLog[applied2 + 1]
           sr == Apply(sm2, op)
       IN  /\ sm2' = sr[1]
           /\ resps2' = Append(resps2, sr[2])
           /\ applied2' = applied2 + 1
    /\ UNCHANGED <<sm1, resps1, applied1>>

\* Both replicas have fully applied the log: idle (stutter) to keep the
\* behaviour total without inflating the state graph.
Done ==
    /\ applied1 = LogLen
    /\ applied2 = LogLen
    /\ UNCHANGED vars

Next == Step1 \/ Step2 \/ Done

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(*                              INVARIANTS                                  *)
----------------------------------------------------------------------------

(***************************************************************************)
(* TypeOK: every variable is well-typed.  States are total ref-maps; the   *)
(* response sequences draw from the finite response alphabet; applied      *)
(* counters are within log bounds.                                         *)
(***************************************************************************)
Responses == {"Updated", "Deleted", "Conflict"}

\* A well-formed state-machine value: a total ref-map whose entries are
\* either NONE or a proper [target, hlc, version] record.  Checked
\* structurally per-ref (not via `\in [Refs -> Entry \cup {NONE}]`) because
\* `Entry` has an infinite `hlc: Nat` field that TLC cannot enumerate.
WellFormedSM(sm) ==
    /\ DOMAIN sm = Refs
    /\ \A n \in Refs :
          \/ sm[n] = NONE
          \/ /\ DOMAIN sm[n] = {"target", "hlc", "version"}
             /\ sm[n].target \in Objects
             /\ sm[n].hlc \in Nat
             /\ sm[n].version \in Nat

TypeOK ==
    /\ WellFormedSM(sm1)
    /\ WellFormedSM(sm2)
    /\ applied1 \in 0 .. LogLen
    /\ applied2 \in 0 .. LogLen
    /\ \A i \in DOMAIN resps1 : resps1[i] \in Responses
    /\ \A i \in DOMAIN resps2 : resps2[i] \in Responses

(***************************************************************************)
(* RoutingTotality: shard_for is a total function into the shard index     *)
(* space, and each ref maps to EXACTLY ONE shard (its routing image is a   *)
(* singleton).  Falsified by BadRouting, which double-assigns one ref.     *)
(***************************************************************************)
RoutingTotality ==
    /\ \A r \in Refs : ROUTES(r) \subseteq Shards
    /\ \A r \in Refs : Cardinality(ROUTES(r)) = 1

(***************************************************************************)
(* Partition: the per-shard owned sets partition the ref-name space --     *)
(* their union is all of Refs (none orphaned) and they are pairwise        *)
(* disjoint (none double-owned).  Also falsified by BadRouting (the        *)
(* double-assigned ref lands in two shards' owned sets, breaking           *)
(* disjointness).                                                          *)
(***************************************************************************)
Partition ==
    /\ UNION { OwnedBy(s) : s \in Shards } = Refs
    /\ \A s1, s2 \in Shards : s1 # s2 => OwnedBy(s1) \cap OwnedBy(s2) = {}

(***************************************************************************)
(* DeterminismInv (ApplyDeterminism, operational form): whenever the two   *)
(* replicas have applied the same committed prefix length, their state     *)
(* machines and response sequences are identical -- confluence over every  *)
(* scheduler interleaving TLC explores.  This is the property §2.3's       *)
(* "followers apply the same entry -> identical state" depends on.         *)
(*                                                                         *)
(* It can only hold because Apply is a pure function of (state, op): the   *)
(* negative-control discussion (README) shows that an apply which mints an *)
(* hlc itself, rather than reading op.hlc, breaks this invariant.          *)
(***************************************************************************)
DeterminismInv ==
    (applied1 = applied2) => (sm1 = sm2 /\ resps1 = resps2)

(***************************************************************************)
(* PrefixFunctional: a replica's applied state is a pure FUNCTION of the   *)
(* first `applied` log entries only -- it equals folding Apply over        *)
(* OpLog[1..applied] from InitState.  This is the "state after prefix k    *)
(* depends only on the first k entries" clause: it pins each replica to    *)
(* the canonical fold, so determinism is not a coincidence of the          *)
(* scheduler but a structural property of the apply function.              *)
(***************************************************************************)
PrefixOf(k) == [ i \in 1 .. k |-> OpLog[i] ]

PrefixFunctional ==
    /\ << sm1, resps1 >> = ApplyLog(InitState, << >>, PrefixOf(applied1))
    /\ << sm2, resps2 >> = ApplyLog(InitState, << >>, PrefixOf(applied2))

=============================================================================
