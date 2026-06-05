-------------------------- MODULE CrossShardTxn --------------------------
(***************************************************************************)
(* Model-checked TLA+ specification of the Ledge Phase 4b cross-shard      *)
(* atomic-commit protocol (two-phase commit over a replicated, durable     *)
(* per-transaction decision record).                                       *)
(*                                                                         *)
(* This models the ACTUAL `TxnCoordinator::commit_atomic` algorithm in     *)
(* crates/ledge-cluster/src/txn.rs plus the `TxnResolver` recovery path,   *)
(* at protocol altitude:                                                   *)
(*                                                                         *)
(*   TxnBegin (durable PENDING record on the coord shard)                  *)
(*     -> Prepare each participant ref: NO-WAIT lock.  A ref already        *)
(*        prepared (locked) by ANOTHER txn votes NO immediately (it never   *)
(*        blocks/waits) -- this is the deadlock-freedom mechanism.          *)
(*     -> all YES? durable TxnDecide{commit:TRUE} (THE commit point);       *)
(*        any NO?  durable TxnDecide{commit:FALSE}.                         *)
(*     -> Phase 2 sweep: CommitPrepared (apply staged, release) on commit,  *)
(*        AbortPrepared (release) on abort, per participant, in any order.   *)
(*     -> TxnEnd GCs the record.                                            *)
(*                                                                         *)
(* Crash recovery (presumed-abort, spec 3.4 / TxnResolver):                *)
(*   The coordinator may STOP (crash) at ANY step.  A prepared lock left    *)
(*   behind is resolved against the DURABLE decision record:                *)
(*     - durable Commit  -> roll FORWARD (CommitPrepared)                    *)
(*     - durable Abort   -> release (AbortPrepared)                          *)
(*     - no / PENDING decision past TTL  -> PRESUMED ABORT (release).        *)
(*   The resolver NEVER rolls forward without a durable Commit: the commit   *)
(*   point precedes any CommitPrepared, so a crash before TxnDecide is safe. *)
(*                                                                         *)
(* CONCURRENCY: we model TWO transactions that may CONTEND on a SHARED ref. *)
(* Each ref is owned by one participant shard.  A ref's `lockedBy` field is  *)
(* the no-wait lock: at most one txn holds it; a second txn's Prepare on a   *)
(* locked ref votes NO.  This is the cross-shard contention the no-wait      *)
(* protocol must resolve without deadlock (spec 3.2, 3.4).                   *)
(*                                                                         *)
(* The `BadCoord` constant injects the negative control: a faulty           *)
(* coordinator that rolls a participant FORWARD (commit-prepared) with NO    *)
(* durable Commit decision -- the precise safety violation the real         *)
(* protocol's "commit point precedes CommitPrepared" ordering forbids.      *)
(* With BadCoord = TRUE a split (one ref committed, a sibling aborted)       *)
(* becomes reachable and Atomicity (and NoDirtyRead) MUST fire.             *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Txns,         \* set of transaction ids, e.g. {t1, t2}
    Refs,         \* set of ref names (each owned by a participant shard)
    Writes,       \* [Txns -> SUBSET Refs] : the ref-set each txn commits over
    BadCoord      \* BOOLEAN: FALSE = correct 2PC; TRUE = negative control

\* Writes is a total function from txns to NON-EMPTY ref-sets (a txn commits
\* over at least one ref).  Checked structurally; we cannot write
\* `Writes \in [Txns -> SUBSET Refs]` against an override, so assert the shape.
ASSUME DOMAIN Writes = Txns
ASSUME \A t \in Txns : Writes[t] \subseteq Refs /\ Writes[t] # {}
ASSUME BadCoord \in BOOLEAN

(***************************************************************************)
(* Concrete instance for TLC, supplied via `Writes <- WritesDef` in the    *)
(* .cfg (config files cannot parse function literals), mirroring            *)
(* `Hash <- HashDef` in Sharding.tla and `reach <- ReachDef` in            *)
(* GcReachability.tla.                                                      *)
(*                                                                         *)
(* Two transactions over three refs, CONTENDING on the shared ref rS:       *)
(*   t1 writes {rA, rS}   (a 2-shard cross-shard txn)                        *)
(*   t2 writes {rS, rB}   (a 2-shard cross-shard txn)                        *)
(* Both want rS, so exactly one can lock it; the other's Prepare on rS       *)
(* votes NO (no-wait) and that txn aborts cleanly -- the contention case.    *)
(***************************************************************************)
WritesDef == ( "t1" :> {"rA", "rS"} @@ "t2" :> {"rS", "rB"} )

----------------------------------------------------------------------------
VARIABLES
    decision,   \* [Txns -> {"none","commit","abort"}] : durable decision record
    phase,      \* [Txns -> {"begin","prepared","decided","done"}] : coord progress
    coordUp,    \* [Txns -> BOOLEAN] : that txn's coordinator process alive
    voted,      \* [Txns -> [Refs -> {"none","yes","no"}]] : per-ref prepare vote
    lockedBy,   \* [Refs -> Txns \cup {NONE}] : the no-wait per-ref lock holder
    applied     \* [Refs -> [Txns -> {"none","committed","aborted"}]] : phase-2 outcome

vars == <<decision, phase, coordUp, voted, lockedBy, applied>>

NONE == "NONE"           \* model sentinel: a ref with no lock holder

(***************************************************************************)
(* Per-txn convenience predicates.                                         *)
(***************************************************************************)
\* Every ref this txn writes has voted YES (Prepare phase complete, all locked).
AllYes(t) == \A r \in Writes[t] : voted[t][r] = "yes"
\* Some ref this txn writes voted NO (lost a no-wait lock race or precondition).
AnyNo(t)  == \E r \in Writes[t] : voted[t][r] = "no"
\* Every ref this txn writes has voted (yes or no): the Prepare phase is done.
AllVoted(t) == \A r \in Writes[t] : voted[t][r] # "none"

\* `RefPrepare`'s CAS precondition, abstracted.  In the real protocol a
\* prepare votes YES only if the ref's current COMMITTED target still equals
\* the `expected` the committing txn captured at workspace-commit time.  Once
\* ANOTHER txn has committed this ref, its committed value has moved on, so a
\* later txn's stale `expected` no longer matches and that prepare votes NO.
\* We model exactly that: a ref is "already committed by someone else" iff some
\* OTHER txn has applied = "committed" on it.  A prepare against such a ref
\* fails its precondition (votes NO), never YES -- this is the per-ref CAS that
\* makes at most one of two concurrent committers of a shared ref succeed.
CommittedByOther(t, r) ==
    \E u \in Txns : u # t /\ applied[r][u] = "committed"

----------------------------------------------------------------------------
Init ==
    /\ decision = [t \in Txns |-> "none"]
    /\ phase    = [t \in Txns |-> "begin"]
    /\ coordUp  = [t \in Txns |-> TRUE]
    /\ voted    = [t \in Txns |-> [r \in Refs |-> "none"]]
    /\ lockedBy = [r \in Refs |-> NONE]
    /\ applied  = [r \in Refs |-> [t \in Txns |-> "none"]]

----------------------------------------------------------------------------
(***************************************************************************)
(* Phase 1 -- Prepare (no-wait lock).  Coordinator t alive, before decision. *)
(*                                                                         *)
(* PrepareYes(t, r): ref r is FREE (no lock holder) -> t takes the lock and   *)
(* votes YES.  This is `RefPrepare` returning Vote(true): precondition holds  *)
(* AND the ref is unlocked.                                                  *)
(*                                                                         *)
(* PrepareNo(t, r): ref r is already LOCKED BY ANOTHER txn -> t votes NO      *)
(* WITHOUT waiting and WITHOUT taking the lock.  This is the no-wait abort   *)
(* that breaks any would-be deadlock cycle (spec 3.2): a Prepare on a busy   *)
(* ref never blocks -- it votes NO and the txn proceeds to abort.            *)
(***************************************************************************)
PrepareYes(t, r) ==
    /\ coordUp[t]
    /\ phase[t] = "begin"
    /\ decision[t] = "none"
    /\ r \in Writes[t]
    /\ voted[t][r] = "none"            \* not yet voted on this ref
    /\ lockedBy[r] = NONE              \* ref is FREE: acquire it
    /\ ~CommittedByOther(t, r)         \* CAS precondition: committed value unmoved
    /\ lockedBy' = [lockedBy EXCEPT ![r] = t]
    /\ voted' = [voted EXCEPT ![t][r] = "yes"]
    /\ UNCHANGED <<decision, phase, coordUp, applied>>

PrepareNo(t, r) ==
    /\ coordUp[t]
    /\ phase[t] = "begin"
    /\ decision[t] = "none"
    /\ r \in Writes[t]
    /\ voted[t][r] = "none"
    \* Vote NO without waiting when the precondition cannot be met:
    \*   (a) the ref is BUSY -- locked by another txn (no-wait lock contention); OR
    \*   (b) the ref was already COMMITTED by another txn -- t's `expected` is now
    \*       stale (the committed value moved), so the CAS precondition fails.
    /\ \/ (lockedBy[r] # NONE /\ lockedBy[r] # t)
       \/ CommittedByOther(t, r)
    /\ voted' = [voted EXCEPT ![t][r] = "no"]   \* NO lock taken (no-wait)
    /\ UNCHANGED <<decision, phase, coordUp, lockedBy, applied>>

(***************************************************************************)
(* Advance the coordinator from "begin" to "prepared" once every ref this   *)
(* txn writes has voted (the Prepare loop in commit_atomic has finished      *)
(* collecting votes, including the short-circuit on a NO).                   *)
(***************************************************************************)
EndPrepare(t) ==
    /\ coordUp[t]
    /\ phase[t] = "begin"
    /\ AllVoted(t)
    /\ phase' = [phase EXCEPT ![t] = "prepared"]
    /\ UNCHANGED <<decision, coordUp, voted, lockedBy, applied>>

(***************************************************************************)
(* Decision -- the COMMIT POINT.  Durable `TxnDecide` on the coord shard.    *)
(* Commit iff ALL refs voted YES; otherwise Abort.  Only the coordinator,    *)
(* only once, only after Prepare completed.                                  *)
(***************************************************************************)
DecideCommit(t) ==
    /\ coordUp[t]
    /\ phase[t] = "prepared"
    /\ decision[t] = "none"
    /\ AllYes(t)
    /\ decision' = [decision EXCEPT ![t] = "commit"]
    /\ phase' = [phase EXCEPT ![t] = "decided"]
    /\ UNCHANGED <<coordUp, voted, lockedBy, applied>>

DecideAbort(t) ==
    /\ coordUp[t]
    /\ phase[t] = "prepared"
    /\ decision[t] = "none"
    /\ AnyNo(t)
    /\ decision' = [decision EXCEPT ![t] = "abort"]
    /\ phase' = [phase EXCEPT ![t] = "decided"]
    /\ UNCHANGED <<coordUp, voted, lockedBy, applied>>

(***************************************************************************)
(* Phase 2 -- roll forward / release.  Driven by the DURABLE decision, so    *)
(* it is correct whether issued by the live coordinator OR the resolver      *)
(* after a crash (both read the same durable record).                        *)
(*                                                                         *)
(* CommitParticipant(t, r): durable Commit -> apply the staged value and      *)
(*   release the lock (`RefCommitPrepared`).  Only a ref this txn actually    *)
(*   locked (lockedBy = t) is committed.                                      *)
(* AbortParticipant(t, r):  durable Abort -> release the lock if held         *)
(*   (`RefAbortPrepared`); a ref that voted NO was never locked, so its       *)
(*   applied-state simply becomes "aborted".                                  *)
(***************************************************************************)
CommitParticipant(t, r) ==
    /\ decision[t] = "commit"
    /\ r \in Writes[t]
    /\ lockedBy[r] = t                 \* this txn holds the lock (it prepared YES)
    /\ applied[r][t] = "none"
    /\ applied' = [applied EXCEPT ![r][t] = "committed"]
    /\ lockedBy' = [lockedBy EXCEPT ![r] = NONE]
    /\ UNCHANGED <<decision, phase, coordUp, voted>>

AbortParticipant(t, r) ==
    /\ decision[t] = "abort"
    /\ r \in Writes[t]
    /\ applied[r][t] = "none"
    /\ applied' = [applied EXCEPT ![r][t] = "aborted"]
    \* release the lock iff THIS txn held it (a NO-voted ref was never locked
    \* by t; a YES-voted ref was, and abort releases it).
    /\ lockedBy' = [lockedBy EXCEPT ![r] = IF lockedBy[r] = t THEN NONE ELSE lockedBy[r]]
    /\ UNCHANGED <<decision, phase, coordUp, voted>>

(***************************************************************************)
(* Finish: once every ref this txn writes is resolved (committed/aborted),   *)
(* TxnEnd GCs the record.  Modeled as the coordinator reaching "done".       *)
(***************************************************************************)
AllResolved(t) == \A r \in Writes[t] : applied[r][t] # "none"

Finish(t) ==
    /\ decision[t] \in {"commit", "abort"}
    /\ AllResolved(t)
    /\ phase[t] # "done"
    /\ phase' = [phase EXCEPT ![t] = "done"]
    /\ UNCHANGED <<decision, coordUp, voted, lockedBy, applied>>

(***************************************************************************)
(* Crash: the coordinator for txn t may STOP at ANY step.  This freezes the  *)
(* live coordinator's own actions for t (PrepareYes/No, EndPrepare, Decide   *)
(* require coordUp); recovery proceeds only via the durable decision record  *)
(* (CommitParticipant/AbortParticipant/PresumedAbort, which do NOT need       *)
(* coordUp -- they are the resolver re-driving phase 2 / presumed abort).     *)
(***************************************************************************)
Crash(t) ==
    /\ coordUp[t]
    /\ coordUp' = [coordUp EXCEPT ![t] = FALSE]
    /\ UNCHANGED <<decision, phase, voted, lockedBy, applied>>

(***************************************************************************)
(* Presumed abort (resolver): the coordinator crashed with NO durable        *)
(* decision (decision = "none") past the TTL.  The resolver RELEASES every    *)
(* prepared lock and marks the ref aborted -- it NEVER installs a staged      *)
(* value (presumed abort only ever releases).  Then it stamps a terminal     *)
(* Abort decision so the record is monotone Pending->Abort (matching          *)
(* TxnResolver::resolve_once phase 3).                                        *)
(*                                                                         *)
(* PresumedAbortRef(t, r): release one locked ref of a crashed, undecided txn.*)
(* PresumedDecide(t): once all its refs are released, write the terminal      *)
(*   durable Abort (so DecisionDurability sees a stable Abort, never a flip). *)
(***************************************************************************)
PresumedAbortRef(t, r) ==
    /\ ~coordUp[t]                     \* coordinator known dead
    /\ decision[t] = "none"            \* no durable decision (presumed abort)
    /\ r \in Writes[t]
    /\ applied[r][t] = "none"
    /\ applied' = [applied EXCEPT ![r][t] = "aborted"]
    /\ lockedBy' = [lockedBy EXCEPT ![r] = IF lockedBy[r] = t THEN NONE ELSE lockedBy[r]]
    /\ UNCHANGED <<decision, phase, coordUp, voted>>

PresumedDecide(t) ==
    /\ ~coordUp[t]
    /\ decision[t] = "none"
    /\ AllResolved(t)                  \* every ref released
    /\ decision' = [decision EXCEPT ![t] = "abort"]   \* terminal Abort (monotone)
    /\ UNCHANGED <<phase, coordUp, voted, lockedBy, applied>>

(***************************************************************************)
(* NEGATIVE CONTROL -- a FAULTY coordinator that rolls a ref FORWARD          *)
(* (commit-prepared) with NO durable Commit decision.  This is exactly the    *)
(* violation the real protocol forbids by ordering the durable TxnDecide      *)
(* BEFORE any CommitPrepared, and by the resolver never rolling forward        *)
(* without a durable Commit.  Only enabled when BadCoord = TRUE.              *)
(*                                                                         *)
(* With it enabled, txn t can commit one of its refs while its sibling ref    *)
(* (lost to a competing txn / aborted) ends aborted -> a split -> Atomicity   *)
(* violated; and a ref becomes "committed" with decision # "commit" ->         *)
(* NoDirtyRead violated.                                                      *)
(***************************************************************************)
BadCommit(t, r) ==
    /\ BadCoord
    /\ r \in Writes[t]
    /\ lockedBy[r] = t
    /\ applied[r][t] = "none"
    /\ applied' = [applied EXCEPT ![r][t] = "committed"]   \* NO decision check!
    /\ lockedBy' = [lockedBy EXCEPT ![r] = NONE]
    /\ UNCHANGED <<decision, phase, coordUp, voted>>

----------------------------------------------------------------------------
Next ==
    \/ \E t \in Txns, r \in Refs : PrepareYes(t, r)
    \/ \E t \in Txns, r \in Refs : PrepareNo(t, r)
    \/ \E t \in Txns : EndPrepare(t)
    \/ \E t \in Txns : DecideCommit(t)
    \/ \E t \in Txns : DecideAbort(t)
    \/ \E t \in Txns, r \in Refs : CommitParticipant(t, r)
    \/ \E t \in Txns, r \in Refs : AbortParticipant(t, r)
    \/ \E t \in Txns : Finish(t)
    \/ \E t \in Txns : Crash(t)
    \/ \E t \in Txns, r \in Refs : PresumedAbortRef(t, r)
    \/ \E t \in Txns : PresumedDecide(t)
    \/ \E t \in Txns, r \in Refs : BadCommit(t, r)

(***************************************************************************)
(* Fairness: every enabled non-crash action eventually fires, so a           *)
(* DURABLY-COMMITTED txn's phase-2 roll-forward (and a crashed-undecided      *)
(* txn's presumed-abort release) is guaranteed to complete -- what            *)
(* DecisionDurability needs.  We deliberately do NOT make Crash fair (a       *)
(* crash is a fault, not an obligation), and weak fairness on the recovery    *)
(* actions models "the resolver eventually runs".                            *)
(***************************************************************************)
Fairness ==
    /\ \A t \in Txns : WF_vars(EndPrepare(t))
    /\ \A t \in Txns : WF_vars(DecideCommit(t) \/ DecideAbort(t))
    /\ \A t \in Txns, r \in Refs : WF_vars(CommitParticipant(t, r))
    /\ \A t \in Txns, r \in Refs : WF_vars(AbortParticipant(t, r))
    /\ \A t \in Txns, r \in Refs : WF_vars(PresumedAbortRef(t, r))
    /\ \A t \in Txns : WF_vars(PresumedDecide(t))

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
(*                              INVARIANTS                                  *)
----------------------------------------------------------------------------

(***************************************************************************)
(* TypeOK: every variable is well-typed.                                    *)
(***************************************************************************)
TypeOK ==
    /\ decision \in [Txns -> {"none", "commit", "abort"}]
    /\ phase    \in [Txns -> {"begin", "prepared", "decided", "done"}]
    /\ coordUp  \in [Txns -> BOOLEAN]
    /\ voted    \in [Txns -> [Refs -> {"none", "yes", "no"}]]
    /\ lockedBy \in [Refs -> (Txns \cup {NONE})]
    /\ applied  \in [Refs -> [Txns -> {"none", "committed", "aborted"}]]

(***************************************************************************)
(* ATOMICITY: no transaction is PARTIALLY applied.  For each txn, the refs   *)
(* it writes are EITHER all committed-or-pending OR all aborted-or-pending --*)
(* never a state where one ref of a txn is committed while another ref of     *)
(* THE SAME txn is aborted.  (A pending ref is mid-sweep and will join the     *)
(* decided side; the forbidden state is a committed/aborted SPLIT within one   *)
(* txn.)  This is the all-or-nothing guarantee `commit_atomic` returns.        *)
(*                                                                         *)
(* Falsified by the negative control: BadCommit rolls one ref forward with    *)
(* no Commit decision while a sibling ref aborts -> a split -> violation.      *)
(***************************************************************************)
Atomicity ==
    \A t \in Txns :
        ~ ( /\ \E r \in Writes[t] : applied[r][t] = "committed"
            /\ \E r \in Writes[t] : applied[r][t] = "aborted" )

(***************************************************************************)
(* NODIRTYREAD: a ref is "committed" for a txn ONLY if that txn's durable     *)
(* decision is "commit".  A staged value becomes visible (the committed       *)
(* state replaces the old one) strictly AFTER the commit point -- never on a   *)
(* prepared-but-undecided ref, and never under an Abort decision.  Readers     *)
(* observe committed state only, never an uncommitted staged intent.           *)
(*                                                                         *)
(* Falsified by the negative control: BadCommit makes a ref "committed" with   *)
(* the decision still "none" -> a dirty (uncommitted-intent) read becomes      *)
(* observable.                                                                  *)
(***************************************************************************)
NoDirtyRead ==
    \A t \in Txns :
        \A r \in Writes[t] :
            (applied[r][t] = "committed") => (decision[t] = "commit")

(***************************************************************************)
(* NODEADLOCK (safety form): the no-wait lock discipline makes a cyclic       *)
(* wait-for impossible.  A deadlock would require two txns each HOLDING a      *)
(* lock the other is BLOCKED waiting to acquire.  In this protocol a Prepare   *)
(* never blocks: a txn that finds a ref locked by another votes NO            *)
(* immediately (PrepareNo) and proceeds to abort.  We assert the structural   *)
(* witness of that: there is NO pair of distinct txns that are each STUCK      *)
(* (un-decided, un-crashed, not done) with a shared ref that one holds and     *)
(* the other still needs but has NOT yet voted on.                            *)
(*                                                                         *)
(* `Blocked(t, r)` would mean: t wants r (r \in Writes[t]), has not voted on   *)
(* r, r is locked by someone else, AND t cannot make progress.  But PrepareNo  *)
(* is ALWAYS enabled in exactly that situation, so t is never blocked -- it     *)
(* can always vote NO.  The invariant asserts no txn is ever in a              *)
(* genuinely-stuck wait: for every (t, r) where t wants r, has not voted, and  *)
(* r is held by another live-or-pending txn, the no-wait escape (PrepareNo)    *)
(* is enabled.  Equivalently: no reachable state exhibits a blocking wait.     *)
(***************************************************************************)
\* A txn is "waiting on r" if it wants r, is still in its prepare phase, has
\* not voted on r, and r is locked by some OTHER txn.  No-wait means this
\* situation is always escapable via PrepareNo (which is enabled here), so it
\* is never a true block.
WaitingOn(t, r) ==
    /\ coordUp[t]
    /\ phase[t] = "begin"
    /\ r \in Writes[t]
    /\ voted[t][r] = "none"
    /\ lockedBy[r] # NONE
    /\ lockedBy[r] # t

\* The no-wait escape PrepareNo(t, r) is enabled exactly when t is waiting on
\* r.  NoDeadlock: whenever a txn is "waiting" on a ref, the no-wait abort is
\* available -- so the wait is never a block and no cyclic wait can form.
NoDeadlock ==
    \A t \in Txns, r \in Refs :
        WaitingOn(t, r) => ENABLED PrepareNo(t, r)

(***************************************************************************)
(* LockExclusion: a ref is held by at most one txn (the no-wait lock is        *)
(* mutually exclusive).  Trivially typed (lockedBy is a function into          *)
(* Txns \cup {NONE}), but stated explicitly as the lock-safety witness:        *)
(* whenever two txns both want a shared ref, only the holder ever commits it;  *)
(* the other voted NO and aborts.  This underpins both Atomicity (no two txns  *)
(* both apply the same ref) and NoDeadlock (the loser does not wait).          *)
(***************************************************************************)
NoDoubleCommitOfRef ==
    \A r \in Refs :
        ~ ( \E t1, t2 \in Txns :
                /\ t1 # t2
                /\ applied[r][t1] = "committed"
                /\ applied[r][t2] = "committed" )

(***************************************************************************)
(* DECISIONDURABILITY (safety component): once a txn's durable decision is     *)
(* set, it NEVER flips.  A crash + resolve can never turn a durable Commit     *)
(* into Abort or vice versa.  Combined with the temporal liveness below, this  *)
(* is the full durability guarantee.  We assert the no-flip safety as a state  *)
(* invariant over the two consequences a flip would produce:                   *)
(*   - roll-forward (a committed ref) only ever happens under a Commit          *)
(*     decision (this is NoDirtyRead, re-used);                                 *)
(*   - the presumed-abort path only ever writes Abort when the decision was     *)
(*     "none" (never overwriting a Commit) -- structural in PresumedDecide.     *)
(* The remaining no-flip obligation (Commit !-> Abort and Abort !-> Commit) is  *)
(* enforced by every Decide/PresumedDecide action guarding on decision="none", *)
(* so no action ever mutates a non-"none" decision; we witness that with        *)
(* CommittedRefImpliesCommitDecision, which a flip to Abort would break.        *)
(***************************************************************************)
\* A ref committed under txn t means t's decision is (and stays) "commit": a
\* later flip to "abort" would falsify this, so it is the durability witness.
CommittedRefImpliesCommitDecision ==
    \A t \in Txns :
        (\E r \in Writes[t] : applied[r][t] = "committed") => decision[t] = "commit"
\* An aborted ref under txn t means t's decision is NOT "commit": a flip from a
\* presumed/real Abort to Commit would falsify this.
AbortedRefImpliesNotCommitDecision ==
    \A t \in Txns :
        (\E r \in Writes[t] : applied[r][t] = "aborted") => decision[t] # "commit"

DecisionStable ==
    /\ CommittedRefImpliesCommitDecision
    /\ AbortedRefImpliesNotCommitDecision

(***************************************************************************)
(* DECISIONDURABILITY (temporal / liveness): once a txn is durably committed,  *)
(* EVERY ref it writes EVENTUALLY reaches "committed" (roll-forward completes   *)
(* under any crash interleaving -- the resolver re-drives phase 2 from the      *)
(* durable Commit record).  Dually, a durably-aborted txn's refs all reach      *)
(* "aborted".  These hold because phase-2 actions need only the durable         *)
(* decision (not a live coordinator) and are weakly fair.                       *)
(***************************************************************************)
CommitRollsForward ==
    \A t \in Txns :
        (decision[t] = "commit") ~>
            (\A r \in Writes[t] : applied[r][t] = "committed")

AbortRollsBack ==
    \A t \in Txns :
        (decision[t] = "abort") ~>
            (\A r \in Writes[t] : applied[r][t] = "aborted")

DecisionDurability ==
    /\ CommitRollsForward
    /\ AbortRollsBack

=============================================================================
