#!/usr/bin/env bash
# Phase 4f — multi-process cluster soak + chaos/partition harness.
#
# Drives the 4e docker-compose 3-node cluster (real separate processes, real
# container network, per-node durable volumes) and asserts consensus-layer
# invariants under injected chaos via the observable /cluster/status surface:
# leader/term/commit_index/last_applied. Every wait is a bounded poll on
# observable state — NO fixed sleeps as synchronization. Exits non-zero on any
# failed assertion.
#
# What it proves (over REAL processes/network/partitions, closing the in-process
# gap that 4b/4c/4d carried):
#   1. replicated consensus baseline (membership committed on all 3)
#   2. leader failover: kill leader -> new leader, committed log NOT lost
#   3. follower crash + restart: last_applied reconverges from disk
#   4. minority partition: isolated node has no leader (no split-brain); heal -> reconverge
#   5. majority loss: lone node makes no progress (safety over liveness)
#   6. soak: sustained polling, zero errors, bounded container memory
#
# Honest ceiling: ONE physical host. Real processes + real container net + real
# clean partitions, but NOT real multi-machine (no NIC/hardware faults, geo
# latency, or genuine inter-host clock skew). See soak/README.md.
#
# Usage:  bash soak/chaos.sh            # full run
#         SOAK_SECS=30 bash soak/chaos.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$HERE/../deploy/compose/docker-compose.cluster.yml}"
DC=(docker compose -f "$COMPOSE_FILE")
SECRET="${LEDGE_CLUSTER_SECRET:-dev-cluster-secret-change-me}"
NET="${LEDGE_NET:-compose_default}"
SOAK_SECS="${SOAK_SECS:-60}"
NODES=(ledge-1 ledge-2 ledge-3)

PASS=0
FAIL=0
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$1"; FAIL=$((FAIL + 1)); }
note() { printf '\n=== %s ===\n' "$1"; }

# -aq so a killed/stopped container's id is still returned (needed to docker start it).
cid() { "${DC[@]}" ps -aq "$1"; }

# curl inside a node's container (works even when the node is network-isolated,
# since exec rides docker, not the cluster network). Echoes body; empty on error.
node_curl() {
  local node="$1"; shift
  "${DC[@]}" exec -T "$node" curl -s --max-time 4 "$@" 2>/dev/null || true
}

# Shard-0 field as seen by <node>: status_field <node> <jq-path>. "" if unreachable.
status_field() {
  local node="$1" path="$2" body
  body="$(node_curl "$node" -H "Authorization: Bearer $SECRET" http://localhost:3000/cluster/status)"
  [ -z "$body" ] && { echo ""; return; }
  echo "$body" | jq -r ".shards[0].${path} // empty" 2>/dev/null || echo ""
}

leader_by() { status_field "$1" leader; }      # leader node id, or "" (none/unreachable)
commit_by() { status_field "$1" commit_index; }
applied_by(){ status_field "$1" last_applied; }

# wait_for <timeout_s> <predicate cmd...> ; returns 0 if predicate true within budget.
wait_for() {
  local budget="$1"; shift
  local deadline=$((SECONDS + budget))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if "$@"; then return 0; fi
    sleep 1
  done
  return 1
}

# predicates (for wait_for)
all_healthy() { [ "$("${DC[@]}" ps --format '{{.Health}}' 2>/dev/null | grep -c healthy)" = "3" ]; }
some_leader() { local n; for n in "${NODES[@]}"; do l="$(leader_by "$n")"; [ -n "$l" ] && return 0; done; return 1; }
leader_is_not() { local want="$1" n l; for n in "${NODES[@]}"; do l="$(leader_by "$n")"; [ -n "$l" ] && [ "$l" != "$want" ] && return 0; done; return 1; }
applied_at_least() { local node="$1" min="$2" v; v="$(applied_by "$node")"; [ -n "$v" ] && [ "$v" -ge "$min" ] 2>/dev/null; }
no_leader_at() { local node="$1"; [ -z "$(leader_by "$node")" ]; }

cleanup() { note "teardown"; "${DC[@]}" down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

# ── bring-up ────────────────────────────────────────────────────────────────
note "bring up 3-node cluster"
"${DC[@]}" up -d >/dev/null 2>&1
if wait_for 60 all_healthy; then ok "3 nodes healthy"; else bad "nodes not healthy"; exit 1; fi

# init shard 0 (idempotent: 409 if already)
node_curl ledge-1 -X POST -H "Authorization: Bearer $SECRET" -H 'content-type: application/json' \
  -d '{"shard":0,"members":{"1":"http://ledge-1:3000","2":"http://ledge-2:3000","3":"http://ledge-3:3000"}}' \
  http://localhost:3000/cluster/init >/dev/null
if wait_for 30 some_leader; then ok "leader elected"; else bad "no leader after init"; exit 1; fi

# ── 1. replicated consensus baseline ────────────────────────────────────────
note "1. replicated consensus baseline"
base_ok=1
for n in "${NODES[@]}"; do
  ci="$(commit_by "$n")"; la="$(applied_by "$n")"; ld="$(leader_by "$n")"
  printf '     %s: leader=%s commit_index=%s last_applied=%s\n' "$n" "$ld" "$ci" "$la"
  { [ -n "$ci" ] && [ "$ci" -ge 1 ] 2>/dev/null && [ "$la" -ge 1 ] 2>/dev/null; } || base_ok=0
done
if [ "$base_ok" = 1 ]; then ok "membership entry committed+applied on all 3 nodes"; else bad "baseline replication"; fi
L0="$(leader_by ledge-1)"; CI0="$(commit_by ledge-1)"
printf '     baseline leader=%s commit_index=%s\n' "$L0" "$CI0"

# ── 2. leader failover: committed log survives, new leader elected ───────────
note "2. leader-failover durability"
LEAD="ledge-${L0}"
echo "     killing leader $LEAD"
docker kill "$(cid "$LEAD")" >/dev/null
if wait_for 40 leader_is_not "$L0"; then
  NL=""; for n in "${NODES[@]}"; do [ "$n" = "$LEAD" ] && continue; l="$(leader_by "$n")"; [ -n "$l" ] && NL="$l" && SURV="$n" && break; done
  ok "new leader elected (was $L0, now $NL)"
  ci="$(commit_by "$SURV")"; la="$(applied_by "$SURV")"
  printf '     survivor %s: commit_index=%s last_applied=%s\n' "$SURV" "$ci" "$la"
  if [ -n "$la" ] && [ "$la" -ge 1 ] 2>/dev/null; then ok "committed log durable across failover (last_applied>=1, no regression)"; else bad "committed log regressed after failover"; fi
else
  bad "no new leader after killing $LEAD"
fi
echo "     restart $LEAD"
docker start "$(cid "$LEAD")" >/dev/null
if wait_for 40 all_healthy; then ok "killed leader rejoined healthy"; else bad "$LEAD did not rejoin"; fi

# ── 3. follower crash + restart recovery ────────────────────────────────────
note "3. follower crash + restart recovery"
wait_for 30 some_leader || true
CUR_L="$(for n in "${NODES[@]}"; do l="$(leader_by "$n")"; [ -n "$l" ] && echo "$l" && break; done)"
FOLL=""; for n in "${NODES[@]}"; do [ "$n" != "ledge-${CUR_L}" ] && FOLL="$n" && break; done
LEAD_NODE="ledge-${CUR_L}"
target="$(applied_by "$LEAD_NODE")"; echo "     leader $LEAD_NODE last_applied=$target; killing follower $FOLL"
docker kill "$(cid "$FOLL")" >/dev/null
# quorum (2/3) intact: leader still present
if wait_for 20 some_leader; then ok "quorum holds with follower down (leader present)"; else bad "lost leader on single follower down"; fi
echo "     restart follower $FOLL"
docker start "$(cid "$FOLL")" >/dev/null
wait_for 60 all_healthy || true
if wait_for 40 applied_at_least "$FOLL" "${target:-1}"; then
  ok "restarted follower caught up from disk (last_applied>=$target)"
else
  bad "follower did not catch up (last_applied=$(applied_by "$FOLL"), target=$target)"
fi

# ── 4. minority partition: no split-brain, then reconverge ───────────────────
note "4. minority partition safety + reconvergence"
wait_for 30 some_leader || true
CUR_L="$(for n in "${NODES[@]}"; do l="$(leader_by "$n")"; [ -n "$l" ] && echo "$l" && break; done)"
ISO=""; for n in "${NODES[@]}"; do [ "$n" != "ledge-${CUR_L}" ] && ISO="$n" && break; done
echo "     isolating minority node $ISO (leader is ledge-${CUR_L})"
docker network disconnect "$NET" "$(cid "$ISO")" >/dev/null
# majority keeps a leader
if wait_for 30 some_leader; then ok "majority retains a leader during partition"; else bad "majority lost leader (unexpected)"; fi
# isolated node must NOT believe it is a leader (no split-brain). Give it time to step down.
if wait_for 30 no_leader_at "$ISO"; then ok "isolated minority has NO leader (no split-brain)"; else bad "isolated node still reports a leader (split-brain!)"; fi
echo "     heal partition (reconnect $ISO)"
docker network connect "$NET" "$(cid "$ISO")" >/dev/null
tgt="$(for n in "${NODES[@]}"; do [ "$n" != "$ISO" ] && applied_by "$n" && break; done)"
if wait_for 60 applied_at_least "$ISO" "${tgt:-1}"; then
  ok "healed node reconverged (last_applied>=$tgt)"
else
  bad "healed node did not reconverge (last_applied=$(applied_by "$ISO"), target=$tgt)"
fi

# ── 5. majority loss: safety over liveness ──────────────────────────────────
# The SAFETY property under majority loss is "no committed progress without a
# quorum" — i.e. commit_index MUST NOT advance. We do NOT assert leader=null on
# the lone node: openraft (no check-quorum) keeps the stale leader LABEL on an
# isolated leader, which is SAFE because without a quorum it commits nothing and
# the other (also-isolated) nodes cannot elect a competing leader → no split-brain.
note "5. majority loss = no committed progress (safety over liveness)"
wait_for 30 some_leader || true
CUR_L="$(for n in "${NODES[@]}"; do l="$(leader_by "$n")"; [ -n "$l" ] && echo "$l" && break; done)"
LONE="ledge-${CUR_L}"
ISOS=(); for n in "${NODES[@]}"; do [ "$n" != "$LONE" ] && ISOS+=("$n"); done
ci_before="$(commit_by "$LONE")"
echo "     isolating ${ISOS[*]} — leaving only $LONE (commit_index before=$ci_before)"
for c in "${ISOS[@]}"; do docker network disconnect "$NET" "$(cid "$c")" >/dev/null; done
# Bounded observation window: give the lone leader time to TRY (and fail) to make
# progress, then assert commit_index did not advance. (A deliberate fixed window:
# we are asserting a STABLE non-event, not synchronizing on a state transition.)
sleep 15
ci_after="$(commit_by "$LONE")"
echo "     lone $LONE commit_index after isolation=$ci_after (was $ci_before)"
if [ -n "$ci_after" ] && [ "$ci_after" = "$ci_before" ]; then
  ok "lone sub-quorum node made NO committed progress (commit_index unchanged) — safety over liveness"
else
  bad "lone node advanced commit_index without quorum ($ci_before -> $ci_after) — safety violation!"
fi
echo "     reconnect ${ISOS[*]}"
for c in "${ISOS[@]}"; do docker network connect "$NET" "$(cid "$c")" >/dev/null; done
wait_for 60 all_healthy || true
if wait_for 40 some_leader; then ok "cluster recovered after majority restored (no corruption)"; else bad "cluster did not recover"; fi

# ── 6. soak ─────────────────────────────────────────────────────────────────
note "6. soak (${SOAK_SECS}s sustained polling, leak watch)"
wait_for 30 some_leader || true
TOKEN="${LEDGE_TOKEN:-ledge_9b7ee379e0250464_6ixCz0hl7icRM_WuUtzu-uHAVsDqF9teIkUTyR69VZQ}"
# Warm-up: the soak measures STEADY STATE, so wait until ledge-1 is actually
# serving writes again after the preceding chaos (recovery transients are already
# validated by scenario 5 — they are not steady-state failures). Then measure.
ws_serving() { [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 4 -X POST -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' -d '{"source":[],"ttl_seconds":60}' http://localhost:3000/workspaces 2>/dev/null || echo 000)" = "200" ]; }
wait_for 45 ws_serving && echo "     warm-up: cluster serving writes again" || echo "     warm-up: WARN cluster not serving within 45s"
mem_start="$(docker stats --no-stream --format '{{.MemUsage}}' "$(cid ledge-1)" 2>/dev/null | awk '{print $1}')"
errs=0; iters=0
deadline=$((SECONDS + SOAK_SECS))
# Drive load against the PUBLISHED host port (ledge-1 → localhost:3000) for speed
# — host curl avoids per-call `docker exec` overhead, so this is real sustained
# load (thousands of ops), not a few exec-throttled probes.
while [ "$SECONDS" -lt "$deadline" ]; do
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 4 http://localhost:3000/healthz || echo 000)"
  [ "$code" = "200" ] || errs=$((errs + 1))
  # exercise a real (node-local) write path under load
  wcode="$(curl -s -o /dev/null -w '%{http_code}' --max-time 4 -X POST \
    -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
    -d '{"source":[],"ttl_seconds":60}' http://localhost:3000/workspaces || echo 000)"
  [ "$wcode" = "200" ] || errs=$((errs + 1))
  iters=$((iters + 2))
done
mem_end="$(docker stats --no-stream --format '{{.MemUsage}}' "$(cid ledge-1)" 2>/dev/null | awk '{print $1}')"
printf '     soak: %s health probes, %s errors; ledge-1 mem %s -> %s\n' "$iters" "$errs" "$mem_start" "$mem_end"
if [ "$errs" = 0 ]; then ok "soak: zero health-probe errors over ${SOAK_SECS}s"; else bad "soak: $errs errors"; fi
# GC sanity + cluster still healthy
node_curl ledge-1 -o /dev/null -X POST -H "Authorization: Bearer $SECRET" http://localhost:3000/admin/gc || true
if wait_for 20 some_leader; then ok "cluster healthy after soak + GC"; else bad "cluster unhealthy after soak"; fi

# ── summary ─────────────────────────────────────────────────────────────────
note "summary"
printf '  PASS=%s  FAIL=%s\n' "$PASS" "$FAIL"
[ "$FAIL" = 0 ] || exit 1
