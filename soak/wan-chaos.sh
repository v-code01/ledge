#!/usr/bin/env bash
# Emulated-WAN + clock-skew chaos harness.
#
# Extends the Phase 4f single-host soak (which did CLEAN partitions on a real
# multi-process cluster) with the two conditions that suite explicitly deferred:
# DEGRADED networks (latency / jitter / loss / reordering / asymmetric partition,
# via `tc netem`) and CLOCK SKEW (per-node CLOCK_REALTIME offset via libfaketime
# — +5s / -7s — skewing HLC wall time while CLOCK_MONOTONIC, and thus the Raft /
# tokio timers, stay real).
#
# It asserts consensus-layer invariants under each condition via /cluster/status
# (leader / term / commit_index / last_applied), and proves a real git push
# replicates with no data loss while clocks disagree and the link is lossy+slow.
#
# HONEST CEILING (unchanged from 4f, stated again): ONE physical host. The
# kernel and the monotonic clock are shared; netem emulates WAN conditions and
# libfaketime emulates wall-clock disagreement, but this is NOT geographically
# separate hardware. It closes "clean partitions only / no skew"; it does not
# substitute for a real multi-machine deployment. See soak/README.md.
#
# Usage:  bash soak/wan-chaos.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$HERE/../deploy/compose/docker-compose.soak.yml}"
DC=(docker compose -f "$COMPOSE_FILE")
SECRET="${LEDGE_CLUSTER_SECRET:-dev-cluster-secret-change-me}"      # INTERNAL (/cluster/*)
ADMIN="${LEDGE_ADMIN_TOKEN:-ledge_9b7ee379e0250464_6ixCz0hl7icRM_WuUtzu-uHAVsDqF9teIkUTyR69VZQ}" # CLIENT
NODES=(ledge-1 ledge-2 ledge-3)
ENTRY="http://localhost:3000"

PASS=0; FAIL=0
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$1"; FAIL=$((FAIL + 1)); }
note() { printf '\n=== %s ===\n' "$1"; }

cleanup() {
  note "teardown"
  for n in "${NODES[@]}"; do "${DC[@]}" exec -u 0 -T "$n" tc qdisc del dev eth0 root 2>/dev/null || true; done
  "${DC[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ── observability helpers (mirror soak/chaos.sh) ──────────────────────────────
node_curl() { local n="$1"; shift; "${DC[@]}" exec -T "$n" curl -s --max-time 5 "$@" 2>/dev/null || true; }
status_field() {
  local n="$1" path="$2" body
  body="$(node_curl "$n" -H "Authorization: Bearer $SECRET" http://localhost:3000/cluster/status)"
  [ -z "$body" ] && { echo ""; return; }
  echo "$body" | jq -r ".shards[0].${path} // empty" 2>/dev/null || echo ""
}
leader_by() { status_field "$1" leader; }
commit_by() { status_field "$1" commit_index; }
applied_by() { status_field "$1" last_applied; }
all_healthy() { for n in "${NODES[@]}"; do "${DC[@]}" exec -T "$n" curl -fsS --max-time 3 http://localhost:9090/healthz >/dev/null 2>&1 || return 1; done; }
some_leader() { for n in "${NODES[@]}"; do [ -n "$(leader_by "$n")" ] && return 0; done; return 1; }
wait_for() { local to="$1" fn="$2" t=0; while [ "$t" -lt "$to" ]; do "$fn" && return 0; sleep 1; t=$((t+1)); done; return 1; }

# netem helpers (run tc as root via exec -u 0; cap_add NET_ADMIN in compose)
netem_on()    { "${DC[@]}" exec -u 0 -T "$1" tc qdisc add dev eth0 root netem "${@:2}" 2>&1 | tail -1; }
netem_clear() { "${DC[@]}" exec -u 0 -T "$1" tc qdisc del dev eth0 root 2>/dev/null || true; }
netem_all()   { for n in "${NODES[@]}"; do netem_on "$n" "$@" >/dev/null; done; }
netem_clear_all() { for n in "${NODES[@]}"; do netem_clear "$n"; done; }

# leader stays a single agreed value across a poll window (no churn / split-brain)
majority_single_leader() {
  # among reachable nodes, collect distinct non-empty leader ids; want exactly 1
  local seen="" n l
  for n in "${NODES[@]}"; do l="$(leader_by "$n")"; [ -n "$l" ] && seen="$seen $l"; done
  echo "$seen" | tr ' ' '\n' | sort -u | grep -c . | grep -qx 1
}

# ── bring up + init ───────────────────────────────────────────────────────────
note "bring up skewed soak cluster (ledge:soak; n2 +5s, n3 -7s)"
# Clean any prior state first so back-to-back runs never race a lingering
# teardown (a leftover volume/container can make `up` no-op into an unhealthy set).
"${DC[@]}" down -v >/dev/null 2>&1 || true
"${DC[@]}" up -d >/dev/null 2>&1 || true
if wait_for 90 all_healthy; then ok "3 nodes healthy"; else bad "nodes not healthy"; exit 1; fi
node_curl ledge-1 -X POST -H "Authorization: Bearer $SECRET" -H 'content-type: application/json' \
  -d '{"shard":0,"members":{"1":"http://ledge-1:3000","2":"http://ledge-2:3000","3":"http://ledge-3:3000"}}' \
  http://localhost:3000/cluster/init >/dev/null
if wait_for 30 some_leader; then ok "leader elected under clock skew"; else bad "no leader after init"; exit 1; fi

# ── 1. clock skew is REAL + baseline replication ──────────────────────────────
note "1. clock skew verified + baseline consensus under skew"
HOST="$(date +%s)"
S2="$("${DC[@]}" exec -T ledge-2 date +%s)"; S3="$("${DC[@]}" exec -T ledge-3 date +%s)"
printf '     host=%s  ledge-2=%s (%+ds)  ledge-3=%s (%+ds)\n' "$HOST" "$S2" "$((S2-HOST))" "$S3" "$((S3-HOST))"
if [ "$((S2-HOST))" -ge 3 ] && [ "$((HOST-S3))" -ge 3 ]; then ok "nodes run on genuinely skewed wall clocks (+/- seconds)"; else bad "skew not applied"; fi
base=1
for n in "${NODES[@]}"; do
  ci="$(commit_by "$n")"; la="$(applied_by "$n")"
  printf '     %s: leader=%s commit=%s applied=%s\n' "$n" "$(leader_by "$n")" "$ci" "$la"
  { [ "${ci:-0}" -ge 1 ] && [ "${la:-0}" -ge 1 ]; } 2>/dev/null || base=0
done
[ "$base" = 1 ] && ok "membership committed+applied on all 3 despite skew" || bad "baseline replication under skew"

# ── 2. real git push replicates with NO DATA LOSS under skew + latency ─────────
note "2. data durability: git push under skew + 80ms latency, clone-back identical"
netem_all delay 80ms 20ms
CI_BEFORE="$(commit_by ledge-1)"
WS_JSON="$(curl -s -X POST -H "Authorization: Bearer $ADMIN" -H 'content-type: application/json' \
  -d '{"source":[],"ttl_seconds":3600}' "$ENTRY/workspaces" || true)"
WS="$(printf '%s' "$WS_JSON" | jq -r '.id // .workspace_id // empty' 2>/dev/null || true)"
if [ -z "$WS" ]; then bad "workspace create failed: ${WS_JSON:0:120}"; fi
TMP="$(mktemp -d)"; SRC="$TMP/src"; mkdir -p "$SRC"
( cd "$SRC" && git init -q -b main && git config user.email t@l && git config user.name t \
   && echo one > a.txt && git add . && git commit -qm c1 \
   && echo two >> a.txt && git add . && git commit -qm c2 \
   && git -c http.extraHeader="Authorization: Bearer $ADMIN" push -q "$ENTRY/ws/$WS/" HEAD:refs/heads/main ) \
   && PUSH_OK=1 || PUSH_OK=0
SRC_HEAD="$( cd "$SRC" && git rev-parse HEAD )"
CLONE="$TMP/clone"
git -c http.extraHeader="Authorization: Bearer $ADMIN" clone -q "$ENTRY/ws/$WS" "$CLONE" 2>/dev/null && CLONE_HEAD="$( cd "$CLONE" && git rev-parse HEAD )" || CLONE_HEAD="(clone failed)"
sleep 2; CI_AFTER="$(commit_by ledge-1)"
printf '     ws=%s push_ok=%s commit %s->%s  src=%s clone=%s\n' "$WS" "$PUSH_OK" "$CI_BEFORE" "$CI_AFTER" "${SRC_HEAD:0:12}" "${CLONE_HEAD:0:12}"
[ "$PUSH_OK" = 1 ] && [ "$SRC_HEAD" = "$CLONE_HEAD" ] && ok "git push replicated + cloned back byte-identical under skew+latency" || bad "data round-trip under skew+latency"
# all replicas converge on the advanced commit index
conv=1; for n in "${NODES[@]}"; do [ "$(commit_by "$n")" -ge "${CI_AFTER:-0}" ] 2>/dev/null || conv=0; done
[ "$conv" = 1 ] && ok "all 3 replicas converged to the post-push commit index" || bad "replica convergence after push"
rm -rf "$TMP"; netem_clear_all

# ── 3. latency + jitter: leader stays stable, no regression ───────────────────
note "3. latency+jitter (120ms +/-40ms, all links): leader stable, no commit regression"
CI3="$(commit_by ledge-1)"; L3="$(leader_by ledge-1)"
netem_all delay 120ms 40ms distribution normal
stable=1; for _ in $(seq 1 8); do sleep 1; [ "$(leader_by ledge-1)" = "$L3" ] || stable=0; done
CI3b="$(commit_by ledge-1)"
printf '     leader held=%s (%s)  commit %s->%s\n' "$stable" "$L3" "$CI3" "$CI3b"
[ "$stable" = 1 ] && ok "leader did not churn under latency+jitter" || bad "leader churned under latency"
[ "${CI3b:-0}" -ge "${CI3:-0}" ] 2>/dev/null && ok "commit index did not regress under latency" || bad "commit regressed under latency"
netem_clear_all

# ── 4. packet loss: converges to a single leader, no regression ───────────────
note "4. packet loss (12% all links): single leader, no commit regression"
CI4="$(commit_by ledge-1)"
netem_all loss 12%
if wait_for 25 majority_single_leader; then ok "cluster holds exactly one leader under 12% loss"; else bad "no single leader under loss"; fi
netem_clear_all
if wait_for 20 majority_single_leader; then ok "single leader after loss cleared"; else bad "no reconverge after loss"; fi
[ "$(commit_by ledge-1)" -ge "${CI4:-0}" ] 2>/dev/null && ok "no commit regression across loss episode" || bad "commit regressed across loss"

# ── 5. reordering: consistency maintained ─────────────────────────────────────
note "5. packet reordering (delay 60ms, 30% reordered): stays consistent"
netem_all delay 60ms reorder 30% 50%
if wait_for 20 majority_single_leader; then ok "single leader under reordering"; else bad "leader lost under reordering"; fi
netem_clear_all

# ── 6. asymmetric partition: isolate the leader's egress, majority re-elects ───
note "6. asymmetric partition (leader egress 100% loss): majority re-elects, no split-brain"
LEAD_ID="$(leader_by ledge-1)"; LEAD="ledge-${LEAD_ID}"; CI6="$(commit_by ledge-1)"
# pick a majority node that is NOT the leader to observe from
OBS=""; for n in "${NODES[@]}"; do [ "$n" != "$LEAD" ] && OBS="$n" && break; done
printf '     isolating %s (was leader); observing from %s\n' "$LEAD" "$OBS"
netem_on "$LEAD" loss 100% >/dev/null
new_leader() { local l; l="$(leader_by "$OBS")"; [ -n "$l" ] && [ "$l" != "$LEAD_ID" ]; }
if wait_for 30 new_leader; then ok "majority elected a NEW leader (!= isolated old leader)"; else bad "majority did not re-elect"; fi
# the majority's two reachable nodes must agree on that new leader (no split-brain in the partition)
NL="$(leader_by "$OBS")"; agree=1
for n in "${NODES[@]}"; do [ "$n" = "$LEAD" ] && continue; [ "$(leader_by "$n")" = "$NL" ] || agree=0; done
[ "$agree" = 1 ] && ok "the reachable majority agrees on one leader (no split-brain)" || bad "majority disagreed on leader"
netem_clear "$LEAD"
if wait_for 30 majority_single_leader; then ok "cluster reconverged to a single leader after heal" || true; else bad "no reconverge after heal"; fi
[ "$(commit_by "$OBS")" -ge "${CI6:-0}" ] 2>/dev/null && ok "no committed-log regression across the partition" || bad "commit regressed across partition"

# ── summary ───────────────────────────────────────────────────────────────────
note "summary"
printf 'RESULT: %d PASS / %d FAIL\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
