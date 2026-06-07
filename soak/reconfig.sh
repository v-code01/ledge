#!/usr/bin/env bash
# Phase 4g — live shard-membership reconfiguration harness over a REAL 4-node
# docker-compose cluster (separate processes/network/volumes). Proves add/remove
# of a shard's replicas at runtime via POST /cluster/{shard}/reconfigure, with the
# committed Raft log preserved. Poll-based (no fixed-sleep sync); exits non-zero on
# any failed assertion.
#
# Scenarios:
#   0. init {1,2,3} (node 4 pre-provisioned but NOT a voter) → leader elected
#   1. GROW: reconfigure → {1,2,3,4}; node 4 becomes a voter AND catches up from
#      the Raft log (last_applied converges) — live add, data-complete
#   2. SHRINK: reconfigure → {1,2,3}; node 4 dropped, cluster still serves, no
#      committed-log regression — live remove
# (Replacing a different node is the same mechanism; removing the CURRENT leader
#  triggers an openraft leadership transfer — handled by openraft, avoided here for
#  determinism. Honest: see soak/README.md.)
#
# Ceiling: ONE physical host (real processes/net, not multi-machine). See README.
#
# Usage: bash soak/reconfig.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$HERE/../deploy/compose/docker-compose.reconfig.yml}"
DC=(docker compose -f "$COMPOSE_FILE")
SECRET="${LEDGE_CLUSTER_SECRET:-dev-cluster-secret-change-me}"

PASS=0; FAIL=0
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$1"; FAIL=$((FAIL + 1)); }
note() { printf '\n=== %s ===\n' "$1"; }

# curl inside node N's container (rides docker, not the cluster network).
ncurl() { local n="$1"; shift; "${DC[@]}" exec -T "ledge-$n" curl -s --max-time 6 "$@" 2>/dev/null || true; }
sfield() { # sfield <node> <jq-path-after .shards[0].>
  local body; body="$(ncurl "$1" -H "Authorization: Bearer $SECRET" http://localhost:3000/cluster/status)"
  [ -z "$body" ] && { echo ""; return; }
  echo "$body" | jq -r ".shards[0].$2 // empty" 2>/dev/null || echo ""
}
leader()       { sfield "$1" leader; }
applied()      { sfield "$1" last_applied; }
voters_csv()   { local b; b="$(ncurl "$1" -H "Authorization: Bearer $SECRET" http://localhost:3000/cluster/status)"; [ -z "$b" ] && { echo ""; return; }; echo "$b" | jq -r '.shards[0].voters | sort | join(",")' 2>/dev/null || echo ""; }

wait_for() { local budget="$1"; shift; local end=$((SECONDS + budget)); while [ "$SECONDS" -lt "$end" ]; do if "$@"; then return 0; fi; sleep 1; done; return 1; }
all_healthy() { [ "$("${DC[@]}" ps --format '{{.Health}}' 2>/dev/null | grep -c healthy)" = "4" ]; }
some_leader() { local n; for n in 1 2 3; do [ -n "$(leader "$n")" ] && return 0; done; return 1; }
cur_leader()  { local n l; for n in 1 2 3 4; do l="$(leader "$n")"; [ -n "$l" ] && { echo "$l"; return; }; done; }
voters_eq()   { [ "$(voters_csv "$1")" = "$2" ]; }
voters_has4() { case ",$(voters_csv "$1")," in *,4,*) return 0;; *) return 1;; esac; }
applied_ge()  { local v; v="$(applied "$1")"; [ -n "$v" ] && [ "$v" -ge "$2" ] 2>/dev/null; }

reconfigure() { # reconfigure <leader_node> <members-json>
  ncurl "$1" -X POST -H "Authorization: Bearer $SECRET" -H 'content-type: application/json' \
    -d "{\"members\":$2}" -w '%{http_code}' http://localhost:3000/cluster/0/reconfigure
}

cleanup() { note "teardown"; "${DC[@]}" down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

note "bring up 4-node cluster (node 4 pre-provisioned, not yet a voter)"
"${DC[@]}" up -d >/dev/null 2>&1
if wait_for 60 all_healthy; then ok "4 nodes healthy"; else bad "nodes not healthy"; exit 1; fi

note "0. init shard 0 with voters {1,2,3}"
ncurl 1 -X POST -H "Authorization: Bearer $SECRET" -H 'content-type: application/json' \
  -d '{"shard":0,"members":{"1":"http://ledge-1:3000","2":"http://ledge-2:3000","3":"http://ledge-3:3000"}}' \
  http://localhost:3000/cluster/init >/dev/null
if wait_for 30 some_leader; then ok "leader elected"; else bad "no leader after init"; exit 1; fi
L="$(cur_leader)"; echo "     leader=ledge-$L; voters(leader)=$(voters_csv "$L")"
if [ "$(voters_csv "$L")" = "1,2,3" ]; then ok "baseline voters = {1,2,3}"; else bad "baseline voters = $(voters_csv "$L") (want 1,2,3)"; fi

note "1. GROW → {1,2,3,4} (live add node 4)"
L="$(cur_leader)"; base_applied="$(applied "$L")"
echo "     POST reconfigure to leader ledge-$L (leader last_applied=$base_applied)"
rc="$(reconfigure "$L" '{"1":"http://ledge-1:3000","2":"http://ledge-2:3000","3":"http://ledge-3:3000","4":"http://ledge-4:3000"}' )"
echo "     reconfigure HTTP $rc"
case "$rc" in *200) ok "reconfigure(grow) accepted (200)";; *) bad "reconfigure(grow) HTTP $rc";; esac
if wait_for 45 voters_has4 "$(cur_leader)"; then ok "node 4 is now a VOTER (live add)"; else bad "node 4 not a voter after grow (voters=$(voters_csv "$(cur_leader)"))"; fi
if wait_for 45 applied_ge 4 "${base_applied:-1}"; then ok "node 4 caught up from Raft log (last_applied>=$base_applied)"; else bad "node 4 did not catch up (last_applied=$(applied 4), target=$base_applied)"; fi
echo "     voters now: $(voters_csv "$(cur_leader)")"

note "2. SHRINK → {1,2,3} (live remove node 4)"
L="$(cur_leader)"; pre_applied="$(applied "$L")"
echo "     POST reconfigure to leader ledge-$L"
rc="$(reconfigure "$L" '{"1":"http://ledge-1:3000","2":"http://ledge-2:3000","3":"http://ledge-3:3000"}' )"
echo "     reconfigure HTTP $rc"
case "$rc" in *200) ok "reconfigure(shrink) accepted (200)";; *) bad "reconfigure(shrink) HTTP $rc";; esac
if wait_for 45 voters_eq "$(cur_leader)" "1,2,3"; then ok "node 4 removed; voters = {1,2,3}"; else bad "voters after shrink = $(voters_csv "$(cur_leader)") (want 1,2,3)"; fi
# committed log must not regress + cluster still serves
L="$(cur_leader)"; post_applied="$(applied "$L")"
if [ -n "$post_applied" ] && [ "$post_applied" -ge "${pre_applied:-1}" ] 2>/dev/null; then ok "committed log did not regress across shrink (last_applied $pre_applied -> $post_applied)"; else bad "committed log regressed ($pre_applied -> $post_applied)"; fi
if wait_for 20 some_leader; then ok "cluster still serving (leader present) after shrink"; else bad "no leader after shrink"; fi

note "summary"
printf '  PASS=%s  FAIL=%s\n' "$PASS" "$FAIL"
[ "$FAIL" = 0 ] || exit 1
