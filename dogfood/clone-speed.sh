#!/usr/bin/env bash
# Cold-clone eager warming — proof by measurement.
#
# Question: does precomputing the upload-pack response at write/boot time make
# the FIRST clone of a workspace fast (a cache hit) instead of an on-demand pack
# build? We measure the same ~2,200-object Ledge source three ways, all over
# HTTP (a fair same-transport comparison):
#
#   1. COLD  — import via sync-import (which does NOT warm), repack, then time
#              the FIRST clone. This is a single, inherently one-shot cold event
#              (the clone itself populates the cache as a side effect).
#   2. WARM  — restart the server (boot-warm repopulates the cache from the
#              packed store), then time the FIRST clone in the fresh process.
#              No clone has happened in this process — the hit is from boot-warm.
#   3. git   — `git daemon` over git:// for a same-machine baseline.
#
# Honest by construction: COLD is one shot because a cold first-clone only
# happens once; WARM/git are best-of-3.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/release/ledge"
PORT=3041
GIT_DAEMON_PORT=9419
BASE="http://127.0.0.1:$PORT"
DATA="$(mktemp -d)"
CLONES="$(mktemp -d)"
GITROOT="$(mktemp -d)"
RESULTS="$ROOT/dogfood/results/2026-06-12-cold-clone-warm.txt"
SRV_PID=""
DAEMON_PID=""

log() { echo "[clone-speed] $*"; }

cleanup() {
  [ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null || true
  [ -n "$DAEMON_PID" ] && kill "$DAEMON_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  rm -rf "$DATA" "$CLONES" "$GITROOT"
}
trap cleanup EXIT

start_server() {
  LEDGE__SERVER__ADDR="127.0.0.1:$PORT" \
  LEDGE__SERVER__DATA_DIR="$DATA" \
  LEDGE__SYNC__ENABLED=true \
  LEDGE__METRICS__ENABLED=false \
  "$BIN" start >/tmp/ledge-clonespeed.log 2>&1 &
  SRV_PID=$!
  for _ in $(seq 1 60); do
    if curl -fsS "$BASE/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  log "server did not become healthy"; tail -20 /tmp/ledge-clonespeed.log; exit 1
}

stop_server() {
  [ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null || true
  wait "$SRV_PID" 2>/dev/null || true
  SRV_PID=""
}

# best-of-N clone wall time (seconds, via /usr/bin/time -p). $1=url $2=runs
best_clone() {
  local url="$1" runs="$2" best="" t
  for i in $(seq 1 "$runs"); do
    local dest="$CLONES/c_${RANDOM}_$i"
    t=$( { /usr/bin/time -p git clone -q "$url" "$dest" ; } 2>&1 | awk '/^real/{print $2}' )
    rm -rf "$dest"
    if [ -z "$best" ] || awk "BEGIN{exit !($t < $best)}"; then best="$t"; fi
  done
  echo "$best"
}

# single timed clone that ALSO keeps the dir (for HEAD check). $1=url $2=dest
timed_clone() {
  local url="$1" dest="$2"
  { /usr/bin/time -p git clone -q "$url" "$dest" ; } 2>&1 | awk '/^real/{print $2}'
}

head_of() { git -C "$1" rev-parse HEAD; }

# ── 1. Boot, import the source (no warm), repack ──────────────────────────────
start_server
log "importing $ROOT via sync-import (no warm trigger)"
IMP=$(curl -fsS -X POST "$BASE/sync/import" \
  -H 'content-type: application/json' \
  -d "{\"upstream_url\":\"file://$ROOT\"}")
WS=$(echo "$IMP" | sed -n 's/.*"workspace_id":"\([^"]*\)".*/\1/p')
[ -n "$WS" ] || { log "import failed: $IMP"; exit 1; }
log "workspace=$WS"
log "repacking"
curl -fsS -X POST "$BASE/admin/repack" >/dev/null

# ── 2. COLD: first clone, cache empty (one-shot) ──────────────────────────────
COLD_DEST="$CLONES/cold"
COLD=$(timed_clone "$BASE/ws/$WS" "$COLD_DEST")
COLD_HEAD=$(head_of "$COLD_DEST")
log "COLD first clone: ${COLD}s  HEAD=$COLD_HEAD"

# ── 3. WARM: restart (boot-warm), first clone in the fresh process ────────────
stop_server
start_server
log "server restarted (boot-warm ran); timing first clone of a never-cloned process"
WARM_DEST="$CLONES/warm"
WARM_FIRST=$(timed_clone "$BASE/ws/$WS" "$WARM_DEST")
WARM_HEAD=$(head_of "$WARM_DEST")
rm -rf "$WARM_DEST"
WARM_BEST=$(best_clone "$BASE/ws/$WS" 3)
log "WARM first clone (boot-warmed): ${WARM_FIRST}s  best-of-3: ${WARM_BEST}s  HEAD=$WARM_HEAD"
stop_server

# ── 4. git daemon baseline (same machine, git:// transport) ───────────────────
git clone -q --bare "$ROOT" "$GITROOT/ledge.git"
git daemon --reuseaddr --listen=127.0.0.1 --port="$GIT_DAEMON_PORT" \
  --base-path="$GITROOT" --export-all >/tmp/ledge-gitdaemon.log 2>&1 &
DAEMON_PID=$!
sleep 1
GIT_BEST=$(best_clone "git://127.0.0.1:$GIT_DAEMON_PORT/ledge.git" 3)
GIT_HEAD=$(git -C "$GITROOT/ledge.git" rev-parse HEAD)
log "git over git:// best-of-3: ${GIT_BEST}s  HEAD=$GIT_HEAD"
kill "$DAEMON_PID" 2>/dev/null || true; DAEMON_PID=""

# ── 5. Assert + record ────────────────────────────────────────────────────────
PASS=0; FAIL=0
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1"; FAIL=$((FAIL+1)); fi; }

{
  echo "Ledge cold-clone eager warming — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "source: $ROOT (Ledge's own source)"
  echo
  echo "COLD  first clone (cache empty)          : ${COLD}s"
  echo "WARM  first clone (boot-warmed, one-shot): ${WARM_FIRST}s"
  echo "WARM  clone best-of-3                     : ${WARM_BEST}s"
  echo "git   clone best-of-3 (git://)           : ${GIT_BEST}s"
  echo
  echo "HEAD cold=$COLD_HEAD warm=$WARM_HEAD git=$GIT_HEAD"
  echo
  check "cold and warm clone the same HEAD"      "[ \"$COLD_HEAD\" = \"$WARM_HEAD\" ]"
  check "warm first clone is faster than cold"   "awk 'BEGIN{exit !($WARM_FIRST < $COLD)}'"
  check "warm first clone beats git cold clone"  "awk 'BEGIN{exit !($WARM_FIRST < $GIT_BEST)}'"
  echo
  echo "RESULT: $PASS PASS / $FAIL FAIL"
} | tee "$RESULTS"

[ "$FAIL" -eq 0 ]
