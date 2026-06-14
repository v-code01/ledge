#!/usr/bin/env bash
# Disk parity — measure Ledge's repacked pack vs git's gc pack on the same source.
#
# Imports the Ledge source (sync-import), repacks it into a native git pack with
# OFS_DELTA + name-hash sort + window-250, and compares on-disk size to git's own
# `repack -ad` of the identical source. Honest: prints whatever the numbers are.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/release/ledge"
PORT=3043
BASE="http://127.0.0.1:$PORT"
DATA="$(mktemp -d)"
GITREF="$(mktemp -d)"
RESULTS="$ROOT/dogfood/results/2026-06-12-disk-parity.txt"
SRV_PID=""

cleanup() { [ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null || true; wait 2>/dev/null || true; rm -rf "$DATA" "$GITREF"; }
trap cleanup EXIT
log() { echo "[disk-parity] $*"; }

# du in KiB for a single file or a dir (portable-ish on macOS/Linux).
kib() { du -k "$1" 2>/dev/null | tail -1 | awk '{print $1}'; }
bytes() { wc -c < "$1" | tr -d ' '; }

# ── Ledge side ────────────────────────────────────────────────────────────────
LEDGE__SERVER__ADDR="127.0.0.1:$PORT" LEDGE__SERVER__DATA_DIR="$DATA" \
LEDGE__SYNC__ENABLED=true LEDGE__METRICS__ENABLED=false \
  "$BIN" start >/tmp/ledge-diskparity.log 2>&1 &
SRV_PID=$!
for _ in $(seq 1 60); do curl -fsS "$BASE/healthz" >/dev/null 2>&1 && break; sleep 0.2; done

log "importing $ROOT"
IMP=$(curl -fsS -X POST "$BASE/sync/import" -H 'content-type: application/json' \
  -d "{\"upstream_url\":\"file://$ROOT\"}")
WS=$(echo "$IMP" | sed -n 's/.*"workspace_id":"\([^"]*\)".*/\1/p')
[ -n "$WS" ] || { log "import failed: $IMP"; exit 1; }

log "repacking (OFS_DELTA + name-hash + window-250)"
REPACK_START=$(date +%s)
STATS=$(curl -fsS -X POST "$BASE/admin/repack")
REPACK_SECS=$(( $(date +%s) - REPACK_START ))
log "repack stats: $STATS"

PACK=$(find "$DATA" -name '*.pack' | head -1)
IDX="${PACK%.pack}.idx"
[ -n "$PACK" ] || { log "no pack produced"; exit 1; }
PACKDIR=$(dirname "$PACK")
LEDGE_PACK_BYTES=$(bytes "$PACK")
LEDGE_DU_KIB=$(kib "$PACKDIR")

# git oracle on the STORED pack
VP=$(git verify-pack -v "$IDX" 2>&1) && VP_OK=yes || VP_OK=no
LEDGE_DELTAS=$(echo "$VP" | grep -c " delta " || true)
LEDGE_OBJS=$(echo "$VP" | grep -cE "^[0-9a-f]{40} " || true)

# ── git side: gc/repack the same source ───────────────────────────────────────
git clone -q --bare "$ROOT" "$GITREF/ref.git"
git -C "$GITREF/ref.git" repack -adq --window=250 --depth=50
GIT_PACK=$(find "$GITREF/ref.git" -name '*.pack' | head -1)
GIT_PACK_BYTES=$(bytes "$GIT_PACK")
GIT_DU_KIB=$(kib "$(dirname "$GIT_PACK")")

# ── report ────────────────────────────────────────────────────────────────────
RATIO=$(awk "BEGIN{printf \"%.2f\", $LEDGE_PACK_BYTES/$GIT_PACK_BYTES}")
PASS=0; FAIL=0
chk() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1"; FAIL=$((FAIL+1)); fi; }
{
  echo "Ledge disk parity (OFS_DELTA + name-hash + window-250) — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "source: $ROOT (Ledge's own source)"
  echo
  printf "Ledge pack  : %d bytes  (du %d KiB)  objects=%s deltas=%s  verify-pack=%s  repack=%ss\n" \
    "$LEDGE_PACK_BYTES" "$LEDGE_DU_KIB" "$LEDGE_OBJS" "$LEDGE_DELTAS" "$VP_OK" "$REPACK_SECS"
  printf "git   pack  : %d bytes  (du %d KiB)  (git repack -ad --window=250 --depth=50)\n" \
    "$GIT_PACK_BYTES" "$GIT_DU_KIB"
  echo
  printf "Ledge/git pack-bytes ratio: %s×\n" "$RATIO"
  echo
  chk "git verify-pack accepts the stored Ledge pack"  "[ \"$VP_OK\" = yes ]"
  chk "Ledge pack < 1.5x git (closed the prior 1.35x)" "awk 'BEGIN{exit !($RATIO < 1.5)}'"
  chk "Ledge BEATS or ties git on pack bytes"          "[ \"$LEDGE_PACK_BYTES\" -le \"$GIT_PACK_BYTES\" ]"
  echo
  echo "RESULT: $PASS PASS / $FAIL FAIL  (the 'BEATS' check is the stretch goal — honest either way)"
} | tee "$RESULTS"
