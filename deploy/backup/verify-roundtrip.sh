#!/usr/bin/env bash
# Proves the backup/restore runbook end-to-end against a REAL ledge server and a
# REAL git client: push data, back it up (hot AND cold), wipe, restore into a
# fresh data dir, and assert the pushed ref comes back byte-identical.
#
#   bash deploy/backup/verify-roundtrip.sh
#
# Requires: a built `ledge` binary (release or debug) and `git` + `curl` + `jq`.
set -euo pipefail
cd "$(dirname "$0")/../.."
HERE=deploy/backup

BIN=${LEDGE_BIN:-}
[ -n "$BIN" ] || BIN=$(ls target/release/ledge target/debug/ledge 2>/dev/null | head -1) || true
[ -n "$BIN" ] && [ -x "$BIN" ] || { echo "no ledge binary; run 'cargo build -p ledge-server' first"; exit 1; }
for t in git curl jq tar; do command -v "$t" >/dev/null || { echo "missing tool: $t"; exit 1; }; done

WORK=$(mktemp -d)
CLIENT=127.0.0.1:38080
METRICS=127.0.0.1:39090
URL="http://$CLIENT"
PID=
export HOME="$WORK/home"; mkdir -p "$HOME"
export GIT_CONFIG_GLOBAL="$WORK/gitconfig" GIT_CONFIG_SYSTEM=/dev/null GIT_TERMINAL_PROMPT=0
git config --global user.email "t@ledge.test"
git config --global user.name "ledge test"
git config --global init.defaultBranch main
git config --global protocol.version 2

PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); echo "  PASS: $*"; }
bad(){ FAIL=$((FAIL+1)); echo "  FAIL: $*"; }

stop_node(){ [ -n "$PID" ] && kill "$PID" 2>/dev/null || true; [ -n "$PID" ] && wait "$PID" 2>/dev/null || true; PID=; }
cleanup(){ stop_node; rm -rf "$WORK"; }
trap cleanup EXIT

start_node(){ # $1 = data dir
  LEDGE__SERVER__ADDR="$CLIENT" LEDGE__METRICS__ADDR="$METRICS" \
    LEDGE__SERVER__DATA_DIR="$1" "$BIN" start >"$WORK/ledge.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 80); do
    curl -fsS "$URL/healthz" >/dev/null 2>&1 && return 0
    kill -0 "$PID" 2>/dev/null || { echo "server died on boot; log:"; tail -20 "$WORK/ledge.log"; return 1; }
    sleep 0.25
  done
  echo "server did not become healthy; log:"; tail -20 "$WORK/ledge.log"; return 1
}

remote_main(){ git ls-remote "$1" refs/heads/main 2>/dev/null | awk '{print $1}'; }

# ── 1. Seed: start on D1, create a workspace, push a commit ──────────────────
D1="$WORK/d1"
start_node "$D1" || exit 1
WID=$(curl -fsS -X POST "$URL/workspaces" -H 'content-type: application/json' \
        -d '{"source":[],"ttl_seconds":86400}' | jq -r .id)
[ -n "$WID" ] && [ "$WID" != null ] || { echo "workspace create failed"; exit 1; }
WS="$URL/ws/$WID"
R="$WORK/seed"; git clone -q "$WS" "$R" 2>/dev/null || git init -q "$R"
( cd "$R"; git remote add origin "$WS" 2>/dev/null || git remote set-url origin "$WS"
  echo "ledge backup roundtrip $(uname -s)" > data.txt
  git add data.txt; git commit -q -m "seed commit"
  git push -q origin HEAD:refs/heads/main )
SHA=$(cd "$R" && git rev-parse HEAD)
echo "seeded ws=$WID main=$SHA"
[ "$(remote_main "$WS")" = "$SHA" ] && ok "live server serves the pushed ref" || bad "live ref mismatch"

# ── 2. HOT backup (server still running) ────────────────────────────────────
bash "$HERE/backup.sh" --hot --url "$URL" --data-dir "$D1" --out "$WORK/hot.tar.gz"

# ── 3. Stop, then COLD backup ───────────────────────────────────────────────
stop_node
bash "$HERE/backup.sh" --data-dir "$D1" --out "$WORK/cold.tar.gz"

# ── 4. Restore + verify each backup into a FRESH data dir ───────────────────
verify_restore(){ # $1 = label, $2 = tarball
  local label=$1 tar=$2 dd="$WORK/restore_$1"
  bash "$HERE/restore.sh" --in "$tar" --data-dir "$dd"
  start_node "$dd" || { bad "$label: restored server failed to boot"; return; }
  local got; got=$(remote_main "$WS")
  [ "$got" = "$SHA" ] && ok "$label restore: ref matches ($got)" || bad "$label restore: got '$got' want '$SHA'"
  stop_node
}
verify_restore hot  "$WORK/hot.tar.gz"
verify_restore cold "$WORK/cold.tar.gz"

echo "── verify-roundtrip: $PASS passed, $FAIL failed ──"
[ "$FAIL" -eq 0 ]
