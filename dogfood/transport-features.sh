#!/usr/bin/env bash
# Transport-feature proof: one command that demonstrates the object-transfer
# savings of Ledge's git-protocol features against a real `git` client.
#
#   - incremental fetch (have-line negotiation): a fetch after one new commit
#     transfers ~3 objects, not the whole history.
#   - shallow clone (--depth N): exactly N commits.
#   - partial clone (--filter=blob:none): a blobless initial pack, blobs
#     lazily fetched on checkout.
#
# Complements clone-speed.sh (latency) and disk-parity.sh (pack size). Honest:
# measured against the running server with a real git client; counts come from
# `git count-objects` / `rev-list`, not from the server's own reports.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/release/ledge"
PORT=3071
BASE="http://127.0.0.1:$PORT"
DATA="$(mktemp -d)"
WORK="$(mktemp -d)"
RESULTS="$ROOT/dogfood/results/2026-06-15-transport-features.txt"
SRV=""
cleanup() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true; wait 2>/dev/null || true; rm -rf "$DATA" "$WORK"; }
trap cleanup EXIT
log() { echo "[transport] $*"; }
objtotal() { git -C "$1" count-objects -v | awk '/^count:|^in-pack:/{s+=$2} END{print s}'; }

[ -x "$BIN" ] || { echo "build first: cargo build --release -p ledge-server"; exit 1; }

LEDGE__SERVER__ADDR="127.0.0.1:$PORT" LEDGE__SERVER__DATA_DIR="$DATA" LEDGE__METRICS__ENABLED=false \
  "$BIN" start >/tmp/ledge-transport.log 2>&1 &
SRV=$!
for _ in $(seq 1 60); do curl -fsS "$BASE/healthz" >/dev/null 2>&1 && break; sleep 0.2; done

# A 20-commit source repo.
SRC="$WORK/src"; mkdir -p "$SRC"
git -C "$SRC" init -q -b main; git -C "$SRC" config user.email t@l; git -C "$SRC" config user.name t
for i in $(seq 0 19); do echo "line $i" > "$SRC/f.txt"; git -C "$SRC" add .; git -C "$SRC" commit -qm "c$i"; done
git -C "$SRC" push -q "$BASE/feat" main:refs/heads/main
TOTAL_COMMITS=$(git -C "$SRC" rev-list --count HEAD)

PASS=0; FAIL=0
chk() { if eval "$2"; then echo "  PASS: $1"; PASS=$((PASS+1)); else echo "  FAIL: $1"; FAIL=$((FAIL+1)); fi; }

# ── incremental fetch ─────────────────────────────────────────────────────────
FULL="$WORK/full"; git clone -q "$BASE/feat" "$FULL"
FULL_OBJS=$(objtotal "$FULL")
echo "newline" > "$SRC/f.txt"; git -C "$SRC" add .; git -C "$SRC" commit -qm c-new
git -C "$SRC" push -q "$BASE/feat" main:refs/heads/main
BEFORE=$(objtotal "$FULL"); git -C "$FULL" fetch -q origin; AFTER=$(objtotal "$FULL")
FETCH_DELTA=$((AFTER-BEFORE))

# ── shallow clone ─────────────────────────────────────────────────────────────
SH="$WORK/shallow"; git clone --depth 1 -q "$BASE/feat" "$SH"
SH_COMMITS=$(git -C "$SH" rev-list --count HEAD)

# ── partial clone ─────────────────────────────────────────────────────────────
PC="$WORK/partial"; git clone --filter=blob:none -q "$BASE/feat" "$PC"
PC_PROMISOR=$(git -C "$PC" config remote.origin.promisor 2>/dev/null || echo false)
# Checkout lazily fetched the CURRENT blob (working tree is correct); the older
# historical versions of f.txt stay missing on purpose — that IS partial clone.
PC_HEAD_OK=$([ "$(cat "$PC/f.txt" 2>/dev/null)" = "newline" ] && echo yes || echo no)
PC_MISSING=$(git -C "$PC" rev-list --objects --all --missing=print 2>/dev/null | grep -c '^?' || true)

{
  echo "Ledge transport-feature proof — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "source: a ${TOTAL_COMMITS}-commit repo (full clone holds $FULL_OBJS objects)"
  echo
  printf "incremental fetch (1 new commit): transferred %d objects (vs %d full history)\n" "$FETCH_DELTA" "$FULL_OBJS"
  printf "shallow clone (--depth 1)       : %d commit(s)\n" "$SH_COMMITS"
  printf "partial clone (--filter=blob:none): promisor=%s, working-tree-ok=%s, historical-blobs-lazy=%d\n" "$PC_PROMISOR" "$PC_HEAD_OK" "$PC_MISSING"
  echo
  chk "incremental fetch moved only the new objects (<=8, not the history)" "[ \"$FETCH_DELTA\" -le 8 ] && [ \"$FETCH_DELTA\" -ge 1 ]"
  chk "shallow --depth 1 yielded exactly 1 commit"                          "[ \"$SH_COMMITS\" = 1 ]"
  chk "partial clone is a promisor repo"                                    "[ \"$PC_PROMISOR\" = true ]"
  chk "partial clone checkout lazily fetched the needed blob (working tree correct)" "[ \"$PC_HEAD_OK\" = yes ]"
  chk "partial clone left historical blobs unfetched (genuinely partial)"   "[ \"$PC_MISSING\" -ge 1 ]"
  echo
  echo "RESULT: $PASS PASS / $FAIL FAIL"
} | tee "$RESULTS"

[ "$FAIL" -eq 0 ]
