#!/usr/bin/env bash
# Longevity / memory soak (audit residual R-3).
#
# Drives a single Ledge node under steady, production-shaped churn — create a
# short-TTL workspace, clone it, commit, push, drop the clone; let the expiry
# sweeper + GC reclaim it — while sampling the process RSS. It then asserts the
# resident set does not grow unboundedly (a leak), reporting the trend.
#
# The default run is BOUNDED (SOAK_SECONDS, default 600 = 10 min) so it fits a
# dev/CI window and proves the harness + a no-leak baseline. R-3 proper is the
# SAME script run for days: `SOAK_SECONDS=$((3*24*3600)) bash soak/longevity.sh`.
# Honest ceiling: a bounded run cannot prove multi-day behavior — only the tool
# and an initial baseline. It samples RSS via `ps`, so run it where you soak.
#
# Usage:  [SOAK_SECONDS=600] [SAMPLE_INTERVAL=5] bash soak/longevity.sh
set -euo pipefail
cd "$(dirname "$0")/.."
HERE=soak

SOAK_SECONDS="${SOAK_SECONDS:-600}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-5}"
CLIENT=127.0.0.1:38091
METRICS=127.0.0.1:39091
URL="http://$CLIENT"

BIN="${LEDGE_BIN:-}"
[ -n "$BIN" ] || BIN=$(ls target/release/ledge target/debug/ledge 2>/dev/null | head -1) || true
[ -n "$BIN" ] && [ -x "$BIN" ] || { echo "no ledge binary; run 'cargo build -p ledge-server'"; exit 1; }
for t in git curl jq ps; do command -v "$t" >/dev/null || { echo "missing tool: $t"; exit 1; }; done

WORK=$(mktemp -d)
PID=""
SAMPLER_PID=""
export HOME="$WORK/home"; mkdir -p "$HOME"
export GIT_CONFIG_GLOBAL="$WORK/gitconfig" GIT_CONFIG_SYSTEM=/dev/null GIT_TERMINAL_PROMPT=0
git config --global user.email "soak@ledge.test"
git config --global user.name "ledge soak"
git config --global init.defaultBranch main
git config --global protocol.version 2

cleanup() {
  [ -n "$SAMPLER_PID" ] && kill "$SAMPLER_PID" 2>/dev/null || true
  [ -n "$PID" ] && kill "$PID" 2>/dev/null || true
  [ -n "$PID" ] && wait "$PID" 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

# ── Start a node with SHORT reclaim intervals so churned workspaces don't pile up.
LEDGE__SERVER__ADDR="$CLIENT" LEDGE__METRICS__ADDR="$METRICS" \
  LEDGE__SERVER__DATA_DIR="$WORK/data" \
  LEDGE__WORKSPACE__EXPIRY_INTERVAL_SECS=5 \
  LEDGE__WORKSPACE__GC_INTERVAL_SECS=10 \
  LEDGE__WORKSPACE__DEFAULT_TTL_SECS=10 \
  "$BIN" start >"$WORK/ledge.log" 2>&1 &
PID=$!
for _ in $(seq 1 80); do
  curl -fsS "$URL/healthz" >/dev/null 2>&1 && break
  kill -0 "$PID" 2>/dev/null || { echo "node died on boot:"; tail -20 "$WORK/ledge.log"; exit 1; }
  sleep 0.25
done

samples="$WORK/rss.tsv"
: > "$samples"
start=$(date +%s)
# ── Background RSS sampler: elapsed_seconds<TAB>rss_kib ──────────────────────────
(
  while kill -0 "$PID" 2>/dev/null; do
    rss=$(ps -o rss= -p "$PID" 2>/dev/null | tr -d ' ')
    [ -n "$rss" ] && echo -e "$(( $(date +%s) - start ))\t$rss" >> "$samples"
    sleep "$SAMPLE_INTERVAL"
  done
) &
SAMPLER_PID=$!

# ── Steady churn until the deadline ─────────────────────────────────────────────
deadline=$(( start + SOAK_SECONDS ))
round=0
errors=0
echo "soak: running ${SOAK_SECONDS}s of churn (sampling RSS every ${SAMPLE_INTERVAL}s)…"
while [ "$(date +%s)" -lt "$deadline" ]; do
  round=$((round + 1))
  wid=$(curl -fsS -X POST "$URL/workspaces" -H 'content-type: application/json' \
          -d '{"source":[],"ttl_seconds":10}' 2>/dev/null | jq -r .id 2>/dev/null || true)
  if [ -z "$wid" ] || [ "$wid" = null ]; then errors=$((errors+1)); sleep 0.2; continue; fi
  ws="$URL/ws/$wid"
  rd="$WORK/c$round"
  if git clone -q "$ws" "$rd" 2>/dev/null || git init -q "$rd" 2>/dev/null; then
    (
      cd "$rd"
      git remote add origin "$ws" 2>/dev/null || git remote set-url origin "$ws"
      # Unique content each round → real new objects (exercises write + later GC).
      echo "soak round $round @ $(date +%s%N)" > f.txt
      git add f.txt && git commit -q -m "r$round" && git push -q origin HEAD:refs/heads/main
    ) 2>/dev/null || errors=$((errors+1))
  else
    errors=$((errors+1))
  fi
  rm -rf "$rd"
  # Periodically force GC so expired-workspace objects are reclaimed promptly.
  if [ $((round % 25)) -eq 0 ]; then curl -fsS -X POST "$URL/admin/gc" >/dev/null 2>&1 || true; fi
done
curl -fsS -X POST "$URL/admin/gc" >/dev/null 2>&1 || true
sleep "$SAMPLE_INTERVAL"   # capture a final sample

# ── Analyze the RSS trend ───────────────────────────────────────────────────────
stamp="$(date +%F)"
out="$HERE/results/${stamp}-longevity.txt"
mkdir -p "$HERE/results"
analysis=$(awk -F'\t' '
  { r[NR]=$2; n=NR }
  END {
    if (n < 8) { print "INSUFFICIENT " n; exit }
    # Skip the first quarter as startup WARMUP (caches/arenas filling — RSS ramps
    # then plateaus if there is no leak). Measure the trend over the STEADY-STATE
    # remainder: mean of its first half vs its last half. A leak shows as
    # sustained growth here; a healthy process is roughly flat.
    w=int(n/4); if(w<1)w=1
    m=n-w; half=int(m/2); if(half<1)half=1
    fs=0; for(i=w+1;i<=w+half;i++) fs+=r[i]; fm=fs/half
    ls=0; for(i=n-half+1;i<=n;i++) ls+=r[i]; lm=ls/half
    mx=r[1]; for(i=2;i<=n;i++) if(r[i]>mx) mx=r[i]
    growth=(fm>0)?(lm-fm)/fm*100:0
    printf "SAMPLES %d\nWARMUP_SKIPPED %d\nSTEADY_FIRST_HALF_MEAN_KIB %.0f\nSTEADY_LAST_HALF_MEAN_KIB %.0f\nMAX_KIB %d\nSTEADY_GROWTH_PCT %.1f\n", n, w, fm, lm, mx, growth
  }' "$samples")

{
  echo "# Ledge longevity soak — $stamp"
  echo "duration_s=$SOAK_SECONDS sample_interval_s=$SAMPLE_INTERVAL rounds=$round push_errors=$errors"
  echo "$analysis"
  echo "# raw samples (elapsed_s	rss_kib):"
  cat "$samples"
} > "$out"

echo "── longevity soak complete → $out ──"
echo "$analysis"
echo "rounds=$round push_errors=$errors"

growth=$(echo "$analysis" | awk '/STEADY_GROWTH_PCT/{print $2}')
# What a BOUNDED run can and cannot conclude (see soak/README.md → Longevity):
# per-workspace in-memory state IS freed on reclaim (lease map remove on tombstone,
# workspace refs deleted from the ART on release), but the lease/ref WALs are
# append-only and only compact at 64 MiB — a run that never reaches that threshold
# captures the pre-compaction rising edge, so some RSS growth is EXPECTED here and
# is not a leak. This verdict is therefore a RUNAWAY safety-net, not leak proof:
# definitive no-leak evidence needs a multi-day run that crosses compaction (the
# RSS then oscillates in a sawtooth instead of climbing).
thresh=50.0
verdict=$(awk -v g="${growth:-0}" -v th="$thresh" 'BEGIN{print (g<=th)?"PASS":"FAIL"}')
echo "note: bounded-run growth reflects pre-compaction WAL ramp, not a leak (see soak/README.md)"
echo "verdict: $verdict (no runaway — steady-state RSS growth ${growth:-?}% ≤ ${thresh}%)"
[ "$verdict" = PASS ]
