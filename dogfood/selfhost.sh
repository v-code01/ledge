#!/usr/bin/env bash
# Dogfood: stand up a persistent Ledge, make it host the Ledge source (via the
# sync-import feature), clone that source back OUT of Ledge, and verify the HEAD
# commit SHA-1 is byte-identical. Leaves the instance running.
#
#   bash dogfood/selfhost.sh
#
# Honest ceiling: same machine / SSD (persistent across restarts via the named
# volume + restart policy, NOT off-machine durable). See dogfood/README.md.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
COMPOSE=("docker" "compose" "-f" "$HERE/docker-compose.yml")
TOKEN="ledge_60f65c9f1fc03bc2_De4xeFuCPjWQUU5GKkGpuBPJykCo6e7IjlK2X__1HIY"
CLIENT="http://localhost:3030"
METRICS="http://localhost:9091"

pass=0; fail=0
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$1"; pass=$((pass+1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$1"; fail=$((fail+1)); }
note() { printf '\n=== %s ===\n' "$1"; }

note "0. deploy a persistent self-hosted Ledge (sync+auth, named volume, restart)"
"${COMPOSE[@]}" up -d >/dev/null 2>&1
healthy() { curl -fsS --max-time 4 "$METRICS/healthz" >/dev/null 2>&1; }
for i in $(seq 1 30); do if healthy; then echo "     healthy after ~${i}s"; break; fi; sleep 1; done
if healthy; then ok "instance up + healthy on $CLIENT (metrics $METRICS)"; else bad "instance not healthy"; "${COMPOSE[@]}" logs --tail 20 ledge; exit 1; fi

HOST_HEAD="$(git -C "$REPO_ROOT" rev-parse HEAD)"
HOST_BRANCH="$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD)"
echo "     host source: branch=$HOST_BRANCH HEAD=$HOST_HEAD"

note "1. Ledge imports its OWN source (POST /sync/import file:///srv/ledge-src)"
IMP="$(curl -fsS --max-time 300 -X POST "$CLIENT/sync/import" \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"upstream_url":"file:///srv/ledge-src","ttl_seconds":31536000}' 2>/dev/null || true)"
WS="$(echo "$IMP" | jq -r '.workspace_id // empty' 2>/dev/null || true)"
DEF="$(echo "$IMP" | jq -r '.default_branch // empty' 2>/dev/null || true)"
NREFS="$(echo "$IMP" | jq -r '.refs | length' 2>/dev/null || echo 0)"
echo "     imported: workspace=$WS default_branch=$DEF refs=$NREFS"
if [ -n "$WS" ]; then ok "Ledge ingested its own source ($NREFS refs)"; else bad "import failed: $IMP"; "${COMPOSE[@]}" logs --tail 20 ledge; exit 1; fi

note "2. (probe) raw git push of the source into Ledge — documents the receive-pack delta limit"
WS2="$(curl -fsS -X POST "$CLIENT/workspaces" -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' -d '{"source":[],"ttl_seconds":3600}' 2>/dev/null | jq -r '.id // empty' || true)"
if [ -n "$WS2" ]; then
  if GIT_TERMINAL_PROMPT=0 git -C "$REPO_ROOT" -c http.extraHeader="Authorization: Bearer $TOKEN" push --quiet "http://localhost:3030/ws/$WS2/" "HEAD:refs/heads/main" >/tmp/dogfood_push.log 2>&1; then
    echo "     raw push SUCCEEDED (receive-pack accepted the pack)"; ok "bonus: raw git push works"
  else
    echo "     raw push failed (expected — receive-pack is non-delta; sync-import is the delta-safe path):"
    sed 's/^/       /' /tmp/dogfood_push.log | tail -4
    echo "     [documented limitation, not a harness failure]"
  fi
else
  echo "     (skipped push probe — could not fork a probe workspace)"
fi

note "3. clone Ledge's copy of the source BACK OUT + verify HEAD SHA-1"
# Auth via the Bearer header git sends on every request (Ledge returns a bare 401
# without a WWW-Authenticate challenge, so Basic-in-URL doesn't trigger; extraHeader
# is the robust path — same Bearer the sync import used).
OUT="$(mktemp -d)"
if GIT_TERMINAL_PROMPT=0 git -c http.extraHeader="Authorization: Bearer $TOKEN" clone --quiet "http://localhost:3030/ws/$WS" "$OUT/clone" >/tmp/dogfood_clone.log 2>&1; then
  ok "cloned the source back out of Ledge (http://.../ws/$WS)"
  GOT="$(git -C "$OUT/clone" rev-parse HEAD 2>/dev/null || echo none)"
  echo "     cloned HEAD=$GOT  vs host HEAD=$HOST_HEAD"
  if [ "$GOT" = "$HOST_HEAD" ]; then ok "HEAD SHA-1 byte-identical — Ledge is serving its own source"; else bad "HEAD mismatch ($GOT != $HOST_HEAD)"; fi
  if [ -f "$OUT/clone/Cargo.toml" ] && grep -q "ledge-server" "$OUT/clone/Cargo.toml" 2>/dev/null; then ok "working tree intact (Cargo.toml + ledge-server member present)"; else bad "working tree incomplete"; fi
else
  bad "clone-back failed"; sed 's/^/     /' /tmp/dogfood_clone.log | tail -5
fi
rm -rf "$OUT"

note "summary"
printf '  PASS=%s  FAIL=%s\n' "$pass" "$fail"
echo "  Ledge is now self-hosting its source at: $CLIENT/ws/$WS (instance left RUNNING; \`${COMPOSE[*]} down\` to stop, add -v to wipe the volume)"
[ "$fail" = 0 ] || exit 1
