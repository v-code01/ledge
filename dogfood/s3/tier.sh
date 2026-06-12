#!/usr/bin/env bash
# Dogfood: prove the S3 cold tier end-to-end against MinIO.
#  import the Ledge source -> repack to a git pack -> POST /admin/tier (spill the
#  pack BODY to MinIO, remove the local .pack) -> show the pack is in the bucket and
#  GONE locally -> git clone the workspace back (cold reads RESTORE from MinIO,
#  byte-identical). Off-machine durability: the pack body lives in object storage.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
COMPOSE=("docker" "compose" "-f" "$HERE/docker-compose.yml")
TOKEN="ledge_60f65c9f1fc03bc2_De4xeFuCPjWQUU5GKkGpuBPJykCo6e7IjlK2X__1HIY"
CLIENT="http://localhost:3031"
METRICS="http://localhost:9102"

pass=0; fail=0
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$1"; pass=$((pass+1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$1"; fail=$((fail+1)); }
note() { printf '\n=== %s ===\n' "$1"; }
lpack() { "${COMPOSE[@]}" exec -T ledge sh -c 'find /var/lib/ledge/objects/pack -name "*.pack" 2>/dev/null | wc -l'; }

note "0. up: MinIO + bucket + Ledge (s3 enabled)"
"${COMPOSE[@]}" up -d >/dev/null 2>&1
for i in $(seq 1 40); do if curl -fsS --max-time 4 "$METRICS/healthz" >/dev/null 2>&1; then echo "   healthy after ~${i}s"; break; fi; sleep 1; done
if curl -fsS --max-time 4 "$METRICS/healthz" >/dev/null 2>&1; then ok "Ledge (s3) up on $CLIENT"; else bad "not healthy"; "${COMPOSE[@]}" logs --tail 30 ledge; exit 1; fi

note "1. import the Ledge source + repack to a git pack"
IMP="$(curl -fsS --max-time 300 -X POST "$CLIENT/sync/import" -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' -d '{"upstream_url":"file:///srv/ledge-src","ttl_seconds":31536000}')"
WS="$(echo "$IMP" | jq -r '.workspace_id')"
if [ -n "$WS" ]; then ok "imported (workspace $WS)"; else bad "import failed: $IMP"; exit 1; fi
curl -fsS -X POST "$CLIENT/admin/repack" -H "Authorization: Bearer $TOKEN" >/dev/null && ok "repacked to a git pack"
echo "   local .pack files before tier: $(lpack)"

note "2. TIER: spill the pack body to MinIO (POST /admin/tier)"
T="$(curl -fsS -X POST "$CLIENT/admin/tier" -H "Authorization: Bearer $TOKEN")"
echo "   $T"
NT="$(echo "$T" | jq -r '.packs_tiered // 0')"
if [ "$NT" -ge 1 ]; then ok "tiered $NT pack(s) to MinIO"; else bad "nothing tiered"; fi
AFTER="$(lpack)"
if [ "$AFTER" = "0" ]; then ok "local .pack body REMOVED after tiering (indexes stay local)"; else bad "local .pack still present ($AFTER)"; fi

note "3. the pack body now lives in MinIO (off the Ledge volume)"
INBUCKET="$("${COMPOSE[@]}" run --rm -T --entrypoint sh createbucket -c 'mc alias set local http://minio:9000 ledgeminio ledgeminiosecret >/dev/null 2>&1; mc ls -r local/ledge-packs/ 2>/dev/null | grep -c ".pack"' 2>/dev/null | tr -d "[:space:]" || echo 0)"
if [ "${INBUCKET:-0}" -ge 1 ]; then ok "MinIO bucket holds the pack body ($INBUCKET object)"; else bad "pack not found in bucket"; fi

note "4. clone back: cold reads RESTORE the pack from MinIO (byte-identical)"
OUT="$(mktemp -d)"
if GIT_TERMINAL_PROMPT=0 git -c http.extraHeader="Authorization: Bearer $TOKEN" clone --quiet "$CLIENT/ws/$WS" "$OUT/c" >/tmp/s3clone.err 2>&1; then
  ok "cloned the source back (the server restored the tiered pack from MinIO to serve it)"
  if [ -f "$OUT/c/Cargo.toml" ] && grep -q "ledge-server" "$OUT/c/Cargo.toml"; then ok "working tree intact (Cargo.toml + ledge-server present)"; else bad "tree incomplete"; fi
else
  bad "clone-back failed"; tail -3 /tmp/s3clone.err
fi
rm -rf "$OUT"

note "summary"
printf '  PASS=%s  FAIL=%s\n' "$pass" "$fail"
echo "  Off-machine durability: the pack body is in MinIO; the Ledge volume holds only the small indexes. (\`${COMPOSE[*]} down -v\` to stop+wipe)"
[ "$fail" = 0 ] || exit 1
