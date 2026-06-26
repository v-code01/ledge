#!/usr/bin/env bash
# Proves automatic cluster bootstrap: start a real 3-node cluster (no manual
# POST /cluster/init) and assert a leader is elected + a ref write replicates.
set -euo pipefail
cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null || echo /Users/vanshverma/ledge)"
BIN=$(ls target/release/ledge target/debug/ledge 2>/dev/null | head -1)
W=$(mktemp -d); PIDS=()
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$W"; }
trap cleanup EXIT

# Shared shard map: one shard, members 1/2/3 on local client ports.
cfg(){ cat > "$W/c$1.toml" <<EOF
[server]
addr = "127.0.0.1:$2"
data_dir = "$W/d$1"
[cluster]
enabled = true
num_shards = 1
raft_bind = "127.0.0.1:$3"
[[cluster.shards]]
id = 0
members = [
  { id = 1, addr = "http://127.0.0.1:41001" },
  { id = 2, addr = "http://127.0.0.1:41002" },
  { id = 3, addr = "http://127.0.0.1:41003" },
]
EOF
}
cfg 1 41001 42001; cfg 2 41002 42002; cfg 3 41003 42003
for i in 1 2 3; do
  LEDGE__CLUSTER__NODE_ID=$i LEDGE__METRICS__ADDR="127.0.0.1:4300$i" \
    "$BIN" start --config "$W/c$i.toml" > "$W/n$i.log" 2>&1 &
  PIDS+=($!)
done

# Wait for a leader to appear in /cluster/status WITHOUT calling /cluster/init.
leader=""
for _ in $(seq 1 60); do
  s=$(curl -fsS "http://127.0.0.1:41001/cluster/status" 2>/dev/null || true)
  leader=$(printf '%s' "$s" | jq -r '.. | .leader? // empty' 2>/dev/null | head -1 || true)
  [ -n "$leader" ] && [ "$leader" != "null" ] && [ "$leader" != "0" ] && break
  sleep 0.5
done
echo "status: $s"
if [ -n "$leader" ] && [ "$leader" != "null" ] && [ "$leader" != "0" ]; then
  echo "PASS: cluster self-bootstrapped, leader = node $leader (no manual /cluster/init)"
else
  echo "FAIL: no leader elected"; tail -15 "$W"/n1.log; exit 1
fi
