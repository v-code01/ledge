#!/usr/bin/env bash
# Back up a Ledge data directory to a single tarball.
#
#   Cold (default): the server MUST be stopped. Archives the data dir exactly —
#                   the simplest, unambiguously-consistent backup.
#   Hot  (--hot):   the server keeps running. Calls POST /admin/snapshot to take
#                   a copy-on-write clone of the data dir, archives that, then
#                   removes the temp clone. No downtime. Consistency: objects are
#                   content-addressed + immutable and the ref/lease WALs are
#                   append-only and replay-tolerant, so the clone is always a
#                   valid, restorable Ledge dir; the only thing it can miss is a
#                   write still in flight at the instant of the snapshot.
#
# Usage:
#   backup.sh --data-dir DIR --out FILE.tar.gz            # cold (server stopped)
#   backup.sh --hot --url http://127.0.0.1:3000 \
#             --data-dir DIR --out FILE.tar.gz            # hot (server running)
#
# --data-dir is required in both modes (cold reads it; hot derives the snapshot
# location next to it and the server must be able to write there).
set -euo pipefail

MODE=cold
DATA_DIR=""
OUT=""
URL=""
die() { echo "backup: $*" >&2; exit 1; }
usage() { sed -n '2,25p' "$0"; exit 2; }
while [ $# -gt 0 ]; do
  case "$1" in
    --data-dir) DATA_DIR=${2:?}; shift 2;;
    --out)      OUT=${2:?};      shift 2;;
    --hot)      MODE=hot;        shift;;
    --url)      URL=${2:?};      shift 2;;
    -h|--help)  usage;;
    *) die "unknown arg: $1";;
  esac
done
[ -n "$DATA_DIR" ] && [ -n "$OUT" ] || usage
[ -d "$DATA_DIR" ] || die "data dir not found: $DATA_DIR"
command -v curl >/dev/null || [ "$MODE" = cold ] || die "hot mode needs curl"

mkdir -p "$(dirname "$OUT")"

if [ "$MODE" = cold ]; then
  # Archive the data dir's CONTENTS at the tarball root (objects/ refs/ leases/ lfs/).
  tar -C "$DATA_DIR" -czf "$OUT" .
  echo "backup: cold backup of $DATA_DIR -> $OUT ($(du -h "$OUT" | cut -f1))"
else
  [ -n "$URL" ] || die "hot mode needs --url"
  # /admin/snapshot requires an absolute dest that does NOT already exist; the
  # server (same host) CoW-clones the live data dir into it.
  abs=$(cd "$(dirname "$DATA_DIR")" && pwd)/$(basename "$DATA_DIR")
  dest="${abs}.snap.$$"
  echo "backup: requesting CoW snapshot -> $dest"
  curl -fsS -X POST "$URL/admin/snapshot" \
       -H 'content-type: application/json' \
       -d "{\"dest\":\"$dest\"}" >/dev/null \
    || die "snapshot request failed (is the server up at $URL, admin enabled?)"
  trap 'rm -rf "$dest"' EXIT
  tar -C "$dest" -czf "$OUT" .
  echo "backup: hot backup via snapshot -> $OUT ($(du -h "$OUT" | cut -f1))"
fi
