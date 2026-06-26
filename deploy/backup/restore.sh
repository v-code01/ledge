#!/usr/bin/env bash
# Restore a Ledge data directory from a backup.sh tarball.
#
# The server MUST be stopped before restoring. On boot Ledge replays the ref and
# lease WALs and opens the object store, so a restored dir comes back as the exact
# state captured in the backup.
#
# Usage:
#   restore.sh --in FILE.tar.gz --data-dir DIR [--force]
#
# Refuses to write into a non-empty DIR unless --force (so you don't silently
# merge a backup on top of live data). With --force the dir is emptied first.
set -euo pipefail

IN=""
DATA_DIR=""
FORCE=0
die() { echo "restore: $*" >&2; exit 1; }
usage() { sed -n '2,14p' "$0"; exit 2; }
while [ $# -gt 0 ]; do
  case "$1" in
    --in)       IN=${2:?};       shift 2;;
    --data-dir) DATA_DIR=${2:?}; shift 2;;
    --force)    FORCE=1;         shift;;
    -h|--help)  usage;;
    *) die "unknown arg: $1";;
  esac
done
[ -n "$IN" ] && [ -n "$DATA_DIR" ] || usage
[ -f "$IN" ] || die "backup not found: $IN"

if [ -d "$DATA_DIR" ] && [ -n "$(ls -A "$DATA_DIR" 2>/dev/null)" ]; then
  [ "$FORCE" = 1 ] || die "$DATA_DIR is not empty; stop the server and re-run with --force to overwrite"
  echo "restore: --force: clearing $DATA_DIR"
  rm -rf "${DATA_DIR:?}/"* "${DATA_DIR:?}/".[!.]* 2>/dev/null || true
fi

mkdir -p "$DATA_DIR"
tar -C "$DATA_DIR" -xzf "$IN"
echo "restore: restored $IN -> $DATA_DIR"
echo "restore: now start the server with this data dir and verify (/healthz, then a clone)."
