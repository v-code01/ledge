#!/bin/sh
# SC3028: Docker and Kubernetes both export HOSTNAME into the container
# environment, so it is defined at runtime even though POSIX sh does not populate
# it itself. This is the documented mechanism for StatefulSet ordinal derivation.
# shellcheck disable=SC3028
set -eu
# Optionally derive the Raft node_id from a StatefulSet pod ordinal
# (ledge-0 -> 1, ledge-2 -> 3). Opt-in via LEDGE_DERIVE_NODE_ID_FROM_HOSTNAME so
# single-node and docker-compose deployments are unaffected.
if [ "${LEDGE_DERIVE_NODE_ID_FROM_HOSTNAME:-false}" = "true" ]; then
  ordinal="${HOSTNAME##*-}"
  case "$ordinal" in
    '' | *[!0-9]*)
      echo "entrypoint: cannot parse ordinal from HOSTNAME='${HOSTNAME:-}'" >&2
      exit 1
      ;;
    *)
      LEDGE__CLUSTER__NODE_ID="$((ordinal + 1))"
      export LEDGE__CLUSTER__NODE_ID
      ;;
  esac
fi
exec ledge "$@"
