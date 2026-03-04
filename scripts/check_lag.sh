#!/usr/bin/env bash
set -euo pipefail

GROUP_ID="${1:-my-consumer-group}"
BROKER="${BROKER:-localhost:9092}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for this script" >&2
  exit 1
fi

docker exec kafka-rs-kafka kafka-consumer-groups --bootstrap-server "${BROKER}" --group "${GROUP_ID}" --describe
