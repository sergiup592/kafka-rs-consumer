#!/usr/bin/env bash
set -euo pipefail

TOPIC="${1:-events}"
COUNT="${2:-1000}"
BROKER="${BROKER:-localhost:9092}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for this script" >&2
  exit 1
fi

echo "Producing ${COUNT} messages to topic ${TOPIC} via ${BROKER}..."

for i in $(seq 1 "${COUNT}"); do
  printf '{"id":%s,"event":"test","source":"script"}\n' "$i"
done | docker exec -i kafka-rs-kafka kafka-console-producer --bootstrap-server "${BROKER}" --topic "${TOPIC}"

echo "Done."
