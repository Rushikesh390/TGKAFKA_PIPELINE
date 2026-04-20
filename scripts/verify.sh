#!/bin/bash

set -euo pipefail

echo "Checking sorted output (sample)..."

if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
else
  COMPOSE="docker-compose"
fi

for topic in id name continent; do
  echo ""
  echo "===== Topic: $topic ====="
  $COMPOSE exec -T kafka kafka-console-consumer \
    --topic "$topic" \
    --bootstrap-server kafka:29092 \
    --from-beginning \
    --max-messages 10
done
