#!/bin/bash

echo "Checking sorted output (sample)..."

if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
else
  COMPOSE="docker-compose"
fi

$COMPOSE exec -T kafka kafka-console-consumer \
  --topic id \
  --bootstrap-server kafka:29092 \
  --from-beginning \
  --max-messages 10
