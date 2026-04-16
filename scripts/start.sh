#!/bin/bash

set -euo pipefail

compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"
TOPIC_PARTITIONS="${TOPIC_PARTITIONS:-4}"

echo "Resetting Kafka stack for a clean run..."
compose down -v --remove-orphans >/dev/null 2>&1 || true

echo "Starting Kafka..."
compose up -d zookeeper kafka

echo "Waiting for Kafka to start..."
for attempt in $(seq 1 30); do
  if compose exec -T kafka kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Creating topics..."

compose exec -T kafka kafka-topics \
  --create \
  --if-not-exists \
  --topic source \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --partitions "$TOPIC_PARTITIONS" \
  --replication-factor 1

compose exec -T kafka kafka-topics \
  --create \
  --if-not-exists \
  --topic id \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --partitions "$TOPIC_PARTITIONS" \
  --replication-factor 1

compose exec -T kafka kafka-topics \
  --create \
  --if-not-exists \
  --topic name \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --partitions "$TOPIC_PARTITIONS" \
  --replication-factor 1

compose exec -T kafka kafka-topics \
  --create \
  --if-not-exists \
  --topic continent \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --partitions "$TOPIC_PARTITIONS" \
  --replication-factor 1

echo "Kafka setup completed"
