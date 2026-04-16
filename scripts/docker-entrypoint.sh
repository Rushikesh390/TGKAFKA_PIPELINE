#!/bin/bash

# Docker entrypoint script for Kafka Pipeline
# This script runs inside the container and orchestrates the pipeline.

set -euo pipefail

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_header() {
  echo -e "${BLUE}=== $1 ===${NC}"
}

print_success() {
  echo -e "${GREEN}[ok] $1${NC}"
}

print_warning() {
  echo -e "${YELLOW}[warn] $1${NC}"
}

if [ $# -gt 0 ] && { [ "$1" = "/app/producer" ] || [ "$1" = "/app/consumer" ] || [ "$1" = "/app/merger" ]; }; then
  print_header "Running: $1"
  exec "$@"
fi

print_header "Kafka Streaming Pipeline - Full Run"
print_header "Checking Kafka connectivity"
KAFKA_BROKER="${KAFKA_BROKER:-kafka:29092}"
echo "Using Kafka broker: $KAFKA_BROKER"

KAFKA_HOST="${KAFKA_BROKER%:*}"
KAFKA_PORT="${KAFKA_BROKER##*:}"

wait_for_kafka() {
  local attempts=0
  while [ $attempts -lt 60 ]; do
    if nc -z "$KAFKA_HOST" "$KAFKA_PORT"; then
      return 0
    fi

    attempts=$((attempts + 1))
    echo "Kafka not ready yet, waiting..."
    sleep 2
  done

  return 1
}

if ! wait_for_kafka; then
  print_warning "Kafka did not become reachable on $KAFKA_BROKER"
  exit 1
fi

# Give the broker a brief additional window to finish startup after the port opens.
sleep 5

print_header "Ensuring topics exist"
/app/topics-init

mkdir -p /app/output /app/logs

print_header "Resetting mounted output and logs"
# Compose bind mounts persist between runs, so remove only the generated
# artifacts we own before starting a fresh pipeline execution.
find /app/output -maxdepth 1 -type f -name '*.csv' -delete
find /app/logs -maxdepth 1 -type f \( -name '*.log' -o -name 'overall_report.txt' \) -delete

if /app/scripts/run_all.sh --no-kafka-start; then
  print_success "Pipeline completed"
else
  print_warning "Pipeline failed"
  exit 1
fi

print_header "Pipeline Execution Complete!"
print_success "All stages finished successfully"
echo "Results are in Kafka topics:"
echo "  - id: 50M records sorted by ID"
echo "  - name: 50M records sorted by Name"
echo "  - continent: 50M records sorted by Continent"
