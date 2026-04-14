#!/bin/bash

# Docker entrypoint script for Kafka Pipeline
# This script runs inside the container and orchestrates the pipeline
# Usage: docker run docrushi/kafka-pipeline:v1
# Or specific component: docker run docrushi/kafka-pipeline:v1 /app/producer

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Check if running specific component
if [ $# -gt 0 ] && [ "$1" = "/app/producer" ] || [ "$1" = "/app/consumer" ] || [ "$1" = "/app/merger" ]; then
    print_header "Running: $1"
    exec "$@"
fi

# If no arguments, run full pipeline
print_header "Kafka Streaming Pipeline - Full Run"

# Check Kafka connectivity
print_header "Checking Kafka connectivity"
KAFKA_BROKER="${KAFKA_BROKER:-kafka:9092}"
echo "Using Kafka broker: $KAFKA_BROKER"

# Give Kafka time to start (if running in docker-compose)
sleep 5

# Run pipeline components sequentially
print_header "Starting Producer (generates 50M records)"
if /app/producer; then
    print_success "Producer completed"
else
    echo "Error: Producer failed"
    exit 1
fi

print_header "Starting Consumer (sorts records)"
if /app/consumer; then
    print_success "Consumer completed"
else
    echo "Error: Consumer failed"
    exit 1
fi

print_header "Starting Merger (k-way merge)"
if /app/merger; then
    print_success "Merger completed"
else
    echo "Error: Merger failed"
    exit 1
fi

print_header "Pipeline Execution Complete!"
print_success "All stages finished successfully"
echo "Results are in Kafka topics:"
echo "  - id-sorted: 50M records sorted by ID"
echo "  - name-sorted: 50M records sorted by Name"
echo "  - continent-sorted: 50M records sorted by Continent"

exit 0
