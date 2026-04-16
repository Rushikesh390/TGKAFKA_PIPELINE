#!/bin/bash

# Consumer stage: Read from Kafka, sort by ID/Name/Continent, write to CSV chunks
# Expected runtime: 15-30 minutes
# Optimization: Index-based sorting (no data copies), parallel sorting threads
# Output: CSV chunks in output/ directory

set -e

echo "======================================"
echo "Starting CONSUMER Stage"
echo "======================================"
echo "Reading from Kafka 'source' topic..."
echo "Sorting by: ID (numeric), Name (alphabetic), Continent (alphabetic)"
echo ""

if [ -x "./consumer" ]; then
  ./consumer
else
  go run ./cmd/consumer
fi

echo ""
echo "✓ Consumer completed successfully"
