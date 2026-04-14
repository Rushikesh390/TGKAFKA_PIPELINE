#!/bin/bash

# Producer stage: Generate 50M random records and push to Kafka 'source' topic
# Expected runtime: 5-10 minutes
# Output: Records in Kafka topic 'source'

set -e

echo "======================================"
echo "Starting PRODUCER Stage"
echo "======================================"
echo "Generating 50 million random records..."
echo ""

go run cmd/producer/main.go

echo ""
echo "✓ Producer completed successfully"