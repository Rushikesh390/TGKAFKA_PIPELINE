#!/bin/bash

# Merger stage: K-way merge of sorted CSV chunks, write to Kafka topics
# Expected runtime: 5-15 minutes
# Optimization: Parallel merge (3 topics in parallel), min-heap algorithm
# Output: Final sorted records in Kafka topics (id, name, continent)

set -e

echo "======================================"
echo "Starting MERGER Stage"
echo "======================================"
echo "Merging sorted chunks into final topics..."
echo "Topics: id, name, continent"
echo ""

if [ -x "./merger" ]; then
  ./merger
else
  go run ./cmd/merger
fi

echo ""
echo "✓ Merger completed successfully"
