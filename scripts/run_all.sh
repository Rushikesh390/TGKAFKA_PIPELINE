#!/bin/bash

# Main entry point for running the complete pipeline
# This script coordinates all stages: Producer → Consumer → Merger
# Usage: ./scripts/run_all.sh [--help] [--no-kafka-start]

set -euo pipefail

# Parse arguments
SKIP_KAFKA=false
for arg in "$@"; do
  case $arg in
    --help)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --help              Show this help message"
      echo "  --no-kafka-start    Don't start Kafka (assumes already running)"
      echo ""
      echo "This script runs the complete streaming pipeline:"
      echo "  1. Starts Kafka (unless --no-kafka-start)"
      echo "  2. Generates 50M records (producer)"
      echo "  3. Sorts records by ID/Name/Continent (consumer)"
      echo "  4. Merges sorted chunks (merger)"
      echo ""
      echo "Output:"
      echo "  - Temporary CSV chunks in: ./output/ (removed after a successful merge)"
      echo "  - Merged data in: Kafka topics (id, name, continent)"
      echo "  - Runtime report: ./logs/overall_report.txt"
      exit 0
      ;;
    --no-kafka-start)
      SKIP_KAFKA=true
      ;;
  esac
done

# Check if Kafka needs to be started
if [ "$SKIP_KAFKA" = false ]; then
  echo "Starting Kafka and Zookeeper..."
  ./scripts/start.sh
fi

# Run the unified orchestrator
echo "Starting pipeline execution..."
./scripts/unified_run.sh
