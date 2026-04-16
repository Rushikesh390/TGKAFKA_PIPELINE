#!/bin/bash

# Unified Pipeline Orchestrator
# This script runs all three stages with detailed timing and monitoring.
# Output: Comprehensive runtime report stored in logs/overall_report.txt

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

PRODUCER_BIN="${PRODUCER_BIN:-./producer}"
CONSUMER_BIN="${CONSUMER_BIN:-./consumer}"
MERGER_BIN="${MERGER_BIN:-./merger}"
LOG_DIR="./logs"
REPORT_FILE="$LOG_DIR/overall_report.txt"

mkdir -p "$LOG_DIR" output

now_ms() {
  if [ -r /proc/uptime ]; then
    awk '{printf "%d", $1 * 1000}' /proc/uptime
    return 0
  fi

  echo $(( $(date +%s) * 1000 ))
}

format_duration() {
  local duration_ms=$1
  local total_seconds=$((duration_ms / 1000))
  local minutes=$((total_seconds / 60))
  local seconds=$((total_seconds % 60))
  printf "%sm %ss" "$minutes" "$seconds"
}

OVERALL_START=$(now_ms)

print_header() {
  echo -e "\n${BLUE}================== $1 ==================${NC}\n"
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$REPORT_FILE"
}

print_info() {
  echo -e "${GREEN}[info] $1${NC}"
  echo "  $1" >> "$REPORT_FILE"
}

print_error() {
  echo -e "${RED}[error] $1${NC}"
  echo "ERROR: $1" >> "$REPORT_FILE"
}

print_success() {
  echo -e "${GREEN}[ok] $1${NC}"
  echo "[ok] $1" >> "$REPORT_FILE"
}

resolve_command() {
  local binary_path=$1
  local fallback_command=$2

  if [ -x "$binary_path" ]; then
    echo "$binary_path"
    return 0
  fi

  if command -v go >/dev/null 2>&1; then
    echo "$fallback_command"
    return 0
  fi

  return 1
}

run_stage() {
  local stage_name=$1
  local command=$2

  print_header "STAGE: $stage_name"

  local stage_start stage_end stage_duration
  stage_start=$(now_ms)

  print_info "Starting at: $(date '+%Y-%m-%d %H:%M:%S')"
  print_info "Command: $command"

  if eval "$command" > "$LOG_DIR/${stage_name,,}.log" 2>&1; then
    stage_end=$(now_ms)
    stage_duration=$((stage_end - stage_start))

    print_success "$stage_name completed in: $(format_duration "$stage_duration")"
    echo "  Duration: $(format_duration "$stage_duration") (${stage_duration}ms)" >> "$REPORT_FILE"
    print_info "Stage logs saved to: $LOG_DIR/${stage_name,,}.log"

    case "$stage_name" in
      "PRODUCER")
        if grep -q "Throughput:" "$LOG_DIR/${stage_name,,}.log"; then
          throughput=$(grep "Throughput:" "$LOG_DIR/${stage_name,,}.log" | tail -1)
          print_info "$throughput"
        fi
        ;;
      "CONSUMER")
        if grep -q "Total records processed:" "$LOG_DIR/${stage_name,,}.log"; then
          records=$(grep "Total records processed:" "$LOG_DIR/${stage_name,,}.log" | tail -1)
          print_info "$records"
        fi
        ;;
      "MERGER")
        if grep -q "Merge completed" "$LOG_DIR/${stage_name,,}.log"; then
          merges=$(grep "Merge completed" "$LOG_DIR/${stage_name,,}.log" | wc -l)
          print_info "Completed $merges merge operations"
        fi
        ;;
    esac
  else
    stage_end=$(now_ms)
    stage_duration=$((stage_end - stage_start))

    print_error "$stage_name failed after $(format_duration "$stage_duration")"
    echo "" >> "$REPORT_FILE"
    echo "Stage log:" >> "$REPORT_FILE"
    tail -50 "$LOG_DIR/${stage_name,,}.log" >> "$REPORT_FILE"
    return 1
  fi

  return 0
}

main() {
  {
    echo "========================================="
    echo "Kafka Streaming Pipeline - Execution Report"
    echo "========================================="
    echo "Start time: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "System info:"
    echo "  Cores: $(nproc 2>/dev/null || echo 'unknown')"
    echo "  Memory: $(free -h 2>/dev/null | grep Mem | awk '{print $2}' || echo 'unknown')"
    echo "========================================="
    echo ""
  } > "$REPORT_FILE"

  print_header "Starting Pipeline Execution"
  print_info "Verifying dependencies..."

  PRODUCER_CMD=$(resolve_command "$PRODUCER_BIN" "go run ./cmd/producer") || {
    print_error "Producer binary not found and Go is unavailable"
    exit 1
  }
  CONSUMER_CMD=$(resolve_command "$CONSUMER_BIN" "go run ./cmd/consumer") || {
    print_error "Consumer binary not found and Go is unavailable"
    exit 1
  }
  MERGER_CMD=$(resolve_command "$MERGER_BIN" "go run ./cmd/merger") || {
    print_error "Merger binary not found and Go is unavailable"
    exit 1
  }

  if command -v go >/dev/null 2>&1; then
    print_success "Go found: $(go version)"
  else
    print_info "Using prebuilt binaries inside the runtime image"
  fi

  echo "" >> "$REPORT_FILE"

  if ! run_stage "PRODUCER" "$PRODUCER_CMD"; then
    print_error "Producer failed"
    exit 1
  fi

  echo "" >> "$REPORT_FILE"

  if ! run_stage "CONSUMER" "$CONSUMER_CMD"; then
    print_error "Consumer failed"
    exit 1
  fi

  echo "" >> "$REPORT_FILE"

  if ! run_stage "MERGER" "$MERGER_CMD"; then
    print_error "Merger failed"
    exit 1
  fi

  OVERALL_END=$(now_ms)
  OVERALL_DURATION=$((OVERALL_END - OVERALL_START))

  echo ""
  echo -e "${YELLOW}=========================================${NC}"
  echo -e "${GREEN}[ok] PIPELINE EXECUTION COMPLETE${NC}"
  echo -e "${YELLOW}=========================================${NC}\n"

  echo "Overall Runtime Summary:" | tee -a "$REPORT_FILE"
  echo "================================================" | tee -a "$REPORT_FILE"

  if [ -f "$LOG_DIR/producer.log" ]; then
    producer_time=$(grep "Total time:" "$LOG_DIR/producer.log" 2>/dev/null | tail -1 || echo "Unknown")
    echo "  Producer:  $producer_time" | tee -a "$REPORT_FILE"
  fi

  if [ -f "$LOG_DIR/consumer.log" ]; then
    consumer_time=$(grep "Consumer finished" "$LOG_DIR/consumer.log" 2>/dev/null | tail -1 || echo "Unknown")
    echo "  Consumer:  $consumer_time" | tee -a "$REPORT_FILE"
  fi

  if [ -f "$LOG_DIR/merger.log" ]; then
    merger_count=$(grep "Merge completed" "$LOG_DIR/merger.log" 2>/dev/null | wc -l || echo "Unknown")
    echo "  Merger:    $merger_count operations completed" | tee -a "$REPORT_FILE"
  fi

  echo "" | tee -a "$REPORT_FILE"
  echo -e "${GREEN}Total Time: $(format_duration "$OVERALL_DURATION")${NC}" | tee -a "$REPORT_FILE"
  echo "End time: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$REPORT_FILE"
  echo "================================================" | tee -a "$REPORT_FILE"

  echo "" | tee -a "$REPORT_FILE"
  echo "Performance Analysis:" | tee -a "$REPORT_FILE"
  echo "  Total records: ${TOTAL_RECORDS:-50000000}" | tee -a "$REPORT_FILE"
  throughput=$(awk -v total="${TOTAL_RECORDS:-50000000}" -v duration_ms="$OVERALL_DURATION" 'BEGIN { if (duration_ms > 0) printf "%d", (total * 1000) / duration_ms; else print "N/A" }')
  echo "  Overall throughput: ~${throughput} records/sec" | tee -a "$REPORT_FILE"

  echo "" | tee -a "$REPORT_FILE"
  echo "Output Directory Summary:" | tee -a "$REPORT_FILE"
  if compgen -G "./output/*.csv" > /dev/null; then
    file_count=$(ls -1 ./output/*.csv 2>/dev/null | wc -l || echo "0")
    total_size=$(du -sh ./output 2>/dev/null | cut -f1 || echo "Unknown")
    record_count=$(wc -l ./output/*.csv 2>/dev/null | tail -1 | awk '{print $1}' || echo "Unknown")
    echo "  Files: $file_count" | tee -a "$REPORT_FILE"
    echo "  Size: $total_size" | tee -a "$REPORT_FILE"
    echo "  Total chunk rows across all sort dimensions: $record_count" | tee -a "$REPORT_FILE"
  else
    echo "  Files: 0" | tee -a "$REPORT_FILE"
  fi

  echo "" | tee -a "$REPORT_FILE"
  echo "Report saved to: $REPORT_FILE" | tee -a "$REPORT_FILE"
  print_success "Full report saved to: $REPORT_FILE"
}

main "$@"
