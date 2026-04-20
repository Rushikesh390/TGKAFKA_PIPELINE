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

declare -A STAGE_DURATIONS_MS

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

report_line() {
  echo "$1" >> "$REPORT_FILE"
}

extract_last_line() {
  local pattern=$1
  local file=$2

  if [ -f "$file" ] && grep -q "$pattern" "$file" 2>/dev/null; then
    grep "$pattern" "$file" | tail -1
    return 0
  fi

  return 1
}

extract_merger_slowest_duration() {
  local file=$1

  awk '
    /Merge completed for topic/ {
      for (i = 1; i <= NF; i++) {
        if ($i == "in" && (i + 1) <= NF) {
          value = $(i + 1)
          compare = value
          if (value ~ /ms$/) {
            sub(/ms$/, "", compare)
            compare += 0
          } else if (value ~ /s$/) {
            sub(/s$/, "", compare)
            compare = (compare + 0) * 1000
          } else {
            compare += 0
          }
          if (compare > max) {
            max = compare
            max_display = value
          }
        }
      }
    }
    END {
      if (max_display != "") {
        printf "%s", max_display
      }
    }
  ' "$file"
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
    STAGE_DURATIONS_MS["$stage_name"]=$stage_duration

    print_success "$stage_name completed in: $(format_duration "$stage_duration")"
    echo "  Duration: $(format_duration "$stage_duration") (${stage_duration}ms)" >> "$REPORT_FILE"
    print_info "Stage logs saved to: $LOG_DIR/${stage_name,,}.log"

    case "$stage_name" in
      "PRODUCER")
        if throughput=$(extract_last_line "Throughput:" "$LOG_DIR/${stage_name,,}.log"); then
          print_info "$throughput"
        fi
        ;;
      "CONSUMER")
        if records=$(extract_last_line "Total records processed:" "$LOG_DIR/${stage_name,,}.log"); then
          print_info "$records"
        fi
        ;;
      "MERGER")
        if grep -q "Merge completed" "$LOG_DIR/${stage_name,,}.log" 2>/dev/null; then
          merges=$(grep -c "Merge completed for topic" "$LOG_DIR/${stage_name,,}.log" || true)
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

  echo "Overall Runtime Summary:"
  echo "================================================"
  report_line "Overall Runtime Summary:"
  report_line "================================================"

  if [ -f "$LOG_DIR/producer.log" ]; then
    producer_wall="${STAGE_DURATIONS_MS[PRODUCER]:-0}"
    producer_internal=$(extract_last_line "Total time:" "$LOG_DIR/producer.log" || echo "Internal runtime unavailable")
    summary="  Producer:  wall-clock $(format_duration "$producer_wall") (${producer_wall}ms); $producer_internal"
    echo "$summary"
    report_line "$summary"
  fi

  if [ -f "$LOG_DIR/consumer.log" ]; then
    consumer_wall="${STAGE_DURATIONS_MS[CONSUMER]:-0}"
    consumer_internal=$(extract_last_line "Consumer finished" "$LOG_DIR/consumer.log" || echo "Internal runtime unavailable")
    summary="  Consumer:  wall-clock $(format_duration "$consumer_wall") (${consumer_wall}ms); $consumer_internal"
    echo "$summary"
    report_line "$summary"
  fi

  if [ -f "$LOG_DIR/merger.log" ]; then
    merger_wall="${STAGE_DURATIONS_MS[MERGER]:-0}"
    merger_count=$(grep -c "Merge completed for topic" "$LOG_DIR/merger.log" 2>/dev/null || echo "0")
    merger_slowest=$(extract_merger_slowest_duration "$LOG_DIR/merger.log")
    if [ -n "${merger_slowest:-}" ]; then
      summary="  Merger:    wall-clock $(format_duration "$merger_wall") (${merger_wall}ms); ${merger_count} parallel merge operations, slowest topic completed in ${merger_slowest}"
    else
      summary="  Merger:    wall-clock $(format_duration "$merger_wall") (${merger_wall}ms); ${merger_count} parallel merge operations"
    fi
    echo "$summary"
    report_line "$summary"
  fi

  echo ""
  echo "Total Time: $(format_duration "$OVERALL_DURATION")"
  echo "End time: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "================================================"
  report_line ""
  report_line "Total Time: $(format_duration "$OVERALL_DURATION")"
  report_line "End time: $(date '+%Y-%m-%d %H:%M:%S')"
  report_line "================================================"

  echo ""
  echo "Performance Analysis:"
  echo "  Total records: ${TOTAL_RECORDS:-50000000}"
  report_line ""
  report_line "Performance Analysis:"
  report_line "  Total records: ${TOTAL_RECORDS:-50000000}"
  throughput=$(awk -v total="${TOTAL_RECORDS:-50000000}" -v duration_ms="$OVERALL_DURATION" 'BEGIN { if (duration_ms > 0) printf "%d", (total * 1000) / duration_ms; else print "N/A" }')
  echo "  Overall throughput: ~${throughput} records/sec"
  report_line "  Overall throughput: ~${throughput} records/sec"

  echo ""
  echo "Output Directory Summary:"
  report_line ""
  report_line "Output Directory Summary:"
  if compgen -G "./output/*.csv" > /dev/null; then
    file_count=$(ls -1 ./output/*.csv 2>/dev/null | wc -l || echo "0")
    total_size=$(du -sh ./output 2>/dev/null | cut -f1 || echo "Unknown")
    record_count=$(wc -l ./output/*.csv 2>/dev/null | tail -1 | awk '{print $1}' || echo "Unknown")
    echo "  Files: $file_count"
    echo "  Size: $total_size"
    echo "  Total chunk rows across all sort dimensions: $record_count"
    report_line "  Files: $file_count"
    report_line "  Size: $total_size"
    report_line "  Total chunk rows across all sort dimensions: $record_count"
  else
    echo "  Files: 0"
    echo "  Note: temporary chunk files were cleaned up after a successful merge"
    report_line "  Files: 0"
    report_line "  Note: temporary chunk files were cleaned up after a successful merge"
  fi

  echo ""
  echo "Report saved to: $REPORT_FILE"
  report_line ""
  report_line "Report saved to: $REPORT_FILE"
  print_success "Full report saved to: $REPORT_FILE"
}

main "$@"
