#!/bin/bash

# Unified Pipeline Orchestrator
# This script runs all three stages with detailed timing and monitoring
# Output: Comprehensive runtime report stored in logs/overall_report.txt

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PRODUCER_BIN="${PRODUCER_BIN:-./producer}"
CONSUMER_BIN="${CONSUMER_BIN:-./consumer}"
MERGER_BIN="${MERGER_BIN:-./merger}"
LOG_DIR="./logs"
REPORT_FILE="$LOG_DIR/overall_report.txt"

# Create log directory
mkdir -p "$LOG_DIR"

# Timing variables
OVERALL_START=$(date +%s%N)

# Function to print section header
print_header() {
  echo -e "\n${BLUE}================== $1 ==================${NC}\n"
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$REPORT_FILE"
}

# Function to print info
print_info() {
  echo -e "${GREEN}ℹ $1${NC}"
  echo "  $1" >> "$REPORT_FILE"
}

# Function to print error
print_error() {
  echo -e "${RED}✗ ERROR: $1${NC}"
  echo "ERROR: $1" >> "$REPORT_FILE"
}

# Function to print success
print_success() {
  echo -e "${GREEN}✓ $1${NC}"
  echo "✓ $1" >> "$REPORT_FILE"
}

# Function to run a stage
run_stage() {
  local stage_name=$1
  local command=$2
  local start_marker=$3
  
  print_header "STAGE: $stage_name"
  
  STAGE_START=$(date +%s%N)
  
  print_info "Starting at: $(date '+%Y-%m-%d %H:%M:%S')"
  print_info "Command: $command"
  
  # Run command and capture both stdout and stderr
  if eval "$command" > "$LOG_DIR/${stage_name,,}.log" 2>&1; then
    STAGE_END=$(date +%s%N)
    STAGE_DURATION=$((($STAGE_END - $STAGE_START) / 1000000))  # Convert to ms
    STAGE_SECONDS=$((STAGE_DURATION / 1000))
    STAGE_MINUTES=$((STAGE_SECONDS / 60))
    STAGE_SECS=$((STAGE_SECONDS % 60))
    
    print_success "$stage_name completed in: ${STAGE_MINUTES}m ${STAGE_SECS}s"
    echo "  Duration: ${STAGE_MINUTES}m ${STAGE_SECS}s (${STAGE_DURATION}ms)" >> "$REPORT_FILE"
    
    # Extract key metrics from stage log
    print_info "Stage logs saved to: $LOG_DIR/${stage_name,,}.log"
    
    # Show summary from logs
    case "$stage_name" in
      "PRODUCER")
        if grep -q "Throughput:" "$LOG_DIR/${stage_name,,}.log"; then
          throughput=$(grep "Throughput:" "$LOG_DIR/${stage_name,,}.log" | tail -1)
          print_info "$throughput"
          echo "  $throughput" >> "$REPORT_FILE"
        fi
        ;;
      "CONSUMER")
        if grep -q "Total records processed:" "$LOG_DIR/${stage_name,,}.log"; then
          records=$(grep "Total records processed:" "$LOG_DIR/${stage_name,,}.log" | tail -1)
          print_info "$records"
          echo "  $records" >> "$REPORT_FILE"
        fi
        ;;
      "MERGER")
        if grep -q "Merge completed" "$LOG_DIR/${stage_name,,}.log"; then
          merges=$(grep "Merge completed" "$LOG_DIR/${stage_name,,}.log" | wc -l)
          print_info "Completed $merges merge operations"
          echo "  Completed $merges merge operations" >> "$REPORT_FILE"
        fi
        ;;
    esac
    
  else
    STAGE_END=$(date +%s%N)
    STAGE_DURATION=$((($STAGE_END - $STAGE_START) / 1000000))
    STAGE_SECONDS=$((STAGE_DURATION / 1000))
    
    print_error "$stage_name FAILED after $STAGE_SECONDS seconds"
    echo "" >> "$REPORT_FILE"
    echo "Stage log:" >> "$REPORT_FILE"
    tail -50 "$LOG_DIR/${stage_name,,}.log" >> "$REPORT_FILE"
    return 1
  fi
  
  return 0
}

# Main execution
main() {
  # Initialize report file
  {
    echo "========================================="
    echo "Kafka Streaming Pipeline - Execution Report"
    echo "========================================="
    echo "Start time: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "System info:"
    echo "  Cores: $(nproc || echo 'unknown')"
    echo "  Memory: $(free -h | grep Mem | awk '{print $2}' || echo 'unknown')"
    echo "=========================================\n"
  } > "$REPORT_FILE"
  
  print_header "Starting Pipeline Execution"
  
  # Verify dependencies
  print_info "Verifying dependencies..."
  
  if ! command -v go &> /dev/null; then
    print_error "Go is not installed. Please install Go 1.22+"
    exit 1
  fi
  print_success "Go found: $(go version)"
  
  echo "" >> "$REPORT_FILE"
  
  # Stage 1: Producer
  if ! run_stage "PRODUCER" "go run cmd/producer/main.go"; then
    print_error "Producer failed!"
    exit 1
  fi
  
  echo "" >> "$REPORT_FILE"
  
  # Stage 2: Consumer
  if ! run_stage "CONSUMER" "go run cmd/consumer/main.go"; then
    print_error "Consumer failed!"
    exit 1
  fi
  
  echo "" >> "$REPORT_FILE"
  
  # Stage 3: Merger
  if ! run_stage "MERGER" "go run cmd/consumer/merge_main.go"; then
    print_error "Merger failed!"
    exit 1
  fi
  
  # Calculate overall timing
  OVERALL_END=$(date +%s%N)
  OVERALL_DURATION=$((($OVERALL_END - $OVERALL_START) / 1000000))  # Convert to ms
  OVERALL_SECONDS=$((OVERALL_DURATION / 1000))
  OVERALL_MINUTES=$((OVERALL_SECONDS / 60))
  OVERALL_SECS=$((OVERALL_SECONDS % 60))
  
  # Print final summary
  echo ""
  echo -e "${YELLOW}=========================================${NC}"
  echo -e "${GREEN}✓ PIPELINE EXECUTION COMPLETE${NC}"
  echo -e "${YELLOW}=========================================${NC}\n"
  
  echo "Overall Runtime Summary:" | tee -a "$REPORT_FILE"
  echo "================================================" | tee -a "$REPORT_FILE"
  
  # Stage breakdown (extract from logs if available)
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
  echo -e "${GREEN}Total Time: ${OVERALL_MINUTES}m ${OVERALL_SECS}s${NC}" | tee -a "$REPORT_FILE"
  echo "End time: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$REPORT_FILE"
  echo "================================================" | tee -a "$REPORT_FILE"
  
  # Performance analysis
  echo "" | tee -a "$REPORT_FILE"
  echo "Performance Analysis:" | tee -a "$REPORT_FILE"
  echo "  Total records: 50,000,000" | tee -a "$REPORT_FILE"
  throughput=$(echo "scale=0; 50000000 * 1000 / $OVERALL_DURATION" | bc 2>/dev/null || echo "N/A")
  echo "  Overall throughput: ~${throughput} records/sec" | tee -a "$REPORT_FILE"
  
  # Output directory summary
  echo "" | tee -a "$REPORT_FILE"
  echo "Output Directory Summary:" | tee -a "$REPORT_FILE"
  if [ -d "./output" ]; then
    file_count=$(ls -1 ./output/*.csv 2>/dev/null | wc -l || echo "0")
    total_size=$(du -sh ./output 2>/dev/null | cut -f1 || echo "Unknown")
    echo "  Files: $file_count" | tee -a "$REPORT_FILE"
    echo "  Size: $total_size" | tee -a "$REPORT_FILE"
    
    # Verify record count
    record_count=$(wc -l ./output/*.csv 2>/dev/null | tail -1 | awk '{print $1}' || echo "Unknown")
    echo "  Total records: $record_count" | tee -a "$REPORT_FILE"
  fi
  
  echo "" | tee -a "$REPORT_FILE"
  echo "Report saved to: $REPORT_FILE" | tee -a "$REPORT_FILE"
  
  print_success "\nFull report saved to: $REPORT_FILE"
  
  return 0
}

# Run main function
main "$@"
exit $?
