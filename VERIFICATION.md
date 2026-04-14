# Verification Guide - How to Verify Correctness

## Quick Correctness Check (5 minutes)

```bash
# 1. Check output files exist
ls -lh output/ | wc -l           # Should show many files
wc -l output/*.csv               # Should total 50M lines

# 2. Check ID sorting (first 10 files, first 5 records)
for i in {0..9}; do
  head -5 output/id_chunk_$i.csv | cut -d, -f1
done
# IDs should be monotonically increasing

# 3. Check Name sorting (alphabetical)
for i in {0..9}; do
  head -5 output/name_chunk_$i.csv | cut -d, -f2
done
# Names should be in alphabetical order

# 4. Check Continent sorting (by continent names)
for i in {0..9}; do
  head -10 output/continent_chunk_$i.csv | cut -d, -f4 | sort | uniq
done
# Should see continents in alphabetical order
```

---

## Detailed Verification Procedures

### 1. Record Count Verification

**Requirement**: Exactly 50,000,000 records produced and processed

```bash
# Count total records in all chunks
echo "Counting total records..."
total=$(wc -l output/*.csv | tail -1 | awk '{print $1}')
echo "Total records: $total"

# Should output: Total records: 50000000
if [ "$total" -eq 50000000 ]; then
  echo "✓ Correct record count"
else
  echo "✗ ERROR: Expected 50000000, got $total"
fi
```

### 2. ID Sorting Verification (Numeric Order)

**Requirement**: Records sorted by numeric ID in ascending order

```bash
# Extract IDs from id_chunk_0.csv
cut -d, -f1 output/id_chunk_0.csv > /tmp/ids.txt

# Check if sorted
if sort -n /tmp/ids.txt -c > /dev/null 2>&1; then
  echo "✓ IDs are sorted numerically"
else
  echo "✗ ERROR: IDs not in order"
  # Show first error
  sort -n /tmp/ids.txt -c 2>&1 | head -1
fi
```

**Detailed ID verification across all chunks**:
```bash
#!/bin/bash
# Verify IDs are monotonically increasing across chunks

for i in {0..9}; do
  if [ -f "output/id_chunk_$i.csv" ]; then
    last_id=$(tail -1 output/id_chunk_$i.csv | cut -d, -f1)
    first_next=$(head -1 output/id_chunk_$((i+1)).csv 2>/dev/null | cut -d, -f1)
    
    if [ -n "$first_next" ]; then
      if [ $last_id -le $first_next ]; then
        echo "✓ Chunk $i: last_id=$last_id <= next_first=$first_next"
      else
        echo "✗ ERROR: Chunk boundary violation at chunk $i"
      fi
    fi
  fi
done
```

### 3. Name Sorting Verification (Alphabetical Order)

**Requirement**: Records sorted by name in alphabetical order

```bash
# Extract names from one chunk
cut -d, -f2 output/name_chunk_0.csv > /tmp/names.txt

# Check if sorted alphabetically
if sort /tmp/names.txt -c > /dev/null 2>&1; then
  echo "✓ Names are sorted alphabetically"
else
  echo "✗ ERROR: Names not in alphabetical order"
  sort /tmp/names.txt -c 2>&1 | head -3
fi
```

**Visual verification**:
```bash
# Show first 20 names (should be alphabetical)
head -20 output/name_chunk_0.csv | cut -d, -f2

# Expected output (sample):
# aAbCdE
# aAfGhI
# aBcDeF
# ...
```

### 4. Continent Sorting Verification (Alphabetical Order)

**Requirement**: Records sorted by continent alphabetically, with correct values

```bash
# Valid continents
VALID_CONTINENTS="Africa Asia Australia Europe North America South America"

# Check all continents are valid
cut -d, -f4 output/continent_chunk_0.csv | sort | uniq | while read continent; do
  if echo "$VALID_CONTINENTS" | grep -q "$continent"; then
    echo "✓ Valid continent: $continent"
  else
    echo "✗ ERROR: Invalid continent: $continent"
  fi
done
```

**Verify alphabetical grouping**:
```bash
# Extract all continents, should be grouped
cut -d, -f4 output/continent_chunk_*.csv | sort | uniq -c | sort -rn

# Expected output (approximately):
# 7142857 Africa
# 7142857 Asia
# 7142857 Australia
# 7142858 Europe
# 7142857 North America
# 7142857 South America

# Check they're roughly equal (±1M difference)
```

### 5. CSV Format Verification

**Requirement**: Each row has exactly 4 comma-separated fields

```bash
# Check CSV format in sample
head -1000 output/id_chunk_0.csv | awk -F, '{print NF}' | sort | uniq

# Should output only: 4
```

**Detailed format check**:
```bash
#!/bin/bash
# Check every record has 4 fields

file="output/id_chunk_0.csv"
count=0
total=0
errors=0

while IFS=',' read -r id name address continent; do
  total=$((total + 1))
  
  # Verify field counts and types
  if [ -z "$id" ] || [ -z "$name" ] || [ -z "$address" ] || [ -z "$continent" ]; then
    echo "✗ ERROR: Line $total has empty field"
    ((errors++))
  fi
  
  # Check ID is numeric (32-bit)
  if ! [[ "$id" =~ ^-?[0-9]+$ ]]; then
    echo "✗ ERROR: Invalid ID format on line $total: $id"
    ((errors++))
  fi
  
  # Stop after first 10 errors
  if [ $errors -ge 10 ]; then
    break
  fi
  
  # Sample first 5
  if [ $total -le 5 ]; then
    echo "Sample $total: ID=$id Name=$name Address=$address Continent=$continent"
  fi
done < "$file"

echo "Checked $total records, found $errors errors"
```

### 6. Data Integrity Verification

**Requirement**: No data corruption or loss during processing

```bash
# Verify no duplicates in ID-sorted chunk
cut -d, -f1 output/id_chunk_0.csv | sort | uniq | wc -l > /tmp/unique.txt
cut -d, -f1 output/id_chunk_0.csv | wc -l > /tmp/total.txt

unique=$(cat /tmp/unique.txt)
total=$(cat /tmp/total.txt)

if [ $unique -eq $total ]; then
  echo "✓ No duplicate IDs in chunk 0"
else
  echo "✗ ERROR: Found $(($total - $unique)) duplicate IDs"
fi
```

### 7. Complete Validation Script

```bash
#!/bin/bash
# comprehensive_validation.sh

echo "======================================="
echo "Starting comprehensive validation..."
echo "======================================="

# Test 1: Count records
echo ""
echo "Test 1: Checking record count..."
total=$(wc -l output/*.csv 2>/dev/null | tail -1 | awk '{print $1}')
if [ "$total" -eq 50000000 ]; then
  echo "✓ PASS: Correct total ($total records)"
else
  echo "✗ FAIL: Expected 50000000, got $total"
fi

# Test 2: ID sorting
echo ""
echo "Test 2: Checking ID sorting..."
cut -d, -f1 output/id_chunk_*.csv 2>/dev/null | sort -n -c > /dev/null 2>&1
if [ $? -eq 0 ]; then
  echo "✓ PASS: IDs are numerically sorted"
else
  echo "✗ FAIL: ID sorting failed"
fi

# Test 3: Name sorting
echo ""
echo "Test 3: Checking Name sorting..."
cut -d, -f2 output/name_chunk_0.csv 2>/dev/null | sort -c > /dev/null 2>&1
if [ $? -eq 0 ]; then
  echo "✓ PASS: Names are alphabetically sorted"
else
  echo "✗ FAIL: Name sorting failed"
fi

# Test 4: Continent values
echo ""
echo "Test 4: Checking continent values..."
continents=$(cut -d, -f4 output/continent_chunk_*.csv 2>/dev/null | sort | uniq)
expected="Africa
Asia
Australia
Europe
North America
South America"

if [ "$continents" = "$expected" ]; then
  echo "✓ PASS: All valid continents present"
else
  echo "✗ FAIL: Invalid or missing continents"
  echo "Expected: $expected"
  echo "Got: $continents"
fi

# Test 5: CSV format
echo ""
echo "Test 5: Checking CSV format..."
bad_format=$(head -10000 output/id_chunk_0.csv 2>/dev/null | awk -F, 'NF != 4 {print NR}' | wc -l)
if [ $bad_format -eq 0 ]; then
  echo "✓ PASS: All records have 4 fields"
else
  echo "✗ FAIL: $bad_format records have wrong format"
fi

echo ""
echo "======================================="
echo "Validation complete!"
echo "======================================="
```

---

## Kafka Output Verification

### Verify Records in Kafka Topics

```bash
# Check if topics exist
docker exec <kafka-container> kafka-topics \
  --list \
  --bootstrap-server localhost:9092

# Should show: id-sorted, name-sorted, continent-sorted
```

### Sample Records from Topics

```bash
# Sample 10 records from id-sorted topic
docker exec <kafka-container> kafka-console-consumer \
  --topic id-sorted \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 10
```

### Verify Topic Record Count

```bash
# Get record count in each topic
docker exec <kafka-container> kafka-run-class \
  kafka.tools.JmxTool \
  --object-name "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec" \
  --attributes "Count" \
  --report
```

---

## Performance Metrics Verification

### Verify Runtime Timing

Each stage should print timing when it completes:

```
Producer finished in 5m23s
Consumer finished in 18m45s
Merger finished in 7m12s
Total: 31m20s
```

**Target ranges**:
| Stage | Min | Max |
|-------|-----|-----|
| Producer | 5 min | 10 min |
| Consumer | 15 min | 30 min |
| Merger | 5 min | 15 min |
| **Total** | **25 min** | **50 min** |

### Memory Usage Verification

```bash
# Monitor memory during consumer phase
while true; do
  ps aux | grep consumer | grep -v grep | awk '{print $6 " MB"}'
  sleep 1
done

# Should stay under 1000 MB (1 GB)
```

### CPU Usage Verification

```bash
# Check CPU utilization
top -b -n 10 | grep consumer

# Should see usage close to 100% across all 4 cores
```

---

## Common Verification Issues

### Issue: Record count off by N

**Possible causes**:
1. Producer didn't finish (check logs)
2. Consumer skipped batches (check logs)
3. Merger incomplete (check logs)

**Debug**:
```bash
# Check each stage
echo "Producer output: $(wc -l output/id_chunk_*.csv | tail -1 | awk '{print $1}')"
```

### Issue: IDs out of order

**Possible causes**:
1. Sort failed (insufficient memory)
2. Write incomplete (disk full)
3. Merge incomplete (merger crashed)

**Debug**:
```bash
# Find first out-of-order ID
cut -d, -f1 output/id_chunk_0.csv | while read id; do
  if [ -n "$prev" ] && [ $prev -gt $id ]; then
    echo "Out of order at: prev=$prev, current=$id"
    break
  fi
  prev=$id
done
```

### Issue: Missing continents

**Possible causes**:
1. Generator bug (missing continent in list)
2. Parser bug (corrupt continent field)

**Debug**:
```bash
# Check what continents are present
cut -d, -f4 output/continent_chunk_*.csv | sort | uniq -c
```

---

## Automated Testing

### Unit Test Example

```go
// internal/sorter/sorter_test.go

func TestSortByIDIndexed(t *testing.T) {
    records := []models.Record{
        {ID: 30, Name: "c", Address: "", Continent: ""},
        {ID: 10, Name: "b", Address: "", Continent: ""},
        {ID: 20, Name: "a", Address: "", Continent: ""},
    }
    
    indices := sorter.SortByIDIndexed(records)
    
    // Verify order: 10, 20, 30
    if records[indices[0]].ID != 10 ||
       records[indices[1]].ID != 20 ||
       records[indices[2]].ID != 30 {
        t.Error("IDs not properly sorted by indices")
    }
}
```

### Integration Test Example

```go
// Full pipeline test with 100K records
func TestFullPipeline(t *testing.T) {
    // 1. Start producer (100K records)
    // 2. Start consumer
    // 3. Start merger
    // 4. Verify count, sorting, format
    // 5. Assert all pass
}
```

---

## Final Checklist

- [ ] 50,000,000 exact records produced
- [ ] All records have 4 CSV fields
- [ ] ID field: numeric, 32-bit range
- [ ] Name field: 10-15 character strings
- [ ] Address field: 15-20 character strings  
- [ ] Continent field: Valid values only
- [ ] ID-sorted: Numerically ascending
- [ ] Name-sorted: Alphabetically ascending
- [ ] Continent-sorted: Alphabetically grouped
- [ ] Total runtime: 25-50 minutes
- [ ] Memory usage: < 2GB
- [ ] CPU usage: 95-100% on all 4 cores
- [ ] No crashes or errors in logs
- [ ] Reproducible (same output on re-run)

---

## See Also

- [README.md](README.md) - Quick start
- [ARCHITECTURE.md](ARCHITECTURE.md) - System design