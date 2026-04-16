# Verification Guide

This guide assumes you are using the supported Docker Compose flow.

## 1. Verify the Required Topics Exist

```bash
docker compose exec -T kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --list
```

Expected topics:

- `source`
- `id`
- `name`
- `continent`

## 2. Verify Topic Record Counts

Check offsets for one output topic:

```bash
docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:29092 \
  --topic id
```

Sum the partition offsets on the host:

```bash
docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:29092 \
  --topic id | awk -F: '{sum += $3} END {print sum}'
```

The sum should equal `50000000` for the default run.

Repeat for `name` and `continent`.

## 3. Verify the Chunk File Count

```bash
ls output/*.csv | wc -l
wc -l output/*.csv | tail -1
```

The total line count across chunk files should equal `50000000`.

## 4. Verify `id` Ordering

Read a sample from Kafka and check numeric monotonic order:

```bash
docker compose exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic id \
  --from-beginning \
  --max-messages 20 | cut -d, -f1
```

You should see ascending numeric IDs.

For a stricter local check against chunk files:

```bash
cut -d, -f1 output/id_chunk_0.csv | sort -n -c
```

That command should exit successfully.

## 5. Verify `name` Ordering

```bash
docker compose exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic name \
  --from-beginning \
  --max-messages 20 | cut -d, -f2
```

The names should be alphabetically ordered.

Local chunk check:

```bash
cut -d, -f2 output/name_chunk_0.csv | sort -c
```

## 6. Verify `continent` Ordering

```bash
docker compose exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic continent \
  --from-beginning \
  --max-messages 20 | cut -d, -f4
```

Expected values are drawn only from:

- `Africa`
- `Asia`
- `Australia`
- `Europe`
- `North America`
- `South America`

Local chunk check:

```bash
cut -d, -f4 output/continent_chunk_0.csv | sort -c
```

## 7. Verify the Schema

Check that each sampled line has exactly four fields:

```bash
head -1000 output/id_chunk_0.csv | awk -F, '{print NF}' | sort -u
```

Expected output:

```text
4
```

## 8. Check Runtime Report

The orchestrator writes a consolidated report here:

```bash
cat logs/overall_report.txt
```

This includes:

- overall wall-clock runtime
- per-stage timings
- output summary
- throughput estimate
