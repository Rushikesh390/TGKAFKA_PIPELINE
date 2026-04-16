# Kafka CSV Sorting Pipeline

This project implements the assignment pipeline in Go:

1. Generate `50,000,000` CSV records that match the required schema.
2. Publish the generated records to Kafka topic `source`.
3. Read `source`, externally sort the dataset by `id`, `name`, and `continent`.
4. Publish the final sorted streams to Kafka topics `id`, `name`, and `continent`.

The implementation is designed to be Docker-first, rerunnable, and buildable with `go build ./...`.

## Schema

Each record uses this CSV layout:

```text
id,name,address,continent
```

- `id`: 32-bit integer
- `name`: English letters only, length `10-15`
- `address`: letters, digits, and spaces, length `15-20`
- `continent`: one of `North America`, `Asia`, `South America`, `Europe`, `Africa`, `Australia`

## High-Level Design

```text
Producer -> Kafka topic: source
         -> Consumer -> sorted chunk files on disk
         -> Merger -> Kafka topics: id, name, continent
```

- The producer writes directly to Kafka in batches.
- The consumer reads from all Kafka partitions, processes a bounded in-memory batch, and writes sorted chunk files.
- The merger performs a k-way merge over chunk files and streams final results back to Kafka.

## Quick Start

### Option 1: Docker Compose

Run the full stack with one command:

```bash
docker compose up --abort-on-container-exit --exit-code-from pipeline
```

This starts ZooKeeper, Kafka, and the pipeline container. The pipeline waits for Kafka, runs the full job automatically, writes chunk files to `./output`, and writes the runtime report to `./logs/overall_report.txt`.

### Option 2: Host Run

Use the helper script:

```bash
./scripts/run_all.sh
```

`run_all.sh` calls `scripts/start.sh`, which resets the Kafka stack for a clean run before recreating topics.

## Build

Local Go build:

```bash
go build ./...
```

Docker image build:

```bash
docker build -t kafka-pipeline:latest .
```

## What Was Fixed

- Output topics now match the assignment exactly: `id`, `name`, `continent`.
- The repo now builds cleanly with `go build ./...`.
- The consumer no longer keeps multiple large copied batches in flight by default.
- Kafka networking is consistent between host and container runs.
- Stage failures now fail fast instead of silently continuing.
- The default run path now works with prebuilt binaries inside the runtime image.

## Resource Strategy

The pipeline is tuned for a constrained environment:

- Kafka + ZooKeeper run in Docker with explicit memory and CPU limits in `docker-compose.yml`.
- The consumer defaults to:
  - `CONSUMER_BATCH_SIZE=200000`
  - `CONSUMER_NUM_WORKERS=1`
  - only one queued batch in flight
- Sorting inside each batch still runs in parallel across the three sort dimensions.

This keeps memory bounded while still using CPU for the hot path.

## Useful Scripts

- `scripts/start.sh`: local helper to reset Kafka and create required topics
- `scripts/run_all.sh`: full pipeline run
- `scripts/run_producer.sh`: producer only
- `scripts/run_consumer.sh`: consumer only
- `scripts/run_merge.sh`: merger only
- `scripts/verify.sh`: quick Kafka sample check

## Verification

See [VERIFICATION.md](VERIFICATION.md) for count checks, sort checks, and sample commands.

## Project Layout

```text
cmd/
  producer/        producer entrypoint
  consumer/        chunking + sorting entrypoint
  merger/          k-way merge entrypoint

internal/
  config/          shared runtime configuration
  generator/       random record generation
  kafka/           Kafka readers/writers
  merger/          heap merge implementation
  sorter/          indexed sort + file writing

pkg/
  models/          record model
  utils/           CSV formatting and parsing

scripts/
  *.sh             build, run, setup, verify helpers
```

## More Detail

- [ARCHITECTURE.md](ARCHITECTURE.md) explains algorithms, bottlenecks, and scaling.
- [DOCKER_GUIDE.md](DOCKER_GUIDE.md) covers build/run details for Docker.
