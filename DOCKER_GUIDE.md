# Docker Guide

## Build the Image

```bash
docker build -t kafka-pipeline:latest .
```

The image is a multi-stage build:

- builder stage compiles the Go binaries
- runtime stage contains the binaries, scripts, and documentation only

## Run With Compose

The published image is designed for a single command run:

```bash
docker compose up --abort-on-container-exit --exit-code-from pipeline
```

Compose starts:

- `zookeeper`
- `kafka`
- `pipeline`

Kafka is reachable as:

- `localhost:9092` from the host
- `kafka:29092` from other containers in the Compose network

The `pipeline` service image defaults to the bundled entrypoint, which waits for Kafka and then runs the full orchestrator inside the container automatically.

## Resource Limits

The Compose file sets explicit limits:

- `zookeeper`: `256m`, `0.50` CPU
- `kafka`: `1g`, `1.50` CPU
- `pipeline`: `768m`, `2.00` CPU

Total configured budget: `2GB` memory and `4` CPUs.

## Clean Rerun

For a fully clean rerun during development:

```bash
docker compose down -v
docker compose up --abort-on-container-exit --exit-code-from pipeline
```

For local non-Docker app runs, `scripts/start.sh` is still available to reset Kafka and recreate topics.

## Verify

Quick sample:

```bash
./scripts/verify.sh
```

Detailed verification:

```bash
cat VERIFICATION.md
```

## Optional: Publish to Docker Hub

If you want to publish the image yourself:

```bash
docker tag kafka-pipeline:latest YOUR_USER/kafka-pipeline:latest
docker push YOUR_USER/kafka-pipeline:latest
```

Use your own Docker Hub namespace in the docs and scripts if you publish it externally.
