#!/bin/bash

echo "Checking sorted output (sample)..."

docker exec -it kafka-pipeline-kafka-1 kafka-console-consumer \
--topic id-sorted \
--bootstrap-server localhost:9092 \
--from-beginning \
--max-messages 10