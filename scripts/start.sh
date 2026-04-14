#!/bin/bash

echo "Starting Kafka..."
docker-compose up -d

echo "Waiting for Kafka to start..."
sleep 10

echo "Creating topics.."

docker exec -it tgassignment-kafka-1 kafka-topics \
    --create \
    --topic source\
    --bootstrap-server localhost:9092 \
    --partitions 4 \
    --replication-factor 1

docker exec -it tgassignment-kafka-1 kafka-topics \
    --create \
    --topic id-sorted\
    --bootstrap-server localhost:9092 \
    --partitions 4 \
    --replication-factor 1

docker exec -it tgassignment-kafka-1 kafka-topics \
    --create \
    --topic name-sorted\
    --bootstrap-server localhost:9092 \
    --partitions 4 \
    --replication-factor 1

docker exec -it tgassignment-kafka-1 kafka-topics \
    --create \
    --topic continent-sorted\
    --bootstrap-server localhost:9092 \
    --partitions 4 \
    --replication-factor 1


echo "Kafka setup completed"