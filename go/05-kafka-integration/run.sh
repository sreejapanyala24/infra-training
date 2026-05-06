#!/bin/bash

cd starter || exit 1

echo "starting kafka containers..."
docker compose up -d

echo "waiting for kafka to be ready..."
sleep 25

echo "creating topic..."
docker compose exec kafka kafka-topics --create \
  --topic ad-events \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

echo "starting consumer in background..."
go run consumer.go > consumer.log 2>&1 &
CONSUMER_PID=$!

sleep 5

echo "sending event..."
go run producer.go

sleep 5

echo "consumer output:"
cat consumer.log

kill $CONSUMER_PID 2>/dev/null