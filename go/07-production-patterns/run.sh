#!/bin/bash

cd starter || exit 1

echo "starting kafka and postgres containers..."
docker compose up -d

echo "waiting for services to be ready..."
sleep 25

echo "creating kafka topic..."
docker compose exec kafka kafka-topics --create \
  --topic ad-events \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

echo "creating postgres table..."
docker compose exec postgres psql -U postgres -d eventsdb -c "
CREATE TABLE IF NOT EXISTS events (
  id SERIAL,
  type TEXT,
  payload JSONB
);
"

echo "starting consumer in background..."
go run consumer.go > consumer.log 2>&1 &
CONSUMER_PID=$!

sleep 5

echo "sending event..."
go run producer.go

sleep 5

echo "consumer output:"
cat consumer.log

echo "database rows:"
docker compose exec postgres psql -U postgres -d eventsdb -c "SELECT * FROM events;"

echo "stopping consumer..."
kill $CONSUMER_PID 2>/dev/null