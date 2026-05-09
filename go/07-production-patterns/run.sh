#!/bin/bash

set -e

cd solution || exit 1

echo "======================================"
echo "Cleaning up old consumer processes..."
echo "======================================"

pkill -f "/consumer" || true
pkill -f "go run consumer.go" || true

sleep 2

echo "======================================"
echo "Starting Docker containers..."
echo "======================================"

docker compose up -d

echo "======================================"
echo "Waiting for Postgres..."
echo "======================================"

until docker compose exec postgres pg_isready -U postgres >/dev/null 2>&1; do
  sleep 2
done

echo "Postgres is ready."

echo "======================================"
echo "Waiting for Kafka..."
echo "======================================"

sleep 10

echo "======================================"
echo "Creating Kafka topic: ad-events"
echo "======================================"

docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic ad-events \
  --partitions 1 \
  --replication-factor 1

echo "======================================"
echo "Creating Kafka DLQ topic: ad-events-dlq"
echo "======================================"

docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic ad-events-dlq \
  --partitions 1 \
  --replication-factor 1

echo "======================================"
echo "Creating events table..."
echo "======================================"

docker compose exec postgres psql -U postgres -d eventsdb -c "
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,

    type TEXT NOT NULL,

    payload JSONB NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    kafka_topic TEXT NOT NULL,

    kafka_partition INT NOT NULL,

    kafka_offset BIGINT NOT NULL,

    CONSTRAINT events_kafka_unique
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
"

echo "======================================"
echo "Cleaning old table data..."
echo "======================================"

docker compose exec postgres psql -U postgres -d eventsdb -c "
TRUNCATE TABLE events;
"

echo "======================================"
echo "Starting consumer..."
echo "======================================"

go run consumer.go config.go dlq.go errors.go > consumer.log 2>&1 &
CONSUMER_PID=$!

echo "Consumer PID: $CONSUMER_PID"

sleep 8

echo "======================================"
echo "Running producer..."
echo "======================================"

go run producer.go errors.go

echo "======================================"
echo "Waiting for consumer to process message..."
echo "======================================"

sleep 5

echo "======================================"
echo "Database data:"
echo "======================================"

docker compose exec postgres psql -U postgres -d eventsdb -c "
SELECT * FROM events;
"

echo "======================================"
echo "Consumer logs:"
echo "======================================"

cat consumer.log

echo "======================================"
echo "Stopping consumer..."
echo "======================================"

pkill -P $CONSUMER_PID || true
kill $CONSUMER_PID || true
pkill -f "/consumer" || true
pkill -f "go run consumer.go" || true

sleep 2

echo "======================================"
echo "Verifying no consumer processes remain..."
echo "======================================"

ps aux | grep consumer || true

echo "======================================"
echo "Done."
echo "======================================"