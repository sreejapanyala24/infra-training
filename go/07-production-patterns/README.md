# GO 7 — Production Patterns

## How to Run

Give execute permission (one-time):

```bash
chmod +x run.sh
```

Run the project:

```bash
./run.sh
```

---

## Architecture

```text
Producer → Kafka (ad-events) → Go Consumer → Postgres
                               ↓
                         Dead Letter Queue
                           (ad-events-dlq)
```

Kafka events are consumed safely and stored into Postgres with retry handling, DLQ support, and idempotent processing.

---

## Expected Output

Producer logs:

```text
event sent successfully
```

Consumer logs:

```text
database connected
kafka reader initialized
kafka DLQ writer initialized
processing message offset=0
parsed event event=ad_click user_id=32
event inserted event=ad_click attempt=1
committed message offset=0
```

---

## Validation

Check inserted rows:

```bash
docker compose exec postgres psql -U postgres -d eventsdb -c "SELECT * FROM events;"
```

Expected:

```text
 id |   type   |                payload                 | kafka_topic | kafka_partition | kafka_offset
----+----------+----------------------------------------+--------------+----------------+--------------
  1 | ad_click | {"event":"ad_click","user_id":32}     | ad-events    | 0              | 0
```

The table uses Kafka metadata:

```text
(topic, partition, offset)
```

to prevent duplicate inserts during retries.

---

## Failure Scenarios

### 1. Database Failure

If Postgres becomes unavailable:

```text
insert failed
retrying insert
insert failed after all retries
```

Consumer retries transient failures safely using exponential backoff before skipping the event or sending it to the DLQ.

---

### 2. Invalid Payload

If Kafka contains invalid JSON:

```text
failed to parse event
sending message to DLQ
```

Consumer redirects invalid messages safely to:

```text
ad-events-dlq
```

without crashing.

---

### 3. Graceful Shutdown

Press:

```text
Ctrl+C
```

Expected logs:

```text
shutdown signal received
starting graceful shutdown
consumer shutdown complete
```

No abrupt termination or panic should occur.

---

## Debugging Guide

### 1. Kafka Not Running

**Symptom:**

```text
failed to read message
```

**Debug:**

```bash
docker ps
```

Check if Kafka container is running.

**Fix:**

```bash
docker compose up -d kafka
```

---

### 2. Database Connection Failure

**Symptom:**

```text
database unavailable
```

**Debug:**

```bash
docker ps
```

Check if Postgres container is running.

**Fix:**

```bash
docker compose up -d postgres
```

---

### 3. Invalid Payload

**Symptom:**

```text
failed to parse event
```

**Debug:**

Check consumer logs:

```bash
cat consumer.log
```

**Fix:**

Send valid JSON payloads only:

```json
{
  "event": "ad_click",
  "user_id": 2
}
```

---

### 4. Multiple Consumer Processes

**Symptom:**

Duplicate rows inserted into database.

**Debug:**

```bash
ps aux | grep consumer
```

**Fix:**

```bash
pkill -f consumer
```

Ensure only one consumer process is running.

---

## Claude Usage + Critique

### Did Claude overcomplicate design?

Claude implemented valid production patterns like:
- retries
- graceful shutdown
- structured logs
- connection pooling



---

### Did you simplify to production minimum?

Yes.

We kept the required production-safe behaviors:
- retries
- timeout handling
- structured logs
- graceful shutdown

