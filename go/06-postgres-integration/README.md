# GO 6 — Postgres Integration

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
Kafka → Go Consumer → Postgres
```

Kafka events are consumed and stored into Postgres.

---

## Table Schema

```sql
CREATE TABLE events (
  id SERIAL,
  type TEXT,
  payload JSONB
);
```

---

## Expected Output

```text
event sent
stored event ad_click user=123
```

---

## Validation

Check inserted rows:

```sql
SELECT * FROM events;
```

Expected:

```text
 id |   type    |                payload
----+-----------+----------------------------------
  1 | ad_click  | {"event":"ad_click","user_id":123}
```

---

## Failure Scenarios

### 1. Postgres Down

If Postgres is unavailable:

```text
error: database unavailable
```

Consumer should fail safely without crashing unexpectedly.

---

### 2. Invalid Payload

If Kafka contains invalid JSON:

```text
error: invalid payload
```

Consumer skips the message safely.

---

## Debugging Guide

### 1. Postgres Down

**Symptom:**

```text
error: database unavailable
```

**Debug:**

```bash
docker ps
```

Check if the Postgres container is running.

**Fix:**

```bash
docker compose up -d postgres
```

---

### 2. Invalid Payload

**Symptom:**

```text
error: invalid payload
```

**Debug:**

Check the consumer log:

```bash
cat starter/consumer.log
```

**Fix:**

Send valid JSON payloads only:

```json
{
  "event": "ad_click",
  "user_id": 123
}
```

---

## Claude Usage + Critique

### Did Claude suggest unsafe DB handling?

Partially.

Both Claude’s code and our implementation used hardcoded database credentials:

```go
"host=localhost port=5432 user=postgres password=postgres dbname=eventsdb sslmode=disable"
```

This is acceptable for local development, but not ideal for production systems.

A production-style approach would use environment variables instead of hardcoded credentials.

---

### Did you fix connection leaks?

No major connection leak existed in Claude’s implementation.

Claude already handled resource cleanup using:
- `defer db.Close()`
- `database/sql` connection pooling

We kept the same safe cleanup approach and added explicit pool limits:

```go
db.SetMaxOpenConns(5)
db.SetMaxIdleConns(5)
db.SetConnMaxLifetime(5 * time.Minute)
```

This improved connection reuse and prevented uncontrolled database connection growth during long-running event processing.