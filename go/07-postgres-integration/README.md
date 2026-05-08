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
Kafka → Go Consumer → Postgres
```

Kafka events are consumed and stored into Postgres with production-safe handling.

---

## Expected Output

```text
event sent
```

Consumer logs:

```text
database connected
kafka reader initialized
event stored
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
  1 | ad_click  | {"event":"ad_click","user_id":2}
```

---

## Failure Scenarios

### 1. Database Failure

If Postgres becomes unavailable:

```text
insert failed
retrying insert
insert failed after all retries
```

Consumer retries safely before skipping the event.

---

### 2. Invalid Payload

If Kafka contains invalid JSON:

```text
invalid payload
```

Consumer skips the message safely without crashing.

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
invalid payload
```

**Debug:**

Check consumer logs:

```bash
cat starter/consumer.log
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

