# GO 5 — Kafka Integration (First Real System)

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

## Expected Output

```text
event sent

consumer output:
received event ad_click user=123
```

---

## Failure Scenarios

### 1. Kafka Unavailable

If Kafka is not running, producer retries safely:

```text
error: kafka unavailable retry=1
error: kafka unavailable retry=2
error: kafka unavailable retry=3
error: failed to send event
```

---

### 2. Consumer Down

Producer still sends events successfully because Kafka stores messages independently of consumers.

---

## Debugging Guide

### 1. Kafka Unavailable

**Cause:**
- Kafka container not running
- Kafka still starting
- Topic does not exist

**Debug:**

Check running containers:

```bash
docker ps
```

Check topic list:

```bash
docker compose exec kafka kafka-topics --list \
  --bootstrap-server localhost:9092
```

**Fix:**

Start Kafka:

```bash
docker compose up -d
```

Create topic:

```bash
docker compose exec kafka kafka-topics --create \
  --topic ad-events \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists
```

---

### 2. Consumer Down

**Cause:**
- consumer process not running

**Debug:**

Check consumer logs:

```bash
cat starter/consumer.log
```

**Fix:**

Run the project again:

```bash
./run.sh
```

---

## Claude Usage + Critique

### Did Claude assume “perfect Kafka”?

Partially.

Claude handled several Kafka-related failures, but still relied on some default Kafka behaviors.

Handled in Claude’s code:
- Kafka broker unavailable handling
- producer retry logic
- malformed JSON handling in consumer

However, the consumer relied on default Kafka behaviors such as:
- reading only newly arriving messages for a consumer group
- automatic offset management
- assuming Kafka topics already exist

---

### Did Claude handle failure?

Partially.

Claude handled:
- Kafka unavailable errors
- malformed consumer messages
- producer retries

Missing or simplified areas:
- consumer offset configuration
- consumer retry/backoff behavior
- explicit offset management
- graceful shutdown handling

---

### Did you add missing retry logic?

Yes.

The final producer implementation includes:
- 3 retry attempts
- delay between retries
- graceful failure output
- early return on successful send


