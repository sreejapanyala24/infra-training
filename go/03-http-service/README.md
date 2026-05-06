# GO 3 — HTTP Service (Infra Style)

## How to Run

Give execute permission (one-time):

```bash
chmod +x run.sh
```

Start the service:

```bash
./run.sh
```

Test endpoints:

```bash
curl localhost:8080/health
curl localhost:8080/ready
curl localhost:8080/xyz
```

---

## Expected Output

Service start:

```text
starting server on :8080
```

Health endpoint:

```json
{"status":"ok"}
```

Ready endpoint:

```json
{"ready":true}
```

Unknown route:

```text
404 not found
```

Request logs (middleware):

```text
method=GET path=/health
method=GET path=/ready
method=GET path=/xyz
```

---

## Failure Scenario

### Port Already in Use

Command:

```bash
./run.sh
```

Output:

```text
error: port already in use
```

---

## Debugging Step

Check which process is using port 8080:

```bash
lsof -i :8080
```

Kill the process:

```bash
kill -9 <PID>
```

## Claude Usage + Critique

### Did Claude add too many frameworks?
No, Claude used only the Go standard library. However, it introduced several advanced production features (graceful shutdown, signal handling, timeouts, port checks) that were not required for this assignment.

---

### Did you strip it down to stdlib?
Yes. The final implementation uses only the Go standard library (`net/http`, `fmt`) and avoids any external dependencies.

---

### Did you avoid overengineering?
Yes. The solution was simplified to include only what was required:

- `/health` endpoint
- `/ready` endpoint
- custom `404 not found` response
- basic error handling for port conflicts
- simple request logging middleware

Unnecessary features from Claude’s version were removed to keep the implementation minimal and aligned with the assignment.