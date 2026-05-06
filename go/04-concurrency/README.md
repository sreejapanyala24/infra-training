# GO 4 — Concurrency (Real Infra Pattern)

## How to Run

Give execute permission (one-time):

```bash
chmod +x run.sh
```

Run the program:

```bash
./run.sh
```

---

## Expected Output

Output order may vary because jobs run concurrently:

```text
job=1 done
job=3 done
job=2 done
...
completed jobs=100
```

---

## Failure Scenario

### Race Condition With Shared Counter

If multiple goroutines update a shared counter directly:

```go
completed++
```

the final count may become incorrect:

```text
completed jobs=94
completed jobs=97
```

---

## Debugging Step

Run with Go race detector:

```bash
go run -race starter/main.go 
```

If shared state is updated unsafely, Go will print:

```text
WARNING: DATA RACE
```

Fix:
- avoid shared counters
- use channels for communication between goroutines
- count completed jobs in the main goroutine

---

## Claude Usage + Critique

### Did Claude correctly identify race condition?

Yes. Claude correctly identified that using `counter++` from multiple goroutines causes a race condition because multiple goroutines can read and write the same variable at the same time.

---

### Did it suggest unnecessary abstractions?

Yes. Claude suggested more abstractions than required for this assignment:

1. `WorkerPool` struct (over-engineered for a simple worker pool)
2. `Job` struct (not needed since only integer job IDs were used)
3. Multiple implementations bundled together instead of one clean final solution

---

### Did you simplify solution?

Yes. The solution was simplified to a minimal channel-based worker pool implementation.

The final version keeps only:
- goroutines
- channels
- `sync.WaitGroup`
- fixed-size worker pool
- simple job completion counting through a `results` channel
