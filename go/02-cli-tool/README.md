# GO 2 — CLI Tool for Infra Operations

## 1. How to Run

1. Give execute permission (one-time)

```bash
chmod +x run.sh
```

2. Run with default command

```bash
./run.sh
```

3. Run with custom command

```bash
./run.sh create-topic --name ads --partitions 3
./run.sh create-topic --name ads
./run.sh delete-topic --name ads
./run.sh delete-topic
```

---

## 2. Expected Output

```text
topic=ads partitions=3 status=created
```

---

## 3. Failure Scenario

```bash
./run.sh create-topic --name ads
```

Expected:

```text
error: invalid partitions
```

Explanation:

The command fails because `--partitions` is required and must be greater than 0.

---

## 4. Debugging Step

To debug the above failure:

### Step 1: Check CLI input

```go
fmt.Println(os.Args)
```

This verifies whether `--partitions` was passed.

---

### Step 2: Check parsed values

```go
fmt.Println("partitions:", *partitions)
```

If it prints `0`, it means the flag was not provided or not parsed correctly.

---

## 5. Claude Usage + Critique

Claude created a clean CLI structure using `FlagSet` and command routing.

However, it introduced unnecessary complexity by:

* adding in-memory state (`topics` map)
* allowing default values for partitions

I simplified the implementation by:

* removing state (keeping CLI stateless)
* enforcing strict validation

