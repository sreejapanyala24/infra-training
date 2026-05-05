# GO 1 — Config Reader + Structured Logging

## Run

### 1. Give execute permission (one-time)

```bash
chmod +x run.sh
```

---

### 2. Run with default config

```bash
./run.sh
```

---

### 3. Run with custom file

```bash
./run.sh config.json
./run.sh missing.json
```
---

## Expected Output

```text
service=ad-ingestion env=dev region=us-west-1 status=started
```

---

## Failure Scenarios

### 1. File Not Found

**Command:**

```bash
./run.sh missing.json
```

**Output:**

```text
error: file not found
```

---

### 2. Cannot Open File (Permission Issues)

**Command:**

```bash
./run.sh config.json
```

**Output:**

```text
error: cannot open file
```

---

### 3. Invalid JSON

**Example invalid file:**

```json
{
  "service": "ad-ingestion",
  "env": "dev"
  "region": "us-west-1"
}
```

**Command:**

```bash
./run.sh config.json
```

**Output:**

```text
error: invalid config
```

---

## Debugging Guide

### 1. error: file not found

**Cause:**

* File does not exist
* Wrong file path

**Debug:**

```bash
ls
```

**Fix:**

* Ensure file exists in the current directory

---

### 2. error: cannot open file

**Cause:**

* File exists but lacks read permissions

**Debug:**

```bash
ls -l config.json
```

**Fix:**

```bash
chmod +r config.json
```

---

### 3. error: invalid config

**Cause:**

* Malformed JSON

**Debug:**

```bash
cat config.json
```

**Fix:**

Use valid JSON:

```json
{
"service": "ad-ingestion",
"env": "dev",
"region": "us-west-1"
}
```

---

## Claude Usage + Critique

### Prompt Used

"Write Go config loader"

---

### Observations

Claude over-engineered the solution. It assumed the need for a reusable configuration library, while the actual requirement was a simple one-off JSON config loader.

**Unnecessary additions:**

* Support for JSON, YAML, and TOML (only JSON is used)
* Dot-notation access (e.g., `database.host`) instead of struct mapping
* Thread-safety using mutexes (not needed for single-threaded code)
* Type-safe getter methods (`GetString`, `GetInt`, etc.)
* External dependencies (`yaml`, `toml`) when zero dependencies were sufficient

---

### Error Handling Comparison

Claude handled file-related errors correctly.

---

### Did you simplify or fix anything?

No changes were made to my code.
