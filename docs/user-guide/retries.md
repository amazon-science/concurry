# Retry Mechanisms

**Build robust systems that handle flaky networks and API failures gracefully.**

## The Problem: "The Network is Unreliable"

We've all written code like this:

```python
# The "Try/Catch Hell"
import time

def fetch_data(url):
    for i in range(3):
        try:
            return requests.get(url)
        except Exception:
            time.sleep(2)
    raise Exception("It failed again!")
```

This boilerplate pollutes your business logic. It mixes *what* you want to do (fetch data) with *how* to handle failure (loops, sleeps, catches).

## The Solution: Declarative Reliability

Concurry moves this logic out of your function and into the worker definition.

```python
# The "Concurry Way"
from concurry import worker

@worker(mode="thread")
class APIWorker:
    def fetch_data(self, url: str):
        # Clean business logic!
        return requests.get(url)

worker = APIWorker.options(
    num_retries=3,                    # Try 3 times
    retry_algorithm="exponential",    # Wait 1s, 2s, 4s...
    retry_on=[ConnectionError]        # Only retry network errors
).init()

result = worker.fetch_data("http://api.com").result()
```

### Key Benefits
1.  **Separation of Concerns**: Your code stays clean.
2.  **Smart Backoff**: Built-in Exponential, Linear, and Fibonacci strategies.
3.  **Resource Aware**: Automatically releases Rate Limits during wait times.
4.  **Context Aware**: Retry based on exception type, attempt number, or even output validation.

---

## Decision Matrix: Choosing a Strategy

| Algorithm | Pattern | Best For... |
| :--- | :--- | :--- |
| **`exponential`** | 1s, 2s, 4s, 8s... | **Network / Cloud APIs**. Gives the system time to recover from overload. |
| **`linear`** | 1s, 2s, 3s, 4s... | **Rate Limits**. When you just need to wait out a quota. |
| **`fixed`** | 1s, 1s, 1s... | **Short Glitches**. Quick localized failures. |
| **`fibonacci`** | 1s, 1s, 2s, 3s... | **Complex Systems**. A balanced middle-ground. |

---

## Common Patterns

### 1. The "Circuit Breaker" (Fail Fast)
Sometimes you *don't* want to retry. If a user sends invalid data (400 Bad Request), retrying won't help.

```python
worker = APIWorker.options(
    num_retries=5,
    # Only retry transient network errors
    retry_on=[ConnectionError, TimeoutError],
    # Let ValueErrors (like bad input) crash immediately
).init()
```

### 2. The "Validator" (Retry on Bad Output)
LLMs and flaky APIs sometimes return 200 OK but bad data (e.g., empty JSON). Use `retry_until` to validate the *result*.

```python
def is_valid_json(result, **ctx):
    return "data" in result

@worker(mode="thread")
class LLMWorker: ...

worker = LLMWorker.options(
    num_retries=3,
    retry_until=is_valid_json  # Retry if this returns False!
).init()
```

### 3. The "Jitter" (Avoid Thundering Herds)
If 1000 workers all retry at exactly 1 second, they will hammer the API again simultaneously. Concurry adds **Jitter** by default.

```python
worker = APIWorker.options(
    retry_algorithm="exponential",
    retry_jitter=0.5  # Randomize wait times by +/- 50%
).init()
```

---

## Advanced: Fine-Grained Control

For complex workers, you might want aggressive retries on some methods but none on others.

### Per-Method Configuration
You can pass a dictionary instead of a single value. The `"*"` key sets the default.

```python
@worker(mode="thread")
class DatabaseWorker: ...

worker = DatabaseWorker.options(
    num_retries={
        "*": 0,              # Default: No retries (safe)
        "read_data": 3,      # Retrying reads is safe
        "write_log": 5       # Critical logs must be written
    },
    retry_wait={
        "*": 1.0,
        "write_log": 5.0     # Wait longer for write retries
    }
).init()
```

### Dynamic Filters
For maximum control, pass a function to `retry_on`.

```python
def smart_retry(exception, attempt, **ctx):
    # Stop retrying if we've been trying for > 30 seconds
    if ctx['elapsed_time'] > 30:
        return False
    return isinstance(exception, ConnectionError)

worker = APIWorker.options(retry_on=smart_retry).init()
```

## See Also
*   [**Workers**](workers.md) for basic worker options.
*   [**Configuration**](configuration.md) for setting global defaults.
