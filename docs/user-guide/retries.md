# Retry Mechanisms

Concurry provides a comprehensive retry system for handling transient failures in worker method calls. The retry mechanism supports multiple backoff strategies, exception filtering, output validation, and seamless integration with all worker features.

## Overview

The retry system allows you to automatically retry failed operations with:

- **Multiple backoff strategies**: Exponential, Linear, and Fibonacci with configurable jitter
- **Exception filtering**: Retry on specific exception types or using custom logic
- **Output validation**: Retry when output doesn't meet requirements (e.g., LLM response validation)
- **Full context**: Retry filters receive attempt number, elapsed time, and more
- **Actor-side execution**: Retries happen on the worker side for efficiency
- **Automatic limit release**: Resource limits are automatically released between retry attempts

## Basic Usage

### Simple Retry Configuration

Configure retries when creating a worker:

```python
from concurry import Worker

class APIWorker(Worker):
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
    
    def fetch_data(self, id: int) -> dict:
        # May raise ConnectionError, TimeoutError, etc.
        response = requests.get(f"{self.endpoint}/{id}")
        return response.json()

# Retry up to 3 times on any exception
worker = APIWorker.options(
    mode="thread",
    num_retries=3,
    retry_algorithm="exponential",
    retry_wait=1.0,
    retry_jitter=0.3
).init(endpoint="https://api.example.com")

result = worker.fetch_data(123).result()
worker.stop()
```

### Retry Parameters

All retry parameters are passed to `Worker.options()`:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `num_retries` | int | 0 | Maximum number of retry attempts after initial failure |
| `retry_on` | type \| callable \| list | `[Exception]` | Exception types or filters that trigger retries |
| `retry_algorithm` | str | "exponential" | Backoff strategy: "exponential", "linear", "fibonacci" |
| `retry_wait` | float | 1.0 | Base wait time in seconds between retries |
| `retry_jitter` | float | 0.3 | Jitter factor (0-1) for randomizing wait times |
| `retry_until` | callable \| list | None | Validation functions for output |

## Retry Algorithms

### Exponential Backoff (Default)

Doubles the wait time with each retry attempt:

```python
worker = MyWorker.options(
    mode="thread",
    num_retries=5,
    retry_algorithm="exponential",
    retry_wait=1.0,  # Base wait time
    retry_jitter=0.3  # Add randomness
).init()

# Wait times (with jitter):
# Attempt 1: random(0, 2.0)   seconds  (base_wait * 2^0)
# Attempt 2: random(0, 4.0)   seconds  (base_wait * 2^1)
# Attempt 3: random(0, 8.0)   seconds  (base_wait * 2^2)
# Attempt 4: random(0, 16.0)  seconds  (base_wait * 2^3)
# Attempt 5: random(0, 32.0)  seconds  (base_wait * 2^4)
```

**Best for**: Network requests, API calls, distributed systems

### Linear Backoff

Increases wait time linearly:

```python
worker = MyWorker.options(
    mode="thread",
    num_retries=5,
    retry_algorithm="linear",
    retry_wait=2.0,
    retry_jitter=0.3
).init()

# Wait times (with jitter):
# Attempt 1: random(0, 2.0)   seconds  (base_wait * 1)
# Attempt 2: random(0, 4.0)   seconds  (base_wait * 2)
# Attempt 3: random(0, 6.0)   seconds  (base_wait * 3)
# Attempt 4: random(0, 8.0)   seconds  (base_wait * 4)
# Attempt 5: random(0, 10.0)  seconds  (base_wait * 5)
```

**Best for**: Rate-limited APIs, predictable backoff patterns

### Fibonacci Backoff

Wait times follow the Fibonacci sequence:

```python
worker = MyWorker.options(
    mode="thread",
    num_retries=5,
    retry_algorithm="fibonacci",
    retry_wait=1.0,
    retry_jitter=0.3
).init()

# Wait times (with jitter):
# Attempt 1: random(0, 1.0)   seconds  (base_wait * fib(1) = 1)
# Attempt 2: random(0, 1.0)   seconds  (base_wait * fib(2) = 1)
# Attempt 3: random(0, 2.0)   seconds  (base_wait * fib(3) = 2)
# Attempt 4: random(0, 3.0)   seconds  (base_wait * fib(4) = 3)
# Attempt 5: random(0, 5.0)   seconds  (base_wait * fib(5) = 5)
```

**Best for**: Balancing aggressive and conservative retry strategies

### Full Jitter

All strategies use the "Full Jitter" algorithm from AWS:

```
sleep = random_between(0, calculated_wait)
```

This prevents thundering herd problems by randomizing retry timing. Set `retry_jitter=0` to disable.

## Exception Filtering

### Retry on Specific Exceptions

Specify which exceptions should trigger retries:

```python
class NetworkWorker(Worker):
    def fetch(self, url: str) -> str:
        # May raise ConnectionError, TimeoutError, HTTPError, etc.
        return requests.get(url).text

# Only retry on network-related errors
worker = NetworkWorker.options(
    mode="thread",
    num_retries=3,
    retry_on=[ConnectionError, TimeoutError]
).init()

# ConnectionError or TimeoutError → retry
# HTTPError or other exceptions → fail immediately
```

### Custom Exception Filters

Use callables for complex exception filtering logic:

```python
def should_retry_api_error(exception, **context):
    """Retry only on specific API error codes."""
    if isinstance(exception, requests.HTTPError):
        # Retry on 429 (rate limit) or 503 (service unavailable)
        return exception.response.status_code in [429, 503]
    return isinstance(exception, (ConnectionError, TimeoutError))

worker = APIWorker.options(
    mode="thread",
    num_retries=5,
    retry_on=should_retry_api_error
).init()
```

### Multiple Filters

Combine exception types and custom filters:

```python
worker = MyWorker.options(
    mode="thread",
    num_retries=3,
    retry_on=[
        ConnectionError,  # Always retry on connection errors
        lambda exception, **ctx: (
            isinstance(exception, ValueError) and "retry" in str(exception)
        )  # Retry ValueError only if message contains "retry"
    ]
).init()
```

### Context in Filters

Exception filters receive rich context:

```python
def smart_retry_filter(exception, attempt, elapsed_time, method_name, **kwargs):
    """Advanced retry logic using context."""
    # Don't retry after 30 seconds
    if elapsed_time > 30:
        return False
    
    # Give up after 3 attempts for certain errors
    if isinstance(exception, ValueError) and attempt >= 3:
        return False
    
    # Always retry network errors
    if isinstance(exception, ConnectionError):
        return True
    
    return False

worker = MyWorker.options(
    mode="thread",
    num_retries=5,
    retry_on=smart_retry_filter
).init()
```

**Available context**:
- `exception`: The exception that was raised
- `attempt`: Current attempt number (1-indexed)
- `elapsed_time`: Seconds since first attempt
- `method_name`: Name of the method being called
- `args`: Original positional arguments
- `kwargs`: Original keyword arguments

## Output Validation

Use `retry_until` to retry when output doesn't meet requirements, even without exceptions.

### Simple Validation

```python
class LLMWorker(Worker):
    def generate_json(self, prompt: str) -> dict:
        """Generate JSON from LLM (may return invalid JSON)."""
        response = self.llm.generate(prompt)
        return json.loads(response)  # May fail or return wrong structure

def is_valid_json(result, **context):
    """Check if result has required fields."""
    return isinstance(result, dict) and "data" in result and "status" in result

worker = LLMWorker.options(
    mode="thread",
    num_retries=5,
    retry_until=is_valid_json  # Retry until validation passes
).init()

# Will retry up to 5 times until result has required fields
result = worker.generate_json("Generate user data").result()
```

### Multiple Validators

All validators must pass for the result to be accepted:

```python
def has_required_fields(result, **ctx):
    return "id" in result and "name" in result

def has_valid_values(result, **ctx):
    return result.get("id", 0) > 0 and len(result.get("name", "")) > 0

worker = DataWorker.options(
    mode="thread",
    num_retries=3,
    retry_until=[has_required_fields, has_valid_values]
).init()
```

### Combining Exceptions and Validation

```python
worker = LLMWorker.options(
    mode="thread",
    num_retries=5,
    retry_on=[json.JSONDecodeError, KeyError],  # Retry on parsing errors
    retry_until=lambda result, **ctx: len(result.get("text", "")) > 100  # And until long enough
).init()
```

### RetryValidationError

When validation fails after all retries, `RetryValidationError` is raised:

```python
from concurry import RetryValidationError

try:
    result = worker.generate_json(prompt).result()
except RetryValidationError as e:
    print(f"Failed after {e.attempts} attempts")
    print(f"All results: {e.all_results}")
    print(f"Validation errors: {e.validation_errors}")
    
    # Use the last result even though validation failed
    last_output = e.all_results[-1]
```

## Integration with Workers

### Custom Workers with Retry

Retries work with all worker types:

```python
class DataProcessor(Worker):
    def __init__(self, multiplier: int):
        self.multiplier = multiplier
    
    def process(self, value: int) -> int:
        # May fail transiently
        return self.fetch_and_multiply(value)

# Retry configuration applies to all methods
worker = DataProcessor.options(
    mode="thread",
    num_retries=3,
    retry_algorithm="exponential"
).init(multiplier=10)

result = worker.process(5).result()
```

### TaskWorker with Retry

`TaskWorker.submit()` and `TaskWorker.map()` support retries:

```python
from concurry import TaskWorker

def flaky_function(x):
    if random.random() < 0.5:
        raise ConnectionError("Transient error")
    return x * 2

# Configure retry for task submissions
worker = TaskWorker.options(
    mode="process",
    num_retries=3,
    retry_on=[ConnectionError]
).init()

# Automatically retries on failure
future = worker.submit(flaky_function, 10)
result = future.result()  # Will retry up to 3 times

# Works with map() too
results = list(worker.map(flaky_function, range(10)))

worker.stop()
```

### Async Functions with Retry

Retries work seamlessly with async functions:

```python
class AsyncAPIWorker(Worker):
    async def fetch_data(self, url: str) -> dict:
        """Async method with retry."""
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                return await response.json()

worker = AsyncAPIWorker.options(
    mode="asyncio",  # Use asyncio mode for best performance
    num_retries=3,
    retry_on=[aiohttp.ClientError]
).init()

result = worker.fetch_data("https://api.example.com/data").result()
worker.stop()
```

### All Execution Modes

Retries work across all execution modes:

```python
# Thread mode - good for I/O-bound with retries
worker = MyWorker.options(mode="thread", num_retries=3).init()

# Process mode - good for CPU-bound with retries
worker = MyWorker.options(mode="process", num_retries=3).init()

# Asyncio mode - best for async I/O with retries
worker = MyWorker.options(mode="asyncio", num_retries=3).init()

# Ray mode - distributed execution with retries
worker = MyWorker.options(mode="ray", num_retries=3).init()
```

## Integration with Worker Pools

Retries work transparently with worker pools:

```python
class APIWorker(Worker):
    def fetch(self, id: int) -> dict:
        return requests.get(f"https://api.example.com/{id}").json()

# Pool of 10 workers, each with retry configuration
pool = APIWorker.options(
    mode="thread",
    max_workers=10,
    num_retries=3,
    retry_algorithm="exponential",
    retry_on=[ConnectionError, TimeoutError]
).init()

# Each request to the pool will retry on failure
futures = [pool.fetch(i) for i in range(100)]
results = [f.result() for f in futures]

pool.stop()
```

**Key Points**:
- Each worker in the pool has the same retry configuration
- Retries happen on the worker that received the request
- Load balancing happens before retry logic (not during retries)
- Pool statistics don't include retry attempts (only successful dispatches)

## Integration with Limits

Retries automatically release and reacquire limits between attempts:

### Resource Limits with Retry

```python
from concurry import ResourceLimit

class DatabaseWorker(Worker):
    def query(self, sql: str) -> list:
        # Acquire connection from limit
        with self.limits.acquire(requested={"connections": 1}) as acq:
            result = execute_query(sql)
            acq.update(usage={"connections": 1})
            return result

worker = DatabaseWorker.options(
    mode="thread",
    num_retries=3,
    retry_on=[DatabaseError],
    limits=[ResourceLimit(key="connections", capacity=5)]
).init()

# If query fails, limit is automatically released before retry
result = worker.query("SELECT * FROM users").result()
```

**How it works**:
1. Limit is acquired before method execution
2. Method executes
3. If it fails and should retry:
   - Limit is automatically released
   - Wait for retry delay
   - Limit is reacquired for next attempt
4. If it succeeds or retries exhausted:
   - Limit is released normally

### Rate Limits with Retry

```python
from concurry import RateLimit

class APIWorker(Worker):
    def call_api(self, endpoint: str) -> dict:
        with self.limits.acquire(requested={"requests": 1}) as acq:
            response = requests.get(f"{self.base_url}/{endpoint}")
            acq.update(usage={"requests": 1})
            return response.json()

worker = APIWorker.options(
    mode="thread",
    num_retries=5,
    retry_algorithm="exponential",
    limits=[RateLimit(key="requests", window_seconds=60, capacity=100)]
).init()

# Retries will respect rate limit (released between attempts)
```

### Shared Limits with Retry

When using shared limits across a pool, retries automatically coordinate:

```python
from concurry import LimitSet, ResourceLimit

# Create shared limit
shared_limits = LimitSet(
    limits=[ResourceLimit(key="db_connections", capacity=10)],
    shared=True,
    mode="thread"
)

# Pool shares the limit
pool = DatabaseWorker.options(
    mode="thread",
    max_workers=20,  # 20 workers share 10 connections
    num_retries=3,
    limits=shared_limits
).init()

# Each worker's retries properly release/acquire shared limits
```

### Call Limits with Retry

```python
from concurry import CallLimit

# Limit total concurrent calls per worker
worker = MyWorker.options(
    mode="thread",
    num_retries=3,
    limits=[CallLimit(window_seconds=1, capacity=10)]  # Max 10 calls/sec
).init()

# Retry attempts don't count against call limit (automatically managed)
```

## Advanced Patterns

### Retry with Context-Aware Validation

```python
def validate_result(result, attempt, elapsed_time, **ctx):
    """Accept lower quality results after multiple attempts."""
    if attempt <= 2:
        # First 2 attempts: strict validation
        return result.get("confidence", 0) > 0.9
    else:
        # Later attempts: relaxed validation
        return result.get("confidence", 0) > 0.7

worker = MLWorker.options(
    mode="thread",
    num_retries=5,
    retry_until=validate_result
).init()
```

### Conditional Retry Based on Method Arguments

```python
def should_retry_depending_on_args(exception, args, kwargs, **ctx):
    """Retry logic that considers the original arguments."""
    # Don't retry for premium users (args[0] is user_id)
    if "premium" in kwargs.get("user_type", ""):
        return False
    
    # Retry for standard users on network errors
    return isinstance(exception, ConnectionError)

worker = UserDataWorker.options(
    mode="thread",
    num_retries=3,
    retry_on=should_retry_depending_on_args
).init()
```

### Exponential Backoff with Max Wait

```python
def calculate_wait_with_cap(attempt, config):
    """Custom wait calculator with maximum."""
    from concurry.core.retry import calculate_retry_wait
    wait = calculate_retry_wait(attempt, config)
    return min(wait, 30.0)  # Cap at 30 seconds

# Use standard exponential but with your own cap logic
worker = MyWorker.options(
    mode="thread",
    num_retries=10,
    retry_algorithm="exponential",
    retry_wait=1.0
).init()
```

### Retry with Fallback Values

```python
from concurry import RetryValidationError

def fetch_with_fallback(worker, key):
    """Fetch data with automatic fallback on validation failure."""
    try:
        return worker.fetch(key).result()
    except RetryValidationError as e:
        # Use the best result from all attempts
        return max(e.all_results, key=lambda r: r.get("score", 0))

worker = DataWorker.options(
    mode="thread",
    num_retries=3,
    retry_until=lambda r, **ctx: r.get("score", 0) > 0.8
).init()

result = fetch_with_fallback(worker, "data_key")
```

## Performance Considerations

### Retry Overhead

- **No overhead when disabled** (`num_retries=0`, the default)
- **Minimal overhead on success** (~microseconds for retry config check)
- **Overhead on retry**: Wait time + re-execution time
- **Actor-side retries**: No round-trip overhead between retries

### Choosing Retry Parameters

```python
# Fast-fail for non-critical operations
worker = MyWorker.options(
    mode="thread",
    num_retries=1,
    retry_algorithm="linear",
    retry_wait=0.1
).init()

# Aggressive retry for critical operations
worker = CriticalWorker.options(
    mode="thread",
    num_retries=10,
    retry_algorithm="exponential",
    retry_wait=1.0,
    retry_jitter=0.5  # More randomness
).init()
```

### Retry vs Circuit Breaker

Consider using a circuit breaker pattern for:
- Cascading failures
- Protecting downstream services
- Fast failure when system is down

Retries are best for:
- Transient network errors
- Rate limiting
- Eventually consistent operations

## Best Practices

### 1. Be Specific with Exception Types

```python
# ❌ Too broad - will retry on bugs
worker = MyWorker.options(
    mode="thread",
    num_retries=5,
    retry_on=[Exception]  # Catches everything
).init()

# ✅ Specific - only retries transient errors
worker = MyWorker.options(
    mode="thread",
    num_retries=5,
    retry_on=[ConnectionError, TimeoutError, HTTPError]
).init()
```

### 2. Use Exponential Backoff for Network Calls

```python
# ✅ Good for network operations
worker = APIWorker.options(
    mode="thread",
    num_retries=5,
    retry_algorithm="exponential",
    retry_wait=1.0
).init()
```

### 3. Set Reasonable Retry Limits

```python
# ❌ Too many retries - wastes time
worker = MyWorker.options(num_retries=100).init()

# ✅ Reasonable for most use cases
worker = MyWorker.options(num_retries=3).init()

# ✅ More for critical operations
worker = CriticalWorker.options(num_retries=7).init()
```

### 4. Combine Retries with Timeouts

```python
worker = APIWorker.options(
    mode="thread",
    num_retries=3,
    retry_on=[TimeoutError]
).init()

# Set timeout when calling
future = worker.fetch_data(url)
try:
    result = future.result(timeout=30)  # Total timeout including retries
except TimeoutError:
    print("Operation timed out after retries")
```

### 5. Log Retry Attempts

```python
import logging

def retry_with_logging(exception, attempt, **ctx):
    """Log retry attempts for monitoring."""
    logging.warning(
        f"Retry attempt {attempt} for {ctx['method_name']}: {exception}"
    )
    return isinstance(exception, (ConnectionError, TimeoutError))

worker = MyWorker.options(
    mode="thread",
    num_retries=3,
    retry_on=retry_with_logging
).init()
```

### 6. Test Retry Logic

```python
import pytest
from concurry import RetryValidationError

def test_retry_on_transient_error():
    """Test that worker retries on transient errors."""
    worker = MyWorker.options(
        mode="sync",  # Use sync for testing
        num_retries=3,
        retry_on=[ConnectionError]
    ).init()
    
    # Should succeed after retries
    result = worker.fetch_data().result()
    assert result is not None

def test_retry_exhaustion():
    """Test that retries eventually give up."""
    worker = MyWorker.options(
        mode="sync",
        num_retries=2,
        retry_until=lambda r, **ctx: False  # Always fails validation
    ).init()
    
    with pytest.raises(RetryValidationError) as exc_info:
        worker.process().result()
    
    assert exc_info.value.attempts == 3  # Initial + 2 retries
```

## API Reference

### RetryConfig

Complete configuration for retry behavior (automatically created from `Worker.options()`):

```python
from concurry import RetryConfig, RetryAlgorithm

config = RetryConfig(
    num_retries=3,
    retry_on=[ConnectionError, TimeoutError],
    retry_algorithm=RetryAlgorithm.Exponential,
    retry_wait=1.0,
    retry_jitter=0.3,
    retry_until=lambda result, **ctx: result.get("status") == "ok"
)
```

### RetryValidationError

Exception raised when output validation fails:

```python
class RetryValidationError(Exception):
    attempts: int              # Number of attempts made
    all_results: List[Any]     # Results from all attempts
    validation_errors: List[str]  # Error messages from validators
    method_name: str           # Name of the method that was retried
```

### Retry Algorithms

```python
from concurry import RetryAlgorithm

RetryAlgorithm.Exponential  # Default: 1, 2, 4, 8, 16, ...
RetryAlgorithm.Linear       # 1, 2, 3, 4, 5, ...
RetryAlgorithm.Fibonacci    # 1, 1, 2, 3, 5, 8, ...
```

## See Also

- [Workers Guide](workers.md) - Worker basics and configuration
- [Worker Pools Guide](pools.md) - Pool management and load balancing
- [Limits Guide](limits.md) - Resource and rate limiting
- [Futures Guide](futures.md) - Working with futures and results

