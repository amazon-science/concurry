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

| Parameter | Type | Default Source | Description |
|-----------|------|----------------|-------------|
| `num_retries` | int | `global_config.defaults.num_retries` (0) | Maximum number of retry attempts after initial failure |
| `retry_on` | type \| callable \| list | `global_config.defaults.retry_on` (`[Exception]`) | Exception types or filters that trigger retries |
| `retry_algorithm` | str | `global_config.defaults.retry_algorithm` ("exponential") | Backoff strategy: "exponential", "linear", "fibonacci" |
| `retry_wait` | float | `global_config.defaults.retry_wait` (1.0) | Base wait time in seconds between retries |
| `retry_jitter` | float | `global_config.defaults.retry_jitter` (0.3) | Jitter factor (0-1) for randomizing wait times |
| `retry_until` | callable \| list | `global_config.defaults.retry_until` (None) | Validation functions for output |

**Note**: All retry parameters support per-method configuration using dictionaries. See [Per-Method Configuration](#per-method-configuration) for details.

### Default Configuration

All retry parameters have default values from `global_config` that can be customized:

```python
from concurry import global_config, temp_config

# View current defaults
print(global_config.defaults.num_retries)      # 0 (no retries by default)
print(global_config.defaults.retry_on)          # [Exception] (retry all exceptions)
print(global_config.defaults.retry_algorithm)   # RetryAlgorithm.Exponential
print(global_config.defaults.retry_wait)        # 1.0 (seconds)
print(global_config.defaults.retry_jitter)      # 0.3 (30% jitter)
print(global_config.defaults.retry_until)       # None (no output validation)

# Customize defaults globally
with temp_config(
    global_num_retries=3,
    global_retry_on=[ConnectionError, TimeoutError],
    global_retry_wait=2.0
):
    # All workers created in this context use these defaults
    worker = APIWorker.options(mode="thread").init()
    # Uses num_retries=3, retry_on=[ConnectionError, TimeoutError], retry_wait=2.0

# Customize per execution mode
with temp_config(
    thread_num_retries=5,
    ray_num_retries=10,
    thread_retry_on=[HTTPError]
):
    thread_worker = APIWorker.options(mode="thread").init()  # 5 retries, [HTTPError]
    ray_worker = APIWorker.options(mode="ray").init()        # 10 retries, [Exception] (global)
```

**Key Points**:
- All retry parameters can be overridden per worker via `Worker.options()`
- Use `temp_config()` to temporarily change defaults for multiple workers
- Mode-specific overrides take precedence over global defaults

## Per-Method Configuration

Configure different retry settings for different worker methods using dictionaries.

### Basic Usage

Use a dictionary to specify different retry parameters for different methods:

```python
class APIWorker(Worker):
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
    
    def health_check(self) -> bool:
        """Fast health check, no retry needed."""
        return requests.get(f"{self.endpoint}/health").ok
    
    def fetch_data(self, id: int) -> dict:
        """Moderate priority, retry a few times."""
        response = requests.get(f"{self.endpoint}/data/{id}")
        return response.json()
    
    def critical_operation(self, data: dict) -> bool:
        """Critical operation, retry aggressively."""
        response = requests.post(f"{self.endpoint}/process", json=data)
        return response.ok

# Configure different retries per method
worker = APIWorker.options(
    mode="thread",
    num_retries={
        "*": 0,                    # Default: no retries
        "fetch_data": 3,           # Retry fetch_data 3 times
        "critical_operation": 10   # Retry critical_operation 10 times
    }
).init(endpoint="https://api.example.com")

# health_check: No retries (uses default "*": 0)
# fetch_data: 3 retries
# critical_operation: 10 retries
```

### Dictionary Format

**Required**: All per-method dictionaries must include a `"*"` key for the default value:

```python
{
    "*": default_value,           # Required: default for unlisted methods
    "method_name1": value1,       # Optional: override for method_name1
    "method_name2": value2,       # Optional: override for method_name2
}
```

**All retry parameters support per-method configuration**:

```python
worker = MyWorker.options(
    mode="thread",
    num_retries={"*": 0, "critical": 5},              # Per-method
    retry_wait={"*": 1.0, "critical": 2.0},            # Per-method
    retry_algorithm={"*": "linear", "critical": "exponential"},  # Per-method
    retry_on={"*": [Exception], "critical": [ConnectionError]},  # Per-method
    retry_until={"*": None, "critical": my_validator}  # Per-method
).init()
```

### Mixed Configuration

Mix single values and dictionaries - single values apply to all methods:

```python
worker = APIWorker.options(
    mode="thread",
    num_retries={"*": 0, "fetch": 3, "critical": 10},  # Per-method
    retry_wait=2.0,                                     # Single value: all methods
    retry_algorithm="exponential"                       # Single value: all methods
).init()

# Result:
# - fetch: 3 retries, 2.0s wait, exponential
# - critical: 10 retries, 2.0s wait, exponential
# - others: 0 retries
```

### Partial Dictionaries

Specify only the methods you want to override:

```python
worker = DataWorker.options(
    mode="thread",
    num_retries={
        "*": 3,           # Default: 3 retries
        "fast_method": 0  # Override: no retries for fast_method
    }
).init()

# fast_method: 0 retries
# all_other_methods: 3 retries (default)
```

### LLM/API Worker Example

Common pattern for LLM workers with validation:

```python
from concurry import Worker, RetryAlgorithm

class LLMWorker(Worker):
    def __init__(self, api_key: str):
        self.api_key = api_key
    
    def generate_text(self, prompt: str) -> str:
        """Generate simple text response."""
        return self.llm_api.generate(prompt)
    
    def generate_json(self, prompt: str) -> dict:
        """Generate structured JSON (needs validation)."""
        response = self.llm_api.generate(prompt)
        return json.loads(response)
    
    def generate_code(self, spec: str) -> str:
        """Generate code (highly unreliable)."""
        return self.llm_api.generate_code(spec)

def is_valid_json(result, **ctx):
    """Validate JSON has required structure."""
    return isinstance(result, dict) and "data" in result

def is_valid_code(result, **ctx):
    """Validate code compiles."""
    try:
        compile(result, '<string>', 'exec')
        return True
    except SyntaxError:
        return False

worker = LLMWorker.options(
    mode="thread",
    num_retries={
        "*": 0,              # Default: no retries
        "generate_text": 3,  # Moderate retries for text
        "generate_json": 10, # Aggressive retries for JSON
        "generate_code": 15  # Very aggressive for code
    },
    retry_wait={
        "*": 1.0,
        "generate_code": 3.0  # Longer wait for code generation
    },
    retry_algorithm={
        "*": RetryAlgorithm.Linear,
        "generate_json": RetryAlgorithm.Exponential,
        "generate_code": RetryAlgorithm.Exponential
    },
    retry_on={
        "*": [Exception],
        "generate_json": [json.JSONDecodeError, requests.RequestException]
    },
    retry_until={
        "*": None,
        "generate_json": is_valid_json,
        "generate_code": is_valid_code
    }
).init(api_key="your-api-key")

# generate_text: 3 retries, linear, 1s wait
# generate_json: 10 retries, exponential, 1s wait, validates JSON structure
# generate_code: 15 retries, exponential, 3s wait, validates code syntax
```

### Disabling Retries for Specific Methods

Disable retries for fast methods while keeping them for others:

```python
worker = DatabaseWorker.options(
    mode="thread",
    num_retries={
        "*": 5,             # Default: 5 retries for all methods
        "ping": 0,          # No retries for ping (fast check)
        "get_cache": 0      # No retries for cache lookup (fast)
    }
).init()

# Most database operations: 5 retries
# ping and get_cache: no retries, fail fast
```

### With Worker Pools

Per-method configuration applies to all workers in a pool:

```python
# Each worker in the pool has the same per-method config
pool = APIWorker.options(
    mode="thread",
    max_workers=10,
    num_retries={
        "*": 0,
        "fetch_data": 3,
        "critical_operation": 10
    }
).init()

# All 10 workers:
# - fetch_data: 3 retries
# - critical_operation: 10 retries
# - others: no retries
```

### With TaskWorker

Configure retry for `TaskWorker.submit()` using the special `"submit"` method name:

```python
def flaky_function(x):
    if random.random() < 0.5:
        raise ConnectionError("Transient error")
    return x * 2

worker = TaskWorker.options(
    mode="process",
    num_retries={
        "*": 5,
        "submit": 3  # Configure retry for submit() method
    },
    retry_on={
        "*": [Exception],
        "submit": [ConnectionError, TimeoutError]
    }
).init()

# Submitted functions will retry up to 3 times on network errors
future = worker.submit(flaky_function, 10)
result = future.result()
```

### Error Handling

**Missing Default Key**:

```python
# ERROR: Dictionary must include "*" key
worker = MyWorker.options(
    num_retries={"method_a": 5}  # Missing "*"
).init()
# ValueError: num_retries dict must include '*' key for default value
```

**Unknown Method Names**:

```python
# ERROR: Method doesn't exist on worker
worker = MyWorker.options(
    num_retries={"*": 0, "nonexistent_method": 5}
).init()
# ValueError: num_retries dict contains unknown method names: ['nonexistent_method']
```

### Best Practices

**1. Use "*" Default Wisely**

```python
# ✅ Good: Conservative default, aggressive for critical methods
worker = MyWorker.options(
    num_retries={"*": 0, "critical_method": 10}
).init()

# ❌ Bad: Aggressive default, defeats the purpose
worker = MyWorker.options(
    num_retries={"*": 10, "fast_method": 0}  # Most methods get 10 retries
).init()
```

**2. Combine with Output Validation**

```python
# ✅ Good: Per-method retry + per-method validation
worker = LLMWorker.options(
    num_retries={"*": 0, "generate_json": 10},
    retry_until={"*": None, "generate_json": is_valid_json}
).init()
```

**3. Match Retry Strategy to Method Criticality**

```python
worker = APIWorker.options(
    num_retries={
        "*": 0,              # Default: no retries
        "health_check": 0,   # Fast checks: no retry
        "get_data": 3,       # Read operations: moderate retry
        "post_data": 7,      # Write operations: aggressive retry
        "critical_op": 15    # Critical operations: very aggressive
    },
    retry_algorithm={
        "*": RetryAlgorithm.Linear,
        "critical_op": RetryAlgorithm.Exponential  # More aggressive for critical
    }
).init()
```

**4. Test Each Method's Retry Behavior**

```python
import pytest

def test_per_method_retries():
    worker = MyWorker.options(
        mode="sync",
        num_retries={"*": 0, "critical": 3}
    ).init()
    
    # fast_method should fail immediately (no retries)
    with pytest.raises(ValueError):
        worker.fast_method().result()
    
    # critical should retry 3 times
    result = worker.critical().result()
    assert result is not None
```

### Performance Considerations

**Zero Overhead**:
- Single-value configs (backward compatible): No overhead
- Per-method configs: O(1) dict lookup per method call (~1 microsecond)

**Memory**:
- One `RetryConfig` instance per method with retries
- Methods with `num_retries=0` have no config instance
- Typical overhead: 2-10 KB per worker

**Recommendations**:
- Use per-method config when methods have significantly different requirements
- Use single-value config for uniform retry behavior (simpler, no overhead)
- Don't over-configure - most workers only need 2-3 different retry profiles

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

