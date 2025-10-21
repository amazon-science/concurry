# Task Decorator

The `@task` decorator provides a convenient way to parallelize functions without manual worker management. It automatically creates and initializes a `TaskWorker` bound to your function, enabling easy parallelization with minimal code.

## Signature

```python
@task(*, mode: ExecutionMode = ExecutionMode.Sync, on_demand: bool = <config>, **kwargs)
```

**Parameters:**
- `mode`: Execution mode (sync, thread, process, asyncio, ray). Defaults to `ExecutionMode.Sync`.
- `on_demand`: Create workers on-demand. If not specified, uses `global_config.defaults.task_decorator_on_demand` (defaults to `True`). Automatically set to `False` for Sync and Asyncio modes.
- `**kwargs`: All other `Worker.options()` parameters (blocking, max_workers, limits, retry configuration, etc.)

**Note**: All parameters must be passed as keyword arguments (enforced by `*` in signature).

## Basic Usage

### Simple Function Decoration

```python
from concurry import task

@task(mode="thread", max_workers=4)
def process_item(x):
    return x ** 2

# Call like a regular function (returns a Future)
future = process_item(10)
result = future.result()  # 100

# Use submit() explicitly
future = process_item.submit(10)
result = future.result()

# Use map() for batch processing
results = list(process_item.map(range(10)))
# [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

# Clean up when done
process_item.stop()
```

### Execution Modes

The decorator supports all execution modes:

```python
# Synchronous (for testing/debugging)
@task(mode="sync")
def sync_process(x):
    return x * 2

# Thread-based (I/O-bound tasks)
@task(mode="thread", max_workers=10)
def io_bound_task(url):
    return fetch_data(url)

# Process-based (CPU-bound tasks)
@task(mode="process", max_workers=4)
def cpu_bound_task(data):
    return expensive_computation(data)

# Asyncio-based (async I/O)
@task(mode="asyncio")
async def async_task(url):
    return await async_fetch(url)

# Ray-based (distributed computing)
@task(mode="ray", max_workers=0, on_demand=True)
def distributed_task(data):
    return process_data(data)
```

## Configuration Options

### Worker Pool Configuration

```python
@task(
    mode="thread",
    max_workers=10,              # Pool size
    load_balancing="least_active",  # Load balancing strategy
    on_demand=True,              # Create workers on-demand
    max_queued_tasks=100,        # Submission queue limit
)
def configured_task(x):
    return x * 2
```

### Retry Configuration

```python
@task(
    mode="thread",
    num_retries=3,                  # Retry up to 3 times
    retry_algorithm="exponential",  # Backoff strategy
    retry_wait=1.0,                # Base wait time
    retry_jitter=0.3,              # Jitter factor
    retry_on=[ConnectionError],    # Retry on specific exceptions
)
def api_call(endpoint):
    return requests.get(endpoint).json()
```

### Blocking Mode

```python
@task(mode="thread", blocking=True)
def blocking_task(x):
    return x ** 2

# Returns result directly, not a Future
result = blocking_task(5)  # 25
```

## Progress Bar Integration

Show progress during batch processing:

```python
@task(mode="process", max_workers=4)
def compute(x):
    return expensive_calculation(x)

# Simple progress bar
results = list(compute.map(range(1000), progress=True))

# Custom configuration
results = list(compute.map(
    range(1000),
    progress={
        "desc": "Processing items",
        "ncols": 80,
        "unit": "item"
    }
))
```

## Limits and Rate Limiting

### Automatic Limits Forwarding

The decorator automatically forwards limits to functions that accept a `limits` parameter:

```python
from concurry import task, RateLimit

# Define rate limits
limits = [RateLimit(key="api", capacity=100, window_seconds=60)]

@task(mode="thread", limits=limits)
def call_api(prompt, limits):
    # limits parameter is automatically injected
    with limits.acquire(requested={"api": 1}):
        return external_api(prompt)

# Limits are automatically passed
result = call_api("Hello").result()
```

### Without Limits Parameter

If your function doesn't need to access limits directly, just omit the parameter:

```python
@task(mode="thread", limits=limits)
def simple_task(x):
    # No limits parameter needed
    return x * 2

# Limits are managed automatically by the worker
result = simple_task(5).result()
```

## Advanced Patterns

### Context Manager

Use context managers for automatic cleanup:

```python
@task(mode="thread")
def process(x):
    return x ** 2

with process:
    results = [process(i).result() for i in range(10)]
# Worker automatically stopped
```

### Sharing Decorated Functions

Multiple decorated functions can share limits:

```python
from concurry import task, LimitSet, RateLimit

# Shared limit pool
shared_limits = LimitSet(limits=[
    RateLimit(key="api_tokens", capacity=1000, window_seconds=60)
])

@task(mode="thread", limits=shared_limits)
def task_a(x, limits):
    with limits.acquire(requested={"api_tokens": 100}):
        return api_call_a(x)

@task(mode="thread", limits=shared_limits)
def task_b(x, limits):
    with limits.acquire(requested={"api_tokens": 50}):
        return api_call_b(x)

# Both functions share the 1000 token/min pool
```

### Async Functions

The decorator works seamlessly with async functions:

```python
import asyncio

@task(mode="asyncio")
async def async_process(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()

# Use normally
result = async_process("https://example.com").result()
```

## Worker Lifecycle

### Manual Cleanup

```python
@task(mode="thread")
def process(x):
    return x * 2

# Use the worker
results = [process(i).result() for i in range(10)]

# Manually stop when done
process.stop()
```

### Automatic Cleanup

The decorator adds a `__del__` method for automatic cleanup when the decorated function goes out of scope, but explicit cleanup is recommended:

```python
def main():
    @task(mode="thread")
    def process(x):
        return x * 2
    
    results = [process(i).result() for i in range(10)]
    # Worker automatically cleaned up when function goes out of scope

main()  # process.__del__() called here
```

## Best Practices

1. **Always call `.stop()`**: Explicitly stop workers when done to ensure proper cleanup
2. **Use context managers**: Prefer `with` statements for automatic cleanup
3. **Choose appropriate mode**: Match execution mode to your workload (I/O vs CPU bound)
4. **Configure on-demand carefully**: On-demand workers are great for bursty workloads but have startup overhead
5. **Use progress bars for long-running tasks**: Help users understand progress
6. **Share limits appropriately**: Use shared `LimitSet` objects to coordinate across multiple workers
7. **Test with sync mode first**: Easier to debug before switching to parallel execution

## Comparison with Manual Worker Creation

### Using @task Decorator

```python
from concurry import task

@task(mode="thread", max_workers=4)
def process(x):
    return x ** 2

result = process(10).result()
process.stop()
```

### Manual Worker Creation

```python
from concurry import TaskWorker

def process(x):
    return x ** 2

worker = TaskWorker.options(mode="thread", max_workers=4).init()
result = worker.submit(process, 10).result()
worker.stop()
```

The decorator approach is more concise and provides a cleaner API for function-level parallelization.

## Limitations

1. **Sync/Asyncio modes don't support on-demand**: These modes don't support `on_demand=True`
2. **Function must be pickleable**: For process and ray modes, the function must be serializable
3. **State is not shared**: Each worker in a pool has independent state
4. **Not suitable for methods**: The decorator is designed for functions, not class methods

## See Also

- [TaskWorker](task-worker.md) - Manual TaskWorker usage
- [Workers](workers.md) - General worker documentation
- [Limits](limits.md) - Rate limiting and resource management
- [Retries](retries.md) - Retry configuration and best practices

