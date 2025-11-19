# Task Decorator

The `@task` decorator provides a convenient way to parallelize functions without manual worker management. It automatically transforms your function into a fully initialized `TaskWorker` instance, enabling easy parallelization with minimal code.

While providing this convenience, it is important to understand that **the decorated symbol is no longer a standard function**.

**Crucially:**
1. The decorated "function" is a **TaskWorker instance**.
2. Calling it invokes the worker's `submit()` method, returning a `Future` (unless `blocking=True`).
3. **By default, it creates a new worker for every call** (On-Demand mode).
4. You must manage its lifecycle (e.g., call `.stop()`) to clean up resources.

## Signature

```python
@task(*, mode: ExecutionMode, on_demand: bool = <config>, **kwargs)
```

**Parameters:**
- `mode`: Execution mode (sync, thread, process, asyncio, ray). **Required**.
- `on_demand`: Create workers on-demand. If not specified, uses `global_config.defaults.task_decorator_on_demand` (defaults to `True`). Automatically set to `False` for Sync and Asyncio modes.
- `**kwargs`: All other `Worker.options()` parameters (blocking, max_workers, limits, retry configuration, etc.)

**Note**: All parameters must be passed as keyword arguments (enforced by `*` in signature).

## Default On-Demand Behavior

By default, the `@task` decorator sets `on_demand=True` (except for Sync/Asyncio modes).

**What this means:**
- **Zero Idle Resources**: No threads, processes, or Ray actors exist when you are not calling the function.
- **Per-Call Creation**: Every time you call `my_task(x)`, a **new worker** is spun up, executes the task, and shuts down.
- **Startup Overhead**: There is a latency cost for creating the worker (low for threads, higher for processes/Ray).

**When to change it:**
If you are calling the function frequently (high throughput) or require low latency, set `on_demand=False` and `max_workers=N` to use a **persistent worker pool** instead.

```python
# Default: New thread created for EVERY call (Good for infrequent tasks)
@task(mode="thread")
def infrequent_job(x): ...

# Persistent Pool: 4 threads stay alive (Good for high throughput)
@task(mode="thread", max_workers=4, on_demand=False)
def frequent_job(x): ...
```

## Basic Usage

### Simple Function Decoration

```python
from concurry import task

# @task creates a TaskWorker instance named 'process_item'
# The original function is bound internally
@task(mode="thread", max_workers=4)
def process_item(x):
    return x ** 2

# process_item is now a TaskWorker instance!
print(type(process_item))  # <class 'concurry.core.worker.task_worker.TaskThreadWorkerProxyPool'>

# Call like a regular function (returns a Future)
# This actually calls process_item.submit(10)
future = process_item(10)
result = future.result()  # 100

# Use submit() explicitly (same as above)
future = process_item.submit(10)
result = future.result()

# Use map() for batch processing
results = list(process_item.map(range(10)))
# [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

# CRITICAL: You must stop the worker when done!
# Since 'process_item' is a worker, it has a .stop() method
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
    max_workers=10,                 # Pool size
    load_balancing="least_active",  # Load balancing strategy
    on_demand=True,                 # Create workers on-demand
    max_queued_tasks=100,           # Submission queue limit
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

The decorator automatically forwards limits to functions that accept a `limits` parameter when `limits` are provided:

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

## Worker Lifecycle and Cleanup

Because the decorated function *is* a worker, you must manage its lifecycle just like any other worker.

### Explicit Stopping (Recommended)

Always call `.stop()` on the decorated function when you are done with it to release resources (threads, processes, Ray actors).

```python
@task(mode="process", max_workers=4)
def heavy_compute(x):
    return x * x

try:
    # Use the worker
    results = list(heavy_compute.map(range(100)))
finally:
    # STOP the worker!
    heavy_compute.stop()
```

### Context Manager (Best Practice)

Since `TaskWorker` supports the context manager protocol, you can use `with` on the decorated function itself:

```python
@task(mode="thread")
def fetch_url(url):
    return requests.get(url).text

# The worker is active only within this block
with fetch_url:
    # fetch_url is the worker instance
    future = fetch_url("http://example.com")
    print(future.result())

# Worker is automatically stopped here
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

