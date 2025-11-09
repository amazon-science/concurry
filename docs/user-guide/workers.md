# Workers

Workers in concurry implement the actor pattern, allowing you to run stateful operations across different execution backends (sync, thread, process, asyncio, ray) with a unified API.

## Overview

A Worker is a class that:
- Maintains its own isolated state
- Executes methods in a specific execution context
- Returns Futures for all method calls (or results directly in blocking mode)
- Can be stopped to clean up resources

## Basic Usage

### Defining a Worker

Define a worker by inheriting from `Worker`:

```python
from concurry import Worker

class DataProcessor(Worker):
    def __init__(self, multiplier: int):
        self.multiplier = multiplier
        self.count = 0

    def process(self, value: int) -> int:
        self.count += 1
        return value * self.multiplier

    def get_count(self) -> int:
        return self.count
```

### Using a Worker

Initialize a worker instance with `.options().init()`:

```python
# Initialize worker with thread execution
worker = DataProcessor.options(mode="thread").init(3)

# Call methods (returns futures)
future = worker.process(10)
result = future.result()  # 30

# Check state
count = worker.get_count().result()  # 1

# Clean up
worker.stop()
```

### Context Manager (Recommended)

Workers support the context manager protocol for automatic cleanup:

```python
# Context manager automatically calls .stop() on exit
with DataProcessor.options(mode="thread").init(3) as worker:
    future = worker.process(10)
    result = future.result()  # 30
# Worker is automatically stopped here

# Works with blocking mode
with DataProcessor.options(mode="thread", blocking=True).init(3) as worker:
    result = worker.process(10)  # Returns 30 directly
# Worker automatically stopped

# Cleanup happens even on exceptions
with DataProcessor.options(mode="thread").init(3) as worker:
    result = worker.process(10).result()
    if result < 50:
        raise ValueError("Result too small")
# Worker is still stopped despite exception
```

**Benefits:**
- ✅ Automatic cleanup - no need to remember `.stop()`
- ✅ Exception safe - worker stopped even on errors
- ✅ Cleaner code - follows Python best practices
- ✅ Works with all modes (sync, thread, process, asyncio, ray)

## Execution Modes

Workers support multiple execution modes:

### Sync Mode

Executes synchronously in the current thread (useful for testing):

```python
worker = DataProcessor.options(mode="sync").init(2)
future = worker.process(10)
result = future.result()  # 20 (already computed)
worker.stop()
```

### Thread Mode

Executes in a dedicated thread (good for I/O-bound tasks):

```python
worker = DataProcessor.options(mode="thread").init(2)
future = worker.process(10)
result = future.result()  # Blocks until complete
worker.stop()
```

### Process Mode

Executes in a separate process (good for CPU-bound tasks):

```python
# Default: Uses forkserver context (safe + fast)
worker = DataProcessor.options(mode="process").init(2)
future = worker.process(10)
result = future.result()
worker.stop()

# Override context if needed
worker = DataProcessor.options(
    mode="process",
    mp_context="spawn"  # or "fork" (not recommended), "forkserver" (default)
).init(2)
```

**Multiprocessing Context:**

- **`forkserver` (default)**: Safe with Ray client, fast startup (~200ms) - **recommended**
- **`spawn`**: Safest but slow (~10-20s startup on Linux, ~1-2s on macOS)
- **`fork`**: Fastest startup (~10ms) but **UNSAFE with Ray client** (causes segfaults)

**⚠️ WARNING**: If you use Ray client mode alongside process workers with `mp_context="fork"`, you will experience segmentation faults. Always use `forkserver` (default) or `spawn` when using Ray client.

### Asyncio Mode

Executes methods with smart routing (ideal for async I/O operations and mixed sync/async workloads):

```python
worker = DataProcessor.options(mode="asyncio").init(2)
future = worker.process(10)
result = future.result()
worker.stop()
```

**Architecture:**

- **Event loop thread**: Runs async methods concurrently in an asyncio event loop
- **Dedicated sync thread**: Executes sync methods without blocking the event loop
- **Smart routing**: Automatically detects method type using `asyncio.iscoroutinefunction()`
- **Return type**: All methods return `ConcurrentFuture` for efficient blocking

**Performance:**

- **Async methods**: 10-50x speedup for concurrent I/O operations
- **Sync methods**: ~13% overhead vs ThreadWorker (minimal impact)

**Best for:**

- HTTP requests and API calls
- Database queries with async drivers  
- WebSocket connections
- Mixed sync/async worker methods

See the [Async Function Support](#async-function-support) section for detailed examples and performance comparisons.

### Ray Mode

Executes using Ray actors for distributed computing:

```python
import ray
ray.init()

# Uses default resource allocation (num_cpus=1, num_gpus=0)
worker = DataProcessor.options(mode="ray").init(2)
future = worker.process(10)
result = future.result()
worker.stop()

# Explicitly specify resources
worker2 = DataProcessor.options(
    mode="ray",
    num_cpus=2,
    num_gpus=1,
    resources={"special_hardware": 1}
).init(2)
future2 = worker2.process(20)
result2 = future2.result()
worker2.stop()

ray.shutdown()
```

**Ray Default Resources:**
- `num_cpus=1`: Each Ray actor is allocated 1 CPU by default
- `num_gpus=0`: No GPU allocation by default  
- These defaults allow Ray workers to be initialized without explicit resource specifications

## Blocking Mode

By default, worker methods return Futures. Use `blocking=True` to get results directly:

```python
# Non-blocking (default)
worker = DataProcessor.options(mode="thread").init(5)
future = worker.process(10)  # Returns future
result = future.result()  # Wait for result

# Blocking mode
worker = DataProcessor.options(mode="thread", blocking=True).init(5)
result = worker.process(10)  # Returns 50 directly
```

## Submitting Arbitrary Functions with TaskWorker

Use `TaskWorker` with `submit()` and `map()` methods to execute arbitrary functions:

```python
from concurry import TaskWorker

def complex_computation(x, y):
    return (x ** 2 + y ** 2) ** 0.5

# Create a task worker
worker = TaskWorker.options(mode="process").init()

# Submit function
future = worker.submit(complex_computation, 3, 4)
result = future.result()  # 5.0

# Also works with lambdas
future2 = worker.submit(lambda x: x * 100, 5)
result2 = future2.result()  # 500

# Use map() for multiple tasks
def square(x):
    return x ** 2

results = list(worker.map(square, range(10)))
print(results)  # [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

worker.stop()
```

## Async Function Support

All workers in concurry can execute both synchronous and asynchronous functions. Async functions (defined with `async def`) are automatically detected and executed correctly across all execution modes.

### Basic Async Worker

Define workers with async methods:

```python
from concurry import Worker
import asyncio

class AsyncDataFetcher(Worker):
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.fetch_count = 0
    
    async def fetch_data(self, endpoint: str) -> dict:
        """Async method that simulates fetching data."""
        await asyncio.sleep(0.1)  # Simulate I/O delay
        self.fetch_count += 1
        return {"url": f"{self.base_url}/{endpoint}", "data": "..."}
    
    def get_count(self) -> int:
        """Regular sync method."""
        return self.fetch_count

# Use with any execution mode
worker = AsyncDataFetcher.options(mode="asyncio").init("https://api.example.com")
future = worker.fetch_data("users")
result = future.result()  # {'url': 'https://api.example.com/users', 'data': '...'}
worker.stop()
```

### Mixing Async and Sync Methods

Workers can have both async and sync methods:

```python
class HybridWorker(Worker):
    def __init__(self):
        self.results = []
    
    async def async_operation(self, x: int) -> int:
        """Async method."""
        await asyncio.sleep(0.01)
        return x * 2
    
    def sync_operation(self, x: int) -> int:
        """Sync method."""
        return x + 10
    
    async def process_batch(self, items: list) -> list:
        """Async method that uses asyncio.gather for concurrency."""
        tasks = [self.async_operation(item) for item in items]
        return await asyncio.gather(*tasks)

worker = HybridWorker.options(mode="asyncio").init()

# Call async method
result1 = worker.async_operation(5).result()  # 10

# Call sync method
result2 = worker.sync_operation(5).result()  # 15

# Process multiple items concurrently
result3 = worker.process_batch([1, 2, 3, 4, 5]).result()  # [2, 4, 6, 8, 10]

worker.stop()
```

### Submitting Async Functions with TaskWorker

Use `TaskWorker.submit()` with async functions:

```python
from concurry import TaskWorker
import asyncio

async def async_compute(x: int, y: int) -> int:
    """Standalone async function."""
    await asyncio.sleep(0.01)
    return x ** 2 + y ** 2

# Submit async function via TaskWorker
worker = TaskWorker.options(mode="asyncio").init()
future = worker.submit(async_compute, 3, 4)
result = future.result()  # 25
worker.stop()
```

### Performance: AsyncIO Worker vs Others

The `AsyncioWorkerProxy` provides **significant performance benefits** for I/O-bound async operations by enabling true concurrent execution. Here's a real-world example with HTTP requests:

```python
import asyncio
import time
import aiohttp

class APIWorker(Worker):
    def __init__(self, base_url: str):
        self.base_url = base_url
    
    def fetch_sync(self, resource_id: int) -> str:
        """Synchronous HTTP request."""
        import urllib.request
        url = f"{self.base_url}/data/{resource_id}"
        with urllib.request.urlopen(url) as response:
            return response.read().decode()
    
    async def fetch_async(self, resource_id: int) -> str:
        """Async HTTP request using aiohttp."""
        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/data/{resource_id}"
            async with session.get(url) as response:
                return await response.text()

# Test with 30 HTTP requests (each with 50ms network latency)
num_requests = 30
base_url = "https://api.example.com"

# SyncWorker: Sequential execution
worker_sync = APIWorker.options(mode="sync").init(base_url)
start = time.time()
futures = [worker_sync.fetch_sync(i) for i in range(num_requests)]
results = [f.result() for f in futures]
time_sync = time.time() - start
worker_sync.stop()

# ThreadWorker: Sequential execution in dedicated thread
worker_thread = APIWorker.options(mode="thread").init(base_url)
start = time.time()
futures = [worker_thread.fetch_sync(i) for i in range(num_requests)]
results = [f.result() for f in futures]
time_thread = time.time() - start
worker_thread.stop()

# AsyncioWorker: Concurrent execution with async/await
worker_async = APIWorker.options(mode="asyncio").init(base_url)
start = time.time()
futures = [worker_async.fetch_async(i) for i in range(num_requests)]
results = [f.result() for f in futures]
time_async = time.time() - start
worker_async.stop()

print("Performance Results (30 requests, 50ms latency each):")
print(f"  SyncWorker:    {time_sync:.3f}s (sequential)")
print(f"  ThreadWorker:  {time_thread:.3f}s (sequential)")
print(f"  AsyncioWorker: {time_async:.3f}s (concurrent)")
print(f"\n  Speedup vs SyncWorker:   {time_sync / time_async:.1f}x")
print(f"  Speedup vs ThreadWorker: {time_thread / time_async:.1f}x")
# Expected output:
# SyncWorker:    1.66s (30 × 50ms ≈ 1.5s)
# ThreadWorker:  1.66s (30 × 50ms ≈ 1.5s) 
# AsyncioWorker: 0.16s (concurrent, ~50ms total)
# Speedup: ~10x faster!
```

**Key Takeaways:**

- **SyncWorker & ThreadWorker**: Execute requests sequentially (~1.66s for 30 requests)
- **AsyncioWorker**: Executes requests concurrently (~0.16s for 30 requests) 
- **Speedup**: 10x+ faster for concurrent I/O operations
- **When to use AsyncioWorker**: Network I/O (HTTP, WebSocket, database), not local file I/O

### Async Support Across Execution Modes

All worker modes correctly execute async functions, but with different performance characteristics:

| Mode | Async Support | Return Type | Performance Notes |
|------|---------------|-------------|-------------------|
| **asyncio** | ✅ Native | `ConcurrentFuture` | **Best for async**: Uses dedicated event loop for async methods, dedicated sync thread for sync methods. Enables true concurrent execution. 10-50x speedup for I/O operations. |
| **thread** | ✅ Via `asyncio.run()` | `ConcurrentFuture` | Correct execution, but no concurrency benefit (each async call blocks the worker thread) |
| **process** | ✅ Via `asyncio.run()` | `ConcurrentFuture` | Correct execution, but no concurrency benefit + serialization overhead |
| **sync** | ✅ Via `asyncio.run()` | `SyncFuture` | Correct execution, runs synchronously (no concurrency) |
| **ray** | ✅ Native + wrapper | `RayFuture` | Native support for async actor methods, TaskWorker wraps async functions |

**AsyncioWorkerProxy Architecture:**

- **Async methods** → Event loop thread (concurrent execution)
- **Sync methods** → Dedicated sync thread (avoids blocking event loop)
- **Smart routing** → Automatic detection via `asyncio.iscoroutinefunction()`
- **Return type** → `ConcurrentFuture` for both sync and async methods

**Recommendation:** Use `mode="asyncio"` for async functions to get maximum performance benefits from concurrent I/O.

### Real-World Example: Async Web Scraper

```python
import asyncio
import aiohttp
from concurry import Worker

class AsyncWebScraper(Worker):
    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self.scraped_count = 0
    
    async def fetch_url(self, url: str) -> dict:
        """Fetch a single URL asynchronously."""
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=self.timeout) as response:
                self.scraped_count += 1
                return {
                    'url': url,
                    'status': response.status,
                    'content': await response.text()
                }
    
    async def fetch_multiple(self, urls: list) -> list:
        """Fetch multiple URLs concurrently."""
        tasks = [self.fetch_url(url) for url in urls]
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    def get_stats(self) -> dict:
        """Get scraping statistics (sync method)."""
        return {'scraped_count': self.scraped_count}

# Initialize async worker
scraper = AsyncWebScraper.options(mode="asyncio").init(timeout=30)

# Scrape multiple URLs concurrently
urls = [
    'https://example.com/page1',
    'https://example.com/page2',
    'https://example.com/page3',
]

# All URLs are fetched concurrently in the event loop
results = scraper.fetch_multiple(urls).result()

# Check stats
stats = scraper.get_stats().result()
print(f"Scraped {stats['scraped_count']} pages")

scraper.stop()
```

### Async Error Handling

Exceptions in async functions are propagated correctly:

```python
class AsyncValidator(Worker):
    async def validate_async(self, value: int) -> int:
        await asyncio.sleep(0.01)
        if value < 0:
            raise ValueError("Value must be positive")
        return value

worker = AsyncValidator.options(mode="asyncio").init()

try:
    result = worker.validate_async(-5).result()
except ValueError as e:
    print(f"Validation error: {e}")  # Original exception type preserved

worker.stop()
```

### Coordinating Multiple Async Worker Calls

When you need to coordinate results from multiple async worker method calls, use regular `wait()` and `gather()` with the Worker futures:

```python
from concurry import Worker, gather, wait
import asyncio

class AsyncAPIWorker(Worker):
    async def fetch_user(self, user_id: int):
        await asyncio.sleep(0.1)
        return {"id": user_id, "name": f"User{user_id}"}
    
    async def fetch_posts(self, user_id: int):
        await asyncio.sleep(0.1)
        return [{"post": i} for i in range(5)]

# Create worker
worker = AsyncAPIWorker.options(mode="asyncio").init()

# Submit multiple async method calls - returns Worker futures
user_future = worker.fetch_user(123)
posts_future = worker.fetch_posts(123)

# Use regular gather() to coordinate Worker futures
user, posts = gather([user_future, posts_future], timeout=10.0)

print(f"User: {user}")
print(f"Posts: {len(posts)} posts")

worker.stop()
```

**Note on `async_wait()` and `async_gather()`:**

Concurry also provides `async_wait()` and `async_gather()` for coordinating raw coroutines in async contexts. However, **Worker method calls return `concurry` futures, not coroutines**, so you should use regular `wait()` and `gather()` with Worker futures.

```python
# ✅ Correct: Use regular gather() with Worker futures
futures = [worker.async_method(i) for i in range(10)]
results = gather(futures, timeout=10.0)

# ❌ Wrong: async_gather() expects coroutines, not Worker futures
# This will not work as expected
futures = [worker.async_method(i) for i in range(10)]
results = await async_gather(futures)  # TypeError or incorrect behavior
```

Use `async_wait()` and `async_gather()` only when working with raw coroutines outside of Workers. See the [Synchronization Guide](synchronization.md#async_wait-and-async_gather) for details.

### Best Practices for Async Workers

1. **Use AsyncIO mode for async functions**: Get maximum concurrency benefits (10-50x speedup)
   ```python
   # ✅ Good: True concurrent execution with 10-50x speedup
   worker = AsyncWorker.options(mode="asyncio").init()
   
   # ❌ Works but slower: No concurrency benefit
   worker = AsyncWorker.options(mode="thread").init()
   ```

2. **Mix sync and async methods freely**: AsyncioWorkerProxy handles both efficiently
   ```python
   class HybridWorker(Worker):
       async def fetch_data(self, url: str) -> dict:
           # Runs in event loop - concurrent execution
           async with aiohttp.ClientSession() as session:
               async with session.get(url) as response:
                   return await response.json()
       
       def process_data(self, data: dict) -> str:
           # Runs in dedicated sync thread - doesn't block event loop
           return json.dumps(data, indent=2)
   
   worker = HybridWorker.options(mode="asyncio").init()
   # Both methods work efficiently without blocking each other
   ```

3. **Use asyncio.gather() for concurrent operations inside worker methods**: Maximum performance
   ```python
   class APIWorker(Worker):
       async def fetch_many(self, urls: list) -> list:
           # ✅ Good: All requests execute concurrently inside the worker
           tasks = [self.fetch_url(url) for url in urls]
           return await asyncio.gather(*tasks)
   
       async def fetch_url(self, url: str) -> str:
           # Individual async method
           async with aiohttp.ClientSession() as session:
               async with session.get(url) as response:
                   return await response.text()
   ```

4. **Use regular `gather()` to coordinate multiple worker calls**: For client-side coordination
   ```python
   from concurry import gather
   
   # Submit multiple worker calls
   futures = [worker.fetch_url(url) for url in urls]
   
   # Coordinate with regular gather()
   results = gather(futures, timeout=30.0, progress=True)
   ```

5. **Use appropriate async libraries**:
   - `aiohttp` for HTTP requests (✅ major speedup)
   - `asyncpg` for PostgreSQL (✅ major speedup)
   - `motor` for MongoDB (✅ major speedup)
   - **Note**: For local file I/O, ThreadWorker or SyncWorker may be faster than AsyncioWorker due to OS-level buffering and small file sizes. Use AsyncioWorker for network I/O and remote files.

6. **Handle exceptions properly**:
   ```python
   async def safe_operation(self):
       try:
           return await risky_async_operation()
       except SpecificError as e:
           return default_value
   ```

## State Management

Each worker instance maintains its own isolated state:

```python
class Counter(Worker):
    def __init__(self):
        self.count = 0

    def increment(self) -> int:
        self.count += 1
        return self.count

# Each worker has separate state
worker1 = Counter.options(mode="thread").init()
worker2 = Counter.options(mode="thread").init()

print(worker1.increment().result())  # 1
print(worker1.increment().result())  # 2
print(worker2.increment().result())  # 1 (separate state)

worker1.stop()
worker2.stop()
```

## Using the @worker Decorator

The `@worker` decorator provides a powerful way to create workers with pre-configured options. 
You can use it in three ways: without parameters, with full configuration, or with the `auto_init` feature for direct instantiation.

### Basic Decorator

Use `@worker` without parameters to make any class a Worker:

```python
from concurry import worker

@worker
class Calculator:
    def __init__(self, base: int):
        self.base = base

    def add(self, x: int) -> int:
        return self.base + x

# Use exactly like a Worker
calc = Calculator.options(mode="thread").init(10)
result = calc.add(5).result()  # 15
calc.stop()
```

### Decorator with Configuration

Configure worker options directly in the decorator:

```python
from concurry import worker

@worker(mode='thread', max_workers=4, num_retries=3)
class LLM:
    def __init__(self, model_name: str):
        self.model_name = model_name
    
    def call_llm(self, prompt: str) -> str:
        # Call LLM API
        return f"Response to: {prompt}"

# Options are pre-configured, but you still use .options().init()
llm = LLM.options().init(model_name='gpt-4')
result = llm.call_llm("What is 1+1?").result()
llm.stop()

# Override decorator settings if needed
llm2 = LLM.options(mode='process', max_workers=8).init(model_name='gpt-4')
llm2.stop()
```

### Auto-Initialization with `auto_init=True`

The most powerful feature: direct class instantiation creates worker instances automatically:

```python
from concurry import worker

@worker(mode='thread', max_workers=4, auto_init=True)
class LLM:
    def __init__(self, model_name: str):
        self.model_name = model_name
    
    def call_llm(self, prompt: str) -> str:
        return f"Response from {self.model_name}: {prompt}"

# Direct instantiation creates a worker! No .options().init() needed
llm = LLM(model_name='gpt-4')

# Returns a future (worker method call)
future = llm.call_llm("What is 1+1?")
result = future.result()

# Clean up
llm.stop()

# Context manager works too
with LLM(model_name='gpt-4') as llm:
    result = llm.call_llm("Hello").result()
# Automatically stopped
```

**Key Points:**
- `auto_init=True` makes `LLM(...)` create a worker instance directly
- No need to call `.options().init()` 
- Worker methods return futures as usual
- Context manager support works automatically
- You can still use `.options().init()` to override settings

### Disabling Auto-Init

Use `auto_init=False` to create plain Python instances:

```python
@worker(mode='thread', max_workers=4, auto_init=False)
class LLM:
    def __init__(self, model_name: str):
        self.model_name = model_name
    
    def call_llm(self, prompt: str) -> str:
        return f"Response: {prompt}"

# Creates a plain Python instance (not a worker)
llm = LLM(model_name='gpt-4')
result = llm.call_llm("What is 1+1?")  # Returns string directly, not a future
print(result)  # "Response: What is 1+1?"

# To create a worker, use .options().init()
worker_llm = LLM.options().init(model_name='gpt-4')
future = worker_llm.call_llm("What is 1+1?")  # Returns future
worker_llm.stop()
```

## Class Inheritance Configuration

You can also configure worker options directly in the class definition using `__init_subclass__`:

```python
from concurry import Worker

class LLM(Worker, mode='thread', max_workers=4, auto_init=True):
    def __init__(self, model_name: str):
        self.model_name = model_name
    
    def call_llm(self, prompt: str) -> str:
        return f"Response from {self.model_name}"

# Direct instantiation creates a worker
llm = LLM(model_name='gpt-4')
result = llm.call_llm("Hello").result()
llm.stop()
```

This syntax is equivalent to using the `@worker` decorator with the same parameters.

### Configuration Priority

When using both decorator and inheritance, decorator parameters take precedence:

```python
@worker(mode='process', max_workers=8)  # Decorator config
class LLM(Worker, mode='thread', max_workers=4):  # Inheritance config
    pass

# Decorator wins: mode='process', max_workers=8
llm = LLM.options().init(...)
```

**Warning:** Mixing decorator and inheritance is discouraged. Choose one approach for clarity.

### Overriding Configuration

Both decorator and inheritance configurations can be overridden at instantiation:

```python
@worker(mode='thread', max_workers=4, auto_init=True)
class LLM:
    def __init__(self, model_name: str):
        self.model_name = model_name

# Use decorator defaults
llm1 = LLM(model_name='gpt-4')  # mode='thread', max_workers=4

# Override at instantiation
llm2 = LLM.options(mode='process', max_workers=8).init(model_name='gpt-4')
# mode='process', max_workers=8

llm1.stop()
llm2.stop()
```

**Configuration Priority (highest to lowest):**
1. Explicit `.options()` parameters
2. `@worker` decorator parameters
3. `class Worker(...)` inheritance parameters
4. `global_config` defaults

## Type Safety and Validation

Workers in concurry leverage [morphic's Typed](https://github.com/yourusername/morphic) for enhanced type safety and validation. While the `Worker` class itself does NOT inherit from `Typed` (to allow flexible `__init__` definitions), the internal `WorkerProxy` classes do, providing automatic validation and type checking.

### Automatic Type Validation

Worker configuration methods use the `@validate` decorator for automatic type checking and conversion:

```python
from concurry import Worker

class DataProcessor(Worker):
    def __init__(self, multiplier: int):
        self.multiplier = multiplier

# String booleans are automatically coerced
worker = DataProcessor.options(mode="thread", blocking="true").init(3)
assert worker.blocking is True  # Converted from string to bool

# ExecutionMode values are validated
worker = DataProcessor.options(mode="thread").init(3)  # Valid
# worker = DataProcessor.options(mode="invalid").init(3)  # Would raise error

worker.stop()
```

### Immutable Configuration

Once a worker is initialized, its configuration fields are immutable:

```python
worker = DataProcessor.options(mode="thread", blocking=False).init(3)

# These fields cannot be modified after creation
# worker.blocking = True  # Raises error
# worker.worker_cls = SomeOtherClass  # Raises error

# Internal state tracking (private attributes) can be updated
worker._stopped = True  # Allowed (with type checking)

worker.stop()
```

### Type Checking on Internal State

Private attributes in worker proxies support automatic type checking:

```python
worker = DataProcessor.options(mode="thread").init(3)

# Internal state is type-checked
worker._stopped = False  # Valid (bool)
# worker._stopped = "not a bool"  # Would raise ValidationError

worker.stop()
```

### Benefits of Typed Integration

1. **Automatic Validation**: Configuration options are validated at creation time
2. **Type Coercion**: String values are automatically converted (e.g., `"true"` → `True`)
3. **Immutability**: Public configuration fields cannot be accidentally modified
4. **Type Safety**: Private attributes are type-checked on updates
5. **Better Error Messages**: Clear validation errors with detailed context

### Worker Class Flexibility

The `Worker` class itself does NOT inherit from `Typed`, giving you complete freedom in defining `__init__`:

```python
# You can use any signature you want
class FlexibleWorker(Worker):
    def __init__(self, a, b, c=10, *args, **kwargs):
        self.a = a
        self.b = b
        self.c = c
        self.args = args
        self.kwargs = kwargs
    
    def process(self):
        return self.a + self.b + self.c

# Works with any initialization pattern
worker = FlexibleWorker.options(mode="sync").init(
    1, 2, c=3, extra1="x", extra2="y"
)
result = worker.process().result()  # 6
worker.stop()
```

This design allows you to use Pydantic, dataclasses, attrs, or plain Python classes for your worker implementations while still benefiting from Typed's validation on the worker proxy layer.

## Model Inheritance and Validation

Workers support powerful validation and type checking through both model inheritance and validation decorators. This section covers all options and their compatibility with different execution modes.

### Overview of Options

| Feature | Sync | Thread | Process | Asyncio | Ray | Notes |
|---------|------|--------|---------|---------|-----|-------|
| **morphic.Typed** | ✅ | ✅ | ✅ | ✅ | ✅ | Full model with validation & hooks (auto-composition wrapper) |
| **pydantic.BaseModel** | ✅ | ✅ | ✅ | ✅ | ✅ | Pydantic validation & serialization (auto-composition wrapper) |
| **@morphic.validate** | ✅ | ✅ | ✅ | ✅ | ✅ | Decorator for methods/__init__ |
| **@pydantic.validate_call** | ✅ | ✅ | ✅ | ✅ | ✅ | Pydantic decorator for validation |

### Worker + morphic.Typed

Inherit from both `Worker` and `Typed` for powerful validation, lifecycle hooks, and frozen immutability:

```python
from concurry import Worker
from morphic import Typed
from pydantic import Field
from typing import List, Optional

class TypedWorker(Worker, Typed):
    """Worker with Typed validation and lifecycle hooks."""
    
    name: str = Field(..., min_length=1, max_length=50)
    value: int = Field(default=0, ge=0)
    tags: List[str] = []
    
    @classmethod
    def pre_initialize(cls, data: dict) -> None:
        """Hook to normalize data before validation."""
        if 'name' in data:
            data['name'] = data['name'].strip().title()
    
    def post_initialize(self) -> None:
        """Hook after initialization."""
        print(f"Initialized worker: {self.name}")
    
    def compute(self, x: int) -> int:
        return self.value * x

# ✅ Works with ALL modes including Ray!
worker = TypedWorker.options(mode="thread").init(
    name="  data processor  ",  # Will be normalized to "Data Processor"
    value=10,
    tags=["ml", "preprocessing"]
)

result = worker.compute(5).result()  # 50
print(worker.name)  # "Data Processor"
worker.stop()

# ✅ Also works with Ray mode (automatic composition wrapper)
worker_ray = TypedWorker.options(mode="ray").init(name="test", value=10)
result_ray = worker_ray.compute(5).result()  # 50
print(result_ray)
worker_ray.stop()
```

**Benefits:**
- ✅ **Works with ALL execution modes including Ray** (via automatic composition wrapper)
- Automatic field validation with Field constraints
- Type coercion (strings → numbers, etc.)
- Lifecycle hooks (`pre_initialize`, `post_initialize`, etc.)
- Immutable by default (frozen=True)
- Excellent error messages
- Zero-overhead composition wrapper (performance optimized)

**Note:**
- Ray support is automatic via composition wrapper (no code changes needed)

### Worker + pydantic.BaseModel

Inherit from both `Worker` and `BaseModel` for Pydantic's full validation power:

```python
from concurry import Worker
from pydantic import BaseModel, Field, field_validator
from typing import Optional

class PydanticWorker(Worker, BaseModel):
    """Worker with Pydantic validation."""
    
    name: str = Field(..., min_length=1, max_length=50)
    age: int = Field(..., ge=0, le=150)
    email: Optional[str] = None
    
    @field_validator('email')
    @classmethod
    def validate_email(cls, v):
        """Custom email validation."""
        if v and '@' not in v:
            raise ValueError("Invalid email format")
        return v
    
    def get_info(self) -> dict:
        return {
            "name": self.name,
            "age": self.age,
            "email": self.email
        }

# ✅ Works with ALL modes including Ray!
worker = PydanticWorker.options(mode="process").init(
    name="Alice",
    age=30,
    email="alice@example.com"
)

info = worker.get_info().result()
print(info)  # {'name': 'Alice', 'age': 30, 'email': 'alice@example.com'}
worker.stop()

# Validation errors are caught at initialization
try:
    worker = PydanticWorker.options(mode="thread").init(
        name="Bob",
        age=-5,  # Invalid: age must be >= 0
        email="invalid"
    )
except Exception as e:
    print(f"Validation error: {e}")

# ✅ Also works with Ray mode (automatic composition wrapper)
worker_ray = PydanticWorker.options(mode="ray").init(name="test", age=25)
info_ray = worker_ray.get_info().result()
print(info_ray)  # {'name': 'test', 'age': 25, 'email': None}
worker_ray.stop()
```

**Benefits:**
- ✅ **Works with ALL execution modes including Ray** (via automatic composition wrapper)
- Full Pydantic validation capabilities
- Custom validators with `@field_validator`
- JSON serialization/deserialization
- Excellent IDE support
- Rich error messages
- Zero-overhead composition wrapper (performance optimized)

**Note:**
- Ray support is automatic via composition wrapper (no code changes needed)

### @morphic.validate Decorator (Ray Compatible!)

Use `@validate` decorator for method and `__init__` validation without class inheritance:

```python
from concurry import Worker
from morphic import validate

class ValidatedWorker(Worker):
    """Worker with @validate decorator on methods."""
    
    @validate
    def __init__(self, name: str, multiplier: int = 2):
        """Validated __init__ with type coercion."""
        self.name = name
        self.multiplier = multiplier
    
    @validate
    def process(self, value: int, scale: float = 1.0) -> float:
        """Process with automatic type validation and coercion."""
        return (value * self.multiplier) * scale
    
    @validate
    async def async_process(self, value: int) -> int:
        """Async method with validation."""
        import asyncio
        await asyncio.sleep(0.01)
        return value * self.multiplier

# ✅ Works with ALL modes including Ray!
worker = ValidatedWorker.options(mode="ray").init(
    name="validator",
    multiplier="5"  # String coerced to int
)

# Strings are automatically coerced to correct types
result = worker.process("10", scale="2.0").result()
print(result)  # 100.0 (10 * 5 * 2.0)

# Also works with async methods
result = worker.async_process("7").result()
print(result)  # 35

worker.stop()

# Works with all other modes too
for mode in ["sync", "thread", "process", "asyncio"]:
    worker = ValidatedWorker.options(mode=mode).init(name="test", multiplier=3)
    result = worker.process("5", scale=2.0).result()
    assert result == 30.0
    worker.stop()
```

**Benefits:**
- ✅ **Works with Ray mode** (unlike Typed/BaseModel)
- Automatic type coercion (strings → numbers)
- Works on methods and `__init__`
- Works with async methods
- Minimal overhead
- Can be used selectively on specific methods

**Use Cases:**
- Ray workers that need validation
- Workers where only specific methods need validation
- Gradual migration from unvalidated to validated code

### @pydantic.validate_call Decorator (Ray Compatible!)

Use Pydantic's `@validate_call` decorator for method validation:

```python
from concurry import Worker
from pydantic import validate_call, Field
from typing import Annotated

class PydanticValidatedWorker(Worker):
    """Worker with @validate_call decorator."""
    
    @validate_call
    def __init__(self, base: int, name: str = "default"):
        """Validated __init__ with Pydantic."""
        self.base = base
        self.name = name
    
    @validate_call
    def compute(
        self,
        x: Annotated[int, Field(ge=0, le=100)],
        y: int = 0
    ) -> int:
        """Compute with strict validation using Field constraints."""
        return (x + y) * self.base
    
    @validate_call
    def process_list(self, values: list[int]) -> int:
        """Process a list with validation."""
        return sum(v * self.base for v in values)

# ✅ Works with ALL modes including Ray!
worker = PydanticValidatedWorker.options(mode="ray").init(
    base=3,
    name="pydantic_validator"
)

# Field constraints are enforced
result = worker.compute(x="50", y="10").result()  # Types coerced
print(result)  # 180 ((50 + 10) * 3)

# Validation errors are raised for invalid inputs
try:
    worker.compute(x=150, y=0).result()  # x must be <= 100
except Exception as e:
    print(f"Validation error: {e}")

# List validation
result = worker.process_list([1, 2, 3, 4, 5]).result()
print(result)  # 45 (sum([1,2,3,4,5]) * 3)

worker.stop()
```

**Benefits:**
- ✅ **Works with Ray mode**
- Full Pydantic validation features
- Field constraints with `Annotated`
- Strict type checking
- Rich error messages

**Use Cases:**
- Ray workers with strict validation requirements
- API-like workers that need robust input validation
- Workers interfacing with external systems

### Ray Mode: Universal Support with Automatic Composition Wrapper

**✅ ALL Validation Approaches Work with Ray:**

Concurry automatically applies a **composition wrapper** to workers that inherit from `morphic.Typed` or `pydantic.BaseModel`, making them fully compatible with Ray mode without any code changes required.

```python
from concurry import Worker
from morphic import Typed, validate
from pydantic import BaseModel, Field, validate_call

# Option 1: Worker + Typed (fully supported)
class TypedWorker(Worker, Typed):
    name: str
    value: int

worker1 = TypedWorker.options(mode="ray").init(name="test", value=10)
result1 = worker1.compute(5).result()
worker1.stop()
# ✅ Works! (automatic composition wrapper)

# Option 2: Worker + BaseModel (fully supported)
class PydanticWorker(Worker, BaseModel):
    name: str
    value: int

worker2 = PydanticWorker.options(mode="ray").init(name="test", value=10)
result2 = worker2.compute(5).result()
worker2.stop()
# ✅ Works! (automatic composition wrapper)

# Option 3: @validate decorator
class ValidatedWorker(Worker):
    @validate
    def __init__(self, name: str, value: int):
        self.name = name
        self.value = value
    
    @validate
    def compute(self, x: int) -> int:
        return self.value * x

worker3 = ValidatedWorker.options(mode="ray").init(name="test", value="10")
# ✅ Works with validation and type coercion!

# Option 4: @validate_call decorator
class PydanticDecoratedWorker(Worker):
    @validate_call
    def __init__(self, name: str, value: int):
        self.name = name
        self.value = value
    
    @validate_call
    def compute(self, x: int) -> int:
        return self.value * x

worker4 = PydanticDecoratedWorker.options(mode="ray").init(name="test", value=10)
# ✅ Works with Pydantic validation!

# Option 5: Plain Worker (no validation)
class PlainWorker(Worker):
    def __init__(self, name: str, value: int):
        self.name = name
        self.value = value

worker5 = PlainWorker.options(mode="ray").init(name="test", value=10)
# ✅ Works!
```

**How the Composition Wrapper Works:**

When you use a Typed/BaseModel worker with Ray mode, concurry automatically:

1. **Detects** the worker inherits from Typed or BaseModel
2. **Creates** a plain Python wrapper class using composition
3. **Delegates** user-defined methods to the wrapped Typed/BaseModel instance
4. **Excludes** infrastructure methods (model_dump, model_validate, etc.) from the wrapper
5. **Maintains** full validation, type checking, and field constraints

**Benefits:**

- ✅ **Zero code changes** - your existing Typed/BaseModel workers just work
- ✅ **Full validation** - all Pydantic features preserved
- ✅ **Zero overhead** - performance-optimized method delegation
- ✅ **Transparent** - behaves identically to non-Ray modes
- ✅ **Universal** - same wrapper used for all modes (consistent behavior)

**Technical Details:**

The composition wrapper solves the historical Ray + Pydantic incompatibility by:
- Avoiding Ray's `__setattr__` conflicts with Pydantic's frozen models
- Preventing retry logic from wrapping infrastructure methods
- Providing a clean separation between user code and framework code

For architecture details, see [Universal Composition Wrapper](../architecture/workers.md#universal-composition-wrapper-for-typedbasemodel-workers)

### Choosing the Right Approach

**All approaches now work with Ray mode thanks to the automatic composition wrapper!**

**Use morphic.Typed when:**
- You need lifecycle hooks (`pre_initialize`, `post_initialize`, etc.)
- You want immutable workers by default
- You want the most seamless model integration
- ✅ Works with ALL modes including Ray

**Use pydantic.BaseModel when:**
- You need Pydantic's full validation capabilities
- You want JSON serialization/deserialization
- You need custom validators with `@field_validator`
- ✅ Works with ALL modes including Ray

**Use @validate decorator when:**
- You only need validation on specific methods
- You want minimal overhead
- You prefer morphic's validation style
- ✅ Works with ALL modes including Ray

**Use @validate_call decorator when:**
- You want Pydantic's validation features
- You need Field constraints with `Annotated`
- You prefer Pydantic's validation style
- ✅ Works with ALL modes including Ray

**Use plain Worker when:**
- You don't need validation
- You want absolute maximum performance
- You're handling validation elsewhere
- ✅ Works with ALL modes including Ray

### Mixing Approaches

You can mix validation decorators with model inheritance across all execution modes:

```python
from concurry import Worker
from morphic import Typed, validate
from pydantic import Field

class HybridWorker(Worker, Typed):
    """Typed worker with additional validated methods."""
    
    name: str = Field(..., min_length=1)
    base_value: int = Field(default=10, ge=0)
    
    @validate
    def compute_with_validation(self, x: int, multiplier: float = 1.0) -> float:
        """Extra validation on this specific method."""
        return self.base_value * x * multiplier
    
    def compute_simple(self, x: int) -> int:
        """No extra validation."""
        return self.base_value * x

# Works with ALL modes including Ray
worker = HybridWorker.options(mode="thread").init(
    name="hybrid",
    base_value=5
)

# Both methods work
result1 = worker.compute_with_validation("10", multiplier="2.0").result()
result2 = worker.compute_simple(10).result()

print(result1)  # 100.0 (with @validate coercion)
print(result2)  # 50 (no coercion)
worker.stop()
```

## Multiple Workers

You can initialize and use multiple workers in parallel:

```python
# Initialize multiple workers
workers = [
    DataProcessor.options(mode="thread").init(i)
    for i in range(1, 4)
]

# Submit tasks to all workers
futures = [w.process(10) for w in workers]

# Collect results
results = [f.result() for f in futures]
print(results)  # [10, 20, 30]

# Clean up
for w in workers:
    w.stop()
```

## Architecture and Implementation

### Common Fields in Base Class

The worker implementation has been refactored for better maintainability and consistency:

**Base `WorkerProxy` Fields:**
- `worker_cls`: The worker class to instantiate
- `blocking`: Whether method calls return results directly
- `init_args`: Positional arguments for worker initialization  
- `init_kwargs`: Keyword arguments for worker initialization

**Subclass-Specific Fields:**
- `RayWorkerProxy`: `num_cpus`, `num_gpus`, `resources`
- `ProcessWorkerProxy`: `mp_context`
- Other proxies have no additional public fields

This design eliminates redundancy - common fields are defined once in the base class, and worker proxy implementations access them directly without copying to private attributes.

### Consistent Exception Propagation

All worker proxy implementations follow a consistent pattern for exception handling:

1. **Validation errors** (setup, configuration) fail fast
2. **Execution errors** are stored in futures and raised on `.result()`
3. **Original exception types** are preserved across all modes
4. **Exception messages** and tracebacks are maintained

This consistency makes it easier to switch between execution modes without changing error handling code.

## Best Practices

### Choose the Right Execution Mode

- **sync**: Testing and debugging
- **thread**: I/O-bound operations (network requests, file I/O)
- **process**: CPU-bound operations (data processing, computation)
- **asyncio**: **Async I/O operations (async libraries, coroutines)** - provides major performance benefits for async functions
- **ray**: Distributed computing (large-scale parallel processing)

**For async functions**: Always use `mode="asyncio"` to get the best performance. Other modes can execute async functions correctly but won't provide concurrency benefits.

### Resource Management

Always call `stop()` to clean up resources:

```python
worker = DataProcessor.options(mode="process").init(2)
try:
    result = worker.process(10).result()
    # ... use result
finally:
    worker.stop()
```

Or use the built-in context manager (recommended):

```python
# Workers have built-in context manager support
with DataProcessor.options(mode="thread").init(2) as worker:
    result = worker.process(10).result()
    # worker.stop() called automatically

# Also works with pools
with DataProcessor.options(mode="thread", max_workers=5).init(2) as pool:
    results = [pool.process(i).result() for i in range(10)]
    # All workers stopped automatically
```

### Exception Handling

Exceptions in worker methods are consistently propagated across all execution modes, preserving the original exception type and message.

#### Consistent Exception Behavior

All worker implementations now raise the **original exception** when `.result()` is called:

```python
class Validator(Worker):
    def validate(self, value: int) -> int:
        if value < 0:
            raise ValueError("Value must be positive")
        return value
    
    def divide(self, a: int, b: int) -> float:
        return a / b

worker = Validator.options(mode="process").init()

# ValueError is raised as-is (not wrapped)
try:
    result = worker.validate(-5).result()
except ValueError as e:
    print(f"Got ValueError: {e}")  # Original exception type

# ZeroDivisionError is raised as-is
try:
    result = worker.divide(10, 0).result()
except ZeroDivisionError as e:
    print(f"Got ZeroDivisionError: {e}")  # Original exception type

worker.stop()
```

#### Exception Handling by Mode

| Mode | Setup Errors | Execution Errors |
|------|--------------|------------------|
| **sync** | Immediate | In `SyncFuture`, raised on `result()` |
| **thread** | Via future | Original exception raised on `result()` |
| **process** | Via future | **Original exception** raised on `result()` |
| **asyncio** | Immediate | Original exception raised on `result()` |
| **ray** | Immediate | Wrapped in `RayTaskError` (Ray's behavior) |

**Key Improvement:** Process mode now raises the original exception instead of wrapping it in `RuntimeError`, making debugging easier and behavior consistent across all modes.

#### Non-Existent Method Errors

Configuration errors (like calling non-existent methods) are handled consistently:

```python
worker = DataProcessor.options(mode="thread").init(2)

# Sync and Ray modes: fail immediately
try:
    worker.nonexistent_method()  # AttributeError raised immediately
except AttributeError as e:
    print(f"Method not found: {e}")

# Thread/Process/Asyncio modes: fail when calling result()
try:
    future = worker.nonexistent_method()
    future.result()  # AttributeError raised here
except AttributeError as e:
    print(f"Method not found: {e}")

worker.stop()
```

## TaskWorker

`TaskWorker` is a concrete worker implementation that provides an `Executor`-like interface (`submit()` and `map()`) for executing arbitrary functions. It's useful when you just need to execute functions in different execution contexts without defining custom worker methods.

### Basic Usage

```python
from concurry import TaskWorker

# Initialize a task worker
worker = TaskWorker.options(mode="thread").init()

# Submit arbitrary functions using submit()
def compute(x, y):
    return x ** 2 + y ** 2

future = worker.submit(compute, 3, 4)
result = future.result()  # 25

# Use map() for multiple tasks
def square(x):
    return x ** 2

results = list(worker.map(square, range(10)))
print(results)  # [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

worker.stop()
```

### Use Cases

TaskWorker is particularly useful for:

- Quick prototyping without defining custom worker classes
- Building higher-level abstractions like WorkerExecutor or WorkerPool
- Executing multiple tasks with `map()` for batch processing
- Testing worker functionality without custom methods

### Example: Processing Multiple Tasks with map()

```python
from concurry import TaskWorker

# Initialize a process-based task worker for CPU-intensive work
worker = TaskWorker.options(mode="process").init()

# Use map() for batch processing
def factorial(n):
    if n <= 1:
        return 1
    return n * factorial(n - 1)

results = list(worker.map(factorial, range(1, 11)))
print(results)  # [1, 2, 6, 24, 120, 720, 5040, 40320, 362880, 3628800]

worker.stop()
```

### Example: Using submit() for Individual Tasks

```python
from concurry import TaskWorker

worker = TaskWorker.options(mode="thread").init()

# Submit individual tasks
futures = [worker.submit(factorial, i) for i in range(1, 11)]
results = [f.result() for f in futures]

print(results)  # [1, 2, 6, 24, 120, 720, 5040, 40320, 362880, 3628800]

worker.stop()
```

### Comparison with Custom Workers

**Use TaskWorker when:**
- You don't need custom methods
- You're executing arbitrary functions
- You want the familiar `concurrent.futures.Executor` interface (`submit()` and `map()`)
- You want a quick solution without boilerplate

**Use Custom Worker when:**
- You need stateful operations
- You want named, documented methods
- Your worker has complex initialization
- You're building a reusable component

### Example: TaskWorker vs Custom Worker

```python
# Using TaskWorker (simpler, Executor-like interface)
task_worker = TaskWorker.options(mode="thread").init()
result = task_worker.submit(lambda x: x * 2, 10).result()
task_worker.stop()

# Using Custom Worker (more structure, better for complex logic)
class Calculator(Worker):
    def __init__(self, multiplier):
        self.multiplier = multiplier
        self.count = 0
    
    def compute(self, x):
        self.count += 1
        return x * self.multiplier

calc_worker = Calculator.options(mode="thread").init(2)
result = calc_worker.compute(10).result()  # 20
count = calc_worker.count  # State is maintained
calc_worker.stop()
```

## Automatic Future Unwrapping

One of the most powerful features in concurry is automatic future unwrapping, which enables seamless composition of workers. When you pass a `BaseFuture` (returned by any worker method) as an argument to another worker method, concurry automatically unwraps it by calling `.result()` before passing the value to the receiving worker.

### Basic Future Unwrapping

By default, all futures passed as arguments are automatically unwrapped:

```python
from concurry import Worker

class DataSource(Worker):
    def __init__(self, value: int):
        self.value = value
    
    def get_data(self) -> int:
        return self.value * 10

class DataProcessor(Worker):
    def __init__(self):
        pass
    
    def process(self, data: int) -> int:
        return data + 100

# Initialize workers with different execution modes
source = DataSource.options(mode="thread").init(5)
processor = DataProcessor.options(mode="process").init()

# Get data from source (returns a future)
future_data = source.get_data()  # Future -> 50

# Pass future directly to processor - it's automatically unwrapped!
result = processor.process(future_data).result()  # 50 + 100 = 150

print(result)  # 150
source.stop()
processor.stop()
```

**What happened:**
1. `source.get_data()` returns a `BaseFuture` wrapping the value `50`
2. When passed to `processor.process()`, concurry automatically calls `future_data.result()` to get `50`
3. The processor receives the materialized value `50`, not the future object

### Nested Structure Unwrapping

Future unwrapping works recursively through nested data structures like lists, tuples, dicts, and sets:

```python
class MathWorker(Worker):
    def __init__(self, base: int):
        self.base = base
    
    def add(self, x: int) -> int:
        return self.base + x
    
    def multiply(self, x: int) -> int:
        return self.base * x

class Aggregator(Worker):
    def __init__(self):
        pass
    
    def sum_list(self, numbers: list) -> int:
        return sum(numbers)
    
    def sum_nested(self, data: dict) -> int:
        """Sum all integers in a nested structure."""
        total = 0
        for value in data.values():
            if isinstance(value, int):
                total += value
            elif isinstance(value, list):
                total += sum(value)
            elif isinstance(value, dict):
                total += self.sum_nested(value)
        return total

# Create workers
math_worker = MathWorker.options(mode="thread").init(10)
aggregator = Aggregator.options(mode="thread").init()

# Create multiple futures
f1 = math_worker.add(5)   # Future -> 15
f2 = math_worker.add(10)  # Future -> 20
f3 = math_worker.add(15)  # Future -> 25

# Pass list of futures - all automatically unwrapped
result1 = aggregator.sum_list([f1, f2, f3]).result()
print(result1)  # 15 + 20 + 25 = 60

# Pass deeply nested structure with futures
nested_data = {
    "values": [f1, f2],           # Futures in a list
    "extra": {"bonus": f3},       # Future in nested dict
    "constant": 100                # Regular value (not a future)
}
result2 = aggregator.sum_nested(nested_data).result()
print(result2)  # 15 + 20 + 25 + 100 = 160

math_worker.stop()
aggregator.stop()
```

**Supported Collections:**
- `list`: `[future1, future2]`
- `tuple`: `(future1, future2)`
- `dict`: `{"key": future}` (only values are unwrapped, not keys)
- `set`: `{future1, future2}`
- `frozenset`: `frozenset([future1, future2])`

### Cross-Worker Communication

Future unwrapping works seamlessly across different worker types:

```python
# Thread worker produces data
producer = DataSource.options(mode="thread").init(100)

# Process worker consumes it (different execution context)
consumer = DataProcessor.options(mode="process").init()

future = producer.get_data()  # ThreadWorker future
result = consumer.process(future).result()  # Unwrapped and passed to ProcessWorker

print(result)  # 1100
producer.stop()
consumer.stop()
```

The future is automatically materialized on the client side and the value is passed to the receiving worker, regardless of worker type.

### Ray Zero-Copy Optimization

When passing futures between Ray workers, concurry uses a special optimization to avoid data serialization:

```python
import ray
ray.init()

from concurry import Worker

class RayCompute(Worker):
    def __init__(self, multiplier: int):
        self.multiplier = multiplier
    
    def compute(self, x: int) -> int:
        return x * self.multiplier

# Two Ray workers
worker1 = RayCompute.options(mode="ray").init(10)
worker2 = RayCompute.options(mode="ray").init(5)

# Worker1 produces a RayFuture (wrapping an ObjectRef)
future = worker1.compute(100)  # RayFuture wrapping ObjectRef -> 1000

# When passed to worker2, the ObjectRef is passed directly (zero-copy!)
# No data serialization occurs - Ray handles data movement internally
result = worker2.compute(future).result()  # 1000 * 5 = 5000

print(result)  # 5000
worker1.stop()
worker2.stop()
ray.shutdown()
```

**How it works:**
- **RayFuture → RayWorker**: ObjectRef passed directly (zero-copy)
- **Other Future → RayWorker**: Value materialized before passing
- **RayFuture → Other Worker**: Value materialized before passing

This optimization significantly improves performance for Ray-to-Ray communication by avoiding unnecessary data movement through the client.

### Disabling Future Unwrapping

In some cases, you may want to pass futures as objects (e.g., for inspection or custom handling). Use `unwrap_futures=False`:

```python
from concurry import Worker
from concurry.core.future import BaseFuture

class FutureInspector(Worker):
    def __init__(self):
        pass
    
    def is_future(self, obj) -> bool:
        return isinstance(obj, BaseFuture)
    
    def get_future_id(self, obj) -> str:
        if isinstance(obj, BaseFuture):
            return obj.uuid
        return "not a future"

# Create worker with unwrapping disabled
inspector = FutureInspector.options(
    mode="thread",
    unwrap_futures=False  # Pass futures as objects
).init()

# Create a producer
producer = DataSource.options(mode="thread").init(42)
future = producer.get_data()

# Inspector receives the future object, not the value
is_fut = inspector.is_future(future).result()
print(is_fut)  # True

fut_id = inspector.get_future_id(future).result()
print(fut_id)  # sync-future-abc123...

inspector.stop()
producer.stop()
```

### Mixing Futures and Regular Values

You can freely mix futures with regular values in your arguments:

```python
aggregator = Aggregator.options(mode="thread").init()
math_worker = MathWorker.options(mode="thread").init(10)

f1 = math_worker.add(5)  # Future -> 15

# Mix futures with regular values
result = aggregator.sum_list([f1, 20, 30, 40]).result()
print(result)  # 15 + 20 + 30 + 40 = 105

aggregator.stop()
math_worker.stop()
```

### Exception Handling

Exceptions in futures are properly propagated during unwrapping:

```python
class FailingWorker(Worker):
    def __init__(self):
        pass
    
    def may_fail(self, value: int) -> int:
        if value < 0:
            raise ValueError("Value must be positive")
        return value * 2

producer = FailingWorker.options(mode="thread").init()
consumer = DataProcessor.options(mode="thread").init()

# Create a future that will fail
failing_future = producer.may_fail(-5)

# When unwrapping, the ValueError is raised
try:
    result = consumer.process(failing_future).result()
except ValueError as e:
    print(f"Caught error during unwrapping: {e}")
    # Output: Caught error during unwrapping: Value must be positive

producer.stop()
consumer.stop()
```

The original exception type and message are preserved through the unwrapping process.

### Performance Considerations

**Zero-Copy Scenarios (No Data Movement):**
- Sync/Thread/Asyncio workers: Already share memory space
- Ray → Ray: ObjectRef passed directly (Ray handles data movement)

**Single Copy Scenarios (Optimal):**
- Thread → Process: Value materialized once and serialized to process
- Process → Ray: Value materialized once and passed to Ray
- Any worker type → Different type: One serialization step

**What Doesn't Happen:**
- ❌ Client doesn't materialize and re-serialize for compatible workers
- ❌ No double serialization (worker → client → worker)
- ❌ No unnecessary data copies

### Real-World Example: Data Pipeline

```python
from concurry import Worker
import ray

ray.init()

class DataFetcher(Worker):
    """Fetch data from source (I/O bound - use thread)."""
    def __init__(self, source: str):
        self.source = source
    
    def fetch(self, query: str) -> dict:
        # Simulate fetching data
        return {"source": self.source, "query": query, "rows": [1, 2, 3, 4, 5]}

class DataTransformer(Worker):
    """Transform data (CPU bound - use process or ray)."""
    def __init__(self, scale: int):
        self.scale = scale
    
    def transform(self, data: dict) -> dict:
        # Expensive transformation
        data["rows"] = [x * self.scale for x in data["rows"]]
        data["transformed"] = True
        return data

class DataAggregator(Worker):
    """Aggregate results (use ray for distributed aggregation)."""
    def __init__(self):
        self.count = 0
    
    def aggregate(self, datasets: list) -> dict:
        self.count += len(datasets)
        all_rows = []
        for dataset in datasets:
            all_rows.extend(dataset["rows"])
        return {"total_rows": len(all_rows), "sum": sum(all_rows), "count": self.count}

# Build pipeline with different execution modes
fetcher = DataFetcher.options(mode="thread").init("database")
transformer = DataTransformer.options(mode="ray").init(10)
aggregator = DataAggregator.options(mode="ray").init()

# Fetch data (returns futures)
data1 = fetcher.fetch("SELECT * FROM table1")
data2 = fetcher.fetch("SELECT * FROM table2")
data3 = fetcher.fetch("SELECT * FROM table3")

# Transform data (futures automatically unwrapped)
transformed1 = transformer.transform(data1)
transformed2 = transformer.transform(data2)
transformed3 = transformer.transform(data3)

# Aggregate results (list of futures automatically unwrapped)
result = aggregator.aggregate([transformed1, transformed2, transformed3]).result()

print(result)
# Output: {'total_rows': 15, 'sum': 450, 'count': 3}

# Clean up
fetcher.stop()
transformer.stop()
aggregator.stop()
ray.shutdown()
```

In this pipeline:
1. Data is fetched by a thread worker (I/O bound)
2. Each dataset is transformed by a Ray worker (CPU bound, distributed)
3. Results are aggregated by another Ray worker
4. All futures are automatically unwrapped at each stage
5. Ray → Ray communication uses zero-copy ObjectRefs

### Best Practices

**1. Let Unwrapping Happen Automatically:**
```python
# ✅ Good: Let concurry handle unwrapping
future = producer.get_data()
result = consumer.process(future).result()

# ❌ Avoid: Manual unwrapping (unnecessary)
future = producer.get_data()
value = future.result()  # Extra step
result = consumer.process(value).result()
```

**2. Use Ray for Distributed Pipelines:**
```python
# ✅ Good: Ray workers benefit from zero-copy ObjectRefs
ray_worker1 = Worker.options(mode="ray").init()
ray_worker2 = Worker.options(mode="ray").init()
result = ray_worker2.process(ray_worker1.compute(x)).result()
```

**3. Mix Execution Modes Appropriately:**
```python
# ✅ Good: Match execution mode to task characteristics
fetcher = DataFetcher.options(mode="thread").init()    # I/O bound
processor = DataProcessor.options(mode="process").init()  # CPU bound
result = processor.transform(fetcher.fetch()).result()  # Seamless
```

**4. Handle Exceptions Gracefully:**
```python
# ✅ Good: Catch exceptions during unwrapping
try:
    result = consumer.process(risky_future).result()
except ValueError as e:
    print(f"Pipeline failed: {e}")
```

**5. Only Disable Unwrapping When Necessary:**
```python
# ✅ Good: Only disable when you need to inspect futures
inspector = FutureInspector.options(unwrap_futures=False).init()

# ❌ Avoid: Disabling unnecessarily complicates code
worker = Worker.options(unwrap_futures=False).init()  # Why?
```

## Submission Queue and Non-Blocking Behavior

One of the key design principles in concurry is that **user submissions are always non-blocking**. You can submit thousands of tasks to workers instantly without your code ever blocking. The `max_queued_tasks` parameter controls internal backpressure from worker proxies to execution backends, not user-facing submission.

### Non-Blocking Submissions

When you call a worker method, it returns a future immediately without blocking:

```python
from concurry import Worker
import time

class SlowWorker(Worker):
    def slow_task(self, duration: float) -> str:
        time.sleep(duration)
        return f"Completed after {duration}s"

# Create worker with submission queue
worker = SlowWorker.options(
    mode="thread",
    max_queued_tasks=5  # Only 5 tasks in-flight to thread at once
).init()

start = time.time()

# All 1000 submissions return INSTANTLY (non-blocking!)
futures = [worker.slow_task(2.0) for _ in range(1000)]

submit_time = time.time() - start
print(f"Submitted 1000 tasks in {submit_time:.3f}s")  # ~0.001s (instant!)

# Tasks execute over time as queue drains
results = [f.result() for f in futures]
total_time = time.time() - start
print(f"Total execution: {total_time:.1f}s")  # ~400s (sequential execution)

worker.stop()
```

**Key Observation:** Submission takes milliseconds, execution takes minutes. Your code never blocks during submission.

### Two-Layer Architecture

The submission system has two distinct layers:

**Layer 1: User → Worker Proxy** (Always Non-Blocking)
- You submit tasks by calling worker methods
- Returns futures instantly
- No limit on number of submissions
- Your code never blocks

**Layer 2: Worker Proxy → Execution Backend** (Controlled by `max_queued_tasks`)
- Worker proxy manages internal queue to backend (thread/process/ray)
- Only `max_queued_tasks` tasks are in-flight to backend at once
- Prevents overloading execution context
- Automatically releases slots as tasks complete

```python
# Conceptual view of the architecture:
#
# User Code                Worker Proxy         Execution Backend
#    │                          │                        │
#    │  worker.task(1)          │                        │
#    ├─────────────────────────>│                        │
#    │  <return future instantly>│                       │
#    │                          │  forward task 1        │
#    │                          ├───────────────────────>│
#    │  worker.task(2)          │                        │
#    ├─────────────────────────>│                        │
#    │  <return future instantly>│                       │
#    │                          │  forward task 2        │
#    │                          ├───────────────────────>│
#    │  ... submit 1000 more    │                        │
#    │  (all return instantly)  │                        │
#    │                          │  (queue full, wait)    │
#    │                          │  ... tasks 3-1000 wait │
#    │                          │                        │
#    │                          │  <task 1 completes>    │
#    │                          │<───────────────────────│
#    │                          │  forward task 3        │
#    │                          ├───────────────────────>│
```

### How `max_queued_tasks` Works

The `max_queued_tasks` parameter controls how many tasks can be "in-flight" from the worker proxy to the underlying execution context:

```python
worker = MyWorker.options(
    mode="thread",
    max_queued_tasks=10  # Max 10 tasks in the thread at once
).init()

# Submit 100 tasks - all return instantly (non-blocking!)
futures = [worker.process(i) for i in range(100)]

# Behind the scenes:
# - Tasks 0-9: Immediately forwarded to thread
# - Tasks 10-99: Wait in worker proxy's internal queue
# - As each task completes in thread, next task is forwarded
# - Your code never blocks - futures are available immediately
```

**What happens when queue fills up:**
- Worker proxy holds excess tasks internally
- User submissions still return futures instantly
- Tasks are forwarded to backend as slots become available
- This is transparent to your code

### Worker Pools: Also Non-Blocking

With worker pools, both pool dispatch AND worker submission are non-blocking:

```python
pool = MyWorker.options(
    mode="thread",
    max_workers=5,           # 5 workers in pool
    max_queued_tasks=10      # Each worker has queue of 10
).init()

start = time.time()

# All 1000 submissions dispatch instantly (non-blocking!)
# Load balancer selects a worker instantly
# Each worker manages its own queue to its thread
futures = [pool.process(i) for i in range(1000)]

dispatch_time = time.time() - start
print(f"Dispatched 1000 tasks in {dispatch_time:.3f}s")  # ~0.002s

# Total capacity: 5 workers × 10 queue = 50 tasks in-flight to threads
# Remaining 950 tasks wait in worker proxy queues (not in user code)

results = [f.result() for f in futures]
pool.stop()
```

**Pool dispatch flow:**
1. You call `pool.method(args)` → Returns future instantly
2. Load balancer selects a worker instantly
3. Worker proxy queues task internally
4. Worker proxy forwards task to backend when slot available
5. Your code never blocks

### Default Values by Mode

Different modes have different default `max_queued_tasks` values:

| Mode | Default `max_queued_tasks` | Rationale |
|------|----------------------------|-----------|
| `sync` | None (bypassed) | Immediate execution, no queuing needed |
| `asyncio` | None (bypassed) | Event loop handles concurrency |
| `thread` | 100 | High concurrency, large queue |
| `process` | 5 | Limited by CPU cores |
| `ray` | 2 | Minimize data transfer overhead and actor memory |

```python
# Thread mode: Large queue for high I/O concurrency
thread_worker = MyWorker.options(mode="thread").init()
# max_queued_tasks=100 (default)

# Process mode: Small queue for CPU-bound tasks
process_worker = MyWorker.options(mode="process").init()
# max_queued_tasks=5 (default)

# Ray mode: Minimal queue to avoid memory bloat in actors
ray_worker = MyWorker.options(mode="ray").init()
# max_queued_tasks=2 (default)
```

### Disabling the Queue

Set `max_queued_tasks=None` to bypass queuing entirely:

```python
worker = MyWorker.options(
    mode="thread",
    max_queued_tasks=None  # No queue limit
).init()

# All tasks flow immediately to thread
# Thread's internal queue handles backpressure
# Useful for stress testing or when thread queue is more appropriate
```

**When to use `None`:**
- Testing maximum throughput
- Trusting backend's own queuing mechanism
- Worker methods are very fast (microseconds)

**When to use a limit:**
- Controlling memory usage (each future has overhead)
- Preventing Ray actor memory bloat
- Limiting concurrent operations in process workers

### Integration with Blocking Mode

In blocking mode, submission queues are automatically bypassed:

```python
worker = MyWorker.options(
    mode="thread",
    blocking=True,           # Returns results directly
    max_queued_tasks=10      # Ignored in blocking mode
).init()

# Each call blocks until result is ready (sequential execution)
results = [worker.process(i) for i in range(100)]
# No queue needed - execution is sequential
worker.stop()
```

### Integration with Limits

Submission queues and resource limits work together:

```python
from concurry import Worker, ResourceLimit

class DatabaseWorker(Worker):
    def query(self, sql: str) -> list:
        # Acquire resource limit before execution
        with self.limits.acquire(requested={"connections": 1}):
            return execute_query(sql)

worker = DatabaseWorker.options(
    mode="thread",
    max_queued_tasks=10,  # Max 10 tasks queued to thread
    limits=[ResourceLimit(key="connections", capacity=5)]
).init()

# Submit 100 queries - all return futures instantly
futures = [worker.query(f"SELECT {i}") for i in range(100)]

# Submission queue: Controls tasks flowing to thread (10 at once)
# Resource limit: Controls concurrent DB connections (5 at once)
# Both limits work independently and transparently

results = [f.result() for f in futures]
worker.stop()
```

### On-Demand Workers

On-demand workers automatically bypass submission queues since they're ephemeral:

```python
pool = MyWorker.options(
    mode="thread",
    on_demand=True,         # Create worker per request
    max_workers=10,         # Max 10 concurrent workers
    max_queued_tasks=5      # Ignored for on-demand
).init()

# Each request creates a new worker (no queue needed)
# Pool-level concurrency limit (max_workers) provides backpressure
futures = [pool.process(i) for i in range(100)]
results = [f.result() for f in futures]
pool.stop()
```

### Best Practices

**1. Trust the Defaults**

The default `max_queued_tasks` values are tuned for typical workloads:

```python
# ✅ Good: Use defaults for most cases
worker = MyWorker.options(mode="thread").init()  # max_queued_tasks=100

# ❌ Avoid: Micro-optimizing without measurement
worker = MyWorker.options(mode="thread", max_queued_tasks=73).init()
```

**2. Increase Queue for Ray Workers**

Ray actors benefit from smaller queues to avoid memory bloat:

```python
# ✅ Good: Keep Ray queue small (default is 2)
ray_worker = MyWorker.options(mode="ray").init()

# ⚠️ Caution: Large queues can cause Ray actor OOM
ray_worker = MyWorker.options(
    mode="ray",
    max_queued_tasks=1000  # Might cause memory issues
).init()
```

**3. Remember: Submission is Always Fast**

Your code never blocks on submission, so don't worry about it:

```python
# ✅ Good: Submit freely
worker = MyWorker.options(mode="thread").init()
futures = [worker.task(i) for i in range(10000)]  # Instant!

# ❌ Unnecessary: Batching submissions
# (No benefit - submission is already instant)
for batch in chunked(range(10000), 100):
    futures.extend([worker.task(i) for i in batch])
    time.sleep(0.1)  # Pointless delay
```

**4. Combine with Resource Limits for Full Control**

```python
# ✅ Good: Two-layer control
worker = MyWorker.options(
    mode="thread",
    max_queued_tasks=20,    # Control thread queue depth
    limits=[
        ResourceLimit(key="api_connections", capacity=5)
    ]
).init()

# Submission queue: Prevents overloading thread (20 tasks)
# Resource limit: Prevents overloading API (5 concurrent calls)
# Both work together seamlessly
```

### Summary

- **User submissions are always non-blocking** - call worker methods freely
- **Futures return instantly** - no waiting on submission
- **`max_queued_tasks` controls internal queue** - from worker proxy to execution backend
- **Worker pools also non-blocking** - pool dispatch is instant
- **Trust the defaults** - they're tuned for typical workloads
- **Combine with resource limits** - for fine-grained control

## Retry Mechanisms

Workers support automatic retry of failed operations with configurable strategies, exception filtering, and output validation.

### Basic Retry Configuration

```python
from concurry import Worker

class APIWorker(Worker):
    def fetch_data(self, id: int) -> dict:
        # May fail transiently
        return requests.get(f"https://api.example.com/{id}").json()

# Retry up to 3 times with exponential backoff
worker = APIWorker.options(
    mode="thread",
    num_retries=3,
    retry_algorithm="exponential",  # or "linear", "fibonacci"
    retry_wait=1.0,  # Base wait time in seconds
    retry_jitter=0.3  # Randomization factor (0-1)
).init()

result = worker.fetch_data(123).result()
worker.stop()
```

### Default Values and Configuration

All retry parameters have default values from `global_config` that can be customized globally:

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
    global_retry_algorithm="linear"
):
    # All workers created in this context use these defaults
    worker = APIWorker.options(mode="thread").init()
    # Uses num_retries=3, retry_on=[ConnectionError, TimeoutError], linear algorithm

# Customize per execution mode
with temp_config(
    thread_num_retries=5,
    ray_num_retries=10
):
    thread_worker = APIWorker.options(mode="thread").init()  # 5 retries
    ray_worker = APIWorker.options(mode="ray").init()        # 10 retries
```

**Key Points**:
- `retry_on` defaults to `[Exception]` (retry on all exceptions when `num_retries > 0`)
- `retry_until` defaults to `None` (no output validation)
- All retry parameters can be overridden per worker via `Worker.options()`
- Use `temp_config()` to temporarily change defaults for multiple workers

### Exception Filtering

Retry only on specific exceptions:

```python
# Retry only on network errors
worker = APIWorker.options(
    mode="thread",
    num_retries=5,
    retry_on=[ConnectionError, TimeoutError]
).init()

# Custom retry logic
def should_retry(exception, attempt, **ctx):
    return attempt < 3 and isinstance(exception, APIError)

worker = APIWorker.options(
    mode="thread",
    num_retries=5,
    retry_on=should_retry
).init()
```

### Output Validation

Retry when output doesn't meet requirements:

```python
class LLMWorker(Worker):
    def generate_json(self, prompt: str) -> dict:
        response = self.llm.generate(prompt)
        return json.loads(response)

def is_valid_json(result, **ctx):
    return isinstance(result, dict) and "data" in result

worker = LLMWorker.options(
    mode="thread",
    num_retries=5,
    retry_until=is_valid_json  # Retry until validation passes
).init()

result = worker.generate_json("Generate user data").result()
```

### TaskWorker with Retry

`TaskWorker` fully supports retries for arbitrary functions:

```python
from concurry import TaskWorker

def flaky_function(x):
    if random.random() < 0.5:
        raise ConnectionError("Transient error")
    return x * 2

worker = TaskWorker.options(
    mode="process",
    num_retries=3,
    retry_on=[ConnectionError]
).init()

# Automatically retries on failure
result = worker.submit(flaky_function, 10).result()

# Works with map() too
results = list(worker.map(flaky_function, range(10)))

worker.stop()
```

### Retry Algorithms

Three backoff strategies are available:

| Algorithm | Pattern | Best For |
|-----------|---------|----------|
| **exponential** (default) | 1s, 2s, 4s, 8s, 16s... | Network requests, API calls |
| **linear** | 1s, 2s, 3s, 4s, 5s... | Rate-limited APIs |
| **fibonacci** | 1s, 1s, 2s, 3s, 5s... | Balanced approach |

All strategies apply "Full Jitter" to randomize wait times and prevent thundering herd problems.

### Integration with Limits

Retries automatically release and reacquire resource limits:

```python
from concurry import ResourceLimit

class DatabaseWorker(Worker):
    def query(self, sql: str) -> list:
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

# If query fails, connection is automatically released before retry
```

For comprehensive retry documentation, see the [Retry Mechanisms Guide](retries.md).

## Performance Considerations

### Startup Overhead

Different execution modes have different startup costs:

- **sync**: Instant (no overhead)
- **thread**: ~1ms (thread creation)
- **process**: ~20ms (fork) or ~7s (spawn on macOS)
- **asyncio**: ~10ms (event loop setup)
- **ray**: Variable (depends on Ray cluster)

### Method Call Overhead

- **sync**: None (direct call)
- **thread**: Low (queue communication)
- **process**: Moderate (serialization + IPC)
- **asyncio**: Low (event loop scheduling)
- **ray**: Higher (network + serialization)

### When to Use Workers

Workers are best for:
- Long-running stateful services
- Tasks that benefit from isolation (processes)
- Operations that need resource control (Ray)
- Maintaining state across many operations

For one-off tasks, consider using regular Executors instead.

