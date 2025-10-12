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
worker = DataProcessor.options(
    mode="process",
    mp_context="fork"  # or "spawn", "forkserver"
).init(2)
future = worker.process(10)
result = future.result()
worker.stop()
```

### Asyncio Mode

Executes in an asyncio event loop in a dedicated thread (ideal for async I/O operations):

```python
worker = DataProcessor.options(mode="asyncio").init(2)
future = worker.process(10)
result = future.result()
worker.stop()
```

**Note:** Asyncio mode provides significant performance benefits when using async functions. See the [Async Function Support](#async-function-support) section for details.

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

## Submitting Arbitrary Functions

Use `submit_task()` to execute arbitrary functions in the worker's context:

```python
def complex_computation(x, y):
    return (x ** 2 + y ** 2) ** 0.5

worker = DataProcessor.options(mode="process").init(1)

# Submit function that's not a worker method
future = worker.submit_task(complex_computation, 3, 4)
result = future.result()  # 5.0

# Also works with lambdas
future2 = worker.submit_task(lambda x: x * 100, 5)
result2 = future2.result()  # 500

# Mix method calls and task submission
result3 = worker.process(10).result()  # Uses worker method

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

### Submitting Async Functions

Use `submit_task()` with async functions:

```python
async def async_compute(x: int, y: int) -> int:
    """Standalone async function."""
    await asyncio.sleep(0.01)
    return x ** 2 + y ** 2

# Submit async function to any worker
worker = HybridWorker.options(mode="asyncio").init()
future = worker.submit_task(async_compute, 3, 4)
result = future.result()  # 25
worker.stop()
```

### Performance: AsyncIO Worker vs Others

The `AsyncioWorkerProxy` provides **significant performance benefits** for I/O-bound async operations by leveraging its dedicated event loop for concurrent execution:

```python
import asyncio
import time

class FileReader(Worker):
    async def read_file_async(self, file_path: str) -> str:
        """Read file asynchronously."""
        # Using aiofiles for true async I/O
        try:
            import aiofiles
            async with aiofiles.open(file_path, mode='r') as f:
                return await f.read()
        except ImportError:
            # Fallback to simulate async I/O
            await asyncio.sleep(0.001)
            with open(file_path, 'r') as f:
                return f.read()
    
    def read_file_sync(self, file_path: str) -> str:
        """Read file synchronously."""
        with open(file_path, 'r') as f:
            return f.read()

# Test with multiple files
file_paths = [f"file_{i}.txt" for i in range(100)]

# Sync approach with thread worker
worker_thread = FileReader.options(mode="thread").init()
start = time.time()
futures = [worker_thread.read_file_sync(path) for path in file_paths]
results_sync = [f.result() for f in futures]
time_sync = time.time() - start
worker_thread.stop()

# Async approach with asyncio worker
worker_async = FileReader.options(mode="asyncio").init()
start = time.time()
futures = [worker_async.read_file_async(path) for path in file_paths]
results_async = [f.result() for f in futures]
time_async = time.time() - start
worker_async.stop()

print(f"Sync time: {time_sync:.3f}s")
print(f"Async time: {time_async:.3f}s")
print(f"Speedup: {time_sync / time_async:.1f}x")
# Expected: 5-15x speedup for I/O-bound operations
```

### Async Support Across Execution Modes

All worker modes correctly execute async functions, but with different performance characteristics:

| Mode | Async Support | Performance Notes |
|------|---------------|-------------------|
| **asyncio** | ✅ Native | **Best for async**: Uses dedicated event loop, enables true concurrent execution of multiple async tasks |
| **thread** | ✅ Via `asyncio.run()` | Correct execution, but no concurrency benefit (each async call blocks the worker thread) |
| **process** | ✅ Via `asyncio.run()` | Correct execution, but no concurrency benefit + serialization overhead |
| **sync** | ✅ Via `asyncio.run()` | Correct execution, runs synchronously |
| **ray** | ✅ Native + wrapper | Native support for async actor methods, `submit_task()` wraps async functions |

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

### Best Practices for Async Workers

1. **Use AsyncIO mode for async functions**: Get maximum concurrency benefits
   ```python
   # Good: True concurrent execution
   worker = AsyncWorker.options(mode="asyncio").init()
   
   # Works but slower: No concurrency benefit
   worker = AsyncWorker.options(mode="thread").init()
   ```

2. **Leverage asyncio.gather() for concurrent operations**:
   ```python
   async def process_many(self, items: list):
       tasks = [self.async_operation(item) for item in items]
       return await asyncio.gather(*tasks)
   ```

3. **Mix async and sync methods as needed**:
   ```python
   class Worker(Worker):
       async def fetch_data(self):  # Async for I/O
           return await self.http_get(...)
       
       def process_data(self, data):  # Sync for CPU work
           return expensive_computation(data)
   ```

4. **Use appropriate async libraries**:
   - `aiohttp` for HTTP requests
   - `aiofiles` for file I/O
   - `asyncpg` for PostgreSQL
   - `motor` for MongoDB

5. **Handle exceptions properly**:
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

You can also use the `@worker` decorator instead of inheriting from `Worker`:

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

Or use a context manager pattern:

```python
class ManagedWorker:
    def __init__(self, worker):
        self.worker = worker
    
    def __enter__(self):
        return self.worker
    
    def __exit__(self, *args):
        self.worker.stop()

# Usage
with ManagedWorker(DataProcessor.options(mode="thread").init(2)) as worker:
    result = worker.process(10).result()
    # worker.stop() called automatically
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

`TaskWorker` is a concrete worker implementation designed specifically for submitting arbitrary tasks without defining custom methods. It's useful when you just need to execute functions in different execution contexts without defining a custom worker class.

### Basic Usage

```python
from concurry import TaskWorker

# Initialize a task worker
worker = TaskWorker.options(mode="thread").init()

# Submit arbitrary functions
def compute(x, y):
    return x ** 2 + y ** 2

future = worker.submit_task(compute, 3, 4)
result = future.result()  # 25

worker.stop()
```

### Use Cases

TaskWorker is particularly useful for:

- Quick prototyping without defining custom worker classes
- Building higher-level abstractions like WorkerExecutor or WorkerPool
- Submitting one-off tasks to different execution contexts
- Testing worker functionality without custom methods

### Example: Processing Multiple Tasks

```python
from concurry import TaskWorker

# Initialize a process-based task worker for CPU-intensive work
worker = TaskWorker.options(mode="process").init()

# Submit multiple computational tasks
def factorial(n):
    if n <= 1:
        return 1
    return n * factorial(n - 1)

futures = [worker.submit_task(factorial, i) for i in range(1, 11)]
results = [f.result() for f in futures]

print(results)  # [1, 2, 6, 24, 120, 720, 5040, 40320, 362880, 3628800]

worker.stop()
```

### Comparison with Custom Workers

**Use TaskWorker when:**
- You don't need custom methods
- You're just submitting arbitrary functions
- You want a quick solution without boilerplate

**Use Custom Worker when:**
- You need stateful operations
- You want named, documented methods
- Your worker has complex initialization
- You're building a reusable component

### Example: TaskWorker vs Custom Worker

```python
# Using TaskWorker (simpler, but less structured)
task_worker = TaskWorker.options(mode="thread").init()
result = task_worker.submit_task(lambda x: x * 2, 10).result()
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

