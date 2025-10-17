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
worker = DataProcessor.options(
    mode="process",
    mp_context="fork"  # or "spawn", "forkserver"
).init(2)
future = worker.process(10)
result = future.result()
worker.stop()
```

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

3. **Use asyncio.gather() for concurrent operations**: Maximum performance
   ```python
   class APIWorker(Worker):
       async def fetch_many(self, urls: list) -> list:
           # ✅ Good: All requests execute concurrently
           tasks = [self.fetch_url(url) for url in urls]
           return await asyncio.gather(*tasks)
   
       async def fetch_url(self, url: str) -> str:
           # Individual async method
           async with aiohttp.ClientSession() as session:
               async with session.get(url) as response:
                   return await response.text()
   ```

4. **Use appropriate async libraries**:
   - `aiohttp` for HTTP requests (✅ major speedup)
   - `asyncpg` for PostgreSQL (✅ major speedup)
   - `motor` for MongoDB (✅ major speedup)
   - **Note**: For local file I/O, ThreadWorker or SyncWorker may be faster than AsyncioWorker due to OS-level buffering and small file sizes. Use AsyncioWorker for network I/O and remote files.

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

## Model Inheritance and Validation

Workers support powerful validation and type checking through both model inheritance and validation decorators. This section covers all options and their compatibility with different execution modes.

### Overview of Options

| Feature | Sync | Thread | Process | Asyncio | Ray | Notes |
|---------|------|--------|---------|---------|-----|-------|
| **morphic.Typed** | ✅ | ✅ | ✅ | ✅ | ❌ | Full model with validation & hooks |
| **pydantic.BaseModel** | ✅ | ✅ | ✅ | ✅ | ❌ | Pydantic validation & serialization |
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

# ✅ Works with thread, process, asyncio
worker = TypedWorker.options(mode="thread").init(
    name="  data processor  ",  # Will be normalized to "Data Processor"
    value=10,
    tags=["ml", "preprocessing"]
)

result = worker.compute(5).result()  # 50
print(worker.name)  # "Data Processor"
worker.stop()

# ❌ Does NOT work with Ray
try:
    worker = TypedWorker.options(mode="ray").init(name="test", value=10)
except ValueError as e:
    print(f"Expected error: {e}")
    # ValueError: Cannot create Ray worker with Pydantic-based class
```

**Benefits:**
- Automatic field validation with Field constraints
- Type coercion (strings → numbers, etc.)
- Lifecycle hooks (`pre_initialize`, `post_initialize`, etc.)
- Immutable by default (frozen=True)
- Excellent error messages

**Limitations:**
- Not compatible with Ray mode (use decorators instead)
- Adds small overhead from Pydantic validation

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

# ✅ Works with thread, process, asyncio
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

# ❌ Does NOT work with Ray
try:
    worker = PydanticWorker.options(mode="ray").init(name="test", age=25)
except ValueError as e:
    print("Ray mode not supported with BaseModel")
```

**Benefits:**
- Full Pydantic validation capabilities
- Custom validators with `@field_validator`
- JSON serialization/deserialization
- Excellent IDE support
- Rich error messages

**Limitations:**
- Not compatible with Ray mode
- Slightly more overhead than Typed

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

### Ray Mode: What Works and What Doesn't

**❌ Does NOT Work with Ray:**

```python
# These will raise ValueError
class TypedWorker(Worker, Typed):
    name: str
    value: int

class PydanticWorker(Worker, BaseModel):
    name: str
    value: int

# Both raise: ValueError: Cannot create Ray worker with Pydantic-based class
try:
    worker = TypedWorker.options(mode="ray").init(name="test", value=10)
except ValueError:
    pass  # Expected

try:
    worker = PydanticWorker.options(mode="ray").init(name="test", value=10)
except ValueError:
    pass  # Expected
```

**✅ Works with Ray:**

```python
# Option 1: Plain Worker (no validation)
class PlainWorker(Worker):
    def __init__(self, name: str, value: int):
        self.name = name
        self.value = value

worker = PlainWorker.options(mode="ray").init(name="test", value=10)
# ✅ Works

# Option 2: Use @validate decorator
from morphic import validate

class ValidatedWorker(Worker):
    @validate
    def __init__(self, name: str, value: int):
        self.name = name
        self.value = value
    
    @validate
    def compute(self, x: int) -> int:
        return self.value * x

worker = ValidatedWorker.options(mode="ray").init(name="test", value="10")
# ✅ Works with validation and type coercion!

# Option 3: Use @validate_call decorator
from pydantic import validate_call

class PydanticDecoratedWorker(Worker):
    @validate_call
    def __init__(self, name: str, value: int):
        self.name = name
        self.value = value
    
    @validate_call
    def compute(self, x: int) -> int:
        return self.value * x

worker = PydanticDecoratedWorker.options(mode="ray").init(name="test", value=10)
# ✅ Works with Pydantic validation!
```

**Why the Limitation?**

Ray's `ray.remote()` wraps classes as actors and modifies their `__setattr__` behavior, which conflicts with Pydantic's frozen model implementation. When Ray tries to set internal attributes, it triggers Pydantic's validation, causing `AttributeError`.

Decorators like `@validate` and `@validate_call` don't have this problem because they only wrap individual methods, not the entire class.

**Automatic Detection:**

Concurry automatically detects this incompatibility:

```python
# ValueError raised immediately
worker = TypedWorker.options(mode="ray").init(name="test", value=10)
# ValueError: Cannot create Ray worker with Pydantic-based class 'TypedWorker'.
# Ray's actor wrapping mechanism conflicts with Pydantic's __setattr__ implementation.
# 
# Workaround: Use composition instead of inheritance:
#   class TypedWorker(Worker):
#       def __init__(self, ...):
#           self.config = YourPydanticModel(...)

# UserWarning issued for non-Ray modes (if Ray is installed)
worker = TypedWorker.options(mode="thread").init(name="test", value=10)
# UserWarning: Worker class 'TypedWorker' inherits from Pydantic BaseModel.
# This worker will NOT be compatible with Ray mode...
```

### Choosing the Right Approach

**Use morphic.Typed when:**
- You need lifecycle hooks (`pre_initialize`, `post_initialize`, etc.)
- You want immutable workers by default
- You're NOT using Ray mode
- You want the most seamless integration

**Use pydantic.BaseModel when:**
- You need Pydantic's full validation capabilities
- You want JSON serialization/deserialization
- You're NOT using Ray mode
- You need custom validators

**Use @validate decorator when:**
- You need Ray compatibility ✅
- You only need validation on specific methods
- You want minimal overhead
- You prefer morphic's validation style

**Use @validate_call decorator when:**
- You need Ray compatibility ✅
- You want Pydantic's validation features
- You need Field constraints
- You prefer Pydantic's validation style

**Use plain Worker when:**
- You don't need validation
- You want maximum performance
- You're handling validation elsewhere

### Mixing Approaches

You can mix validation decorators with model inheritance (for non-Ray modes):

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

# Works with thread, process, asyncio (not Ray)
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

