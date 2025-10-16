# API Reference

Welcome to the Concurry API reference documentation. This section provides detailed information about all classes, functions, and modules in the Concurry library.

## Core Modules

### [Workers](workers.md)
Actor pattern implementation for stateful concurrent operations.

**Key Classes:**
- `Worker` - Base class for creating stateful workers
- `TaskWorker` - Concrete worker for submitting arbitrary tasks

**Key Methods:**
- `Worker.options()` - Configure worker execution mode and options
- `Worker.init()` - Initialize the worker
- `TaskWorker.submit()` - Submit arbitrary functions for execution
- `TaskWorker.map()` - Map function over iterables

### [Futures](futures.md)
Unified future interface for working with futures from any concurrency framework.

**Key Classes:**
- `BaseFuture` - Abstract base class providing unified interface
- `SyncFuture` - For immediately available results
- `ConcurrentFuture` - Wraps `concurrent.futures.Future`
- `AsyncioFuture` - Wraps `asyncio.Future`
- `RayFuture` - Wraps Ray's `ObjectRef`

**Key Functions:**
- `wrap_future()` - Automatically wrap any future-like object

### [Limits](limits.md)
Flexible resource protection and rate limiting with composable limit types.

**Key Classes:**
- `Limit` - Abstract base class for all limits
- `RateLimit` - Time-based rate limiting with multiple algorithms
- `CallLimit` - Call counting (special case of RateLimit)
- `ResourceLimit` - Semaphore-based resource limiting
- `LimitSet` - Factory function for thread-safe limit executors

**Key Methods:**
- `LimitSet.acquire()` - Acquire limits for execution
- `Acquisition.update()` - Update usage for rate limits

### [Retry Mechanisms](retries.md)
Automatic retry with configurable strategies, exception filtering, and output validation.

**Key Classes:**
- `RetryConfig` - Configuration for retry behavior
- `RetryValidationError` - Raised when output validation fails after all retries

**Key Functions:**
- `calculate_retry_wait()` - Calculate wait time for retry attempt

### [Progress Tracking](progress.md)
Beautiful, feature-rich progress bars with state tracking.

**Key Classes:**
- `ProgressBar` - Feature-rich progress tracking with tqdm integration

## Module Overview

The Concurry library is organized into focused modules:

```
concurry/
├── __init__.py          # Main exports
├── core/
│   ├── future.py        # Unified future interface
│   ├── config.py        # Configuration enums
│   ├── retry.py         # Retry mechanisms
│   ├── limit/           # Resource and rate limiting
│   │   ├── limit.py     # Limit definitions
│   │   └── limit_set.py # LimitSet factory
│   └── worker/          # Worker pattern implementation
│       ├── base_worker.py   # Worker base classes
│       ├── task_worker.py   # TaskWorker implementation
│       ├── sync_worker.py   # Synchronous worker
│       ├── thread_worker.py # Thread-based worker
│       ├── process_worker.py # Process-based worker
│       ├── asyncio_worker.py # Asyncio-based worker
│       ├── ray_worker.py    # Ray-based worker
│       └── worker_pool.py   # Worker pool implementation
├── executor.py          # Executor function
└── utils/
    └── progress.py      # Progress bar implementation
```

## Quick Reference

### Import Patterns

```python
# Import main classes
from concurry import Worker, TaskWorker, worker

# Import futures
from concurry import BaseFuture, wrap_future

# Import limits
from concurry import LimitSet, RateLimit, CallLimit, ResourceLimit

# Import retry
from concurry import RetryConfig, RetryValidationError, RetryAlgorithm

# Import config
from concurry import ExecutionMode, LoadBalancingAlgorithm, RateLimitAlgorithm

# Import progress
from concurry import ProgressBar
```

### Common Usage Patterns

```python
# Worker pattern
class MyWorker(Worker):
    def process(self, x: int) -> int:
        return x * 2

worker = MyWorker.options(mode="thread").init()
result = worker.process(10).result()
worker.stop()

# TaskWorker pattern
worker = TaskWorker.options(mode="process").init()
result = worker.submit(lambda x: x ** 2, 5).result()
worker.stop()

# Worker with retry
worker = MyWorker.options(
    mode="thread",
    num_retries=3,
    retry_algorithm="exponential"
).init()

# Worker with limits
from concurry import RateLimit

worker = MyWorker.options(
    mode="thread",
    limits=[RateLimit(key="requests", window_seconds=60, capacity=100)]
).init()

# Worker pool
pool = MyWorker.options(
    mode="thread",
    max_workers=10,
    load_balancing="round_robin"
).init()
```

## Type Information

Concurry is fully typed and supports static type checking with mypy. All public APIs include comprehensive type annotations.

### Type Hints

```python
from typing import Any, Optional
from concurry import Worker, BaseFuture

class TypedWorker(Worker):
    def __init__(self, value: int) -> None:
        self.value = value
    
    def process(self, x: int, multiplier: float = 1.0) -> float:
        return (x + self.value) * multiplier

# Type-safe factory
def create_worker(mode: str = "thread") -> TypedWorker:
    return TypedWorker.options(mode=mode).init(value=10)

# Type-safe result handling
def process_future(future: BaseFuture[int], timeout: Optional[float] = None) -> int:
    return future.result(timeout=timeout)
```

## Error Handling

All Concurry APIs use standard Python exceptions:

- `TimeoutError` - Future operation exceeds timeout
- `ValueError` - Invalid arguments or configuration
- `TypeError` - Type validation failures
- `RuntimeError` - Runtime errors (e.g., worker stopped)
- `RetryValidationError` - Output validation failed after retries

## Next Steps

- Browse the detailed API documentation for each module
- Check out the [examples](../examples.md) for practical usage patterns
- Review the [user guide](../user-guide/getting-started.md) for comprehensive tutorials
- Explore [Workers Guide](../user-guide/workers.md) for worker patterns
- Learn about [Retry Mechanisms](../user-guide/retries.md) for fault tolerance
