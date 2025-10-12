# API Reference

Welcome to the Concurry API reference documentation.

## Core Modules

### Futures

The unified future interface for working with futures from any concurrency framework.

- [Futures API Reference](futures.md) - Detailed documentation for `BaseFuture`, `wrap_future()`, and all future implementations

**Key Classes:**
- `BaseFuture` - Abstract base class providing unified interface
- `SyncFuture` - For immediately available results
- `ConcurrentFuture` - Wraps `concurrent.futures.Future`
- `AsyncioFuture` - Wraps `asyncio.Future`
- `RayFuture` - Wraps Ray's `ObjectRef`

**Key Functions:**
- `wrap_future()` - Automatically wrap any future-like object

### Workers

Actor pattern implementation for stateful concurrent operations.

**Key Classes:**
- `Worker` - Base class for creating stateful workers
- `TaskWorker` - Concrete worker for submitting arbitrary tasks
- `WorkerProxy` - Internal proxy handling worker communication

**Key Functions:**
- `@worker` - Decorator to convert classes into workers

### Progress Tracking

Beautiful, feature-rich progress bars with state tracking.

- [Progress API Reference](progress.md) - Detailed documentation for `ProgressBar`

**Key Classes:**
- `ProgressBar` - Feature-rich progress tracking with tqdm integration

## Quick Links

### By Use Case

**Working with Futures:**
```python
from concurry.core.future import wrap_future, BaseFuture
```

**Creating Workers:**
```python
from concurry import Worker, TaskWorker, worker
```

**Progress Tracking:**
```python
from concurry.utils.progress import ProgressBar
```

## Module Organization

```
concurry/
├── core/
│   ├── future.py          # Unified future interface
│   ├── config.py          # Configuration
│   └── worker/            # Worker pattern implementation
│       ├── base_worker.py # Worker base classes and TaskWorker
│       ├── sync_worker.py # Synchronous worker
│       ├── thread_worker.py # Thread-based worker
│       ├── process_worker.py # Process-based worker
│       ├── asyncio_worker.py # Asyncio-based worker
│       └── ray_worker.py  # Ray-based worker
└── utils/
    ├── progress.py        # Progress bar implementation
    ├── environment.py     # Environment detection
    └── frameworks.py      # Framework availability checks
```

## Type Hints

Concurry provides comprehensive type hints for all public APIs:

```python
from concurry.core.future import BaseFuture, wrap_future
from concurry.utils.progress import ProgressBar
from typing import Any, Optional

# Type-safe function signatures
def process_future(future: BaseFuture, timeout: Optional[float] = None) -> Any:
    return future.result(timeout=timeout)

def track_progress(items: list, desc: str) -> None:
    for item in ProgressBar(items, desc=desc):
        process(item)
```

## Error Handling

### Common Exceptions

**TimeoutError:**
Raised when a future operation exceeds the specified timeout.

```python
from concurry.core.future import wrap_future

try:
    result = future.result(timeout=5)
except TimeoutError:
    print("Operation timed out")
```

**ValueError:**
Raised for invalid parameters or configuration.

```python
from concurry.utils.progress import ProgressBar

try:
    pbar = ProgressBar(total=100, miniters=0)  # Invalid
except ValueError as e:
    print(f"Invalid configuration: {e}")
```

## Next Steps

- [Futures API](futures.md) - Complete futures documentation
- [Progress API](progress.md) - Complete progress bar documentation
- [User Guide](../user-guide/getting-started.md) - Learn how to use Concurry
- [Examples](../examples.md) - See practical usage examples

