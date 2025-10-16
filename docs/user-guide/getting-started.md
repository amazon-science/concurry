# Getting Started

This guide will walk you through the core concepts and basic usage of Concurry.

## Core Concepts

Concurry provides five main components:

1. **Workers** - Actor pattern for stateful concurrent operations across sync, thread, process, asyncio, and Ray
2. **Worker Pools** - Scale workers with automatic load balancing and shared resource management
3. **Limits** - Resource and rate limiting with composable limit types
4. **Retry Mechanisms** - Automatic retry with configurable strategies and output validation
5. **Unified Future Interface** - A consistent API for working with futures from any framework
6. **Progress Tracking** - Beautiful, informative progress bars with rich features

## Installation

First, install Concurry:

```bash
pip install concurry
```

For Ray support:

```bash
pip install concurry[ray]
```

## Quick Start: Futures

The unified future interface lets you work with futures from different frameworks using a consistent API:

```python
from concurry.core.future import wrap_future
from concurrent.futures import ThreadPoolExecutor

def compute_task(x):
    """A simple computation task."""
    return x ** 2

# Create futures using any framework
with ThreadPoolExecutor() as executor:
    future = executor.submit(compute_task, 42)
    
    # Wrap in unified interface
    unified_future = wrap_future(future)
    
    # Use consistent API
    result = unified_future.result(timeout=5)
    print(f"Result: {result}")  # Output: Result: 1764
```

### Why Unified Futures?

The unified future interface provides:

1. **Framework Agnostic Code**: Write once, run with any executor
2. **Consistent API**: Same methods across all future types
3. **Async/Await Support**: All futures support `await` syntax
4. **Automatic Wrapping**: Smart detection of future types

### Supported Future Types

Concurry automatically handles:

- `concurrent.futures.Future` (threading, multiprocessing)
- `asyncio.Future`
- Ray's `ObjectRef` (with `concurry[ray]`)
- Custom `BaseFuture` implementations

## Quick Start: Progress Bars

Create beautiful progress bars with rich features:

```python
from concurry.utils.progress import ProgressBar
import time

# Wrap any iterable
items = range(100)
for item in ProgressBar(items, desc="Processing"):
    time.sleep(0.01)
    # Progress bar automatically updates and shows success!

# Or create a manual progress bar
pbar = ProgressBar(total=100, desc="Manual Progress")
for i in range(100):
    time.sleep(0.01)
    pbar.update(1)
pbar.success("Complete!")
```

### Progress Bar Features

- **Automatic State Tracking**: Success/failure/stop with color coding
- **Multiple Styles**: Auto-detect notebook, terminal, or Ray environments
- **Flexible Updates**: Manual or automatic updates with batching
- **Rich Customization**: Colors, units, descriptions, and more

## Combining Futures and Progress

The real power comes from combining both:

```python
from concurry.core.future import wrap_future
from concurry.utils.progress import ProgressBar
from concurrent.futures import ThreadPoolExecutor
import time

def process_item(item):
    """Process a single item."""
    time.sleep(0.1)
    return item * 2

def parallel_processing(items):
    """Process items in parallel with progress tracking."""
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Submit all tasks
        futures = [wrap_future(executor.submit(process_item, i)) for i in items]
        
        # Track progress
        results = []
        for future in ProgressBar(futures, desc="Processing"):
            result = future.result()
            results.append(result)
        
        return results

# Process 20 items in parallel
items = range(20)
results = parallel_processing(items)
print(f"Processed {len(results)} items")
```

## Best Practices

### 1. Always Wrap External Futures

```python
# Good - wrap external futures
future = wrap_future(executor.submit(task))

# Less ideal - work directly with framework futures
# (loses unified interface benefits)
```

### 2. Use Context Managers for Progress Bars

```python
# Manual progress bar
pbar = ProgressBar(total=100, desc="Work")
try:
    for i in range(100):
        # do work
        pbar.update(1)
    pbar.success()
except Exception as e:
    pbar.failure(str(e))
    raise
```

### 3. Set Appropriate Timeouts

```python
# Always set timeouts for future.result()
try:
    result = future.result(timeout=30)
except TimeoutError:
    print("Task took too long")
    future.cancel()
```

### 4. Handle Errors Gracefully

```python
from concurry.utils.progress import ProgressBar

pbar = ProgressBar(total=100)
try:
    for i in range(100):
        if something_goes_wrong():
            raise ValueError("Error occurred")
        pbar.update(1)
    pbar.success()
except Exception as e:
    pbar.failure(f"Failed: {e}")
    raise
```

## Next Steps

Now that you understand the basics, continue your journey with:

- [Workers Guide](workers.md) - **Start here** to learn the actor pattern and build stateful concurrent operations
- [Worker Pools Guide](pools.md) - Scale workers with pools and load balancing
- [Limits Guide](limits.md) - Add resource and rate limiting to your workers
- [Retry Mechanisms Guide](retries.md) - Make your workers fault-tolerant with automatic retries
- [Futures Guide](futures.md) - Learn advanced future patterns
- [Progress Guide](progress.md) - Master progress bar customization
- [Examples](../examples.md) - See real-world usage patterns
- [API Reference](../api/index.md) - Detailed API documentation

