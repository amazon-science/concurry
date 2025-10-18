# Synchronization Primitives

This guide covers Concurry's synchronization primitives for coordinating and collecting results from multiple futures. The `wait()` and `gather()` functions provide powerful, efficient ways to manage concurrent operations across all execution modes.

## Overview

Concurry provides two main synchronization primitives that work seamlessly with all future types:

- **`wait()`**: Wait for futures to complete based on configurable conditions
- **`gather()`**: Collect results from multiple futures in order or as they complete

Both functions support:
- ✅ All future types (sync, thread, process, asyncio, ray)
- ✅ Dictionary inputs with key preservation
- ✅ Adaptive polling strategies
- ✅ Progress tracking
- ✅ Timeout handling
- ✅ Exception management

## Table of Contents

1. [Quick Start](#quick-start)
2. [wait() Function](#wait-function)
3. [gather() Function](#gather-function)
4. [Dictionary Support](#dictionary-support)
5. [Polling Strategies](#polling-strategies)
6. [Progress Tracking](#progress-tracking)
7. [Exception Handling](#exception-handling)
8. [Performance Optimization](#performance-optimization)
9. [Advanced Patterns](#advanced-patterns)
10. [API Reference](#api-reference)

---

## Quick Start

### Basic wait() Usage

```python
from concurry import Worker, wait, ReturnWhen

class DataWorker(Worker):
    def fetch_data(self, id: int) -> dict:
        # Simulate API call
        return {"id": id, "data": f"result_{id}"}

# Create worker and submit tasks
worker = DataWorker.options(mode="thread").init()
futures = [worker.fetch_data(i) for i in range(10)]

# Wait for all to complete
done, not_done = wait(futures, timeout=30.0)

print(f"Completed: {len(done)}, Pending: {len(not_done)}")

# Get results
for future in done:
    result = future.result()
    print(result)

worker.stop()
```

### Basic gather() Usage

```python
from concurry import Worker, gather

# Submit tasks
futures = [worker.fetch_data(i) for i in range(10)]

# Gather all results (blocks until all complete)
results = gather(futures, timeout=30.0)

# Results are in the same order as futures
for i, result in enumerate(results):
    print(f"Task {i}: {result}")
```

---

## wait() Function

The `wait()` function waits for futures to complete based on a specified condition and returns two sets: completed futures and pending futures.

### Function Signature

```python
def wait(
    fs: Union[List, Tuple, Set, Dict, Any],
    *futs,
    timeout: Optional[float] = None,
    return_when: Union[ReturnWhen, str] = ReturnWhen.ALL_COMPLETED,
    polling: Union[PollingAlgorithm, str] = PollingAlgorithm.Adaptive,
    progress: Union[bool, Dict, Callable, None] = None,
    recurse: bool = False,
) -> Tuple[Set[BaseFuture], Set[BaseFuture]]
```

### Return Conditions

Control when `wait()` returns using the `return_when` parameter:

#### ALL_COMPLETED (Default)

Wait until all futures complete:

```python
from concurry import wait, ReturnWhen

futures = [worker.task(i) for i in range(100)]

# Wait for all futures
done, not_done = wait(futures, return_when=ReturnWhen.ALL_COMPLETED)
assert len(not_done) == 0  # All completed
```

#### FIRST_COMPLETED

Return as soon as any single future completes:

```python
# Useful for "race" conditions - get whichever completes first
futures = [
    worker.fetch_from_api1(),
    worker.fetch_from_api2(),
    worker.fetch_from_api3(),
]

done, not_done = wait(futures, return_when=ReturnWhen.FIRST_COMPLETED)

# Process the first result immediately
first_future = done.pop()
result = first_future.result()
print(f"First result: {result}")

# Cancel remaining if not needed
for future in not_done:
    future.cancel()
```

#### FIRST_EXCEPTION

Return when any future raises an exception:

```python
# Monitor multiple operations, react to first failure
futures = [worker.risky_operation(i) for i in range(10)]

done, not_done = wait(futures, return_when=ReturnWhen.FIRST_EXCEPTION, timeout=60.0)

# Check for exceptions
for future in done:
    try:
        exception = future.exception(timeout=0)
        if exception is not None:
            print(f"Task failed: {exception}")
            # Take corrective action
            break
    except Exception as e:
        print(f"Error checking exception: {e}")
```

### Input Formats

`wait()` accepts multiple input formats:

```python
# 1. List of futures (most common)
futures = [worker.task(i) for i in range(10)]
done, not_done = wait(futures)

# 2. Tuple of futures
futures = tuple([worker.task(i) for i in range(10)])
done, not_done = wait(futures)

# 3. Set of futures
futures = {worker.task(i) for i in range(10)}
done, not_done = wait(futures)

# 4. Dict of futures (keys preserved in done set)
futures_dict = {
    "task1": worker.task(1),
    "task2": worker.task(2),
    "task3": worker.task(3),
}
done, not_done = wait(futures_dict)

# 5. Individual futures (variadic)
f1, f2, f3 = worker.task(1), worker.task(2), worker.task(3)
done, not_done = wait(f1, f2, f3)

# 6. Single future
future = worker.task(42)
done, not_done = wait(future)
```

### Timeout Handling

```python
from concurrent.futures import TimeoutError

futures = [worker.slow_task(i) for i in range(100)]

try:
    done, not_done = wait(futures, timeout=10.0)
    print(f"Completed {len(done)}/{len(futures)} within timeout")
except TimeoutError as e:
    print(f"Timeout: {e}")
```

---

## gather() Function

The `gather()` function collects results from multiple futures, either blocking until all complete or yielding results as they arrive (iterator mode).

### Function Signature

```python
def gather(
    fs: Union[List, Tuple, Set, Dict, Any],
    *futs,
    return_exceptions: bool = False,
    iter: bool = False,
    timeout: Optional[float] = None,
    polling: Union[PollingAlgorithm, str] = PollingAlgorithm.Adaptive,
    progress: Union[bool, Dict, Callable, None] = None,
    recurse: bool = False,
) -> Union[List[Any], Dict[Any, Any], Iterator[Tuple[Union[int, Any], Any]]]
```

### Blocking Mode (Default)

Collect all results in the same order as the input futures:

```python
from concurry import gather

# Submit tasks
futures = [worker.compute(i) for i in range(10)]

# Gather blocks until all complete
results = gather(futures, timeout=30.0)

# Results are in order
assert results == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

### Iterator Mode

Process results as they complete (out-of-order):

```python
# Submit long-running tasks
futures = [worker.long_task(i) for i in range(100)]

# Process as they complete
for index, result in gather(futures, iter=True, progress=True):
    print(f"Task {index} completed: {result}")
    # Save result immediately without waiting for others
    save_to_database(index, result)
```

### Return Exceptions

Capture exceptions as values instead of raising them:

```python
# Some tasks may fail
futures = [worker.risky_task(i) for i in range(10)]

# Gather with exception handling
results = gather(futures, return_exceptions=True)

# Check results
for i, result in enumerate(results):
    if isinstance(result, Exception):
        print(f"Task {i} failed: {result}")
    else:
        print(f"Task {i} succeeded: {result}")
```

### Input Formats

Like `wait()`, `gather()` accepts multiple formats:

```python
# 1. List → Returns list
futures = [worker.task(i) for i in range(10)]
results = gather(futures)  # Returns: [r0, r1, r2, ...]

# 2. Dict → Returns dict with same keys
tasks = {"fetch": f1, "process": f2, "save": f3}
results = gather(tasks)  # Returns: {"fetch": r1, "process": r2, "save": r3}

# 3. Variadic → Returns list
results = gather(future1, future2, future3)  # Returns: [r1, r2, r3]
```

---

## Dictionary Support

Both `wait()` and `gather()` have first-class support for dictionaries, making code more self-documenting and maintainable.

### Why Use Dictionaries?

**Before (with lists):**
```python
futures = [
    worker.fetch_users(),
    worker.fetch_products(),
    worker.fetch_orders(),
]
results = gather(futures)
users = results[0]      # Which index was users again?
products = results[1]    # Easy to mix up
orders = results[2]
```

**After (with dicts):**
```python
tasks = {
    "users": worker.fetch_users(),
    "products": worker.fetch_products(),
    "orders": worker.fetch_orders(),
}
results = gather(tasks)
users = results["users"]        # Self-documenting
products = results["products"]  # Clear and safe
orders = results["orders"]
```

### Dict with wait()

```python
from concurry import wait

# Submit named tasks
tasks = {
    "database": worker.sync_database(),
    "cache": worker.update_cache(),
    "search": worker.reindex_search(),
}

# Wait for all
done, not_done = wait(tasks, timeout=300.0)

# Check which completed
print(f"Completed: {len(done)} tasks")

# The futures in 'done' set are the same as in tasks dict
for future in done:
    result = future.result()
    print(f"Task completed with: {result}")
```

### Dict with gather()

```python
# Submit named tasks
tasks = {
    "user_data": worker.fetch_user_data(user_id),
    "preferences": worker.fetch_preferences(user_id),
    "history": worker.fetch_history(user_id),
    "recommendations": worker.get_recommendations(user_id),
}

# Gather - returns dict with same keys
data = gather(tasks, timeout=10.0)

# Access by name
print(f"User: {data['user_data']}")
print(f"Preferences: {data['preferences']}")
print(f"History: {data['history']}")
print(f"Recommendations: {data['recommendations']}")
```

### Dict with Iterator Mode

When using `iter=True` with dicts, the iterator yields `(key, result)` tuples:

```python
tasks = {
    f"batch_{i}": worker.process_batch(i)
    for i in range(100)
}

# Yields (key, result) tuples
for batch_name, result in gather(tasks, iter=True, progress=True):
    print(f"{batch_name}: processed {len(result)} items")
    save_batch_result(batch_name, result)
```

### Dict with Exception Handling

```python
tasks = {
    "critical_task": worker.must_succeed(),
    "optional_task": worker.may_fail(),
    "backup_task": worker.backup_data(),
}

# Capture exceptions
results = gather(tasks, return_exceptions=True, timeout=60.0)

# Check each task
if isinstance(results["critical_task"], Exception):
    print("CRITICAL: Main task failed!")
    raise results["critical_task"]

if isinstance(results["optional_task"], Exception):
    print("Warning: Optional task failed, continuing anyway")

if isinstance(results["backup_task"], Exception):
    print("Error: Backup failed!")
```

### Any Hashable Key Type

Dicts can use any hashable key type:

```python
# String keys
tasks = {"task1": f1, "task2": f2}

# Integer keys
tasks = {1: f1, 2: f2, 3: f3}

# Tuple keys
tasks = {("user", 123): f1, ("user", 456): f2}

# Custom object keys (if hashable)
from dataclasses import dataclass

@dataclass(frozen=True)
class TaskID:
    category: str
    id: int

tasks = {
    TaskID("fetch", 1): worker.fetch(1),
    TaskID("process", 1): worker.process(1),
}

results = gather(tasks)
```

---

## Polling Strategies

Concurry uses adaptive polling to efficiently check future completion without overloading the system. Different strategies are available for different scenarios.

### Available Strategies

#### Adaptive (Default)

Dynamically adjusts polling interval based on completion rate:

```python
from concurry import gather, PollingAlgorithm

# Adaptive is the default
results = gather(futures)

# Or explicitly
results = gather(futures, polling=PollingAlgorithm.Adaptive)

# Or using string
results = gather(futures, polling="adaptive")
```

**How it works:**
- Starts at 10ms interval
- Speeds up (×0.7) when futures complete
- Slows down (×1.3) after 3 consecutive empty checks
- Bounds: 1ms (min) to 100ms (max)

**Best for:** Variable workloads, general use (default)

#### Fixed

Constant polling interval:

```python
# Fixed 10ms interval (default)
results = gather(futures, polling=PollingAlgorithm.Fixed)

# Or string
results = gather(futures, polling="fixed")
```

**How it works:**
- Always polls at fixed interval (default: 10ms)
- Predictable and simple
- No adaptation

**Best for:** Known completion times, debugging

#### Exponential

Exponential backoff with reset on completion:

```python
results = gather(futures, polling=PollingAlgorithm.Exponential)
```

**How it works:**
- Starts at 1ms
- Doubles on each empty check (×2)
- Resets to 1ms on completion
- Max: 500ms

**Best for:** Long-running operations, unknown completion times

#### Progressive

Steps through fixed interval levels:

```python
results = gather(futures, polling=PollingAlgorithm.Progressive)
```

**How it works:**
- Progresses through levels: 1ms → 5ms → 10ms → 50ms → 100ms
- Stays at each level for 5 checks
- Resets to 1ms on completion

**Best for:** Operations with predictable phases

### Choosing a Strategy

```python
# Fast operations (< 1 second)
results = gather(futures, polling="fixed")

# Variable duration (default)
results = gather(futures, polling="adaptive")

# Long-running (minutes)
results = gather(futures, polling="exponential")

# Predictable phases
results = gather(futures, polling="progressive")
```

### Performance Comparison

```python
import time
from concurry import gather, PollingAlgorithm

# Benchmark different strategies
strategies = ["fixed", "adaptive", "exponential", "progressive"]
futures = [worker.task(i) for i in range(100)]

for strategy in strategies:
    start = time.time()
    results = gather(futures, polling=strategy)
    elapsed = time.time() - start
    print(f"{strategy}: {elapsed:.3f}s")
```

---

## Progress Tracking

Both `wait()` and `gather()` support flexible progress tracking through progress bars or custom callbacks.

### Auto Progress Bar

```python
from concurry import gather

futures = [worker.task(i) for i in range(1000)]

# Simple progress bar
results = gather(futures, progress=True)
# Output: Gathering: 100%|██████████| 1000/1000 [00:05<00:00, 200.00result/s]
```

### Custom Progress Bar

```python
# Customize progress bar appearance
results = gather(
    futures,
    progress={
        "desc": "Processing batches",
        "unit": "batch",
        "colour": "green",
        "miniters": 10,  # Update every 10 items
    }
)
```

### Progress Callback

```python
def progress_callback(completed: int, total: int, elapsed: float):
    """Custom progress handler.
    
    Args:
        completed: Number of completed futures
        total: Total number of futures
        elapsed: Elapsed time in seconds
    """
    percent = (completed / total) * 100
    rate = completed / elapsed if elapsed > 0 else 0
    print(f"Progress: {completed}/{total} ({percent:.1f}%) - {rate:.1f} items/sec")

# Use callback
results = gather(futures, progress=progress_callback)
```

### Progress with wait()

```python
# Progress bar
done, not_done = wait(
    futures,
    progress=True,
    return_when=ReturnWhen.ALL_COMPLETED
)

# Custom callback
def wait_callback(completed: int, total: int, elapsed: float):
    print(f"Waiting: {completed}/{total} complete after {elapsed:.1f}s")

done, not_done = wait(futures, progress=wait_callback)
```

### Progress with Iterator Mode

```python
# Progress bar with iterator
for index, result in gather(futures, iter=True, progress=True):
    process_result(result)
    # Progress bar updates automatically

# Callback with iterator
def iter_callback(completed: int, total: int, elapsed: float):
    if completed % 100 == 0:  # Every 100 items
        print(f"Processed: {completed}/{total}")

for index, result in gather(futures, iter=True, progress=iter_callback):
    process_result(result)
```

### Logging Integration

```python
import logging

logger = logging.getLogger(__name__)

def logging_progress(completed: int, total: int, elapsed: float):
    """Log progress to application logger."""
    if completed % 50 == 0 or completed == total:
        logger.info(
            f"Progress: {completed}/{total} ({completed/total*100:.1f}%) "
            f"in {elapsed:.1f}s"
        )

results = gather(futures, progress=logging_progress, timeout=300.0)
```

---

## Exception Handling

Concurry provides flexible exception handling for concurrent operations.

### Default Behavior (Raise on Exception)

By default, exceptions are raised:

```python
from concurry import gather

futures = [
    worker.task(0),
    worker.failing_task(1),  # This will raise ValueError
    worker.task(2),
]

try:
    results = gather(futures)
except ValueError as e:
    print(f"Task failed: {e}")
```

### Return Exceptions Mode

Capture exceptions as values:

```python
futures = [worker.task(i) for i in range(10)]

# Don't raise, return exceptions as values
results = gather(futures, return_exceptions=True)

# Process results
successes = []
failures = []

for i, result in enumerate(results):
    if isinstance(result, Exception):
        failures.append((i, result))
    else:
        successes.append((i, result))

print(f"Successes: {len(successes)}, Failures: {len(failures)}")
```

### Exception Handling with Dicts

```python
tasks = {
    "must_succeed": worker.critical_task(),
    "may_fail": worker.optional_task(),
    "best_effort": worker.experimental_task(),
}

results = gather(tasks, return_exceptions=True)

# Handle each task differently
if isinstance(results["must_succeed"], Exception):
    # Critical failure
    raise RuntimeError("Critical task failed") from results["must_succeed"]

if isinstance(results["may_fail"], Exception):
    # Log and continue
    logger.warning(f"Optional task failed: {results['may_fail']}")
    results["may_fail"] = None  # Use default value

if isinstance(results["best_effort"], Exception):
    # Ignore experimental failures
    results["best_effort"] = None
```

### Exception Handling with Iterator Mode

```python
for index, result in gather(futures, iter=True, return_exceptions=True):
    if isinstance(result, Exception):
        print(f"Task {index} failed: {result}")
        # Log error and continue with next
        log_failure(index, result)
    else:
        print(f"Task {index} succeeded: {result}")
        save_result(index, result)
```

### First Exception Detection

Use `wait()` with `FIRST_EXCEPTION` to react immediately to failures:

```python
from concurry import wait, ReturnWhen

futures = [worker.task(i) for i in range(100)]

# Return as soon as any task fails
done, not_done = wait(
    futures,
    return_when=ReturnWhen.FIRST_EXCEPTION,
    timeout=60.0
)

# Check for exceptions
for future in done:
    try:
        exception = future.exception(timeout=0)
        if exception is not None:
            print(f"First failure detected: {exception}")
            # Cancel remaining tasks
            for f in not_done:
                f.cancel()
            raise exception
    except Exception:
        pass
```

### Timeout vs Exception

```python
from concurrent.futures import TimeoutError

try:
    results = gather(futures, timeout=10.0, return_exceptions=False)
except TimeoutError:
    # Timeout occurred - not all futures completed
    print("Operation timed out")
except Exception as e:
    # A future raised an exception
    print(f"Task failed: {e}")
```

---

## Performance Optimization

### Ray Optimization

For Ray futures, Concurry automatically uses batch checking:

```python
import ray
from concurry import Worker, gather

@ray.remote
class RayWorker(Worker):
    def task(self, x):
        return x * 2

# Create Ray worker
worker = RayWorker.options(mode="ray").init()

# Submit many tasks
futures = [worker.task(i) for i in range(1000)]

# Internally uses single ray.wait() call for efficiency
results = gather(futures)  # Fast!
```

**Performance benefit:** Single IPC call instead of 1000 individual checks.

### Large Batch Handling

```python
from concurry import gather, PollingAlgorithm

# 10,000 futures
futures = [worker.task(i) for i in range(10000)]

# Efficient gathering with adaptive polling
results = gather(
    futures,
    polling=PollingAlgorithm.Adaptive,  # Adapts to completion rate
    progress=True,  # Monitor progress
    timeout=300.0
)
```

### Memory-Efficient Iterator Mode

For very large result sets, use iterator mode to avoid loading all results into memory:

```python
# Process 1 million results
futures = [worker.task(i) for i in range(1_000_000)]

# Iterator mode: low memory footprint
for index, result in gather(futures, iter=True, progress=True):
    # Process one result at a time
    save_to_disk(index, result)
    # Result can be garbage collected immediately
```

### Minimizing Polling Overhead

```python
# For fast operations (< 100ms)
results = gather(futures, polling="fixed")  # Simple, low overhead

# For known slow operations
results = gather(futures, polling="exponential")  # Less CPU usage

# For variable workloads
results = gather(futures, polling="adaptive")  # Balanced (default)
```

### Batching Strategies

```python
# Process in batches
def process_in_batches(items, batch_size=100):
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        futures = [worker.process(item) for item in batch]
        results = gather(futures, timeout=60.0)
        yield from results

# Process 10,000 items in batches of 100
all_results = list(process_in_batches(items, batch_size=100))
```

---

## Advanced Patterns

### Concurrent Map

Implement a concurrent map operation:

```python
from concurry import Worker, gather

class MapWorker(Worker):
    def transform(self, x):
        return x * 2

def concurrent_map(worker, items, batch_size=100):
    """Map function across items concurrently."""
    results = []
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        futures = [worker.transform(item) for item in batch]
        batch_results = gather(futures, timeout=60.0)
        results.extend(batch_results)
    return results

worker = MapWorker.options(mode="thread", max_workers=4).init()
results = concurrent_map(worker, range(1000), batch_size=100)
```

### Racing Requests

Race multiple data sources, use the fastest:

```python
from concurry import wait, ReturnWhen

# Try multiple sources simultaneously
futures = {
    "primary_api": worker.fetch_from_primary(),
    "backup_api": worker.fetch_from_backup(),
    "cache": worker.fetch_from_cache(),
}

# Return as soon as one completes
done, not_done = wait(
    futures,
    return_when=ReturnWhen.FIRST_COMPLETED,
    timeout=5.0
)

# Use the first result
if done:
    fastest = done.pop()
    result = fastest.result()
    print(f"Got result from fastest source: {result}")
    
    # Cancel remaining
    for future in not_done:
        future.cancel()
```

### Streaming Results

Stream results as they complete for real-time processing:

```python
from concurry import gather

# Submit many tasks
futures = [worker.analyze_document(doc) for doc in documents]

# Stream results to client
def stream_results():
    for doc_id, analysis in gather(futures, iter=True, progress=True):
        # Yield result immediately
        yield {
            "document_id": doc_id,
            "analysis": analysis,
            "timestamp": time.time()
        }

# In a web framework
for result in stream_results():
    send_to_client(result)
```

### Conditional Continuation

Continue processing based on results:

```python
# Phase 1: Initial batch
phase1_futures = [worker.phase1(i) for i in range(10)]
phase1_results = gather(phase1_futures)

# Phase 2: Only process successful phase 1 items
phase2_futures = []
for i, result in enumerate(phase1_results):
    if result is not None and result.get("status") == "success":
        phase2_futures.append(worker.phase2(result["data"]))

# Gather phase 2 results
if phase2_futures:
    phase2_results = gather(phase2_futures, return_exceptions=True)
```

### Deadline-Based Processing

Process as many items as possible within a deadline:

```python
import time

def process_with_deadline(items, deadline_seconds):
    """Process as many items as possible within deadline."""
    start = time.time()
    results = []
    
    futures = [worker.process(item) for item in items]
    
    for index, result in gather(futures, iter=True):
        results.append((index, result))
        
        # Check deadline
        elapsed = time.time() - start
        if elapsed >= deadline_seconds:
            print(f"Deadline reached after processing {len(results)} items")
            break
    
    return results

# Process for up to 30 seconds
results = process_with_deadline(large_dataset, deadline_seconds=30.0)
```

### Retrying Failed Tasks

Retry failed tasks with exponential backoff:

```python
from concurry import gather

def gather_with_retry(futures, max_retries=3, backoff=2.0):
    """Gather results with automatic retry on failure."""
    attempt = 0
    retry_futures = futures
    
    while attempt <= max_retries:
        results = gather(retry_futures, return_exceptions=True)
        
        # Separate successes and failures
        successes = []
        failed_indices = []
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed_indices.append(i)
            else:
                successes.append(result)
        
        if not failed_indices:
            return successes
        
        # Retry failed tasks
        print(f"Attempt {attempt + 1}: {len(failed_indices)} tasks failed, retrying...")
        retry_futures = [futures[i] for i in failed_indices]
        
        # Resubmit failed tasks
        retry_futures = [worker.task(i) for i in failed_indices]
        
        attempt += 1
        time.sleep(backoff ** attempt)
    
    # Final attempt
    return gather(retry_futures, return_exceptions=True)
```

---

## API Reference

### wait()

```python
def wait(
    fs: Union[List, Tuple, Set, Dict, Any],
    *futs,
    timeout: Optional[float] = None,
    return_when: Union[ReturnWhen, str] = ReturnWhen.ALL_COMPLETED,
    polling: Union[PollingAlgorithm, str] = PollingAlgorithm.Adaptive,
    progress: Union[bool, Dict, Callable, None] = None,
    recurse: bool = False,
) -> Tuple[Set[BaseFuture], Set[BaseFuture]]
```

**Parameters:**

- `fs`: Primary argument - list/tuple/set/dict of futures, or single future
- `*futs`: Additional futures (only if `fs` is not a structure)
- `timeout`: Maximum time to wait in seconds (None = indefinite)
- `return_when`: When to return - ALL_COMPLETED, FIRST_COMPLETED, or FIRST_EXCEPTION
- `polling`: Polling algorithm - Adaptive, Fixed, Exponential, or Progressive
- `progress`: Progress tracking - bool, dict, or callable
- `recurse`: Recursively process nested structures

**Returns:**

Tuple of `(done, not_done)` sets containing `BaseFuture` instances

**Raises:**

- `TimeoutError`: If timeout expires before condition met
- `ValueError`: If invalid arguments provided

### gather()

```python
def gather(
    fs: Union[List, Tuple, Set, Dict, Any],
    *futs,
    return_exceptions: bool = False,
    iter: bool = False,
    timeout: Optional[float] = None,
    polling: Union[PollingAlgorithm, str] = PollingAlgorithm.Adaptive,
    progress: Union[bool, Dict, Callable, None] = None,
    recurse: bool = False,
) -> Union[List[Any], Dict[Any, Any], Iterator[Tuple[Union[int, Any], Any]]]
```

**Parameters:**

- `fs`: Primary argument - list/tuple/set/dict of futures, or single future
- `*futs`: Additional futures (only if `fs` is not a structure)
- `return_exceptions`: Return exceptions as values instead of raising
- `iter`: Return iterator instead of blocking
- `timeout`: Maximum time to wait for all results (None = indefinite)
- `polling`: Polling algorithm
- `progress`: Progress tracking - bool, dict, or callable
- `recurse`: Recursively process nested structures

**Returns:**

- If `iter=False` and input is list/tuple: List of results in same order
- If `iter=False` and input is dict: Dict with same keys
- If `iter=True`: Generator yielding `(index/key, result)` tuples

**Raises:**

- `Exception`: Any exception from futures (if `return_exceptions=False`)
- `TimeoutError`: If timeout expires before all complete
- `ValueError`: If invalid arguments provided

### ReturnWhen Enum

```python
class ReturnWhen(AutoEnum):
    ALL_COMPLETED = "all_completed"
    FIRST_COMPLETED = "first_completed"
    FIRST_EXCEPTION = "first_exception"
```

### PollingAlgorithm Enum

```python
class PollingAlgorithm(AutoEnum):
    Fixed = "fixed"
    Adaptive = "adaptive"
    Exponential = "exponential"
    Progressive = "progressive"
```

---

## Best Practices

1. **Use dictionaries for named tasks** - More maintainable and self-documenting
2. **Enable progress tracking for long operations** - Better UX and debugging
3. **Use iterator mode for large result sets** - Memory-efficient streaming
4. **Set reasonable timeouts** - Prevent indefinite blocking
5. **Use return_exceptions for fault tolerance** - Continue processing despite failures
6. **Choose appropriate polling strategy** - Match your workload characteristics
7. **Batch large workloads** - Better performance and resource management
8. **Use FIRST_COMPLETED for racing** - Get fastest result from multiple sources
9. **Monitor with progress callbacks** - Integrate with logging/metrics
10. **Cancel unnecessary futures** - Free resources after getting needed results

---

## Migration from old_synch

The old synchronization primitives in `old_synch.py` are deprecated. Here's how to migrate:

### Old wait() → New wait()

```python
# Old API
from concurry.core.old_synch import wait
wait(futures, check_done=True, item_wait=0.001)

# New API
from concurry import wait
wait(futures, polling="adaptive")
```

### Old gather() → New gather()

```python
# Old API
from concurry.core.old_synch import gather
gather(futures, succeeded_only=False)

# New API
from concurry import gather
gather(*futures, return_exceptions=False)
```

### Old gather_iter() → New gather(iter=True)

```python
# Old API
from concurry.core.old_synch import gather_iter
for result in gather_iter(futures):
    process(result)

# New API
from concurry import gather
for index, result in gather(*futures, iter=True):
    process(result)
```

---

## Troubleshooting

### Timeout Issues

**Problem:** Timeouts occurring too frequently

**Solutions:**
- Increase timeout value
- Use iterator mode to process partial results
- Check worker performance
- Use adaptive or exponential polling for variable workloads

### Performance Issues

**Problem:** Slow gathering with many futures

**Solutions:**
- Use appropriate polling strategy (adaptive for most cases)
- For Ray, ensure ray.init() is called
- Use iterator mode to avoid loading all results
- Process in batches for very large workloads

### Memory Issues

**Problem:** High memory usage with large result sets

**Solutions:**
- Use iterator mode (`iter=True`)
- Process results immediately and discard
- Use batching to limit concurrent operations
- Stream results to disk/database

---

## See Also

- [Workers Guide](workers.md) - Creating and managing workers
- [Futures Guide](futures.md) - Understanding future objects
- [Execution Modes](execution-modes.md) - Choosing the right mode
- [Progress Tracking](progress.md) - Advanced progress monitoring

