# Unified Future Interface

The unified future interface is one of Concurry's core features, providing a consistent API for working with futures from different concurrency frameworks.

## The Problem

Python has multiple concurrency frameworks, each with slightly different future APIs:

```python
# Different APIs for different frameworks
from concurrent.futures import Future as ConcurrentFuture
from asyncio import Future as AsyncioFuture

# Each has slightly different behavior
concurrent_future.result(timeout=5)  # OK
asyncio_future.result()  # No timeout parameter!

# Checking status
concurrent_future.done()  # OK
asyncio_future.done()  # OK, but behavior differs

# Adding callbacks
concurrent_future.add_done_callback(fn)  # OK
asyncio_future.add_done_callback(fn)  # Different callback signature!
```

## The Solution: BaseFuture

Concurry provides `BaseFuture`, a unified interface that closely mirrors Python's `concurrent.futures.Future` API and works consistently across all frameworks:

```python
from concurry.core.future import wrap_future, BaseFuture

# Wrap any future type
unified_future: BaseFuture = wrap_future(any_future)

# Consistent API regardless of underlying framework (matches concurrent.futures.Future)
result = unified_future.result(timeout=5)
is_done = unified_future.done()
is_running = unified_future.running()
was_cancelled = unified_future.cancelled()
unified_future.add_done_callback(callback)
```

### API Compatibility with concurrent.futures.Future

`BaseFuture` implements the complete API of Python's `concurrent.futures.Future`, making it a drop-in replacement with two important differences:

1. **Immutability**: `BaseFuture` is immutable (frozen via `Typed`). The `set_result()`, `set_exception()`, and `set_running_or_notify_cancel()` methods exist for API compatibility but raise `NotImplementedError` since results/exceptions are set during initialization.

2. **Thread-Safety**: All operations are thread-safe across all future types, with each implementation using appropriate locking mechanisms to ensure safe concurrent access.

### Behavioral Guarantees

All `BaseFuture` implementations provide **identical behavior** through the public API. This is rigorously tested to ensure consistency:

1. **Exception Types**: All futures raise the same exceptions for the same conditions (matching [`concurrent.futures.Future`](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future)):
   - `concurrent.futures.CancelledError` - When accessing a cancelled future
   - `TimeoutError` - When operations exceed the specified timeout  
   - Original exception - When the computation itself fails
   
   **Important**: Even when wrapping `asyncio.Future` (which raises `asyncio.CancelledError`), we convert it to `concurrent.futures.CancelledError` for API consistency. This ensures your code can catch the same exception types regardless of the underlying framework.

2. **Callbacks**: All `add_done_callback()` implementations:
   - Pass the wrapper future (not the underlying framework future) to callbacks
   - Call callbacks exactly once when the future completes
   - Call callbacks immediately if the future is already done

3. **Cancellation**: Consistent cancellation behavior:
   - `cancel()` returns `False` if already done, `True` if cancellation succeeded
   - Once cancelled, both `result()` and `exception()` raise `CancelledError`

4. **Blocking Behavior**: Both `result()` and `exception()`:
   - Block until the future completes (or timeout expires)
   - Respect the timeout parameter consistently across all implementations
   - `None` timeout means wait indefinitely

5. **Await Support**: All futures support `async/await` syntax through `__await__`, regardless of the underlying framework

This means you can write framework-agnostic code with confidence that it will behave identically whether using threading, asyncio, Ray, or any other backend.

## Core Concepts

### 1. Automatic Future Detection

The `wrap_future()` function automatically detects the future type:

```python
from concurry.core.future import wrap_future
from concurrent.futures import ThreadPoolExecutor
import asyncio

# Threading future
with ThreadPoolExecutor() as executor:
    thread_future = executor.submit(lambda: 42)
    unified = wrap_future(thread_future)  # Returns ConcurrentFuture wrapper

# Asyncio future
async def async_example():
    loop = asyncio.get_event_loop()
    async_future = loop.create_future()
    unified = wrap_future(async_future)  # Returns AsyncioFuture wrapper
```

### 2. Unified Interface

All wrapped futures provide these methods (matching `concurrent.futures.Future`):

```python
from concurry.core.future import BaseFuture

future: BaseFuture = wrap_future(some_future)

# Get result (blocks until complete)
result = future.result(timeout=10)

# Check status
if future.done():
    print("Future is complete")

if future.running():
    print("Future is currently executing")

if future.cancelled():
    print("Future was cancelled")

# Get exception (if any)
exc = future.exception(timeout=5)
if exc:
    print(f"Future raised: {exc}")

# Cancel the future
if future.cancel():
    print("Successfully cancelled")

# Add completion callback
future.add_done_callback(lambda f: print("Done!"))
```

### Immutable Methods

For API compatibility with `concurrent.futures.Future`, these methods exist but raise `NotImplementedError`:

```python
# These methods exist but are not supported due to immutability
try:
    future.set_result(42)
except NotImplementedError:
    print("BaseFuture is immutable - results are set at creation")

try:
    future.set_exception(ValueError("error"))
except NotImplementedError:
    print("BaseFuture is immutable - exceptions are set at creation")

try:
    future.set_running_or_notify_cancel()
except NotImplementedError:
    print("State is managed internally")
```

### Thread-Safety Guarantees

All `BaseFuture` operations are **thread-safe**:

```python
import threading
from concurry.core.future import wrap_future
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor() as executor:
    future = wrap_future(executor.submit(lambda: 42))
    
    # Safe to access from multiple threads simultaneously
    def access_future():
        if future.done():
            result = future.result()
            print(f"Result: {result}")
    
    threads = [threading.Thread(target=access_future) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
```

Implementation details:
- **SyncFuture**: Thread-safe through immutability (no lock needed)
- **ConcurrentFuture**: Delegates to thread-safe `concurrent.futures.Future` (lock for consistency)
- **AsyncioFuture**: Uses internal lock for thread-safe access to asyncio futures
- **RayFuture**: Uses internal lock for thread-safe state management

### BaseFuture Private Members

`BaseFuture` defines only the private members common to all futures (matching `concurrent.futures.Future`):

```python
class BaseFuture(Typed, ABC):
    # Private members common to all futures (matching concurrent.futures.Future)
    _result: Any = None
    _exception: Optional[Exception] = None
    _done: bool = False
    _cancelled: bool = False
    _callbacks: list = []
    _lock: Optional[threading.Lock] = None
```

Framework-specific private members are defined only on the subclasses that need them:

```python
class ConcurrentFuture(BaseFuture):
    _future: PyFuture  # The underlying concurrent.futures.Future

class AsyncioFuture(BaseFuture):
    _future: Any      # The underlying asyncio.Future
    _loop: Any        # The asyncio event loop

class RayFuture(BaseFuture):
    _object_ref: Any  # The Ray ObjectRef

class SyncFuture(BaseFuture):
    # No framework-specific members needed
    pass
```

Each subclass's `post_initialize()` method sets these private members appropriately for its specific framework.

### 3. Async/Await Support

All unified futures support async/await syntax:

```python
from concurry.core.future import wrap_future
from concurrent.futures import ThreadPoolExecutor

async def async_process():
    with ThreadPoolExecutor() as executor:
        # Even threading futures can be awaited!
        future = wrap_future(executor.submit(lambda: 42))
        result = await future  # Works!
        print(f"Result: {result}")
```

## Future Types

### SyncFuture

For immediately available results:

```python
from concurry.core.future import SyncFuture

# Create a future with a result
future = SyncFuture(result_value=42)
print(future.result())  # 42

# Create a future with an exception
future = SyncFuture(exception_value=ValueError("Error"))
try:
    future.result()
except ValueError as e:
    print(f"Got error: {e}")
```

### ConcurrentFuture

Wraps `concurrent.futures.Future`:

```python
from concurry.core.future import ConcurrentFuture, wrap_future
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# Threading - using wrap_future (recommended)
with ThreadPoolExecutor() as executor:
    future = wrap_future(executor.submit(some_task))
    result = future.result()

# Or create directly with keyword argument
with ThreadPoolExecutor() as executor:
    py_future = executor.submit(some_task)
    future = ConcurrentFuture(future=py_future)
    result = future.result()

# Multiprocessing
with ProcessPoolExecutor() as executor:
    future = wrap_future(executor.submit(cpu_intensive_task))
    result = future.result()
```

### AsyncioFuture

Wraps `asyncio.Future`:

```python
from concurry.core.future import AsyncioFuture, wrap_future
import asyncio

async def create_future():
    loop = asyncio.get_event_loop()
    async_future = loop.create_future()
    
    # Wrap it (recommended)
    unified = wrap_future(async_future)
    
    # Or create directly with keyword argument
    unified = AsyncioFuture(future=async_future)
    
    # Set result
    async_future.set_result(42)
    
    # Get result with timeout (unlike native asyncio.Future!)
    result = unified.result(timeout=5)
    return result
```

### RayFuture

Wraps Ray's `ObjectRef` (requires `concurry[ray]`):

```python
try:
    import ray
    from concurry.core.future import RayFuture, wrap_future
    
    ray.init()
    
    @ray.remote
    def remote_task(x):
        return x ** 2
    
    # Ray returns ObjectRef
    object_ref = remote_task.remote(42)
    
    # Wrap it in unified interface (recommended)
    future = wrap_future(object_ref)
    
    # Or create directly with keyword argument
    future = RayFuture(object_ref=object_ref)
    
    # Use consistent API
    result = future.result(timeout=10)
    print(f"Result: {result}")
    
    ray.shutdown()
    
except ImportError:
    print("Ray not installed")
```

## Advanced Patterns

### 1. Framework-Agnostic Functions

Write functions that work with any future type:

```python
from concurry.core.future import BaseFuture, wrap_future
from typing import Any

def wait_for_result(future: Any, default: Any = None) -> Any:
    """Wait for any future type with timeout and default value."""
    unified = wrap_future(future)
    
    try:
        return unified.result(timeout=5)
    except TimeoutError:
        print("Timeout - using default")
        return default
    except Exception as e:
        print(f"Error: {e}")
        return default
```

### 2. Batch Future Processing

Process multiple futures from different frameworks:

```python
from concurry.core.future import wrap_future
from concurrent.futures import ThreadPoolExecutor
from typing import List, Any

def process_mixed_futures(futures: List[Any]) -> List[Any]:
    """Process futures from any framework."""
    # Wrap all futures in unified interface
    unified_futures = [wrap_future(f) for f in futures]
    
    # Process with consistent API
    results = []
    for future in unified_futures:
        try:
            result = future.result(timeout=10)
            results.append(result)
        except Exception as e:
            print(f"Future failed: {e}")
            results.append(None)
    
    return results
```

### 3. Future Composition

Chain futures together:

```python
from concurry.core.future import wrap_future, BaseFuture
from concurrent.futures import ThreadPoolExecutor

def compose_futures(future: BaseFuture, transform_fn) -> BaseFuture:
    """Apply a transformation to a future's result."""
    def callback(f: BaseFuture):
        try:
            result = f.result()
            transformed = transform_fn(result)
            print(f"Transformed: {result} -> {transformed}")
        except Exception as e:
            print(f"Error: {e}")
    
    future.add_done_callback(callback)
    return future

# Usage
with ThreadPoolExecutor() as executor:
    future = wrap_future(executor.submit(lambda: 42))
    composed = compose_futures(future, lambda x: x * 2)
    # Callback will print: Transformed: 42 -> 84
```

### 4. Timeout Handling

Robust timeout handling across frameworks:

```python
from concurry.core.future import wrap_future
import time

def with_retry_on_timeout(future, max_retries=3):
    """Retry getting result on timeout."""
    unified = wrap_future(future)
    
    for attempt in range(max_retries):
        try:
            return unified.result(timeout=5)
        except TimeoutError:
            print(f"Timeout on attempt {attempt + 1}")
            if attempt == max_retries - 1:
                raise
            time.sleep(1)  # Wait before retry
```

### 5. Cancellation Patterns

Handle cancellation consistently:

```python
from concurry.core.future import wrap_future
from concurrent.futures import ThreadPoolExecutor
import time

def cancellable_task(items):
    """Process items with cancellation support."""
    results = []
    
    with ThreadPoolExecutor() as executor:
        futures = [wrap_future(executor.submit(process, item)) for item in items]
        
        try:
            for future in futures:
                # Check if we should cancel
                if should_stop():
                    # Cancel remaining futures
                    for f in futures:
                        f.cancel()
                    break
                
                result = future.result(timeout=5)
                results.append(result)
        except KeyboardInterrupt:
            # Cancel all on interrupt
            for f in futures:
                f.cancel()
            raise
    
    return results
```

## Error Handling

### Common Exceptions

All exceptions match those from [`concurrent.futures.Future`](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future):

```python
from concurry.core.future import wrap_future
from concurrent.futures import CancelledError

future = wrap_future(some_future)

# TimeoutError - result took too long
try:
    result = future.result(timeout=1)
except TimeoutError:
    print("Future didn't complete in time")
    future.cancel()

# CancelledError - future was cancelled
try:
    result = future.result()
except CancelledError:
    print("Future was cancelled")

# Exception from the task itself
try:
    result = future.result()
except ValueError as e:
    print(f"Task raised ValueError: {e}")
```

**Note**: `CancelledError` is from `concurrent.futures`, not `asyncio`. When wrapping asyncio futures, we automatically convert `asyncio.CancelledError` to `concurrent.futures.CancelledError` for API consistency.

### Safe Result Retrieval

```python
from concurry.core.future import wrap_future
from typing import Optional

def safe_result(future, timeout: float = 10) -> Optional[Any]:
    """Safely get result with error handling."""
    unified = wrap_future(future)
    
    if unified.cancelled():
        print("Future was cancelled")
        return None
    
    try:
        return unified.result(timeout=timeout)
    except TimeoutError:
        print("Timeout occurred")
        unified.cancel()
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None
```

## Performance Considerations

### 1. Wrapping Overhead

Wrapping adds minimal overhead:

```python
# If you know the exact type and need maximum performance
from concurrent.futures import Future

future: Future = executor.submit(task)
result = future.result()  # Slightly faster

# For framework-agnostic code (recommended)
unified = wrap_future(future)
result = unified.result()  # Minimal overhead, much more flexible
```

### 2. Already Wrapped Futures

`wrap_future()` is idempotent:

```python
from concurry.core.future import wrap_future

future1 = wrap_future(some_future)
future2 = wrap_future(future1)  # Returns future1, no double-wrapping!

assert future1 is future2  # True
```

## Testing with Futures

### Mock Futures for Testing

```python
from concurry.core.future import SyncFuture

def test_my_function():
    """Test function that accepts futures."""
    # Create mock future with result
    mock_future = SyncFuture(result_value=42)
    
    result = my_function(mock_future)
    
    assert result == expected_value

def test_error_handling():
    """Test error handling with mock future."""
    # Create mock future with exception
    mock_future = SyncFuture(exception_value=ValueError("Test error"))
    
    with pytest.raises(ValueError):
        my_function(mock_future)
```

## Best Practices

### 1. Always Use wrap_future() for External Futures

```python
# Good
future = wrap_future(executor.submit(task))

# Less ideal
future = executor.submit(task)  # Framework-specific
```

### 2. Set Appropriate Timeouts

```python
# Good - prevents hanging
result = future.result(timeout=30)

# Risky - could hang forever
result = future.result()
```

### 3. Handle Cancellation

```python
# Good - check cancellation
if not future.cancelled():
    result = future.result()

# Risky - might raise exception
result = future.result()
```

### 4. Use Type Hints

```python
from concurry.core.future import BaseFuture
from typing import Any

def process_future(future: BaseFuture) -> Any:
    """Process a unified future."""
    return future.result(timeout=10)
```

## Consistency Testing

Concurry includes comprehensive tests to verify that all future implementations behave identically. These tests cover:

- Result and exception retrieval with timeouts
- Cancellation behavior
- Callback invocation and parameter passing
- Exception type consistency (`CancelledError`, `TimeoutError`)
- Await support across all future types
- Edge cases like already-completed futures

You can run these tests yourself:

```bash
pytest tests/core/test_future_consistency.py -v
```

This rigorous testing ensures you can rely on consistent behavior regardless of which execution framework you use.

## Next Steps

- [Progress Guide](progress.md) - Learn about progress tracking
- [Examples](../examples.md) - See real-world usage patterns
- [API Reference](../api/futures.md) - Detailed API documentation

