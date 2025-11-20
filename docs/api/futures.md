# Futures API Reference

Complete API reference for Concurry's unified future interface.

## Overview

All future classes in Concurry are implemented as **frozen dataclasses** for optimal performance and type safety:

- **Performance**: Optimized initialization (< 2.5 µs for `SyncFuture`)
- **Immutability**: Frozen dataclasses prevent modification after creation
- **Type Safety**: Runtime validation in `__post_init__` ensures correct types
- **Thread Safety**: Fast UUID generation using `os.urandom(16).hex()`

All futures implement the complete `concurrent.futures.Future` API with identical behavior across all backends.

## Module: `concurry.core.future`

### Functions

#### wrap_future()

::: concurry.core.future.wrap_future

---

## Classes

### BaseFuture

::: concurry.core.future.BaseFuture
    options:
      show_source: true
      show_bases: true
      members:
        - result
        - cancel
        - cancelled
        - done
        - exception
        - add_done_callback
        - __await__

---

### SyncFuture

::: concurry.core.future.SyncFuture
    options:
      show_source: true
      show_bases: true

---

### ConcurrentFuture

::: concurry.core.future.ConcurrentFuture
    options:
      show_source: true
      show_bases: true

---

### AsyncioFuture

::: concurry.core.future.AsyncioFuture
    options:
      show_source: true
      show_bases: true

---

### RayFuture

::: concurry.core.future.RayFuture
    options:
      show_source: true
      show_bases: true

!!! note "Ray Required"
    This class is only available when Ray is installed: `pip install concurry[ray]`

---

## Usage Examples

### Basic Usage

```python
from concurry.core.future import wrap_future
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor() as executor:
    # Submit a task
    future = executor.submit(lambda x: x ** 2, 42)
    
    # Wrap in unified interface
    unified = wrap_future(future)
    
    # Use consistent API
    result = unified.result(timeout=5)
    print(f"Result: {result}")
```

### Async/Await Support

```python
from concurry.core.future import wrap_future
from concurrent.futures import ThreadPoolExecutor
import asyncio

async def async_example():
    with ThreadPoolExecutor() as executor:
        future = wrap_future(executor.submit(lambda: 42))
        
        # Use await syntax
        result = await future
        print(f"Result: {result}")

asyncio.run(async_example())
```

### Error Handling

```python
from concurry.core.future import wrap_future

future = wrap_future(some_future)

try:
    result = future.result(timeout=10)
except TimeoutError:
    print("Operation timed out")
    future.cancel()
except Exception as e:
    print(f"Task failed: {e}")
```

### Callbacks

```python
from concurry.core.future import wrap_future, BaseFuture

def on_complete(future: BaseFuture):
    try:
        result = future.result()
        print(f"Task completed: {result}")
    except Exception as e:
        print(f"Task failed: {e}")

future = wrap_future(some_future)
future.add_done_callback(on_complete)
```

## Type Signatures

### BaseFuture Methods

```python
from concurry.core.future import BaseFuture
from typing import Any, Callable, Optional

class BaseFuture:
    def result(self, timeout: Optional[float] = None) -> Any:
        """Get the result, blocking if necessary."""
        ...
    
    def cancel(self) -> bool:
        """Attempt to cancel the future."""
        ...
    
    def cancelled(self) -> bool:
        """Check if the future was cancelled."""
        ...
    
    def done(self) -> bool:
        """Check if the future is done."""
        ...
    
    def exception(self, timeout: Optional[float] = None) -> Optional[Exception]:
        """Get the exception raised, if any."""
        ...
    
    def add_done_callback(self, fn: Callable[[BaseFuture], None]) -> None:
        """Add a callback to be called when complete."""
        ...
    
    def __await__(self):
        """Make the future awaitable."""
        ...
```

### wrap_future Function

```python
from concurry.core.future import BaseFuture, wrap_future
from typing import Any

def wrap_future(future: Any) -> BaseFuture:
    """
    Wrap any future-like object in the unified interface.
    
    Args:
        future: A future-like object from any framework
    
    Returns:
        A BaseFuture instance providing the unified interface
    """
    ...
```

## See Also

- [Futures User Guide](../user-guide/futures.md) - Learn how to use futures
- [Quick Recipes](../user-guide/getting-started.md#quick-recipes) - See common usage patterns
- [Progress API](progress.md) - Progress bar API reference

