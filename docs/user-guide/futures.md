# Unified Futures

Concurry's **Unified Future** is the "glue" that holds the system together. It abstracts away the differences between Python's various concurrency models, giving you a single, predictable object to work with.

## The Problem: Future Fragmentation

Python has too many types of "Futures":
1.  `concurrent.futures.Future` (Threading/Multiprocessing)
2.  `asyncio.Future` (AsyncIO)
3.  `ray.ObjectRef` (Ray)

They all do roughly the same thing (represent a result that isn't ready yet), but their APIs differ slightly. This makes writing agnostic code impossible.

## The Solution: `concurry.BaseFuture`

Concurry wraps all of these into a single interface that mimics the standard `concurrent.futures.Future` API.

### Comparison Table

| Feature | `concurry.BaseFuture` | `asyncio.Future` | `concurrent.futures` | `ray.ObjectRef` |
| :--- | :--- | :--- | :--- | :--- |
| **Get Result** | `.result(timeout=X)` | `await` or `.result()` (no timeout) | `.result(timeout=X)` | `ray.get(ref, timeout=X)` |
| **Check Done** | `.done()` | `.done()` | `.done()` | N/A (requires `ray.wait`) |
| **Cancel** | `.cancel()` | `.cancel()` | `.cancel()` | `ray.cancel(ref)` |
| **Callbacks** | `.add_done_callback()` | `.add_done_callback()` | `.add_done_callback()` | N/A (Complex) |
| **Async/Await** | ✅ Yes | ✅ Yes | ❌ No | ✅ Yes (awaitable in Ray) |

### The "Magic" of Automatic Unwrapping

One of Concurry's most powerful features is **Automatic Future Unwrapping**.

When you pass a Future from one Worker into another Worker, Concurry automatically handles the dependency.

```python
# Worker A returns a Future[int]
future_a = worker_a.calculate(10) 

# Worker B expects an int, but we pass it the Future!
# Concurry AUTOMATICALLY waits for future_a and passes the result to B.
future_b = worker_b.process(future_a)

print(future_b.result())
```

**Under the Hood:**
1.  **Client Side**: You pass `future_a` to `worker_b.process`.
2.  **Worker Proxy**: Detects that an argument is a `BaseFuture`.
3.  **Resolution**: Calls `.result()` on the future (efficiently waiting).
4.  **Execution**: Passes the *value* (e.g., `100`) to the actual backend worker.

**Benefit**: You can chain pipelines of workers `A -> B -> C` without writing any boilerplate glue code to wait for results.

## Safe Future Patterns

Working with futures can introduce race conditions or deadlocks if not handled carefully. Here are safe patterns.

### 1. Always Use Timeouts
Never call `.result()` without a timeout in production. It can hang your application indefinitely if a worker freezes.

```python
try:
    result = future.result(timeout=5.0)
        except TimeoutError:
    print("Task took too long!")
    future.cancel()  # Good practice to cancel if you stop waiting
```

### 2. Using `wait()` and `gather()`
Don't loop over futures manually. Use synchronization primitives.

```python
from concurry import gather, wait

# Safe: Blocks until all are done, or raises TimeoutError
results = gather(futures, timeout=10.0)

# Safe: Returns completed and pending sets
done, pending = wait(futures, timeout=5.0)
for f in pending:
                f.cancel()
```

### 3. Exception Handling
Exceptions in workers are captured and re-raised when you call `.result()`.

```python
future = worker.failing_method()

# The exception happens NOW, in your main thread
try:
    future.result()
except ValueError as e:
    print(f"Worker failed with: {e}")
```

## Ray Optimization

When running in `mode="ray"`, Concurry creates `RayFuture` objects wrapping `ObjectRef`.

*   **Zero-Copy Passing**: If you pass a `RayFuture` to another *Ray* worker, Concurry recognizes this. It passes the underlying `ObjectRef` directly. Ray handles the data transfer efficiently (zero-copy), avoiding bringing data back to the driver script unnecessarily.

This makes Concurry a highly efficient abstraction over Ray pipelines.
