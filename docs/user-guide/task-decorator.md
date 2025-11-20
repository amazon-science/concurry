# The @task Decorator

**Add concurrency to any function with a single line of code.**

## The Problem: Boilerplate

Sometimes you don't want to define a `class MyWorker(Worker)`. You just have a function, and you want it to run in the background, or on a separate process, or on a Ray cluster.

Writing a full worker class for a single function feels like overkill.

## The Solution: `@task`

The `@task` decorator transforms a regular function into a fully-managed `TaskWorker`.

```python
from concurry import task

# 1. Decorate your function
@task(mode="thread", max_workers=4)
def heavy_computation(x):
    return x ** 2

# 2. Call it! (Returns a Future)
future = heavy_computation(10)
print(future.result())  # 100

# 3. Map over it
results = list(heavy_computation.map(range(10)))
```

### The "Magic": What actually happens?

When you decorate a function, Concurry replaces it with a `TaskWorker` instance.
*   **It's not a function anymore**: It's an object with methods like `.submit()`, `.map()`, and `.stop()`.
*   **It manages a pool**: The `max_workers=4` argument created a thread pool behind the scenes.

---

## Decision Matrix: @task vs Worker Class

| Feature | `@task` Decorator | `@worker` Class Decorator |
| :--- | :--- | :--- |
| **Use Case** | Simple, stateless functions. | Complex, stateful actors. |
| **State** | No shared state (pure functions). | Can hold state (`self.db_conn`). |
| **Setup** | 1 line. | Class definition + initialization. |
| **Best For** | Scripts, data pipelines, one-offs. | Services, resource managers. |

---

## Advanced Features

### 1. Auto-Injection of Limits
If your function accepts a `limits` argument, Concurry automatically passes the worker's limit set to it.

```python
from concurry import RateLimit

@task(
    mode="thread",
    limits=[RateLimit(key="api", capacity=5)]
)
def fetch(url, limits):
    # The limits object is injected!
    with limits.acquire(requested={"api": 1}):
        return requests.get(url)
```

### 2. On-Demand vs Persistent
By default, `@task` creates a persistent pool. But you can make it ephemeral:

```python
# Creates a NEW thread for every call, then destroys it.
# Good for infrequent tasks.
@task(mode="thread", on_demand=True)
def occasional_job(x):
    ...
```

### 3. Context Manager Cleanup
Since the decorated function is a worker, you should clean it up.

```python
with heavy_computation:
    future = heavy_computation(10)
    # Worker automatically stops at end of block
```

## Caveats

1.  **Lifecycle**: You must call `.stop()` (or use `with`) to clean up resources, especially for Process/Ray modes.
2.  **Serialization**: Arguments and return values must be pickleable (for Process/Ray).
3.  **Type Hints**: Static analysis tools (mypy) might get confused because the type changes from `Callable` to `TaskWorker`.

## See Also
*   [**TaskWorker**](workers.md#taskworker-for-simple-functions) for the underlying implementation.
