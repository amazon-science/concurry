# TaskWorker Architecture

## Overview

The TaskWorker system provides a high-level, `concurrent.futures.Executor`-like interface for executing arbitrary tasks across different execution modes. It consists of three main components:

1. **`TaskWorker`**: A concrete `Worker` subclass that holds optional bound functions
2. **`@task` decorator**: A convenience wrapper that creates bound TaskWorkers for functions
3. **`TaskWorkerMixin` / `TaskWorkerPoolMixin`**: Mixins that add `submit()`, `map()`, and `__call__()` methods

This architecture enables both manual worker management and declarative function-level parallelization.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                       User Code                              │
└───────────────┬─────────────────────────────────────────────┘
                │
                ├─────────────────────────────────────────────┐
                │                                              │
                ▼                                              ▼
    ┌──────────────────────┐                    ┌──────────────────────┐
    │   @task decorator    │                    │   TaskWorker.init()  │
    │  task_decorator.py   │                    │   (Manual usage)     │
    └──────────┬───────────┘                    └──────────┬───────────┘
               │                                           │
               │ Creates and configures                    │
               │                                           │
               ▼                                           ▼
    ┌─────────────────────────────────────────────────────────┐
    │            TaskWorker(Worker)                            │
    │  - __init__(fn: Optional[Callable])                     │
    │  - Stores _bound_fn                                     │
    │  - No custom methods (uses mixins via proxy)            │
    └───────────────────────┬─────────────────────────────────┘
                            │
                            │ WorkerBuilder._create_worker_wrapper()
                            │
                            ▼
           ┌────────────────────────────────────┐
           │  Dynamically Created Proxy Classes  │
           │                                     │
           │  For Single Workers:                │
           │  ┌─────────────────────────────┐   │
           │  │ TaskSyncWorkerProxy         │   │
           │  │ = TaskWorkerMixin +         │   │
           │  │   SyncWorkerProxy           │   │
           │  └─────────────────────────────┘   │
           │                                     │
           │  For Pools (max_workers > 1):       │
           │  ┌─────────────────────────────┐   │
           │  │ TaskThreadWorkerProxyPool   │   │
           │  │ = TaskWorkerPoolMixin +     │   │
           │  │   ThreadWorkerProxyPool     │   │
           │  └─────────────────────────────┘   │
           └─────────────────┬───────────────────┘
                             │
                             │ User interacts via:
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
        __call__()      submit()        map()
     (calls submit)  (executes fn)  (batch submit)
```

## Core Components

### 1. TaskWorker Class

`TaskWorker` is a minimal `Worker` subclass that:
- Accepts an optional `fn` parameter during initialization
- Stores it as `_bound_fn` for later use
- Does NOT define `submit()` or `map()` methods directly (added via mixins)

```python
class TaskWorker(Worker):
    def __init__(self, fn: Optional[Callable] = None):
        super().__init__()
        self._bound_fn = fn
```

**Key Design Decision**: The worker itself is stateless except for the bound function. All execution logic comes from the mixins applied to proxy classes.

### 2. TaskWorkerMixin (Single Worker Proxy)

Applied to individual worker proxies (Sync, Thread, Process, Asyncio, Ray) when `TaskWorker` is used:

```python
class TaskWorkerMixin:
    def __call__(self, *args, **kwargs):
        """Direct invocation → delegates to submit()"""
        return self.submit(*args, **kwargs)
    
    def submit(self, fn = _NO_ARG, *args, **kwargs):
        """Execute function with bound function logic"""
        # 1. Check for bound function
        # 2. Resolve fn parameter (function vs data)
        # 3. Call self._execute_task(fn, *args, **kwargs)
    
    def map(self, fn = _NO_ARG, *iterables, ...):
        """Batch execution with optional progress bar"""
        # 1. Resolve bound function
        # 2. Submit tasks via self._execute_task()
        # 3. Return iterator with optional ProgressBar
```

**Bound Function Resolution Logic**:

The `submit()` and `map()` methods implement sophisticated argument resolution:

1. **If bound function exists** (from `.init(fn=...)`):
   - If `fn` is explicitly passed AND callable → use as override
   - If `fn` is explicitly passed but NOT callable → treat as first data argument
   - If `fn` is not passed (`_NO_ARG`) → use bound function

2. **If no bound function**:
   - `fn` must be provided and callable
   - Raises `TypeError` if missing or not callable

**Example**:
```python
# Bound function case
worker = TaskWorker.options(mode="thread").init(fn=lambda x: x * 2)
worker(5).result()           # Uses bound function, 5 is data → 10
worker.submit(10).result()   # Uses bound function, 10 is data → 20

# Explicit override (advanced)
worker.submit(lambda x: x ** 2, 5).result()  # Override bound function → 25

# No bound function case
worker2 = TaskWorker.options(mode="thread").init()
worker2.submit(lambda x: x * 2, 5).result()  # First arg is function → 10
worker2(5).result()  # ERROR: no function provided
```

### 3. TaskWorkerPoolMixin (Worker Pool Proxy)

Applied to worker pool proxies when `TaskWorker` is used with `max_workers > 1`:

```python
class TaskWorkerPoolMixin:
    def __call__(self, *args, **kwargs):
        """Direct invocation → delegates to submit()"""
        return self.submit(*args, **kwargs)
    
    def submit(self, fn = _NO_ARG, *args, **kwargs):
        """Dispatch to worker in pool with bound function logic"""
        # 1. Check init_kwargs for bound function
        # 2. Resolve fn parameter
        # 3. Dispatch via pool's __getattr__("submit")
    
    def map(self, fn = _NO_ARG, *iterables, ...):
        """Dispatch batch execution to pool"""
        # 1. Resolve bound function
        # 2. Dispatch via pool's __getattr__("map")
```

**Key Difference from Mixin**: Pools access the bound function via `self.init_kwargs['fn']` instead of `self._worker._bound_fn`, since pools don't have direct access to the worker instance.

### 4. @task Decorator

The `@task` decorator is a transformation tool that **replaces** the defined function with a fully initialized `TaskWorker` instance.

**Conceptual Transformation**:
```python
# User writes:
@task(mode="thread", max_workers=4)
def my_func(x):
    return x + 1

# Decorator executes (conceptually):
_original_func = my_func
my_func = TaskWorker.options(mode="thread", max_workers=4).init(fn=_original_func)
```

**Implications**:
1. `my_func` is now a `TaskWorker` object, not a function.
2. `my_func(10)` calls `TaskWorker.__call__(10)`, which calls `TaskWorker.submit(10)`.
3. Configuration parameters like `mode` must be passed to `@task` because they are needed to *construct* the worker immediately.
4. The worker must be stopped using `my_func.stop()`.

```python
@validate
def task(
    *,
    mode: ExecutionMode,
    on_demand: Union[bool, _NO_ARG_TYPE] = _NO_ARG,
    **kwargs: Any,
) -> Callable:
    """Decorator that creates a bound TaskWorker."""
    # 1. Apply on_demand default from global_config
    # 2. Create a decorator function
    # 3. Check if function accepts 'limits' parameter
    # 4. Create TaskWorker with WorkerBuilder
    # 5. Return worker (which is callable via __call__)
```

**Decorator Workflow**:

1. **Configuration Resolution**:
   - If `on_demand` not specified, read from `global_config.defaults.task_decorator_on_demand`
   - Force `on_demand=False` for Sync and Asyncio modes (not supported)

2. **Limits Forwarding** (if applicable):
   - Use `morphic.get_fn_args()` to inspect function signature
   - If function has a `limits` parameter AND `limits` are provided in `@task`, inject `worker.limits`
   - Otherwise, skip injection

3. **Worker Creation**:
   ```python
   builder = TaskWorker.options(mode=mode, on_demand=on_demand, limits=limits, **kwargs)
   worker = builder.init(fn=decorated_function)
   ```

4. **Metadata Preservation**:
   - Copy `__name__`, `__doc__`, `__module__`, `__qualname__` from original function

5. **Lifecycle Management**:
   - Add `__del__` method for automatic cleanup
   - Expose `.stop()` for manual cleanup

**Example Usage**:
```python
from concurry import task, RateLimit

@task(mode="thread", max_workers=4, limits=[RateLimit(...)])
def process_item(x, limits):
    with limits.acquire(...):
        return expensive_operation(x)

# Worker is created and initialized
# Function is bound to worker
# limits parameter is automatically injected
result = process_item(10).result()
process_item.stop()
```

## Dynamic Class Creation

### Worker Proxy Creation

When `TaskWorker` is initialized, `WorkerBuilder._create_worker_wrapper()` dynamically creates proxy classes with mixins:

```python
# In WorkerBuilder._create_worker_wrapper()
if self.worker_cls is TaskWorker or issubclass(self.worker_cls, TaskWorker):
    # Create dynamic class combining mixin with base proxy
    proxy_cls = type(
        f"Task{proxy_cls.__name__}",
        (TaskWorkerMixin, proxy_cls),
        {},
    )
```

This creates classes like:
- `TaskSyncWorkerProxy = TaskWorkerMixin + SyncWorkerProxy`
- `TaskThreadWorkerProxy = TaskWorkerMixin + ThreadWorkerProxy`
- `TaskProcessWorkerProxy = TaskWorkerMixin + ProcessWorkerProxy`

**Method Resolution Order (MRO)**:
1. `TaskWorkerMixin` methods (`submit`, `map`, `__call__`) take precedence
2. Base proxy methods (`_execute_task`, `stop`, etc.) from `SyncWorkerProxy`
3. Common proxy interface from `WorkerProxy`

### Pool Proxy Creation

When `max_workers > 1`, `WorkerBuilder._create_pool()` dynamically applies the pool mixin:

```python
# In WorkerBuilder._create_pool()
if self.worker_cls is TaskWorker or issubclass(self.worker_cls, TaskWorker):
    pool_cls = type(
        f"Task{pool_cls.__name__}",
        (TaskWorkerPoolMixin, pool_cls),
        {},
    )
```

This creates classes like:
- `TaskThreadWorkerProxyPool = TaskWorkerPoolMixin + ThreadWorkerProxyPool`
- `TaskProcessWorkerProxyPool = TaskWorkerPoolMixin + ProcessWorkerProxyPool`
- `TaskRayWorkerProxyPool = TaskWorkerPoolMixin + RayWorkerProxyPool`

## Bound Function Access Patterns

Different proxy types access the bound function differently:

| Proxy Type | Access Pattern | Why? |
|------------|----------------|------|
| `SyncWorkerProxy` | `self._worker._bound_fn` | Direct access to worker instance |
| `ThreadWorkerProxy` | `self.init_kwargs['fn']` | Worker lives in thread, not directly accessible |
| `ProcessWorkerProxy` | `self.init_kwargs['fn']` | Worker lives in subprocess |
| `AsyncioWorkerProxy` | `self.init_kwargs['fn']` | Worker lives in event loop |
| `RayWorkerProxy` | `self.init_kwargs['fn']` | Worker is a Ray actor |
| All Pool Proxies | `self.init_kwargs['fn']` | Pools don't have direct worker access |

**Code Pattern**:
```python
# In TaskWorkerMixin.submit()
bound_fn = None
if hasattr(self, "_worker") and hasattr(self._worker, "_bound_fn"):
    bound_fn = self._worker._bound_fn  # Sync mode
elif hasattr(self, "init_kwargs") and "fn" in self.init_kwargs:
    bound_fn = self.init_kwargs["fn"]  # All others
```

## The `_NO_ARG` Sentinel

The `_NO_ARG` sentinel (defined in `concurry.utils`) is critical for distinguishing "no argument provided" from "`None` provided":

```python
from concurry.utils import _NO_ARG

def submit(self, fn = _NO_ARG, *args, **kwargs):
    if fn is _NO_ARG:
        # User didn't pass fn → use bound function
        fn = self._bound_fn
    elif fn is None:
        # User explicitly passed None → treat as data
        args = (None,) + args
        fn = self._bound_fn
```

**Why Not `fn=None`?**

Using `fn=None` as default would create ambiguity:

```python
# With fn=None default:
def process(x):
    return x is None

worker = TaskWorker.options(...).init(fn=process)
worker(None)  # Ambiguous: is None the function or data?

# With fn=_NO_ARG:
worker(None)  # Clear: None is data (fn not provided)
worker()  # Clear: use bound function (fn not provided)
```

**User-facing API**: Users don't interact with `_NO_ARG` directly; it's an internal implementation detail.

## ProgressBar Integration

The `map()` method supports three progress bar configurations:

1. **`progress=False`** (default): No progress bar
2. **`progress=True`**: Default ProgressBar with total from iterable length
3. **`progress={...}`**: Custom ProgressBar configuration (passed as kwargs)
4. **`progress=ProgressBar(...)`**: User-provided ProgressBar instance

**Implementation**:
```python
def map(self, fn, *iterables, progress=False, ...):
    # ... function resolution logic ...
    
    pbar = None
    if progress:
        total = len(iterables_list[0])
        
        if isinstance(progress, ProgressBar):
            pbar = progress
        elif isinstance(progress, dict):
            pbar = ProgressBar(total=total, **progress)
        else:
            pbar = ProgressBar(total=total)
    
    # Submit tasks
    futures = [self._execute_task(fn, *args) for args in zip(*iterables)]
    
    # Yield results with progress updates
    for future in futures:
        result = future.result(timeout=timeout)
        if pbar:
            pbar.update(1)
        yield result
    
    if pbar:
        pbar.success("Complete!")
```

## Configuration System Integration

### Global Config Defaults

The `@task` decorator respects `global_config.defaults.task_decorator_on_demand`:

```python
# In task_decorator.py
from concurry.config import global_config

local_config = global_config.clone()  # Thread-safe
if on_demand is _NO_ARG:
    if mode in (ExecutionMode.Sync, ExecutionMode.Asyncio):
        on_demand = False
    else:
        on_demand = local_config.defaults.task_decorator_on_demand
```

**Configuration Hierarchy**:
1. Explicit `on_demand` argument → highest priority
2. Mode restrictions (Sync/Asyncio) → override default
3. `global_config.defaults.task_decorator_on_demand` → fallback default

### Mode-Specific Handling

Sync and Asyncio modes don't support `on_demand=True`:

```python
if mode in (ExecutionMode.Sync, ExecutionMode.Asyncio):
    on_demand = False
```

This prevents `ValueError` from being raised when workers are created.

## Execution Flow Examples

### Example 1: Decorated Function with Bound Function

```python
@task(mode="thread", max_workers=4)
def process(x):
    return x ** 2

# What happens when calling process(5)?
# 1. process.__call__(5) is invoked
# 2. __call__ delegates to submit(5)
# 3. submit() checks for bound function:
#    - bound_fn = init_kwargs['fn'] = <function process>
#    - fn = 5 (first argument)
#    - fn is not _NO_ARG → true
#    - callable(fn) → false (it's an int)
#    - Therefore: args = (5,) and fn = bound_fn
# 4. self._execute_task(process, 5) is called
# 5. Returns Future
# 6. User calls .result() → 25
```

### Example 2: Manual TaskWorker with No Bound Function

```python
worker = TaskWorker.options(mode="thread").init()

# What happens when calling worker.submit(lambda x: x * 2, 10)?
# 1. submit(fn=<lambda>, 10) is invoked
# 2. Check for bound function:
#    - bound_fn = None (no function passed to .init())
# 3. fn is not _NO_ARG → true
# 4. callable(fn) → true (it's a lambda)
# 5. Therefore: use fn as-is
# 6. self._execute_task(<lambda>, 10) is called
# 7. Returns Future
# 8. User calls .result() → 20
```

### Example 3: Pool with map()

```python
@task(mode="thread", max_workers=4)
def square(x):
    return x ** 2

# What happens when calling list(square.map(range(10), progress=True))?
# 1. square is a TaskThreadWorkerProxyPool (pool created due to max_workers=4)
# 2. TaskWorkerPoolMixin.map(range(10), progress=True) is invoked
# 3. fn = range(10) (first argument)
# 4. Check for bound function:
#    - bound_fn = init_kwargs['fn'] = <function square>
# 5. fn is not _NO_ARG → true
# 6. callable(fn) → false (range object is not callable)
# 7. Therefore: iterables = (range(10),) and fn = bound_fn
# 8. Initialize ProgressBar(total=10)
# 9. Loop: for i in range(10): pool.submit(square, i)
# 10. Dispatch each submit to pool's __getattr__("submit")
# 11. Pool dispatches to individual workers using load balancing
# 12. Return iterator that yields results and updates progress
```

## Comparison with concurrent.futures

TaskWorker provides an interface similar to `concurrent.futures.Executor`:

| Feature | concurrent.futures | TaskWorker | Notes |
|---------|-------------------|------------|-------|
| `submit(fn, *args)` | ✅ | ✅ | Identical interface |
| `map(fn, *iterables)` | ✅ | ✅ | TaskWorker adds progress bar support |
| `shutdown()` | ✅ | `.stop()` | Different name, same concept |
| Context manager | ✅ | ✅ | Both support `with` statement |
| Bound functions | ❌ | ✅ | TaskWorker-specific feature |
| Direct invocation (`__call__`) | ❌ | ✅ | TaskWorker-specific (when bound) |
| Multiple execution modes | ❌ | ✅ | concurrent.futures only has Thread/Process |
| On-demand workers | ❌ | ✅ | TaskWorker-specific |
| Rate limiting | ❌ | ✅ | TaskWorker integrates with Limits |

## Testing Considerations

### Test Coverage

All TaskWorker tests use pytest fixtures from `conftest.py`:
- `worker_mode`: Parametrizes tests across all execution modes (sync, thread, process, asyncio, ray)
- `pool_mode`: Parametrizes tests across pool-supporting modes (thread, process, ray)

**Example**:
```python
def test_bound_function_submit(self, worker_mode):
    """Test bound function with submit() across all modes."""
    worker = TaskWorker.options(mode=worker_mode).init(fn=lambda x: x * 2)
    result = worker.submit(5).result()
    assert result == 10
    worker.stop()
```

This test runs 5 times (once per mode), ensuring consistent behavior.

### Edge Cases Tested

1. **Bound vs unbound functions**: Both with and without `.init(fn=...)`
2. **`None` as argument**: Ensures `_NO_ARG` sentinel works correctly
3. **Empty iterables**: `map()` with empty lists
4. **Multiple iterables**: `map()` with 2+ iterables (zip behavior)
5. **Progress bar variations**: `True`, `dict`, `ProgressBar` instance
6. **Exception handling**: Tasks that raise exceptions
7. **Timeout behavior**: Tasks that time out
8. **Pool behavior**: Round-robin, least-active load balancing
9. **Async functions**: Asyncio mode support

## Performance Considerations

### On-Demand Workers

**Pros**:
- No idle worker overhead
- Scales automatically with load
- Good for bursty workloads

**Cons**:
- Worker startup cost per task
- No connection pooling benefits
- May hit system resource limits with many concurrent tasks

**Recommendation**: Use `on_demand=True` for:
- Infrequent tasks
- Tasks with long runtime (startup cost is negligible)
- Tasks requiring fresh state

### Bound Functions

Binding a function to a worker has minimal overhead:
- Function reference stored in `_bound_fn` (no serialization)
- Argument resolution adds 1-2 conditional checks per call
- No performance difference compared to passing function explicitly

### Progress Bars

Progress bars have minimal overhead:
- Only initialized if `progress=True` or dict/ProgressBar provided
- Updates are simple counter increments
- Most cost comes from terminal I/O (rendering)

**Recommendation**: Always use progress bars for long-running batch operations (100+ items).

## Future Enhancements

### Potential Features

1. **Async Context Manager Support**:
   ```python
   async with task_worker:
       results = await gather([task_worker.submit(i) for i in range(10)])
   ```

2. **Dynamic Worker Pool Scaling**:
   - Auto-scale workers based on queue depth
   - Shrink pool when idle

3. **Task Priorities**:
   ```python
   @task(mode="thread", max_workers=4)
   def process(x):
       return x ** 2
   
   process.submit(10, priority=5)  # Higher priority
   ```

4. **Distributed Task Coordination**:
   - TaskWorker as Ray actor
   - Automatic result caching
   - Cross-machine task stealing

### Known Limitations

1. **Class Methods**: `@task` doesn't work on class methods (no `self` binding)
2. **Pickling**: Functions must be picklable for process/ray modes
3. **State Isolation**: Each worker in a pool has independent state
4. **Sync Map Semantics**: Sync mode map() doesn't parallelize (executes sequentially)

## See Also

- [Workers Architecture](workers.md) - General worker system design
- [Configuration System](configuration.md) - Global configuration management
- [Limits Architecture](limits.md) - Rate limiting and resource management
- [Progress Bar](../user-guide/progress.md) - Progress tracking features



