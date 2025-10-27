# Synchronization Architecture

This document describes the design, implementation, and maintenance guidelines for Concurry's synchronization primitives (`wait()` and `gather()`).

## Table of Contents

1. [Overview](#overview)
2. [Architecture Layers](#architecture-layers)
3. [BaseFuture Hierarchy](#basefuture-hierarchy)
4. [Polling Strategies](#polling-strategies)
5. [Wait Function](#wait-function)
6. [Gather Function](#gather-function)
7. [Async Wait and Gather Functions](#async-wait-and-gather-functions)
8. [Key Design Decisions](#key-design-decisions)
9. [Performance Considerations](#performance-considerations)
10. [Extension Points](#extension-points)
11. [Common Pitfalls](#common-pitfalls)
12. [Testing Requirements](#testing-requirements)

---

## Overview

The synchronization system provides two main primitives that work across all execution modes (sync, asyncio, thread, process, ray):

- **`wait()`**: Block until specified completion conditions are met
- **`gather()`**: Collect results from multiple futures in order or as they complete

### Design Goals

1. **Cross-Framework Compatibility**: Single API works with all future types
2. **Performance**: Efficient polling, minimal overhead, scalable to thousands of futures
3. **Flexibility**: Support various input patterns (list, dict, individual futures)
4. **Observability**: Progress tracking via progress bars or callbacks
5. **Configurability**: Adaptive polling strategies for different workload patterns

---

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                      User-Facing API                             │
│                   wait() and gather()                            │
│    - Parameter validation                                        │
│    - Input structure handling (list/dict/tuple/set/single)      │
│    - Progress tracking setup                                     │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                    Backend Functions                             │
│   _gather_blocking_backend() / _gather_iter_backend()           │
│    - Wraps all futures with wrap_future()                       │
│    - Delegates to wait() for completion checking                │
│    - Collects results in order or as they complete              │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                    Core Wait Logic                               │
│                    wait() function                               │
│    - Batch completion checking via _check_futures_batch()       │
│    - Polling strategy integration                               │
│    - Return condition evaluation                                │
│    - Timeout management                                         │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                  Efficiency Layer                                │
│              _check_futures_batch()                              │
│    - Ray-specific: Single ray.wait() for batch checking         │
│    - Non-Ray: Individual .done() checks                         │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                  Unified Future Interface                        │
│                    BaseFuture + wrap_future()                    │
│    - SyncFuture (immediate results)                             │
│    - ConcurrentFuture (thread/process futures)                  │
│    - AsyncioFuture (asyncio futures)                            │
│    - RayFuture (Ray ObjectRefs)                                 │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                Underlying Framework Futures                      │
│  concurrent.futures.Future │ asyncio.Future │ ray.ObjectRef     │
└─────────────────────────────────────────────────────────────────┘
```

---

## BaseFuture Hierarchy

### Design Philosophy

`BaseFuture` provides a **unified abstraction** over different future implementations using the **Adapter Pattern**. All futures expose the same API regardless of the underlying framework.

### Class Structure

```
BaseFuture (ABC)
├── SyncFuture          # Immediate results (already computed)
├── ConcurrentFuture    # Thread/Process futures (delegates to concurrent.futures.Future)
├── AsyncioFuture       # Asyncio futures (adds timeout support)
└── RayFuture           # Ray ObjectRefs (adds state tracking + callbacks)
```

### Critical Design Rules

#### Rule 1: State Management Strategy

Each future type has a **different state management strategy** based on its characteristics:

**SyncFuture**:
- **Always done at creation** (`_done=True`)
- Result/exception **immutable** after construction
- **No lock needed** (single-threaded usage)
- **Caches result/exception** in `_result` and `_exception` slots

**ConcurrentFuture**:
- **Pure delegation wrapper** (no state caching)
- Does **NOT cache** `_result`, `_exception`, `_done`, or `_cancelled`
- All state queries **delegate directly** to underlying `concurrent.futures.Future`
- Underlying future is **inherently thread-safe**
- **Only stores**: `uuid`, `_future`, `_callbacks`, `_lock`

**AsyncioFuture**:
- **Pure delegation wrapper** (no state caching)
- Does **NOT cache** `_result`, `_exception`, or `_done`
- All state queries **delegate directly** to underlying `asyncio.Future`
- Uses `_lock` for thread-safe access to asyncio future
- Implements **timeout support** via polling (asyncio futures don't natively support timeouts)
- **Only stores**: `uuid`, `_future`, `_callbacks`, `_lock`, `_poll_interval`

**RayFuture**:
- **Caches state** after fetching from Ray
- **Stores**: `uuid`, `_object_ref`, `_result`, `_exception`, `_done`, `_cancelled`, `_callbacks`, `_lock`
- Uses global monitor thread for automatic callback invocation
- **Critical Bug Pattern**: Must NOT set `_done=True` in `done()` method without fetching result
  - ❌ **WRONG**: `done()` sets `_done=True` without calling `ray.get()`
  - ✅ **CORRECT**: `done()` only calls `ray.wait()` to check, doesn't set `_done`
  - Reason: If `_done=True` but `_result` is still `None`, `result(timeout=0)` will return `None` instead of fetching

#### Rule 2: The Delegation vs. Caching Decision

**When to cache state**:
- Future's underlying framework doesn't provide efficient state querying (Ray)
- State queries are expensive (network calls)
- Results need to be accessed multiple times

**When to delegate**:
- Underlying future is already thread-safe and efficient (concurrent.futures, asyncio)
- Framework manages state better than we can
- Avoids state synchronization bugs

**ConcurrentFuture and AsyncioFuture are pure wrappers** because:
1. `concurrent.futures.Future` and `asyncio.Future` already manage state efficiently
2. Caching would duplicate state and risk inconsistency
3. Delegation is zero-overhead

**RayFuture must cache** because:
1. `ray.get()` is an expensive network call
2. ObjectRef doesn't cache results itself
3. Callbacks require local state tracking

#### Rule 3: Thread Safety Requirements

All futures must be **thread-safe** because synchronization primitives are multi-threaded:
- Multiple threads may call `wait()` or `gather()` simultaneously
- Worker proxy pools dispatch to multiple workers from multiple threads
- Progress callbacks may be invoked from different threads

**Implementation patterns**:
- `SyncFuture`: No lock (immutable after creation)
- `ConcurrentFuture`: Delegates to inherently thread-safe `concurrent.futures.Future`
- `AsyncioFuture`: Uses `threading.Lock()` to protect asyncio future access
- `RayFuture`: Uses `threading.Lock()` to protect cached state

#### Rule 4: Exception Consistency

**All futures must raise the same exception types**:
- `concurrent.futures.CancelledError` (not `asyncio.CancelledError`)
- `TimeoutError` (not `ray.exceptions.GetTimeoutError`)
- Original exception from computation (unwrapped)

**AsyncioFuture** must convert:
```python
# ✅ Correct pattern
try:
    return self._future.result()
except asyncio.CancelledError:
    raise CancelledError("Future was cancelled") from None
```

**RayFuture** must convert:
```python
# ✅ Correct pattern
except Exception as e:
    if e.__class__.__name__ == "GetTimeoutError":
        raise TimeoutError("Future did not complete within timeout") from e
```

#### Rule 5: Callback Invocation Rules

**All futures must pass the wrapper (BaseFuture) to callbacks, not the underlying future**:

```python
# ✅ Correct - ConcurrentFuture
def add_done_callback(self, fn: Callable) -> None:
    self._future.add_done_callback(lambda _: fn(self))  # Pass 'self', not '_'

# ✅ Correct - AsyncioFuture
def add_done_callback(self, fn: Callable) -> None:
    def wrapped_callback(fut):
        fn(self)  # Pass wrapper, not 'fut'
    self._future.add_done_callback(wrapped_callback)
```

**Callbacks must be invoked exactly once** when the future completes.

#### Rule 6: The `wrap_future()` Function

**Purpose**: Automatically convert any future-like object into a `BaseFuture`.

**Detection order** (critical - order matters):
```python
def wrap_future(future: Any) -> BaseFuture:
    if isinstance(future, BaseFuture):
        return future  # Already wrapped, idempotent
    elif isinstance(future, concurrent.futures.Future):
        return ConcurrentFuture(future=future)
    elif asyncio.isfuture(future):
        return AsyncioFuture(future=future)
    elif _IS_RAY_INSTALLED and isinstance(future, ray.ObjectRef):
        return RayFuture(object_ref=future)
    else:
        # Fallback: wrap as immediate result
        return SyncFuture(result_value=future)
```

**Why this order**:
1. Check `BaseFuture` first for idempotency
2. Check `concurrent.futures.Future` before asyncio (some futures might satisfy both checks)
3. Check Ray only if installed (avoid import errors)
4. Fallback to `SyncFuture` for non-future values (e.g., `gather([1, 2, future1])`)

### Memory Optimization

**__slots__ usage is critical** for performance when dealing with thousands of futures:

```python
# Each future type defines minimal slots
class ConcurrentFuture(BaseFuture):
    __slots__ = ("uuid", "_future", "_callbacks", "_lock")
    # Saves 32 bytes per instance vs. caching _result, _exception, _done, _cancelled

class AsyncioFuture(BaseFuture):
    __slots__ = ("uuid", "_future", "_callbacks", "_lock", "_poll_interval")
    # Saves 24 bytes per instance

class RayFuture(BaseFuture):
    __slots__ = ("uuid", "_object_ref", "_result", "_exception", 
                 "_done", "_cancelled", "_callbacks", "_lock")
    # Must cache all state
```

**Impact**: With 10,000 futures, removing unused slots saves 320KB (ConcurrentFuture) or 240KB (AsyncioFuture).

---

## Polling Strategies

### Purpose

Polling strategies control **how frequently** we check future completion. Trade-off:
- **Fast polling**: Low latency, high CPU usage
- **Slow polling**: High latency, low CPU usage
- **Adaptive**: Balance based on workload

### Architecture

All strategies inherit from `BasePollingStrategy` (Registry + MutableTyped):

```python
class BasePollingStrategy(Registry, MutableTyped, ABC):
    @abstractmethod
    def get_next_interval(self) -> float:
        """Return next sleep interval in seconds."""
        pass
    
    @abstractmethod
    def record_completion(self) -> None:
        """Called when futures complete."""
        pass
    
    @abstractmethod
    def record_no_completion(self) -> None:
        """Called when no futures complete."""
        pass
    
    @abstractmethod
    def reset(self) -> None:
        """Reset to initial state."""
        pass
```

### Strategy Implementations

#### FixedPollingStrategy

**Behavior**: Constant interval, no adaptation.

**Best for**: Predictable workloads, testing, benchmarking.

**Parameters** (from `global_config.defaults`):
- `interval`: `polling_fixed_interval` (default: 0.01 seconds = 10ms)

**Algorithm**:
```
Always return: interval
```

#### AdaptivePollingStrategy (Recommended Default)

**Behavior**: Speeds up on completions, slows down when idle.

**Best for**: Variable workloads, unknown completion patterns, general use.

**Parameters** (from `global_config.defaults`):
- `min_interval`: `polling_adaptive_min_interval` (default: 0.001s = 1ms)
- `max_interval`: `polling_adaptive_max_interval` (default: 0.1s = 100ms)
- `current_interval`: `polling_adaptive_initial_interval` (default: 0.01s = 10ms)
- `speedup_factor`: 0.7 (speed up by 30% on completion)
- `slowdown_factor`: 1.3 (slow down by 30% on idle)

**Algorithm**:
```python
def record_completion():
    current_interval = max(min_interval, current_interval * 0.7)  # Speed up
    consecutive_empty = 0

def record_no_completion():
    consecutive_empty += 1
    if consecutive_empty >= 3:  # After 3 empty checks
        current_interval = min(max_interval, current_interval * 1.3)  # Slow down
```

**Why this works**:
- Starts at moderate speed (10ms)
- Speeds up when futures are completing (1ms minimum)
- Slows down after consecutive empty checks (100ms maximum)
- Prevents thrashing by requiring 3 empty checks before slowdown

#### ExponentialPollingStrategy

**Behavior**: Exponential backoff on idle, reset on completion.

**Best for**: Long-running operations, sporadic completions.

**Parameters** (from `global_config.defaults`):
- `initial_interval`: `polling_exponential_initial_interval` (default: 0.001s = 1ms)
- `max_interval`: `polling_exponential_max_interval` (default: 1.0s)
- `multiplier`: 2.0 (double each empty check)

**Algorithm**:
```python
def record_completion():
    current_interval = initial_interval  # Reset to fast

def record_no_completion():
    current_interval = min(max_interval, current_interval * 2.0)  # Double
```

**Behavior over time** (if no completions):
```
1ms → 2ms → 4ms → 8ms → 16ms → 32ms → 64ms → 128ms → 256ms → 512ms → 1000ms (capped)
```

#### ProgressivePollingStrategy

**Behavior**: Steps through fixed interval levels.

**Best for**: Predictable phases, explicit control.

**Parameters** (from `global_config.defaults`):
- `intervals`: Generated from `polling_progressive_min_interval` and `polling_progressive_max_interval`
  - Default: `(0.001, 0.005, 0.01, 0.05, 0.1)` = (1ms, 5ms, 10ms, 50ms, 100ms)
- `checks_before_increase`: 5 (stay at each level for 5 checks)

**Algorithm**:
```python
def record_completion():
    current_index = 0  # Reset to fastest
    checks_at_level = 0

def record_no_completion():
    checks_at_level += 1
    if checks_at_level >= 5:  # After 5 checks at this level
        current_index = min(len(intervals) - 1, current_index + 1)  # Next level
        checks_at_level = 0
```

### Strategy Selection in wait()/gather()

**Default**: `PollingAlgorithm.Adaptive`

**How strategies are created**:

1. User passes `polling` parameter to `wait()` or `gather()`
2. If it's a `BasePollingStrategy` instance, use as-is
3. If it's a `PollingAlgorithm` enum or string, create strategy with config defaults:

```python
from concurry.config import global_config

defaults = global_config.defaults

if polling == PollingAlgorithm.Adaptive:
    strategy = AdaptivePollingStrategy(
        min_interval=defaults.polling_adaptive_min_interval,
        max_interval=defaults.polling_adaptive_max_interval,
        current_interval=defaults.polling_adaptive_initial_interval,
    )
```

**Configuration Integration**:
- All default intervals come from `global_config.defaults`
- No hardcoded values in strategy creation
- Users can customize via `global_config` or by passing strategy instance

---

## Wait Function

### Signature

```python
def wait(
    fs: Union[List, Tuple, Set, Dict, Any],
    *futs,
    timeout: Optional[float] = None,
    return_when: Union[ReturnWhen, str] = ReturnWhen.ALL_COMPLETED,
    polling: Union[PollingAlgorithm, str] = PollingAlgorithm.Adaptive,
    progress: Union[bool, Dict, Callable, None] = None,
    recurse: bool = False,
) -> Tuple[Set[BaseFuture], Set[BaseFuture]]:
```

### Input Patterns

**Pattern 1: List/Tuple/Set** (most common)
```python
futures = [worker.task(i) for i in range(10)]
done, not_done = wait(futures)
```

**Pattern 2: Dictionary** (preserves keys)
```python
tasks = {"download": f1, "process": f2, "upload": f3}
done, not_done = wait(tasks)  # done/not_done contain wrapped futures
```

**Pattern 3: Individual futures** (variadic)
```python
done, not_done = wait(future1, future2, future3)
```

**Pattern 4: Single future**
```python
done, not_done = wait(single_future)
```

### Critical Validation

**Cannot mix structure and variadic args**:
```python
# ❌ ERROR
futures = [f1, f2, f3]
wait(futures, f4, f5)  # Raises ValueError

# ✅ VALID
wait([f1, f2, f3, f4, f5])  # Pass as list
wait(f1, f2, f3, f4, f5)     # Pass individually
```

**Implementation**:
```python
if len(futs) > 0 and isinstance(fs, (list, tuple, set, dict)):
    raise ValueError(
        "Cannot provide both a structure (list/tuple/set/dict) as first argument "
        "and additional futures via *futs. Either pass a structure, or pass individual futures."
    )
```

### Return Conditions

**ReturnWhen.ALL_COMPLETED** (default):
- Wait until **all futures** are done
- Most common use case

**ReturnWhen.FIRST_COMPLETED**:
- Return as soon as **any future** completes
- Useful for racing multiple operations

**ReturnWhen.FIRST_EXCEPTION**:
- Return as soon as **any future raises an exception**
- Useful for fail-fast scenarios

**Implementation**:
```python
# Check after each batch of completions
if return_when == ReturnWhen.FIRST_COMPLETED and len(done) > 0:
    return done, not_done

if return_when == ReturnWhen.FIRST_EXCEPTION:
    for fut in newly_done:
        try:
            if fut.exception(timeout=0) is not None:
                return done, not_done
        except Exception:
            pass

if return_when == ReturnWhen.ALL_COMPLETED and len(not_done) == 0:
    return done, not_done
```

### Core Algorithm

```python
def wait(...):
    # 1. Wrap all futures
    futures_list = [wrap_future(f) for f in ...]
    
    # 2. Initialize sets
    done: Set[BaseFuture] = set()
    not_done: Set[BaseFuture] = set(futures_list)
    
    # 3. Create polling strategy
    strategy = create_strategy(polling, global_config.defaults)
    
    # 4. Initial check
    initial_done = _check_futures_batch(not_done)
    done.update(initial_done)
    not_done.difference_update(initial_done)
    
    # Check if can return early
    if condition_met(return_when, done, not_done):
        return done, not_done
    
    # 5. Main polling loop
    while True:
        # Check timeout
        if timeout and elapsed >= timeout:
            raise TimeoutError(...)
        
        # Batch check
        newly_done = _check_futures_batch(not_done)
        
        if len(newly_done) > 0:
            done.update(newly_done)
            not_done.difference_update(newly_done)
            strategy.record_completion()
            update_progress(...)
            
            # Check return conditions
            if condition_met(return_when, done, not_done):
                return done, not_done
        else:
            strategy.record_no_completion()
        
        # Sleep
        interval = strategy.get_next_interval()
        time.sleep(interval)
```

### Batch Checking Optimization

**Purpose**: Efficiently check multiple futures without O(n) IPC calls.

**Ray Optimization**:
```python
def _check_futures_batch(futures_to_check):
    if len(futures_to_check) == 0:
        return set()
    
    completed = set()
    
    # Separate Ray futures
    if _IS_RAY_INSTALLED:
        ray_futures = []
        ray_future_map = {}
        
        for fut in futures_to_check:
            if hasattr(fut, "_object_ref"):
                ray_futures.append(fut._object_ref)
                ray_future_map[id(fut._object_ref)] = fut
        
        # Batch check ALL Ray futures with single ray.wait() call
        if len(ray_futures) > 0:
            ready, not_ready = ray.wait(
                ray_futures, 
                num_returns=len(ray_futures),  # Check all
                timeout=0  # Non-blocking
            )
            for ref in ready:
                completed.add(ray_future_map[id(ref)])
    
    # Check non-Ray futures individually
    for fut in futures_to_check:
        if fut not in completed:
            if fut.done():
                completed.add(fut)
    
    return completed
```

**Why this matters**:
- Without optimization: 5000 Ray futures = 5000 `ray.wait()` calls (expensive IPC)
- With optimization: 5000 Ray futures = 1 `ray.wait()` call (single IPC)
- Dramatic performance improvement for large batches

---

## Gather Function

### Signature

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
) -> Union[List[Any], Dict[Any, Any], Iterator[Tuple[int, Any]]]:
```

### Return Types

**List input → List output**:
```python
futures = [w.task(i) for i in range(5)]
results = gather(futures)  # Returns: [r0, r1, r2, r3, r4]
```

**Dict input → Dict output** (keys preserved):
```python
tasks = {"download": f1, "process": f2}
results = gather(tasks)  # Returns: {"download": r1, "process": r2}
```

**Iterator mode → Generator** (yields as completed):
```python
for idx, result in gather(futures, iter=True):
    print(f"Future {idx} completed: {result}")

for key, result in gather(tasks, iter=True):
    print(f"Task {key} completed: {result}")
```

### Backend Architecture

**Two backend functions**:
1. `_gather_blocking_backend()`: Waits for all, returns in order
2. `_gather_iter_backend()`: Yields results as they complete

**Dispatch logic**:
```python
def gather(...):
    # Validate and build args
    if len(futs) > 0:
        args = (fs,) + futs
        is_dict_input = False
    else:
        args = (fs,)
        is_dict_input = isinstance(fs, dict)
    
    # Delegate
    if iter:
        return _gather_iter_backend(args, ..., is_dict_input)
    else:
        return _gather_blocking_backend(args, ..., is_dict_input)
```

### Blocking Backend Algorithm

```python
def _gather_blocking_backend(fs, return_exceptions, timeout, polling, progress, recurse, is_dict_input):
    # Special case: Dict input
    if is_dict_input and len(fs) == 1:
        futures_dict = fs[0]
        keys = list(futures_dict.keys())
        futures_list = [wrap_future(v) for v in futures_dict.values()]
        
        # Wait for all
        done, not_done = wait(futures_list, timeout=timeout, ...)
        
        if len(not_done) > 0:
            raise TimeoutError(...)
        
        # Collect results preserving keys
        results_dict = {}
        for key, fut in zip(keys, futures_list):
            try:
                results_dict[key] = fut.result(timeout=0)
            except Exception as e:
                if return_exceptions:
                    results_dict[key] = e
                else:
                    raise
        
        return results_dict
    
    # Special case: List/Tuple/Set input (single structure)
    if len(fs) == 1 and isinstance(fs[0], (list, tuple, set)):
        futures_list = [wrap_future(f) for f in fs[0]]
        
        # Wait for all
        done, not_done = wait(futures_list, timeout=timeout, ...)
        
        if len(not_done) > 0:
            raise TimeoutError(...)
        
        # Collect results in order
        results = []
        for fut in futures_list:
            try:
                results.append(fut.result(timeout=0))
            except Exception as e:
                if return_exceptions:
                    results.append(e)
                else:
                    raise
        
        return results
    
    # General case: Multiple individual futures
    futures_list = [wrap_future(f) for f in fs]
    
    # Wait for all
    done, not_done = wait(futures_list, timeout=timeout, ...)
    
    if len(not_done) > 0:
        raise TimeoutError(...)
    
    # Collect results
    results = []
    for fut in futures_list:
        try:
            results.append(fut.result(timeout=0))
        except Exception as e:
            if return_exceptions:
                results.append(e)
            else:
                raise
    
    return results
```

**Critical detail**: Always call `fut.result(timeout=0)` after `wait()` completes. The future is already done, so `timeout=0` is safe and avoids hanging.

### Iterator Backend Algorithm

```python
def _gather_iter_backend(fs, return_exceptions, timeout, polling, progress, recurse, is_dict_input):
    # Build future list and key mapping
    if is_dict_input and len(fs) == 1:
        keys_list = list(fs[0].keys())
        futures_list = [wrap_future(v) for v in fs[0].values()]
        future_to_key = {id(fut): key for fut, key in zip(futures_list, keys_list)}
    elif len(fs) == 1 and isinstance(fs[0], (list, tuple, set)):
        futures_list = [wrap_future(f) for f in fs[0]]
        future_to_key = {id(fut): i for i, fut in enumerate(futures_list)}
    else:
        futures_list = [wrap_future(f) for f in fs]
        future_to_key = {id(fut): i for i, fut in enumerate(futures_list)}
    
    # Create polling strategy
    strategy = create_strategy(polling, global_config.defaults)
    
    # Track pending
    pending = set(futures_list)
    
    # Main loop
    while len(pending) > 0:
        # Check timeout
        if timeout and elapsed >= timeout:
            raise TimeoutError(...)
        
        # Batch check
        newly_done = _check_futures_batch(pending)
        
        if len(newly_done) > 0:
            # Yield completed futures
            for fut in newly_done:
                key_or_index = future_to_key[id(fut)]
                try:
                    result = fut.result(timeout=0)
                    yield (key_or_index, result)
                except Exception as e:
                    if return_exceptions:
                        yield (key_or_index, e)
                    else:
                        raise
            
            pending.difference_update(newly_done)
            strategy.record_completion()
        else:
            strategy.record_no_completion()
        
        # Sleep
        if len(pending) > 0:
            interval = strategy.get_next_interval()
            time.sleep(interval)
```

**Key difference from blocking backend**:
- **Yields immediately** when futures complete (out of order)
- **Returns `(key/index, result)` tuples** for tracking
- Continues until all futures are yielded

---

## Key Design Decisions

### 1. Why Primary Signature is `fs` (not `*fs`)?

**Rationale**: Most common usage is passing a collection:
```python
futures = [worker.task(i) for i in range(100)]
results = gather(futures)  # Most common - pass list directly
```

**Alternative** (worse):
```python
results = gather(*futures)  # Unpacking required - less intuitive
```

**Design choice**: Optimize for the most common case. Support variadic for convenience, but validate against mixing patterns.

### 2. Why Dict Support with Key Preservation?

**Use case**: Named tasks with meaningful identifiers:
```python
tasks = {
    "download_data": worker.download(),
    "process_data": worker.process(),
    "upload_results": worker.upload(),
}

results = gather(tasks)
print(results["download_data"])  # Access by name

for task_name, result in gather(tasks, iter=True):
    print(f"{task_name} completed: {result}")
```

**Benefit**: Self-documenting code, easier debugging, natural data structure.

### 3. Why `iter=True` Instead of Separate Function?

**Alternatives considered**:
- `gather()` vs. `gather_iter()` (old design)
- `gather()` vs. `as_completed()` (asyncio pattern)

**Chosen**: `gather(iter=True)`

**Rationale**:
1. **Single function** for gathering (simpler API)
2. **Parameter** makes behavior explicit
3. **Same validation** and input patterns for both modes
4. Less duplication in implementation

### 4. Why Batch Checking for Ray?

**Problem**: Individual `ray.wait()` calls are expensive (IPC overhead).

**Solution**: Single `ray.wait()` call with `num_returns=len(futures)`:
```python
ready, not_ready = ray.wait(ray_futures, num_returns=len(ray_futures), timeout=0)
```

**Impact**: 
- 5000 futures: ~50ms for batch check vs. ~5000ms for individual checks
- 100x speedup for large batches

### 5. Why Adaptive Polling as Default?

**Alternatives**:
- Fixed: Predictable but not adaptive
- Exponential: Too aggressive for short tasks
- Progressive: Less adaptive than Adaptive

**Adaptive strategy**:
- Speeds up when futures complete frequently
- Slows down when idle
- Good balance for unknown workloads

**Result**: Best general-purpose default for Concurry users.

### 6. Why No Auto-Detection of Return Type?

**Could do**:
```python
# Auto-detect if user wants list or iterator
def gather(futures, ...):
    if user_expects_iterator:  # How to detect?
        return generator
    else:
        return list
```

**Why not**:
- **Ambiguous**: Can't reliably detect user intent
- **Implicit behavior**: Hard to reason about
- **Type checking**: Difficult for static analysis

**Chosen**: Explicit `iter=True` parameter.

### 7. Why `return_exceptions` Parameter?

**Use case**: Don't want to stop gathering if one task fails:
```python
results = gather(
    [worker.task(i) for i in range(100)],
    return_exceptions=True
)

for i, result in enumerate(results):
    if isinstance(result, Exception):
        print(f"Task {i} failed: {result}")
    else:
        print(f"Task {i} succeeded: {result}")
```

**Alternative** (worse):
```python
# Have to wrap every result access in try/except
results = gather(...)  # Raises on first exception
```

**Benefit**: Collect all results/errors, analyze batch failures.

---

## Async Wait and Gather Functions

### Overview

`async_wait()` and `async_gather()` are async-native synchronization primitives for coordinating coroutines and `asyncio.Future`/`asyncio.Task` objects within async contexts.

**Key characteristics:**
- Themselves `async` functions (must be awaited)
- Accept coroutines, `asyncio.Future`, and `asyncio.Task` objects
- Use polling loop with `await asyncio.sleep()` to yield control to event loop
- Support progress tracking, timeouts, and all features of synchronous counterparts
- Configurable polling interval (default: 100µs = 10,000 checks/sec)

### Design Rationale

#### Problem Context

When `wrap_future()` was extended to support coroutines (by calling `asyncio.ensure_future()` to schedule them on the running event loop), we initially considered making synchronous `wait()` and `gather()` work with coroutines directly.

**Initial approach attempted:**
```python
# In wrap_future()
if asyncio.iscoroutine(future):
    loop = asyncio.get_running_loop()
    task = asyncio.ensure_future(future, loop=loop)
    return AsyncioFuture(future=task)

# User code (would fail):
coros = [async_fetch(i) for i in range(10)]
results = wait(coros)  # ❌ Problem: time.sleep() blocks event loop!
```

**Why this doesn't work:**

The synchronous `wait()` and `gather()` functions use `time.sleep()` in their polling loops. When called from an async context with coroutines:

1. `wrap_future()` schedules coroutines on the running event loop
2. Polling loop calls `time.sleep()` → **blocks the entire event loop**
3. Coroutines can't execute (event loop blocked)
4. Deadlock or indefinite hang

**The fundamental incompatibility:**
- Synchronous `wait()`/`gather()` → uses `time.sleep()` → blocks thread
- Async coroutines → need event loop to run → can't run when event loop blocked
- Result: Cannot mix `time.sleep()` with event loop operations

#### Design Decision: Separate Async Functions

**Solution:** Create dedicated `async_wait()` and `async_gather()` functions that:
1. Are themselves `async` functions (awaitable)
2. Use `await asyncio.sleep()` instead of `time.sleep()`
3. Yield control to event loop during polling
4. Work exclusively with coroutines/asyncio futures

**Benefits:**
- ✅ Clean separation of sync and async code paths
- ✅ No event loop blocking
- ✅ Explicit API (users know they're in async context)
- ✅ Optimal performance (native asyncio integration)
- ✅ Progress bars work correctly (can update during event loop execution)

### Implementation Architecture

#### Core Approach: Polling Loop with Asyncio Integration

Both `async_wait()` and `async_gather()` use a **polling loop that yields control to the event loop** between checks:

```python
async def async_wait(...):
    # Convert coroutines to tasks
    tasks = []
    for item in items:
        if asyncio.iscoroutine(item):
            task = asyncio.ensure_future(item)
            tasks.append(task)
        elif asyncio.isfuture(item) or isinstance(item, asyncio.Task):
            tasks.append(item)
        else:
            # Regular value - wrap in completed future
            future = asyncio.get_running_loop().create_future()
            future.set_result(item)
            tasks.append(future)
    
    # Polling loop
    while True:
        # Check which tasks are done
        done_set = {t for t in tasks if t.done()}
        not_done_set = set(tasks) - done_set
        
        # Update progress
        _update_progress(tracker, len(done_set), total, elapsed)
        
        # Check return condition
        if should_return(return_when, done_set, not_done_set):
            return done_set, not_done_set
        
        # Yield to event loop (critical!)
        await asyncio.sleep(poll_interval)  # ← Non-blocking!
```

**Key implementation details:**

1. **Task Creation:**
   - Coroutines → `asyncio.ensure_future()` → `asyncio.Task`
   - `asyncio.Future`/`asyncio.Task` → passed through
   - Regular values → wrapped in pre-completed `asyncio.Future`

2. **Polling Loop:**
   - Periodically checks `task.done()` for each task
   - Updates progress tracker on each iteration
   - Evaluates return conditions (ALL_COMPLETED, FIRST_COMPLETED, etc.)
   - **Yields control via `await asyncio.sleep(poll_interval)`**

3. **Progress Tracking:**
   - Uses same `_update_progress()` function as sync versions
   - Progress bar updates dynamically as tasks complete
   - Custom callbacks supported

#### Why Polling Instead of Native asyncio.wait()?

**Alternative 1: Single asyncio.wait() Call (No Progress Updates)**

```python
# ❌ Doesn't support progress tracking
async def async_wait_no_progress(tasks, return_when):
    done, pending = await asyncio.wait(
        tasks,
        return_when=return_when
    )
    return done, pending
```

**Problem:**
- `asyncio.wait()` blocks until completion condition is met
- **No opportunity to update progress during waiting**
- Progress bar would be frozen until all tasks complete
- Violates design goal: consistent progress tracking across all sync primitives

**Alternative 2: asyncio.wait() with timeout in Loop (Complex)**

```python
# ❌ Complex, error-prone
async def async_wait_complex(tasks, return_when, progress):
    while True:
        done, pending = await asyncio.wait(
            tasks,
            timeout=0.1,  # Short timeout to check progress
            return_when=asyncio.FIRST_COMPLETED if any_done else return_when
        )
        
        _update_progress(...)
        
        # Complex logic to handle partial completions
        if return_when == ALL_COMPLETED:
            if not pending:
                return done, pending
        elif return_when == FIRST_COMPLETED:
            if done:
                return done, pending
        # ... more complex conditions
```

**Problems:**
- Complex condition handling (asyncio.wait's return_when differs from ours)
- asyncio.wait() modifies task sets → difficult to track original tasks
- Timeout handling inconsistent with sync versions
- More error-prone than explicit polling

**Alternative 3: Callbacks (Event-Driven, No Polling)**

```python
# ❌ Callbacks make progress tracking difficult
async def async_wait_callbacks(tasks, return_when, progress):
    completed_tasks = set()
    
    def on_task_complete(task):
        completed_tasks.add(task)
        _update_progress(tracker, len(completed_tasks), total, elapsed)
        
        if should_return(return_when, completed_tasks, ...):
            # How to return from here?
            event.set()
    
    for task in tasks:
        task.add_done_callback(on_task_complete)
    
    # Wait for event
    await event.wait()
    
    return completed_tasks, pending_tasks
```

**Problems:**
- Difficult to return from callbacks
- Need event synchronization primitives
- Timeout handling complex
- Progress tracking from callbacks is tricky (threading issues)
- Less explicit control flow

**Chosen Solution: Polling Loop with asyncio.sleep()**

```python
async def async_wait(tasks, return_when, progress):
    while True:
        # Check completion
        done_set = {t for t in tasks if t.done()}
        not_done_set = set(tasks) - done_set
        
        # Update progress
        _update_progress(tracker, len(done_set), total, elapsed)
        
        # Check conditions
        if should_return(return_when, done_set, not_done_set):
            return done_set, not_done_set
        
        # Yield to event loop
        await asyncio.sleep(poll_interval)
```

**Why polling wins:**
- ✅ **Simple, explicit control flow** (easy to understand and maintain)
- ✅ **Progress updates on every iteration** (consistent with sync versions)
- ✅ **Non-blocking** (`await asyncio.sleep()` yields to event loop)
- ✅ **Fast enough** (100µs = 10,000 checks/sec)
- ✅ **Timeout handling straightforward** (check elapsed time in loop)
- ✅ **Consistent with synchronous API** (same return conditions, same behavior)

**Performance characteristics:**
- Poll interval: 100 microseconds (configurable)
- CPU overhead: ~0.01% (10k checks/sec * 1µs = 10ms/sec CPU)
- Progress updates: Every poll (10,000 updates/sec if tasks completing)
- Memory: O(n) for task set, no additional overhead

#### Polling Interval Configuration

The polling interval is configurable via `global_config`:

```python
from concurry.config import global_config

# Default: 100 microseconds
assert global_config.defaults.async_wait_poll_interval == 100e-6
assert global_config.defaults.async_gather_poll_interval == 100e-6

# Override globally
with global_config.temp_config(async_wait_poll_interval=50e-6):
    # Now polls every 50 microseconds (20k checks/sec)
    results = await async_gather(coros)
```

**Why 100 microseconds?**
- **Fast enough**: Max latency 0.1ms between task completion and detection
- **Not too fast**: 10,000 checks/sec is reasonable CPU overhead
- **Yields to event loop frequently**: Tasks get CPU time to execute
- **Standard for async operations**: Similar to asyncio internal polling

**Trade-offs:**
- Lower interval (e.g., 10µs): 
  - ✅ Lower latency for detecting completions
  - ❌ Higher CPU overhead
  - ❌ More context switches
- Higher interval (e.g., 1ms):
  - ✅ Lower CPU overhead
  - ❌ Higher latency (up to 1ms)
  - ❌ Progress updates less frequent

### async_wait() Implementation Details

**Function signature:**
```python
async def async_wait(
    fs: Union[List, Tuple, Set, Dict, Any],
    *futs,
    timeout: Optional[float] = None,
    return_when: Union[ReturnWhen, str] = ReturnWhen.ALL_COMPLETED,
    progress: Union[bool, Dict, Callable, None] = None,
) -> Tuple[Set[asyncio.Task], Set[asyncio.Task]]
```

**Key steps:**

1. **Argument Processing:**
   - Handle variadic args, dict/list/set/single inputs
   - Convert `return_when` string to enum
   - Validate timeout

2. **Task Creation:**
   - Convert coroutines to tasks via `asyncio.ensure_future()`
   - Pass through existing `asyncio.Future`/`asyncio.Task` objects
   - Wrap regular values in completed futures

3. **Progress Tracker Setup:**
   - Initialize progress bar or callback
   - Track start time for elapsed calculations

4. **Polling Loop:**
   ```python
   while True:
       elapsed = time.time() - start_time
       
       # Check timeout
       if timeout is not None and elapsed >= timeout:
           raise TimeoutError(...)
       
       # Check task completion
       done_set = {t for t in tasks if t.done()}
       not_done_set = set(tasks) - done_set
       
       # Update progress
       _update_progress(tracker, len(done_set), total, elapsed)
       
       # Check return condition
       if return_when == ReturnWhen.ALL_COMPLETED:
           if len(not_done_set) == 0:
               return done_set, not_done_set
       elif return_when == ReturnWhen.FIRST_COMPLETED:
           if len(done_set) > 0:
               return done_set, not_done_set
       elif return_when == ReturnWhen.FIRST_EXCEPTION:
           # Check for exceptions in done tasks
           for task in done_set:
               if task.exception() is not None:
                   return done_set, not_done_set
       
       # Sleep before next check
       remaining_timeout = None if timeout is None else timeout - elapsed
       sleep_time = min(poll_interval, remaining_timeout or poll_interval)
       if sleep_time > 0:
           await asyncio.sleep(sleep_time)
   ```

5. **Cleanup:**
   - Close progress tracker
   - Return `(done, not_done)` sets

### async_gather() Implementation Details

**Function signature:**
```python
async def async_gather(
    fs: Union[List, Tuple, Set, Dict, Any],
    *futs,
    return_exceptions: bool = False,
    timeout: Optional[float] = None,
    progress: Union[bool, Dict, Callable, None] = None,
) -> Union[List[Any], Dict[Any, Any]]
```

**Key steps:**

1. **Task Creation:** Same as `async_wait()`

2. **Polling Loop:**
   ```python
   while True:
       elapsed = time.time() - start_time
       
       # Check timeout
       if timeout is not None and elapsed >= timeout:
           raise TimeoutError(...)
       
       # Check completion count
       completed_count = sum(1 for t in tasks if t.done())
       _update_progress(tracker, completed_count, total, elapsed)
       
       # All done?
       if completed_count == total:
           break
       
       # Sleep
       await asyncio.sleep(poll_interval)
   ```

3. **Result Collection:**
   ```python
   if input_was_dict:
       results = {}
       for key, task in zip(original_keys, tasks):
           if return_exceptions:
               try:
                   results[key] = await task
               except Exception as e:
                   results[key] = e
           else:
               results[key] = await task  # Raises on exception
       return results
   else:
       results = []
       for task in tasks:
           if return_exceptions:
               try:
                   results.append(await task)
               except Exception as e:
                   results.append(e)
           else:
               results.append(await task)
       return results
   ```

### Integration with Workers

**Important distinction:**

`async_wait()` and `async_gather()` are designed for **raw coroutines and asyncio futures**, not for Concurry Worker futures:

```python
# ✅ Correct: Use with raw coroutines
async def fetch_data(id: int):
    await asyncio.sleep(0.1)
    return {"id": id}

coros = [fetch_data(i) for i in range(10)]
results = await async_gather(coros)  # ✅ Works

# ✅ Correct: Use regular gather() with Worker futures
worker = MyWorker.options(mode="asyncio").init()
futures = [worker.async_method(i) for i in range(10)]
results = gather(futures)  # ✅ Works (sync gather)

# ❌ Wrong: Don't mix Worker futures with async_gather
futures = [worker.async_method(i) for i in range(10)]
results = await async_gather(futures)  # ❌ Won't work correctly
```

**Why this separation?**
- Worker futures (`ConcurrentFuture`, `RayFuture`, etc.) work with synchronous `wait()`/`gather()`
- `async_wait()`/`async_gather()` are for native asyncio operations
- Mixing them would require complex type checking and conversion

### Comparison with asyncio.wait() and asyncio.gather()

| Feature | `asyncio.wait()` | `async_wait()` | `asyncio.gather()` | `async_gather()` |
|---------|------------------|----------------|-------------------|------------------|
| Progress tracking | ❌ No | ✅ Yes (dynamic) | ❌ No | ✅ Yes (dynamic) |
| Timeout support | ✅ Yes | ✅ Yes | ❌ No | ✅ Yes |
| Dict input | ❌ No | ✅ Yes (preserves keys) | ❌ No | ✅ Yes (preserves keys) |
| Return conditions | ✅ Yes | ✅ Yes (compatible) | ❌ N/A | ❌ N/A |
| Progress callbacks | ❌ No | ✅ Yes | ❌ No | ✅ Yes |
| Return type | Set[Task] | Set[Task] | List[Any] | List or Dict |
| Performance | ✅ Native (no polling) | ⚠️ Polling (100µs) | ✅ Native | ⚠️ Polling (100µs) |

**When to use each:**

- **`asyncio.wait()`/`asyncio.gather()`**: When you don't need progress tracking or dict support
- **`async_wait()`/`async_gather()`**: When you need progress tracking, timeouts, or dict support
- **Regular `wait()`/`gather()`**: When working with Concurry Worker futures (any execution mode)

### Performance Characteristics

**Overhead:**
- Polling interval: 100µs (configurable)
- Per-iteration cost: ~1µs (set comprehension + progress update)
- CPU usage: ~0.01-0.1% depending on task completion rate
- Memory: O(n) for task tracking, no additional overhead

**Comparison with native asyncio:**
- `asyncio.wait()`: 0 polling overhead (event-driven)
- `async_wait()`: ~0.01% CPU overhead from polling
- **Trade-off**: Slightly higher CPU for dynamic progress tracking

**Scalability:**
- Tested with 10,000 concurrent tasks
- Progress updates: 10,000/sec (every poll iteration)
- No performance degradation with large task counts

### Error Handling

**Timeout handling:**
```python
async def async_wait_with_timeout(tasks):
    try:
        done, not_done = await async_wait(tasks, timeout=10.0)
    except TimeoutError:
        print("Some tasks didn't complete in time")
        # Can inspect done and not_done if needed
```

**Exception propagation:**
```python
# Without return_exceptions (default)
try:
    results = await async_gather(tasks)
except ValueError as e:
    print(f"A task raised ValueError: {e}")

# With return_exceptions
results = await async_gather(tasks, return_exceptions=True)
for result in results:
    if isinstance(result, Exception):
        print(f"Task failed: {result}")
```

### Testing Considerations

**Key test scenarios:**

1. **Basic functionality:**
   - Coroutines complete successfully
   - Results returned in correct order
   - Dict keys preserved

2. **Progress tracking:**
   - Progress bar updates dynamically
   - Custom callbacks invoked correctly
   - Final progress shows 100% completion

3. **Exception handling:**
   - `return_exceptions=True` captures exceptions
   - `return_exceptions=False` raises first exception
   - Exception types preserved

4. **Timeout handling:**
   - TimeoutError raised correctly
   - Partial results available before timeout

5. **Return conditions:**
   - `ALL_COMPLETED` waits for all
   - `FIRST_COMPLETED` returns immediately
   - `FIRST_EXCEPTION` detects exceptions

6. **Integration:**
   - Works correctly inside async functions
   - Can coordinate multiple async operations
   - Compatible with asyncio.sleep(), asyncio.create_task(), etc.

**Example test:**
```python
@pytest.mark.asyncio
async def test_async_gather_with_progress():
    async def task(x):
        await asyncio.sleep(0.01)
        return x * 2
    
    coros = [task(i) for i in range(100)]
    results = await async_gather(coros, progress=True, timeout=10.0)
    
    assert len(results) == 100
    assert results[0] == 0
    assert results[50] == 100
```

---

## Performance Considerations

### Memory Usage

**With 10,000 futures**:
- `SyncFuture`: 10K * 96 bytes = 960 KB
- `ConcurrentFuture`: 10K * 64 bytes = 640 KB (after slot optimization)
- `RayFuture`: 10K * 128 bytes = 1.28 MB

**Optimization impact**:
- Removed unused slots from `ConcurrentFuture`: Saved 320 KB (32 bytes * 10K)
- Removed unused slots from `AsyncioFuture`: Saved 240 KB (24 bytes * 10K)

### Polling Overhead

**Adaptive polling CPU usage** (5000 futures, 10-second completion):
- Starts at 10ms interval: ~1000 checks
- Speeds up to 1ms on completions: ~2000 checks
- Slows to 100ms on idle: ~100 checks
- **Total checks**: ~3100 (vs. 10,000 with 1ms fixed)

**CPU time**: ~3100 * 0.1ms = 310ms over 10 seconds = 3.1% CPU.

### Ray Batch Checking Scaling

**5000 Ray futures**:
- Individual checks: 5000 * 1ms = 5000ms (5 seconds) per poll cycle
- Batch check: 1 * 10ms = 10ms per poll cycle
- **Speedup**: 500x

### Progress Bar Overhead

**With 10,000 futures and `miniters=100`**:
- Progress updates: 100 calls to `ProgressBar.update()`
- Overhead: ~100 * 0.1ms = 10ms total
- **Negligible** compared to polling

---

## Extension Points

### Adding a New Future Type

**Example**: Add support for Dask futures.

1. **Create DaskFuture class**:
```python
class DaskFuture(BaseFuture):
    __slots__ = ("uuid", "_dask_future", "_result", "_exception", "_done", "_cancelled", "_callbacks", "_lock")
    
    FUTURE_UUID_PREFIX = "dask-future-"
    
    def __init__(self, dask_future):
        # Validate
        if not isinstance(dask_future, dask.distributed.Future):
            raise TypeError(...)
        
        self.uuid = f"{self.FUTURE_UUID_PREFIX}{id(self)}"
        self._dask_future = dask_future
        self._result = None
        self._exception = None
        self._done = False
        self._cancelled = False
        self._callbacks = []
        self._lock = threading.Lock()
    
    def done(self) -> bool:
        # Query Dask future
        return self._dask_future.done()
    
    def result(self, timeout: Optional[float] = None) -> Any:
        # Implement with Dask-specific logic
        ...
    
    # ... implement all abstract methods
```

2. **Update `wrap_future()`**:
```python
def wrap_future(future: Any) -> BaseFuture:
    if isinstance(future, BaseFuture):
        return future
    elif isinstance(future, concurrent.futures.Future):
        return ConcurrentFuture(future=future)
    elif asyncio.isfuture(future):
        return AsyncioFuture(future=future)
    elif _IS_DASK_INSTALLED and isinstance(future, dask.distributed.Future):  # Add here
        return DaskFuture(dask_future=future)
    elif _IS_RAY_INSTALLED and isinstance(future, ray.ObjectRef):
        return RayFuture(object_ref=future)
    else:
        return SyncFuture(result_value=future)
```

3. **Optimize batch checking** (if applicable):
```python
def _check_futures_batch(futures_to_check):
    # ... existing Ray optimization ...
    
    # Add Dask batch checking
    if _IS_DASK_INSTALLED:
        dask_futures = []
        for fut in futures_to_check:
            if hasattr(fut, "_dask_future"):
                dask_futures.append(fut._dask_future)
        
        if len(dask_futures) > 0:
            # Use Dask's batch API if available
            ready = dask.distributed.wait(dask_futures, timeout=0, return_when='FIRST_COMPLETED')
            # Map back to BaseFuture wrappers
            ...
```

**Testing**: Add Dask to `worker_mode` fixture in `conftest.py`.

### Adding a New Polling Strategy

**Example**: Add a `SinusoidalPollingStrategy` that oscillates intervals.

1. **Create strategy class**:
```python
class SinusoidalPollingStrategy(BasePollingStrategy):
    """Oscillating polling interval based on sine wave."""
    
    aliases = ["sinusoidal", PollingAlgorithm.Sinusoidal]  # Add to PollingAlgorithm enum
    
    min_interval: float
    max_interval: float
    current_step: int = 0
    period: int = 20  # Complete cycle in 20 checks
    
    def get_next_interval(self) -> float:
        # Compute sine wave position
        import math
        t = (self.current_step % self.period) / self.period  # 0 to 1
        sine_value = math.sin(2 * math.pi * t)  # -1 to 1
        normalized = (sine_value + 1) / 2  # 0 to 1
        interval = self.min_interval + normalized * (self.max_interval - self.min_interval)
        return interval
    
    def record_completion(self) -> None:
        self.current_step = 0  # Reset to start of cycle
    
    def record_no_completion(self) -> None:
        self.current_step += 1  # Progress through cycle
    
    def reset(self) -> None:
        self.current_step = 0
```

2. **Add to PollingAlgorithm enum**:
```python
class PollingAlgorithm(AutoEnum):
    Fixed = auto()
    Adaptive = auto()
    Exponential = auto()
    Progressive = auto()
    Sinusoidal = auto()  # Add here
```

3. **Update `wait()` strategy creation**:
```python
if polling == PollingAlgorithm.Sinusoidal:
    strategy = SinusoidalPollingStrategy(
        min_interval=defaults.polling_sinusoidal_min_interval,
        max_interval=defaults.polling_sinusoidal_max_interval,
    )
```

4. **Add configuration defaults**:
```python
class GlobalDefaults(MutableTyped):
    # ... existing ...
    polling_sinusoidal_min_interval: float = 0.001
    polling_sinusoidal_max_interval: float = 0.1
```

**Testing**: Add tests to `test_polling.py` for new strategy.

---

## Common Pitfalls

### Pitfall 1: Setting `_done=True` Without Fetching Result (RayFuture Bug)

**Problem**:
```python
# ❌ WRONG
def done(self) -> bool:
    if self._done:
        return True
    
    ready, _ = ray.wait([self._object_ref], timeout=0)
    if len(ready) > 0:
        self._done = True  # ❌ BUG: Set _done but didn't fetch result!
        return True
    return False

# Later...
def result(self, timeout: Optional[float] = None) -> Any:
    if self._done:
        return self._result  # ❌ Returns None!
```

**Why this fails**:
- `done()` sets `_done=True`
- `result()` sees `_done=True`, returns cached `_result`
- But `_result` is still `None` because `ray.get()` was never called

**Fix**:
```python
# ✅ CORRECT
def done(self) -> bool:
    if self._done:
        return True
    
    ready, _ = ray.wait([self._object_ref], timeout=0)
    # Don't set _done here - only set it in result() when actually fetching
    return len(ready) > 0
```

**Lesson**: State caching requires careful synchronization. Only set `_done=True` when the result is actually fetched.

### Pitfall 2: Forgetting to Wrap Futures

**Problem**:
```python
# ❌ WRONG
def custom_function(futures):
    # Assume futures are already BaseFuture
    for fut in futures:
        if fut.done():  # AttributeError if fut is a raw ObjectRef!
            ...
```

**Fix**:
```python
# ✅ CORRECT
from concurry.core.future import wrap_future

def custom_function(futures):
    wrapped = [wrap_future(f) for f in futures]
    for fut in wrapped:
        if fut.done():  # Always works
            ...
```

**Lesson**: Always use `wrap_future()` when accepting future-like objects from external sources.

### Pitfall 3: Caching State in Delegation Wrappers

**Problem**:
```python
# ❌ WRONG - ConcurrentFuture with caching
class ConcurrentFuture(BaseFuture):
    __slots__ = ("uuid", "_future", "_result", "_done", ...)  # ❌ Don't cache!
    
    def done(self) -> bool:
        if self._done:
            return self._done
        self._done = self._future.done()  # ❌ Duplicates state
        return self._done
```

**Why this fails**:
- Duplicate state risks desynchronization
- Wastes memory
- `concurrent.futures.Future` already manages state

**Fix**:
```python
# ✅ CORRECT - Pure delegation
class ConcurrentFuture(BaseFuture):
    __slots__ = ("uuid", "_future", "_callbacks", "_lock")  # No caching
    
    def done(self) -> bool:
        return self._future.done()  # Direct delegation
```

**Lesson**: Only cache state when necessary (e.g., RayFuture). Otherwise, delegate.

### Pitfall 4: Hardcoding Polling Intervals

**Problem**:
```python
# ❌ WRONG
def wait(futures, ...):
    strategy = FixedPollingStrategy(interval=0.01)  # Hardcoded!
```

**Fix**:
```python
# ✅ CORRECT
from concurry.config import global_config

def wait(futures, polling, ...):
    defaults = global_config.defaults
    
    if polling == PollingAlgorithm.Fixed:
        strategy = FixedPollingStrategy(
            interval=defaults.polling_fixed_interval  # From config
        )
```

**Lesson**: All defaults must go through `global_config`. See [Configuration Architecture](configuration.md).

### Pitfall 5: Not Handling Dict Input Correctly

**Problem**:
```python
# ❌ WRONG - Loses keys
def gather(fs, ...):
    if isinstance(fs, dict):
        futures_list = list(fs.values())
        results = [fut.result() for fut in futures_list]
        return results  # ❌ Returns list, not dict!
```

**Fix**:
```python
# ✅ CORRECT - Preserves keys
def gather(fs, ...):
    if isinstance(fs, dict):
        keys = list(fs.keys())
        futures_list = list(fs.values())
        results = [fut.result() for fut in futures_list]
        return dict(zip(keys, results))  # ✅ Returns dict
```

**Lesson**: When dict is provided, output must be dict with same keys.

### Pitfall 6: Blocking Indefinitely Without Timeout

**Problem**:
```python
# ❌ WRONG - No timeout, can hang forever
done, not_done = wait(futures)  # Hangs if futures never complete
```

**Fix**:
```python
# ✅ CORRECT - Always use timeout in production
done, not_done = wait(futures, timeout=30.0)
```

**Lesson**: Always specify timeouts in production code to prevent deadlocks.

### Pitfall 7: Mixing Structure and Variadic Args

**Problem**:
```python
# ❌ WRONG - Ambiguous intent
futures = [f1, f2, f3]
done, not_done = wait(futures, f4, f5)  # Raises ValueError
```

**Why forbidden**:
- Ambiguous: Is `futures` a single future or a list?
- Hard to reason about what was intended

**Fix**:
```python
# ✅ CORRECT - Clear intent
done, not_done = wait([f1, f2, f3, f4, f5])  # Pass as list
# OR
done, not_done = wait(f1, f2, f3, f4, f5)  # Pass individually
```

**Lesson**: Validate input patterns to prevent ambiguous usage.

---

## Testing Requirements

### Must Test Across All Execution Modes

**All tests must use the `worker_mode` fixture**:
```python
def test_feature(self, worker_mode):
    """Test feature across all execution modes."""
    w = MyWorker.options(mode=worker_mode).init()
    # ...
```

**Reason**: Synchronization primitives must work identically across sync, asyncio, thread, process, and ray modes.

### Must Test Edge Cases

**Required edge case tests**:
1. Empty collections: `wait([])`, `gather([])`
2. Single items: `wait(single_future)`, `gather(single_future)`
3. Mixed types: `gather([future1, 42, future2])`
4. Large batches: `wait([futures * 100])`
5. All polling algorithms
6. Dictionary inputs
7. Iterator mode
8. Exception handling with `return_exceptions=True`
9. Timeout behavior (except sync mode)
10. Progress tracking (bar and callback)

### Must Test Return Type Consistency

**Dict input must return dict**:
```python
def test_gather_dict_returns_dict(self, worker_mode):
    tasks = {"t1": f1, "t2": f2}
    results = gather(tasks)
    assert isinstance(results, dict)
    assert list(results.keys()) == ["t1", "t2"]
```

**Iterator mode must yield correct tuples**:
```python
def test_gather_iter_yields_tuples(self, worker_mode):
    futures = [f1, f2, f3]
    items = list(gather(futures, iter=True))
    assert all(isinstance(item, tuple) and len(item) == 2 for item in items)
```

### Must Test Validation

**Test invalid input patterns**:
```python
def test_wait_rejects_mixed_structure_and_varargs(self):
    futures = [f1, f2]
    with pytest.raises(ValueError, match="Cannot provide both"):
        wait(futures, f3, f4)
```

### Performance Testing

**Test scalability** (optional but recommended):
```python
def test_wait_large_batch(self, worker_mode):
    """Test wait() with 1000 futures."""
    w = Worker.options(mode=worker_mode).init()
    futures = [w.task(i) for i in range(1000)]
    
    start = time.time()
    done, not_done = wait(futures, timeout=60.0)
    elapsed = time.time() - start
    
    assert len(done) == 1000
    assert elapsed < 30.0  # Should complete in reasonable time
```

### Must NOT Skip Tests Due to Failures

**❌ NEVER**:
```python
def test_feature(self, worker_mode):
    if worker_mode == "ray":
        pytest.skip("Fails on Ray")  # ❌ WRONG
```

**✅ CORRECT**:
```python
def test_feature(self, worker_mode):
    if worker_mode == "sync":
        pytest.skip("Sync mode doesn't support timeouts")  # ✅ Valid reason
```

**See**: [Cursor Rules: Testing Practices](../../.cursor/rules/testing-practices.mdc) for complete testing guidelines.

---

## Summary

The synchronization architecture provides:

1. **Unified Future Interface** (`BaseFuture` + `wrap_future()`)
   - Works across all execution frameworks
   - Efficient delegation or caching based on framework
   - Consistent exception handling

2. **Adaptive Polling Strategies**
   - Fixed, Adaptive (default), Exponential, Progressive
   - Configurable via `global_config`
   - Extensible via Registry pattern

3. **Efficient Batch Checking**
   - Single `ray.wait()` call for all Ray futures
   - 100x+ speedup for large batches

4. **Flexible Input Patterns**
   - List/tuple/set, dict (preserves keys), individual futures
   - Validated to prevent ambiguous usage

5. **Multiple Output Modes**
   - Blocking: Returns all results in order
   - Iterator: Yields results as they complete
   - Dict: Preserves input keys

6. **Progress Tracking**
   - Progress bars with `tqdm` integration
   - Custom callbacks for advanced use cases

7. **Extension Points**
   - Add new future types by implementing `BaseFuture`
   - Add new polling strategies by implementing `BasePollingStrategy`
   - All integrations use Registry pattern for factory creation

**Key Invariants**:
- All futures must be thread-safe
- All futures must raise consistent exception types
- Delegation wrappers must NOT cache state
- All defaults must go through `global_config`
- Dict inputs must return dicts with same keys
- `result(timeout=0)` after `wait()` must always work

**For more details**:
- Configuration: [Configuration Architecture](configuration.md)
- User Guide: [Synchronization Guide](../user-guide/synchronization.md)
- Testing: [Testing Practices](../../.cursor/rules/testing-practices.mdc)

