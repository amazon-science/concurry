# Architecture: Workers and Worker Pools

This document provides a comprehensive technical overview of the worker and worker pool architecture in Concurry.

## Table of Contents

- [Overview](#overview)
- [Core Abstractions](#core-abstractions)
  - [Worker Base Class](#worker-base-class)
  - [WorkerProxy Hierarchy](#workerproxy-hierarchy)
  - [WorkerProxyPool Hierarchy](#workerproxypool-hierarchy)
  - [WorkerBuilder](#workerbuilder)
- [Execution Modes](#execution-modes)
- [Worker Lifecycle](#worker-lifecycle)
- [Pool Architecture](#pool-architecture)
- [Critical Implementation Details](#critical-implementation-details)
- [Adding New Worker Types](#adding-new-worker-types)
- [Limitations and Gotchas](#limitations-and-gotchas)

## Overview

Concurry implements a **Worker/Proxy pattern** where:
- **Worker**: User-defined class with business logic (plain Python class)
- **WorkerProxy**: Wraps Worker and manages execution context (thread, process, Ray actor, etc.)
- **WorkerProxyPool**: Manages multiple WorkerProxy instances with load balancing

This separation allows the same Worker code to run in different execution contexts (sync, thread, process, asyncio, Ray) without modification.

### Key Design Principles

1. **Worker classes are plain Python** - No inheritance requirements beyond `Worker` base class
2. **Proxy classes handle execution** - All concurrency, serialization, and communication logic
3. **Typed validation** - All proxies and pools inherit from `morphic.Typed` for configuration validation
4. **No shared state between workers** - Each worker maintains isolated state
5. **Unified Future API** - All execution modes return BaseFuture subclasses

## Core Abstractions

### Worker Base Class

```python
class Worker:
    """User-facing base class for all workers."""
    
    @classmethod
    def options(cls, mode, blocking, max_workers, ...) -> WorkerBuilder:
        """Configure worker execution options."""
        ...
    
    def __init__(self, *args, **kwargs):
        """User-defined initialization - completely flexible signature."""
        ...
```

**Key Characteristics:**
- Does NOT inherit from `morphic.Typed` (allows flexible `__init__` signatures)
- Supports cooperative multiple inheritance with `Typed`/`BaseModel` across ALL modes
- Validation decorators (`@validate`, `@validate_call`) work with ALL modes including Ray
- User-defined workers are wrapped by `_create_worker_wrapper()` to inject `limits` and retry logic
- Typed/BaseModel workers automatically use composition pattern for seamless compatibility

**Model Inheritance Support:**
- ✅ Worker + `morphic.Typed`: Full support for ALL modes (sync, thread, process, asyncio, ray)
- ✅ Worker + `pydantic.BaseModel`: Full support for ALL modes (sync, thread, process, asyncio, ray)
- ✅ `@morphic.validate` / `@pydantic.validate_call`: Works with ALL modes including Ray
- ✅ **Automatic Composition Wrapper**: Typed/BaseModel workers transparently use composition pattern

### Universal Composition Wrapper for Typed/BaseModel Workers

Concurry automatically applies a **composition wrapper** when workers inherit from `morphic.Typed` or `pydantic.BaseModel`. This provides seamless compatibility across ALL execution modes without requiring any code changes from users.

#### The Problems Being Solved

**Problem 1: Infrastructure Method Wrapping**

When retry logic is applied via `__getattribute__`, it can accidentally wrap Pydantic/Typed infrastructure methods:

```python
class MyWorker(Worker, Typed):
    name: str
    value: int
    
    def process(self, x: int) -> int:
        return x * self.value

worker = MyWorker.options(
    mode="thread",
    retry_config=RetryConfig(
        num_retries=3,
        retry_until=lambda result, **ctx: validate_result(result)
    )
).init(name="test", value=10)

# PROBLEM: Pydantic's post_set_validate_inputs() gets wrapped with retry logic!
# When setting attributes, retry_until is called with wrong signature
# Result: TypeError or unexpected retry behavior
```

**Problem 2: Ray Serialization Conflicts**

Ray's `ray.remote()` decorator conflicts with Pydantic's `__setattr__` implementation:

```python
class MyWorker(Worker, BaseModel):
    name: str
    value: int
    
    def process(self, x: int) -> int:
        return x * self.value

# PROBLEM: Ray wraps the class and modifies __setattr__
# This breaks Pydantic's frozen model validation
worker = MyWorker.options(mode="ray").init(name="test", value=10)
# Result: ValueError or serialization errors
```

#### How the Composition Wrapper Solves These Problems

Instead of using inheritance, the composition wrapper creates a **plain Python class** that holds the Typed/BaseModel worker internally and delegates only user-defined methods:

```python
# User writes this:
class MyWorker(Worker, Typed):
    name: str
    value: int
    
    def process(self, x: int) -> int:
        return x * self.value

# Concurry automatically transforms it to this (conceptually):
class MyWorker_CompositionWrapper(Worker):
    def __init__(self, name: str, value: int):
        self._wrapped_instance = MyWorker_Original(name=name, value=value)
    
    def process(self, x: int) -> int:
        # Delegate to wrapped instance
        return self._wrapped_instance.process(x)
    
    # Infrastructure methods (model_dump, model_validate, etc.) NOT exposed
```

**Benefits:**

1. **Infrastructure Isolation**: Pydantic methods never exposed at wrapper level → retry logic can't wrap them
2. **Ray Compatibility**: Wrapper is plain Python → no `__setattr__` conflicts with Ray
3. **Transparent to Users**: Workers behave identically, validation still works
4. **Consistent Behavior**: Same code path for all modes (sync, thread, process, asyncio, ray)
5. **Performance Optimized**: Method delegation uses captured closures to avoid repeated `getattr()` calls

#### When is the Composition Wrapper Applied?

The wrapper is applied automatically by `WorkerBuilder` when:

1. Worker class inherits from `morphic.Typed` OR `pydantic.BaseModel`
2. Check is performed at worker creation time (in `.init()`)
3. Applied for **ALL execution modes** (not just Ray)

**Detection Logic** (`_should_use_composition_wrapper`):

```python
def _should_use_composition_wrapper(worker_cls: Type) -> bool:
    """Check if worker needs composition wrapper.
    
    Note: Check Typed FIRST as it's a subclass of BaseModel.
    """
    # Check for Typed first (extends BaseModel)
    try:
        from morphic import Typed
        if isinstance(worker_cls, type) and issubclass(worker_cls, Typed):
            return True
    except ImportError:
        pass
    
    # Check for BaseModel
    try:
        from pydantic import BaseModel
        if isinstance(worker_cls, type) and issubclass(worker_cls, BaseModel):
            return True
    except ImportError:
        pass
    
    return False
```

#### Implementation Details

**Step 1: Wrapper Creation** (`_create_composition_wrapper`):

The wrapper is created dynamically at runtime:

```python
def _create_composition_wrapper(worker_cls: Type) -> Type:
    """Create composition wrapper for BaseModel/Typed workers."""
    from . import Worker as WorkerBase
    
    class CompositionWrapper(WorkerBase):
        """Auto-generated wrapper using composition pattern.
        
        Holds BaseModel/Typed instance internally and delegates
        user-defined method calls to it.
        """
        
        def __init__(self, *args, **kwargs):
            # Create wrapped instance with user's args/kwargs
            self._wrapped_instance = worker_cls(*args, **kwargs)
        
        def __getattr__(self, name: str):
            # Block infrastructure methods
            if _is_infrastructure_method(name):
                raise AttributeError(
                    f"Infrastructure method '{name}' not available. "
                    f"Only user-defined methods are exposed."
                )
            return getattr(self._wrapped_instance, name)
    
    # Copy user-defined methods to wrapper class
    import inspect
    for attr_name in dir(worker_cls):
        # Skip private methods and infrastructure methods
        if attr_name.startswith("_"):
            continue
        if _is_infrastructure_method(attr_name):
            continue
        if attr_name not in worker_cls.__dict__:
            continue  # Inherited from parent
        
        attr = getattr(worker_cls, attr_name)
        if not callable(attr) or isinstance(attr, type):
            continue
        
        # Create delegating method (with performance optimization)
        is_async = inspect.iscoroutinefunction(attr)
        setattr(CompositionWrapper, attr_name, 
                make_method(attr_name, is_async, attr))
    
    return CompositionWrapper
```

**Step 2: Infrastructure Method Detection** (`_is_infrastructure_method`):

To avoid wrapping Pydantic methods, we maintain a cached set of method names:

```python
def _is_infrastructure_method(
    method_name: str,
    _cache: Optional[Dict[str, Set[str]]] = None
) -> bool:
    """Check if method belongs to Typed or BaseModel infrastructure.
    
    Uses function-level caching via mutable default argument for O(1) lookup.
    Cache is populated on first call and reused for all subsequent calls.
    """
    if _cache is None:
        _cache = {}
    
    # Populate cache on first call
    if len(_cache) == 0:
        try:
            from morphic import Typed
            _cache["typed_methods"] = set(dir(Typed))
        except ImportError:
            _cache["typed_methods"] = set()
        
        try:
            from pydantic import BaseModel
            _cache["basemodel_methods"] = set(dir(BaseModel))
        except ImportError:
            _cache["basemodel_methods"] = set()
    
    # O(1) lookup in cached sets
    return (method_name in _cache["typed_methods"] or 
            method_name in _cache["basemodel_methods"])
```

**Step 3: Performance-Optimized Method Delegation**:

Critical optimization: Capture unbound method in closure to avoid `getattr()` on every call:

```python
def make_method(method_name, is_async, unbound_method):
    """Create delegating method with captured unbound method.
    
    OPTIMIZATION: Captures unbound_method in closure to avoid slow
    getattr(self._wrapped_instance, method_name) on every invocation.
    This saves ~200ns per call, critical for tight loops.
    """
    
    if is_async:
        async def async_delegating_method(self, *args, **kwargs):
            # Fast: Call unbound method with wrapped instance directly
            return await unbound_method(self._wrapped_instance, *args, **kwargs)
        
        async_delegating_method.__name__ = method_name
        return async_delegating_method
    else:
        def delegating_method(self, *args, **kwargs):
            # Fast: Call unbound method with wrapped instance directly
            return unbound_method(self._wrapped_instance, *args, **kwargs)
        
        delegating_method.__name__ = method_name
        return delegating_method
```

**Without optimization** (slow):
```python
def delegating_method(self, *args, **kwargs):
    method = getattr(self._wrapped_instance, method_name)  # ~200ns overhead!
    return method(*args, **kwargs)
```

**With optimization** (fast):
```python
def delegating_method(self, *args, **kwargs):
    return unbound_method(self._wrapped_instance, *args, **kwargs)  # Direct call
```

**Step 4: Limits Injection**:

The `limits` attribute must be accessible to user methods. Since user methods execute on `_wrapped_instance`, limits are set there:

```python
# In _create_worker_wrapper:
class WorkerWithLimitsAndRetry(worker_cls):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # ... create limit_pool ...
        
        # Check if this is a composition wrapper
        if hasattr(self, "_wrapped_instance"):
            # Set limits on wrapped instance (where user methods execute)
            object.__setattr__(self._wrapped_instance, "limits", limit_pool)
        else:
            # Set limits on self (plain worker)
            object.__setattr__(self, "limits", limit_pool)
```

**Why `object.__setattr__`?** Bypasses Pydantic's frozen model validation, allowing us to inject `limits` after construction.

#### Behavior and Edge Cases

**User-Defined Methods**:
```python
class MyWorker(Worker, Typed):
    name: str
    
    def process(self, x: int) -> int:
        return x * 2

worker = MyWorker.options(mode="thread").init(name="test")
result = worker.process(5).result()  # ✅ Works - delegates to wrapped instance
```

**Infrastructure Methods** (blocked at wrapper level):
```python
worker.model_dump()  # ❌ AttributeError: Infrastructure method not available
worker.model_validate({...})  # ❌ AttributeError
worker.__pydantic_fields__  # ❌ AttributeError
```

**Validation Still Works**:
```python
# Validation happens during __init__ of wrapped instance
worker = MyWorker.options(mode="thread").init(name=123)  
# ❌ ValidationError: name must be string
```

**Async Methods**:
```python
class AsyncWorker(Worker, Typed):
    name: str
    
    async def fetch(self, url: str) -> str:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                return await resp.text()

worker = AsyncWorker.options(mode="asyncio").init(name="fetcher")
result = worker.fetch("https://example.com").result()  # ✅ Works
```

**Accessing Validated Fields**:
```python
class MyWorker(Worker, Typed):
    multiplier: int
    
    def process(self, x: int) -> int:
        return x * self.multiplier  # ✅ Accesses validated field

worker = MyWorker.options(mode="ray").init(multiplier=3)
result = worker.process(5).result()  # Returns 15
```

**Limits Integration**:
```python
class MyWorker(Worker, Typed):
    name: str
    
    def process(self, x: int) -> int:
        with self.limits.acquire(requested={"tokens": 100}):
            # ✅ limits accessible via self
            return x * 2

worker = MyWorker.options(
    mode="ray",
    limits=[RateLimit(key="tokens", capacity=1000, window_seconds=1)]
).init(name="test")
```

#### Performance Characteristics

**Method Call Overhead**:

| Worker Type | Plain Worker | Composition Wrapper | Overhead |
|------------|--------------|---------------------|----------|
| Sync | 3.2µs/call | 3.2µs/call | 0% (optimized) |
| Thread | 77.9µs/call | 77.9µs/call | 0% (optimized) |
| Asyncio | 15.6µs/submit | 15.6µs/submit | 0% (optimized) |
| Ray | ~2ms/call | ~2ms/call | 0% (network dominates) |

**Why Zero Overhead?**

1. **Unbound Method Capture**: Avoids `getattr()` on every call (~200ns saved)
2. **Method Caching**: Wrapper methods created once, cached forever
3. **Direct Call**: `unbound_method(instance, *args)` is as fast as `instance.method(*args)`

**Memory Overhead**:

- One extra object per worker (`_wrapped_instance`)
- Negligible: ~64 bytes for object header + attributes

#### Comparison: Composition vs Inheritance

**With Composition** (current implementation):

```python
class MyWorker_CompositionWrapper(Worker):
    def __init__(self, name: str, value: int):
        self._wrapped_instance = MyWorker_Original(name, value)
    
    def process(self, x: int) -> int:
        return self._wrapped_instance.process(x)

# Retry logic applied via __getattribute__ on wrapper
# Only 'process' exposed → infrastructure methods safe
```

**Without Composition** (old approach, problematic):

```python
class MyWorker(Worker, Typed):
    name: str
    value: int
    
    def process(self, x: int) -> int:
        return x * self.value

# Retry logic applied via __getattribute__ on MyWorker
# ALL methods exposed → infrastructure methods get wrapped!
# Result: post_set_validate_inputs() wrapped with retry logic → crashes
```

#### Lifecycle Integration

The composition wrapper is applied early in the worker lifecycle:

```
User calls Worker.options(mode, ...).init(args, kwargs)
    ↓
WorkerBuilder created
    ↓
WorkerBuilder.init() called
    ↓
_apply_composition_wrapper_if_needed()
    ↓
├─ Check: _should_use_composition_wrapper(worker_cls)
│   ├─ Is subclass of Typed? → YES
│   └─ Is subclass of BaseModel? → YES
│   ↓
├─ Create wrapper: worker_cls = _create_composition_wrapper(worker_cls)
│   ├─ Create CompositionWrapper class
│   ├─ Copy user-defined methods with delegation
│   ├─ Block infrastructure methods
│   └─ Return wrapper class
│   ↓
└─ Continue with wrapped class
    ↓
_create_worker_wrapper(worker_cls, limits, retry)
    ↓
Create WorkerProxy (Thread/Process/Ray/etc.)
    ↓
Worker instance created from wrapped class
    ↓
User methods work transparently
```

#### Testing and Validation

The composition wrapper is tested comprehensively:

1. **Basic functionality**: Method calls work correctly
2. **Validation**: Pydantic validation errors raised properly
3. **Field access**: Validated fields accessible in methods
4. **Limits integration**: `self.limits` works as expected
5. **Worker pools**: Composition wrapper works with pools
6. **All modes**: Tested across sync, thread, process, asyncio, ray
7. **Edge cases**: Optional fields, defaults, constraints, hooks
8. **Performance**: Meets performance targets for tight loops

See `tests/core/worker/test_pydantic_integration.py` for comprehensive test coverage.

#### Why Universal (All Modes)?

Initially, the composition wrapper was Ray-specific to solve the serialization issue. However, applying it universally provides significant benefits:

1. **Consistent Behavior**: Same code path for all modes eliminates edge cases
2. **Simpler Logic**: No mode-specific branching in `_create_worker_wrapper`
3. **Infrastructure Isolation**: Prevents retry logic wrapping Pydantic methods in ALL modes
4. **Easier Maintenance**: One implementation to test and optimize
5. **Future-Proof**: Any mode that conflicts with Pydantic automatically works

The performance optimization (unbound method capture) ensures zero overhead, making the universal application practical.

### WorkerProxy Hierarchy

All WorkerProxy classes inherit from `WorkerProxy(Typed, ABC)`:

```
WorkerProxy (Typed, ABC)
├── SyncWorkerProxy           # Direct execution in current thread
├── ThreadWorkerProxy         # Thread + queue-based communication
├── ProcessWorkerProxy        # Process + multiprocessing queues + cloudpickle
├── AsyncioWorkerProxy        # Event loop + sync thread for mixed execution
└── RayWorkerProxy            # Ray actor + ObjectRef futures
```

**Common Interface:**
```python
class WorkerProxy(Typed, ABC):
    # Public configuration (immutable after creation)
    worker_cls: Type[Worker]
    blocking: bool
    unwrap_futures: bool
    init_args: tuple
    init_kwargs: dict
    limits: Optional[Any]          # LimitPool instance
    retry_config: Optional[Any]    # RetryConfig instance
    max_queued_tasks: Optional[int]
    mode: ClassVar[ExecutionMode]  # Set by subclass
    
    # Private attributes (mutable, not serialized)
    _stopped: bool = PrivateAttr(default=False)
    _options: dict = PrivateAttr(default_factory=dict)
    _method_cache: dict = PrivateAttr(default_factory=dict)
    _submission_semaphore: Optional[Any] = PrivateAttr(default=None)
    
    # Abstract methods that subclasses must implement
    def _execute_method(self, method_name: str, *args, **kwargs) -> BaseFuture:
        """Execute a worker method and return a future."""
        ...
    
    # Common behavior
    def __getattr__(self, name: str) -> Callable:
        """Intercept method calls and dispatch via _execute_method."""
        ...
    
    def stop(self, timeout: float = 30) -> None:
        """Stop the worker and clean up resources."""
        ...
    
    def __enter__(self) / __exit__(self):
        """Context manager support for automatic cleanup."""
        ...
```

**Key Implementation Rules:**

1. **Mode as ClassVar**: Each proxy sets `mode: ClassVar[ExecutionMode]` at class level, NOT passed as parameter
2. **Typed Configuration**: All config fields are immutable public attributes validated by Pydantic
3. **Private Attributes**: Use `PrivateAttr()` for mutable state, initialized in `post_initialize()`
4. **Method Caching**: `__getattr__` caches method wrappers in `_method_cache` for performance
5. **Submission Queue**: Use `_submission_semaphore` (BoundedSemaphore) to limit in-flight tasks per worker
6. **Future Unwrapping**: Automatically unwrap BaseFuture arguments before execution (unless `unwrap_futures=False`)

#### SyncWorkerProxy

**Execution Model**: Direct execution in current thread
**Future Type**: `SyncFuture` (caches result/exception at creation)

```python
class SyncWorkerProxy(WorkerProxy):
    mode: ClassVar[ExecutionMode] = ExecutionMode.Sync
    _worker: Any = PrivateAttr()  # Worker instance stored directly
    
    def _execute_method(self, method_name: str, *args, **kwargs) -> SyncFuture:
        method = getattr(self._worker, method_name)
        try:
            result = _invoke_function(method, *args, **kwargs)
            return SyncFuture(result_value=result)
        except Exception as e:
            return SyncFuture(exception_value=e)
```

**Characteristics:**
- No threads, no queues, no asynchronous communication
- Async functions executed via `asyncio.run()` (blocks until complete)
- Zero overhead for simple testing and debugging
- Submission queue bypassed (max_queued_tasks ignored)

#### ThreadWorkerProxy

**Execution Model**: Dedicated worker thread + command queue
**Future Type**: `ConcurrentFuture` (wraps `concurrent.futures.Future`)

```python
class ThreadWorkerProxy(WorkerProxy):
    mode: ClassVar[ExecutionMode] = ExecutionMode.Threads
    command_queue_timeout: confloat(ge=0)  # From global config
    
    _command_queue: Any = PrivateAttr()  # queue.Queue
    _futures: Dict[str, Any] = PrivateAttr()  # uuid -> ConcurrentFuture
    _futures_lock: Any = PrivateAttr()  # threading.Lock
    _thread: Any = PrivateAttr()  # threading.Thread
```

**Architecture:**
1. Main thread (client): Submits commands to queue, returns future immediately
2. Worker thread: Processes commands from queue, sets results on futures

**Communication Flow:**
```
Client Thread                    Worker Thread
    │                                │
    │ 1. Create future               │
    │ 2. Store in _futures dict      │
    │ 3. Put (uuid, method, args)    │
    ├───────────────────────────────>│
    │ 4. Return future               │ 5. Get command from queue
    │                                │ 6. Execute method
    │                                │ 7. Set result on future
    │                                │ 8. Remove from _futures dict
    │ 9. future.result() blocks      │
    │    until worker sets result    │
```

**Characteristics:**
- Async functions executed via `asyncio.run()` in worker thread (no concurrency)
- Command queue timeout checked via `queue.get(timeout=command_queue_timeout)`
- Futures tracked in dict for cancellation on `stop()`

#### ProcessWorkerProxy

**Execution Model**: Separate process + multiprocessing queues + cloudpickle serialization
**Future Type**: `ConcurrentFuture` (wraps `concurrent.futures.Future`)

```python
class ProcessWorkerProxy(WorkerProxy):
    mode: ClassVar[ExecutionMode] = ExecutionMode.Processes
    mp_context: Literal["fork", "spawn", "forkserver"] = "fork"
    result_queue_timeout: confloat(ge=0)
    result_queue_cleanup_timeout: confloat(ge=0)
    
    _command_queue: Any = PrivateAttr()  # mp.Queue
    _result_queue: Any = PrivateAttr()   # mp.Queue
    _futures: dict = PrivateAttr()       # uuid -> PyFuture
    _futures_lock: Any = PrivateAttr()   # threading.Lock
    _process: Any = PrivateAttr()        # mp.Process
    _result_thread: Any = PrivateAttr()  # threading.Thread
```

**Architecture:**
1. Main process (client): Sends commands to `_command_queue`
2. Worker process: Executes commands, sends results to `_result_queue`
3. Result thread: Reads from `_result_queue`, sets results on futures

**Communication Flow:**
```
Main Process                Worker Process         Result Thread
    │                           │                      │
    │ 1. Serialize args         │                      │
    │ 2. Put command in queue   │                      │
    ├──────────────────────────>│                      │
    │ 3. Return future          │ 4. Get command       │
    │                           │ 5. Deserialize       │
    │                           │ 6. Execute method    │
    │                           │ 7. Serialize result  │
    │                           │ 8. Put in result_queue
    │                           ├─────────────────────>│
    │                           │                      │ 9. Get result
    │                           │                      │ 10. Deserialize
    │                           │                      │ 11. Set on future
    │ 12. future.result()       │                      │
```

**Characteristics:**
- **Worker class serialization**: Uses `cloudpickle.dumps()` to serialize worker class
- **Async functions**: Executed via `asyncio.run()` in worker process (no concurrency)
- **Exception preservation**: Original exception types preserved across process boundary
- **Separate result thread**: Required because `Queue.get()` from another process blocks
- **Multiprocessing context**: Supports fork, spawn, or forkserver

**Critical Serialization Details:**
- Worker class is serialized ONCE at proxy creation
- Limits passed as list of Limit objects (or LimitPool), recreated inside worker process
- RetryConfig serialized and passed to worker process
- Args/kwargs serialized per method call

#### AsyncioWorkerProxy

**Execution Model**: Event loop thread + dedicated sync thread for sync methods
**Future Type**: `ConcurrentFuture` (wraps `concurrent.futures.Future`)

```python
class AsyncioWorkerProxy(WorkerProxy):
    mode: ClassVar[ExecutionMode] = ExecutionMode.Asyncio
    loop_ready_timeout: confloat(ge=0)
    thread_ready_timeout: confloat(ge=0)
    sync_queue_timeout: confloat(ge=0)
    
    _loop: Any = PrivateAttr(default=None)       # asyncio.EventLoop
    _worker: Any = PrivateAttr(default=None)      # Worker instance
    _loop_thread: Any = PrivateAttr()             # threading.Thread (runs event loop)
    _sync_thread: Any = PrivateAttr()             # threading.Thread (runs sync methods)
    _sync_queue: Any = PrivateAttr()              # queue.Queue (for sync methods)
    _futures: Dict[str, Any] = PrivateAttr()      # uuid -> ConcurrentFuture
```

**Architecture:**
1. Event loop thread: Runs `asyncio` event loop for async methods
2. Sync worker thread: Executes sync methods without blocking event loop
3. Main thread: Routes method calls to appropriate thread

**Method Routing:**
```python
def _execute_method(self, method_name, *args, **kwargs):
    method = getattr(self._worker, method_name)
    is_async = asyncio.iscoroutinefunction(method)
    
    if is_async:
        # Route to event loop for concurrent execution
        self._loop.call_soon_threadsafe(schedule_async_task)
    else:
        # Route to sync thread to avoid blocking event loop
        self._sync_queue.put((future, method_name, args, kwargs))
```

**Characteristics:**
- **True async concurrency**: Multiple async methods can run concurrently in event loop
- **Sync method isolation**: Sync methods don't block event loop
- **Best for I/O-bound async**: HTTP requests, database queries, WebSocket connections
- **10-50x speedup**: For concurrent I/O operations vs sequential execution
- **~13% overhead**: For sync methods vs ThreadWorker (minimal impact)

**Performance Comparison (30 HTTP requests, 50ms latency each):**
- SyncWorker: 1.66s (sequential)
- ThreadWorker: 1.66s (sequential)
- ProcessWorker: 1.67s (sequential)
- **AsyncioWorker: 0.16s (concurrent)** ✅ 10x faster!

#### RayWorkerProxy

**Execution Model**: Ray actor (distributed process) + ObjectRef futures
**Future Type**: `RayFuture` (wraps Ray ObjectRef)

```python
class RayWorkerProxy(WorkerProxy):
    mode: ClassVar[ExecutionMode] = ExecutionMode.Ray
    actor_options: Optional[Dict[str, Any]] = None  # Ray resource options
    
    _ray_actor: Any = PrivateAttr()              # Ray actor handle
    _futures: Dict[str, Any] = PrivateAttr()      # uuid -> RayFuture
    _futures_lock: Any = PrivateAttr()            # threading.Lock
```

**Architecture:**
1. Client process: Holds actor handle, submits method calls
2. Ray actor: Separate process (possibly remote machine), executes methods
3. Ray cluster: Manages scheduling, data transfer, fault tolerance

**Communication Flow:**
```
Client Process              Ray Actor              Ray Cluster
    │                          │                       │
    │ 1. Get actor handle      │                       │
    │ 2. actor.method.remote() │                       │
    ├─────────────────────────────────────────────────>│
    │ 3. Return ObjectRef      │                       │ 4. Schedule task
    │                          │<──────────────────────┤
    │                          │ 5. Execute method     │
    │                          │ 6. Store result       │
    │                          ├──────────────────────>│
    │ 7. ray.get(ObjectRef)    │                       │ 8. Retrieve result
    │<─────────────────────────────────────────────────┤
```

**Characteristics:**
- **Zero-copy optimization**: RayFuture → ObjectRef passed directly (no serialization)
- **Cross-worker futures**: Other BaseFuture types materialized before passing
- **Native async support**: Ray handles async methods automatically
- **Resource allocation**: Via `actor_options={"num_cpus": 2, "num_gpus": 1, "resources": {...}}`
- **Distributed execution**: Actor can run on any node in Ray cluster
- **Fault tolerance**: Ray handles actor failures and restarts

**Future Unwrapping with Zero-Copy:**
```python
def _unwrap_future_for_ray(obj):
    if isinstance(obj, RayFuture):
        return obj._object_ref  # Zero-copy: pass ObjectRef directly
    elif isinstance(obj, BaseFuture):
        return obj.result()  # Cross-worker: materialize value
    return obj
```

**Retry Logic for Ray:**
Ray actors bypass `__getattribute__`, so retry logic must be pre-applied to methods at class level:
```python
worker_cls_to_use = _create_worker_wrapper(
    self.worker_cls, 
    self.limits, 
    self.retry_config, 
    for_ray=True  # Pre-wrap methods at class level
)
```

### WorkerProxyPool Hierarchy

All WorkerProxyPool classes inherit from `WorkerProxyPool(Typed, ABC)`:

```
WorkerProxyPool (Typed, ABC)
├── InMemoryWorkerProxyPool       # Sync, Thread, Asyncio workers
├── MultiprocessWorkerProxyPool   # Process workers
└── RayWorkerProxyPool            # Ray workers
```

**Common Interface:**
```python
class WorkerProxyPool(Typed, ABC):
    # Public configuration (immutable after creation)
    worker_cls: Type[Worker]
    mode: ExecutionMode
    max_workers: int
    load_balancing: LoadBalancingAlgorithm
    on_demand: bool
    blocking: bool
    unwrap_futures: bool
    limits: Optional[Any]  # Shared LimitPool
    init_args: tuple
    init_kwargs: dict
    on_demand_cleanup_timeout: confloat(ge=0)
    on_demand_slot_max_wait: confloat(ge=0)
    max_queued_tasks: Optional[int]
    retry_config: Optional[Any]
    
    # Private attributes
    _load_balancer: Any = PrivateAttr()
    _workers: List[Any] = PrivateAttr()
    _stopped: bool = PrivateAttr()
    _method_cache: Dict[str, Callable] = PrivateAttr()
    _on_demand_workers: List[Any] = PrivateAttr()
    _on_demand_lock: Any = PrivateAttr()
    _on_demand_counter: int = PrivateAttr()
    _worker_semaphores: List[Any] = PrivateAttr()
    
    # Abstract methods
    def _initialize_pool(self) -> None:
        """Create all workers (or prepare for on-demand)."""
        ...
    
    def _create_worker(self, worker_index: int) -> Any:
        """Create a single worker with unique index."""
        ...
    
    def _get_on_demand_limit(self) -> Optional[int]:
        """Get max concurrent on-demand workers."""
        ...
    
    # Common behavior
    def __getattr__(self, name: str) -> Callable:
        """Intercept method calls and dispatch to load-balanced worker."""
        ...
    
    def get_pool_stats(self) -> dict:
        """Get pool statistics."""
        ...
    
    def stop(self, timeout: float = 30) -> None:
        """Stop all workers in pool."""
        ...
```

**Key Architecture Decisions:**

1. **Client-Side Pool**: Pool lives on client, manages remote workers (not a remote actor itself)
2. **Load Balancer**: Selects worker index, tracks active/total calls per worker
3. **Per-Worker Queues**: Each worker has independent submission semaphore
4. **Shared Limits**: All workers share same LimitSet instances
5. **On-Demand Workers**: Created per request, destroyed after completion
6. **Worker Indices**: Sequential indices (0, 1, 2, ...) for round-robin in LimitPool

#### Load Balancing

Implemented via `BaseLoadBalancer` subclasses:
- **RoundRobin**: Distribute requests evenly in circular fashion
- **LeastActiveLoad**: Select worker with fewest active (in-flight) calls
- **LeastTotalLoad**: Select worker with fewest total (lifetime) calls
- **Random**: Random worker selection (best for on-demand)

**Load Balancer Lifecycle:**
```python
def method_wrapper(*args, **kwargs):
    # 1. Select worker
    worker_idx = self._load_balancer.select_worker(len(self._workers))
    
    # 2. Acquire worker's submission semaphore (blocks if queue full)
    self._worker_semaphores[worker_idx].acquire()
    
    # 3. Record start
    self._load_balancer.record_start(worker_idx)
    
    # 4. Execute method
    result = getattr(self._workers[worker_idx], name)(*args, **kwargs)
    
    # 5. Wrap future to release semaphore and record completion
    return self._wrap_future_with_tracking(result, worker_idx)
```

**Future Wrapping for Semaphore Release:**
```python
def _wrap_future_with_tracking(self, future, worker_idx):
    def on_complete(f):
        self._load_balancer.record_complete(worker_idx)
        self._worker_semaphores[worker_idx].release()
    
    future.add_done_callback(on_complete)
    return future
```

#### On-Demand Workers

**Lifecycle:**
1. **Creation**: New worker created per request
2. **Execution**: Single method call
3. **Cleanup**: Worker stopped after result available
4. **Tracking**: Stored in `_on_demand_workers` list during execution

**Concurrency Limits:**
- Thread: `max(1, cpu_count() - 1)`
- Process: `max(1, cpu_count() - 1)`
- Ray: Unlimited (cluster manages resources)

**Cleanup Strategy:**
```python
def _wrap_future_with_cleanup(self, future, worker):
    def cleanup_callback(f):
        # Schedule cleanup in separate thread to avoid deadlock
        def deferred_cleanup():
            worker.stop(timeout=self.on_demand_cleanup_timeout)
        
        threading.Thread(target=deferred_cleanup, daemon=True).start()
    
    future.add_done_callback(cleanup_callback)
    return future
```

**Critical**: Cleanup must happen in separate thread to avoid deadlock. Calling `worker.stop()` from within a callback can cause deadlocks because `stop()` may try to cancel futures that are invoking this callback.

### WorkerBuilder

WorkerBuilder is the factory that creates workers or pools based on configuration:

```python
class WorkerBuilder(Typed):
    # Public configuration
    worker_cls: Type["Worker"]
    mode: ExecutionMode
    blocking: bool
    max_workers: Optional[int]
    load_balancing: Optional[LoadBalancingAlgorithm]
    on_demand: bool
    max_queued_tasks: Optional[int]
    num_retries: int
    retry_on: Optional[Any]
    retry_algorithm: RetryAlgorithm
    retry_wait: float
    retry_jitter: float
    retry_until: Optional[Any]
    options: dict
    
    def init(self, *args, **kwargs) -> Union[WorkerProxy, WorkerProxyPool]:
        if self._should_create_pool():
            return self._create_pool(args, kwargs)
        else:
            return self._create_single_worker(args, kwargs)
```

**Responsibilities:**
1. Validate configuration (max_workers, on_demand compatibility)
2. Apply defaults from global config
3. Process limits parameter (create LimitPool)
4. Create retry config if num_retries > 0
5. Check Ray + Pydantic incompatibility
6. Decide single worker vs pool
7. Instantiate appropriate proxy/pool class

**Limits Processing:**
```python
def _transform_worker_limits(limits, mode, is_pool, worker_index):
    if limits is None:
        return empty LimitPool
    if isinstance(limits, LimitPool):
        return limits
    if isinstance(limits, list) and all isinstance(Limit):
        return LimitPool([LimitSet(limits, shared=is_pool, mode)])
    if isinstance(limits, list) and all isinstance(BaseLimitSet):
        return LimitPool(limits)  # Multi-region limits
    if isinstance(limits, BaseLimitSet):
        if not limits.shared and is_pool:
            raise ValueError("Pool requires shared=True")
        return LimitPool([limits])
```

**Worker Wrapping:**
```python
def _create_worker_wrapper(worker_cls, limits, retry_config, for_ray=False):
    class WorkerWithLimitsAndRetry(worker_cls):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            
            # Set limits (create LimitPool from list if needed)
            if isinstance(limits, list):
                limit_set = LimitSet(limits, shared=False, mode=Sync)
                limit_pool = LimitPool([limit_set])
            else:
                limit_pool = limits
            
            object.__setattr__(self, "limits", limit_pool)
        
        def __getattribute__(self, name):
            attr = super().__getattribute__(name)
            
            if for_ray:
                # Ray: Pre-wrap methods at class level
                return attr
            
            if has_retry and not name.startswith("_") and callable(attr):
                return create_retry_wrapper(attr, retry_config)
            
            return attr
    
    if for_ray and has_retry:
        # Pre-wrap all methods at class level for Ray
        for method_name in dir(worker_cls):
            if not method_name.startswith("_"):
                method = getattr(worker_cls, method_name)
                if callable(method):
                    wrapped = create_retry_wrapper(method, retry_config)
                    setattr(WorkerWithLimitsAndRetry, method_name, wrapped)
    
    return WorkerWithLimitsAndRetry
```

## Execution Modes

| Mode | Worker Proxy | Pool Support | Concurrency | Serialization | Best For |
|------|--------------|--------------|-------------|---------------|----------|
| `sync` | SyncWorkerProxy | No | None | None | Testing, debugging |
| `thread` | ThreadWorkerProxy | Yes | Thread-level | None (shared memory) | I/O-bound tasks |
| `process` | ProcessWorkerProxy | Yes | Process-level | cloudpickle | CPU-bound tasks |
| `asyncio` | AsyncioWorkerProxy | No | Event loop | None (shared memory) | Async I/O (HTTP, DB) |
| `ray` | RayWorkerProxy | Yes | Distributed | Ray serialization | Distributed computing |

**Default max_workers (pools):**
- Sync: 1 (fixed)
- Asyncio: 1 (fixed)
- Thread: 24
- Process: 4
- Ray: 0 (unlimited on-demand)

**Default load_balancing:**
- Persistent pools: `round_robin`
- On-demand pools: `random`

**Default max_queued_tasks (submission queue):**
- Sync: None (bypassed)
- Asyncio: None (bypassed)
- Thread: 100
- Process: 5
- Ray: 2

## Worker Lifecycle

### Initialization

```
User calls Worker.options(mode, ...).init(args, kwargs)
    ↓
WorkerBuilder created with configuration
    ↓
WorkerBuilder.init() called
    ↓
├─ If max_workers=1 or None: _create_single_worker()
│   ↓
│   1. Select appropriate WorkerProxy class
│   2. Process limits → LimitPool
│   3. Create retry config
│   4. Instantiate proxy
│   5. proxy.post_initialize() called
│   6. Worker wrapper created (_create_worker_wrapper)
│   7. Worker instance created with user args/kwargs
│   8. Limits and retry logic injected
│   9. Return proxy
│
└─ If max_workers>1 or on_demand: _create_pool()
    ↓
    1. Select appropriate WorkerProxyPool class
    2. Process limits → shared LimitPool
    3. Create retry config
    4. Instantiate pool
    5. pool.post_initialize() called
    6. Load balancer created
    7. For persistent pools: _initialize_pool()
       └─ Create N workers with sequential indices
    8. Return pool
```

### Method Execution (Single Worker)

```
user calls worker.method(args, kwargs)
    ↓
WorkerProxy.__getattr__("method") intercepts
    ↓
Check _method_cache for cached wrapper
    ↓
If not cached, create method_wrapper:
    ↓
    1. Check if stopped
    2. Acquire submission semaphore (if configured)
    3. Check stopped again (atomic with semaphore)
    4. Call _execute_method(name, args, kwargs)
    5. Wrap future to release semaphore on completion
    6. Return future (or result if blocking=True)
    ↓
Cache wrapper in _method_cache
    ↓
Return wrapper to user
    ↓
User calls wrapper(args) → future returned
    ↓
User calls future.result() → blocks until complete
```

### Method Execution (Pool)

```
user calls pool.method(args, kwargs)
    ↓
WorkerProxyPool.__getattr__("method") intercepts
    ↓
Check _method_cache for cached wrapper
    ↓
If not cached, create method_wrapper:
    ↓
    ├─ If on_demand:
    │   1. Wait for on-demand slot (blocks if limit reached)
    │   2. Check if stopped
    │   3. Increment counter, get worker_index
    │   4. Create worker with _create_worker(worker_index)
    │   5. Track in _on_demand_workers
    │   6. Call worker.method(args, kwargs)
    │   7. Wrap future for cleanup after completion
    │   8. Return future (or result if blocking=True)
    │
    └─ If persistent:
        1. Check workers exist and not stopped
        2. Select worker: idx = load_balancer.select_worker(N)
        3. Acquire worker's submission semaphore (blocks if full)
        4. Check stopped again (atomic with semaphore)
        5. Record start: load_balancer.record_start(idx)
        6. Call worker.method(args, kwargs)
        7. Wrap future to:
           - Release worker's semaphore
           - Record completion in load balancer
        8. Return future (or result if blocking=True)
```

### Shutdown

```
user calls worker.stop() or pool.stop()
    ↓
Set _stopped = True
    ↓
Cancel all pending futures
    ↓
├─ Single Worker:
│   └─ Mode-specific cleanup:
│       - Sync: No-op
│       - Thread: Put None in queue, join thread
│       - Process: Put None in queue, join process + result thread
│       - Asyncio: Stop sync thread, stop event loop
│       - Ray: ray.kill(actor)
│
└─ Pool:
    1. Stop all persistent workers
    2. Stop all on-demand workers
    3. Clear worker lists
    4. (Workers handle their own cleanup)
```

### Context Manager

```python
with Worker.options(mode="thread").init() as worker:
    result = worker.method().result()
# worker.stop() called automatically
```

## Pool Architecture

### Persistent Pool

```
Client Process
│
├─ WorkerProxyPool (client-side)
│   ├─ LoadBalancer (round-robin, least-active, etc.)
│   ├─ Shared LimitPool (all workers share)
│   ├─ Worker 0 (WorkerProxy, index=0)
│   │   ├─ Submission Semaphore (max_queued_tasks)
│   │   ├─ LimitPool (copy of shared, offset by index=0)
│   │   └─ Worker instance (with limits, retry)
│   ├─ Worker 1 (WorkerProxy, index=1)
│   │   ├─ Submission Semaphore
│   │   ├─ LimitPool (copy of shared, offset by index=1)
│   │   └─ Worker instance
│   └─ Worker N-1 (WorkerProxy, index=N-1)
│       ├─ Submission Semaphore
│       ├─ LimitPool (copy of shared, offset by index=N-1)
│       └─ Worker instance
│
└─ Method calls dispatched via LoadBalancer
```

**Key Characteristics:**
- All workers created at initialization
- Load balancer distributes calls
- Each worker has own submission semaphore
- All workers share same LimitSet instances (via LimitPool)
- Worker indices used for round-robin in LimitPool

### On-Demand Pool

```
Client Process
│
├─ WorkerProxyPool (client-side)
│   ├─ LoadBalancer (typically Random)
│   ├─ Shared LimitPool (all workers share)
│   ├─ _on_demand_workers list (tracks active ephemeral workers)
│   ├─ _on_demand_lock (thread-safe access to list)
│   └─ _on_demand_counter (sequential indices)
│
├─ On method call:
│   1. Wait for slot (if limit enforced)
│   2. Create worker with unique index
│   3. Add to _on_demand_workers
│   4. Execute method
│   5. Wrap future for cleanup
│
└─ On future completion:
    1. Callback triggers
    2. Schedule deferred cleanup (separate thread)
    3. Call worker.stop()
    4. Remove from _on_demand_workers
```

**Key Characteristics:**
- No persistent workers
- Workers created per request
- Workers destroyed after completion
- Cleanup happens in separate thread (avoid deadlock)
- Concurrency limited by `_get_on_demand_limit()`
- Random load balancing (default)

## Critical Implementation Details

### 1. Typed Integration

All proxies and pools inherit from `morphic.Typed`:
- Public fields are immutable and validated
- Private attributes use `PrivateAttr()`
- `post_initialize()` called after validation
- `object.__setattr__()` used to set private attrs during initialization

```python
class WorkerProxy(Typed, ABC):
    worker_cls: Type[Worker]  # Immutable public field
    _stopped: bool = PrivateAttr(default=False)  # Mutable private attr
    
    def post_initialize(self) -> None:
        # Use object.__setattr__ to bypass frozen model
        object.__setattr__(self, "_stopped", False)
        object.__setattr__(self, "_method_cache", {})
```

### 2. Mode as ClassVar

Each proxy sets `mode: ClassVar[ExecutionMode]` at class level:
```python
class ThreadWorkerProxy(WorkerProxy):
    mode: ClassVar[ExecutionMode] = ExecutionMode.Threads
```

This avoids passing mode as a parameter, reducing serialization size.

### 3. Submission Queue (max_queued_tasks)

Client-side semaphore limits in-flight tasks per worker:
```python
# In WorkerProxy.post_initialize():
if self.max_queued_tasks is not None:
    self._submission_semaphore = threading.BoundedSemaphore(self.max_queued_tasks)

# In WorkerProxy.__getattr__():
if self._submission_semaphore:
    self._submission_semaphore.acquire()  # Blocks if queue full

# Wrap future to release on completion:
def on_complete(f):
    self._submission_semaphore.release()

future.add_done_callback(on_complete)
```

**Purpose**: Prevent memory exhaustion from thousands of pending futures, especially for Ray actors.

**Bypassed for**:
- Sync mode (immediate execution)
- Asyncio mode (event loop handles concurrency)
- Blocking mode (sequential execution)
- On-demand workers (pool already limits concurrency)

### 4. Future Unwrapping

Automatically unwrap BaseFuture arguments before execution (unless `unwrap_futures=False`):
```python
def _unwrap_futures_in_args(args, kwargs, unwrap_futures):
    if not unwrap_futures:
        return args, kwargs
    
    # Fast-path: check if any futures or collections present
    has_future_or_collection = ...
    if not has_future_or_collection:
        return args, kwargs  # No unwrapping needed
    
    # Recursively unwrap using morphic.map_collection
    unwrapped_args = tuple(
        map_collection(arg, _unwrap_future_value, recurse=True) 
        for arg in args
    )
    ...
```

**Ray Zero-Copy Optimization**:
```python
def _unwrap_future_for_ray(obj):
    if isinstance(obj, RayFuture):
        return obj._object_ref  # Zero-copy!
    elif isinstance(obj, BaseFuture):
        return obj.result()  # Materialize
    return obj
```

### 5. Worker Wrapper Creation

`_create_worker_wrapper()` injects limits and retry logic:
```python
def _create_worker_wrapper(worker_cls, limits, retry_config, for_ray=False):
    if not retry_config or retry_config.num_retries == 0:
        # Only limits, no retry
        class WorkerWithLimits(worker_cls):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                
                # Create LimitPool from list if needed
                if isinstance(limits, list):
                    limit_set = LimitSet(limits, shared=False, mode=Sync)
                    limit_pool = LimitPool([limit_set])
                else:
                    limit_pool = limits
                
                # Use object.__setattr__ to bypass frozen Pydantic models
                object.__setattr__(self, "limits", limit_pool)
        
        return WorkerWithLimits
    
    # With retry logic
    class WorkerWithLimitsAndRetry(worker_cls):
        def __init__(self, *args, **kwargs):
            # Same as above for limits
            ...
        
        def __getattribute__(self, name):
            attr = super().__getattribute__(name)
            
            if not for_ray and not name.startswith("_") and callable(attr):
                # Wrap method with retry logic
                return create_retry_wrapper(attr, retry_config, ...)
            
            return attr
    
    if for_ray:
        # Pre-wrap methods at class level (Ray bypasses __getattribute__)
        for method_name in dir(worker_cls):
            if not method_name.startswith("_"):
                method = getattr(worker_cls, method_name)
                if callable(method):
                    wrapped = create_retry_wrapper(method, retry_config, ...)
                    setattr(WorkerWithLimitsAndRetry, method_name, wrapped)
    
    return WorkerWithLimitsAndRetry
```

**Key Points**:
- Always sets `self.limits` (even if empty LimitPool)
- Uses `object.__setattr__()` to support frozen Pydantic models
- For Ray: Pre-wraps methods at class level (bypasses `__getattribute__`)
- For other modes: Wraps methods dynamically via `__getattribute__`

### 6. Load Balancing State

Load balancer tracks per-worker statistics:
```python
class LeastActiveLoadBalancer(BaseLoadBalancer):
    _active_calls: Dict[int, int]  # worker_id -> active count
    _total_dispatched: int
    _lock: threading.Lock
    
    def select_worker(self, num_workers):
        with self._lock:
            # Find worker with minimum active calls
            min_active = min(self._active_calls.values())
            for i in range(num_workers):
                if self._active_calls[i] == min_active:
                    return i
    
    def record_start(self, worker_id):
        with self._lock:
            self._active_calls[worker_id] += 1
            self._total_dispatched += 1
    
    def record_complete(self, worker_id):
        with self._lock:
            self._active_calls[worker_id] -= 1
```

### 7. Shared Limits Across Pool

All workers in pool share same LimitSet instances:
```python
# In WorkerBuilder._create_pool():
limits = _transform_worker_limits(
    limits=self.options.get("limits"),
    mode=execution_mode,
    is_pool=True,  # Creates shared LimitSet
    worker_index=0  # Placeholder
)

# In WorkerProxyPool._create_worker():
worker_limits = _transform_worker_limits(
    limits=self.limits,  # Shared LimitSet
    mode=self.mode,
    is_pool=False,
    worker_index=i  # Unique index for round-robin
)

# Each worker gets a LimitPool with:
# - Same LimitSet instances (shared state)
# - Unique worker_index (for round-robin offset)
```

## Adding New Worker Types

To add a new execution mode (e.g., `Dask`, `Celery`):

### 1. Create WorkerProxy Subclass

```python
class DaskWorkerProxy(WorkerProxy):
    # Set mode at class level
    mode: ClassVar[ExecutionMode] = ExecutionMode.Dask
    
    # Add mode-specific config fields
    dask_scheduler: str
    
    # Add private attributes
    _dask_future: Any = PrivateAttr()
    _client: Any = PrivateAttr()
    
    def post_initialize(self) -> None:
        super().post_initialize()
        
        # Initialize Dask client
        import dask.distributed
        self._client = dask.distributed.Client(self.dask_scheduler)
        
        # Create worker wrapper
        worker_cls = _create_worker_wrapper(
            self.worker_cls, 
            self.limits, 
            self.retry_config
        )
        
        # Submit worker to Dask cluster
        self._dask_future = self._client.submit(worker_cls, ...)
    
    def _execute_method(self, method_name, *args, **kwargs):
        # Submit method call to Dask worker
        dask_future = self._client.submit(
            lambda w: getattr(w, method_name)(*args, **kwargs),
            self._dask_future
        )
        return DaskFuture(dask_future=dask_future)
    
    def stop(self, timeout=30):
        super().stop(timeout)
        self._client.close()
```

### 2. Create Future Subclass

```python
class DaskFuture(BaseFuture):
    __slots__ = ("_dask_future",)
    FUTURE_UUID_PREFIX = "dask-"
    
    def __init__(self, dask_future):
        super().__init__()
        self._dask_future = dask_future
    
    def result(self, timeout=None):
        return self._dask_future.result(timeout=timeout)
    
    def done(self):
        return self._dask_future.done()
    
    def cancel(self):
        return self._dask_future.cancel()
    
    # ... implement other BaseFuture methods
```

### 3. Create Pool Subclass (if supported)

```python
class DaskWorkerProxyPool(WorkerProxyPool):
    dask_scheduler: str
    
    def _initialize_pool(self):
        for i in range(self.max_workers):
            worker = self._create_worker(worker_index=i)
            self._workers.append(worker)
    
    def _create_worker(self, worker_index=0):
        # Process limits with worker_index
        worker_limits = _transform_worker_limits(
            limits=self.limits,
            mode=self.mode,
            is_pool=False,
            worker_index=worker_index
        )
        
        return DaskWorkerProxy(
            worker_cls=self.worker_cls,
            dask_scheduler=self.dask_scheduler,
            limits=worker_limits,
            ...
        )
    
    def _get_on_demand_limit(self):
        return None  # Dask manages resources
```

### 4. Update ExecutionMode Enum

```python
class ExecutionMode(AutoEnum):
    Sync = alias("sync")
    Threads = alias("thread", "threads")
    Processes = alias("process", "processes")
    Asyncio = alias("asyncio", "async")
    Ray = alias("ray")
    Dask = alias("dask")  # New!
```

### 5. Update WorkerBuilder

```python
# In WorkerBuilder._create_single_worker():
elif execution_mode == ExecutionMode.Dask:
    from .dask_worker import DaskWorkerProxy
    proxy_cls = DaskWorkerProxy

# In WorkerBuilder._create_pool():
elif execution_mode == ExecutionMode.Dask:
    pool_cls = DaskWorkerProxyPool
```

### 6. Update wrap_future()

```python
def wrap_future(future):
    # ... existing checks ...
    elif hasattr(future, "_dask_future"):
        return future  # Already a DaskFuture
    # ... try to import Dask and check type ...
```

### 7. Add Configuration Defaults

```python
class GlobalDefaults(Typed):
    # ... existing fields ...
    
    class Dask(Typed):
        blocking: bool = False
        max_workers: int = 8
        load_balancing: LoadBalancingAlgorithm = LoadBalancingAlgorithm.RoundRobin
        max_queued_tasks: Optional[int] = 10
        # ... other Dask-specific defaults ...
    
    dask: Dask = Dask()
```

### 8. Add Tests

```python
class TestDaskWorker:
    def test_basic_execution(self):
        worker = SimpleWorker.options(mode="dask").init()
        result = worker.compute(5).result()
        assert result == expected
        worker.stop()
    
    def test_dask_pool(self):
        pool = SimpleWorker.options(
            mode="dask", 
            max_workers=4
        ).init()
        results = [pool.compute(i).result() for i in range(10)]
        assert len(results) == 10
        pool.stop()
```

## Limitations and Gotchas

### 1. Typed/BaseModel Workers and Infrastructure Methods

**Note**: This is NOT a limitation anymore! As of the Universal Composition Wrapper implementation, workers inheriting from `morphic.Typed` or `pydantic.BaseModel` work seamlessly across **ALL execution modes** including Ray.

**What Changed**:
- **Before**: Ray + Typed/BaseModel raised `ValueError` due to serialization conflicts
- **After**: Automatic composition wrapper enables Ray support with zero code changes

**Example** (now works in all modes):
```python
class MyWorker(Worker, Typed):
    name: str
    value: int
    
    def process(self, x: int) -> int:
        return x * self.value

# ✅ Works in ALL modes (sync, thread, process, asyncio, ray)
worker = MyWorker.options(mode="ray").init(name="test", value=10)
result = worker.process(5).result()  # Returns 50
```

See the [Universal Composition Wrapper](#universal-composition-wrapper-for-typedbasemodel-workers) section for implementation details.

### 2. Async Functions in Non-Asyncio Modes

**Limitation**: Async functions work but don't provide concurrency benefits

**Why**: Other modes use `asyncio.run()` which blocks until completion

**Example**:
```python
class APIWorker(Worker):
    async def fetch(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                return await resp.text()

# ThreadWorker: Each fetch() blocks the thread
worker = APIWorker.options(mode="thread").init()
urls = [f"http://api.example.com/{i}" for i in range(10)]
futures = [worker.fetch(url) for url in urls]  # Sequential, ~10 seconds

# AsyncioWorker: fetch() runs concurrently in event loop
worker = APIWorker.options(mode="asyncio").init()
futures = [worker.fetch(url) for url in urls]  # Concurrent, ~1 second
```

**Best Practice**: Use `mode="asyncio"` for async I/O-bound tasks.

### 3. Submission Queue vs Resource Limits

**Two separate mechanisms**:
1. **Submission Queue** (`max_queued_tasks`): Client-side, limits pending futures
2. **Resource Limits** (`limits`): Worker-side, limits concurrent operations

**Example**:
```python
# Submission queue: Max 10 futures in-flight
# Resource limit: Max 5 concurrent executions
worker = MyWorker.options(
    mode="ray",
    max_queued_tasks=10,
    limits=[ResourceLimit(key="slots", capacity=5)]
).init()

# Submit 100 tasks:
futures = [worker.task(i) for i in range(100)]
# - First 10 submit immediately (submission queue)
# - Next 90 block on submission queue
# - Inside worker: Max 5 execute concurrently (resource limit)
```

### 4. On-Demand Workers and Limits

**Issue**: Each on-demand worker gets own LimitPool copy

**Impact**: Limits are NOT shared across on-demand workers

**Example**:
```python
pool = MyWorker.options(
    mode="thread",
    on_demand=True,
    limits=[ResourceLimit(key="connections", capacity=10)]
).init()

# Creates 5 workers, each with capacity=10 → 50 total connections!
```

**Solution**: Don't use limits with on-demand workers, or use persistent pool.

### 5. Method Caching and Callable Attributes

**Issue**: `__getattr__` caches method wrappers by name

**Problem**: If worker class has callable attributes that change, cache becomes stale

**Example**:
```python
class DynamicWorker(Worker):
    def __init__(self):
        self.processor = lambda x: x * 2
    
    def update_processor(self, new_func):
        self.processor = new_func

worker = DynamicWorker.options(mode="thread").init()
worker.processor(5)  # Returns 10
worker.update_processor(lambda x: x * 3)
worker.processor(5)  # Still returns 10! (cached wrapper)
```

**Solution**: Clear `_method_cache` when updating callable attributes, or use regular methods instead of callable attributes.

### 6. Exception Handling in Pools

**Behavior**: Exceptions don't stop the pool

**Example**:
```python
pool = MyWorker.options(mode="thread", max_workers=4).init()

futures = [pool.task(i) for i in range(10)]
# If task(5) raises exception, other tasks continue
# Exception stored in futures[5], not propagated

try:
    results = [f.result() for f in futures]
except Exception as e:
    # Only raised when accessing futures[5].result()
    ...
```

**Best Practice**: Use `gather(return_exceptions=True)` to collect all results/exceptions.

### 7. Worker State and Pools

**Limitation**: Worker state is per-worker, not per-pool

**Example**:
```python
class Counter(Worker):
    def __init__(self):
        self.count = 0
    
    def increment(self):
        self.count += 1
        return self.count

pool = Counter.options(mode="thread", max_workers=4).init()

# Each worker has own count
results = [pool.increment().result() for _ in range(10)]
# Results: [1, 1, 1, 1, 2, 2, 2, 2, 3, 3] (depends on load balancing)
# NOT: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
```

**Solution**: Use shared state mechanisms (Redis, database, etc.) or single worker.

### 8. Stop Timeout and Cleanup

**Issue**: `stop()` timeout is per-operation, not total

**Example**:
```python
pool = MyWorker.options(mode="thread", max_workers=10).init()
pool.stop(timeout=5)
# May take up to 50 seconds! (5s × 10 workers)
```

**Best Practice**: Set appropriate timeout based on pool size.

### 9. Cloudpickle Serialization Quirks

**Issue**: Process and Ray workers serialize worker class

**Limitations**:
- Local variables from outer scope captured by closures
- Large dependencies increase serialization time
- Some objects can't be pickled (open files, database connections)

**Example**:
```python
# BAD: Captures entire DataFrame in closure
df = pd.DataFrame(...)  # 1GB

class Processor(Worker):
    def process(self, row_id):
        return df.iloc[row_id]  # Serializes entire df!

worker = Processor.options(mode="process").init()
```

**Solution**: Pass data as arguments, not via closures:
```python
class Processor(Worker):
    def __init__(self, df):
        self.df = df

worker = Processor.options(mode="process").init(df)
```

### 10. Load Balancer State and Restarts

**Issue**: Load balancer state lost on pool restart

**Example**:
```python
pool = MyWorker.options(
    mode="thread",
    max_workers=4,
    load_balancing="least_total"
).init()

# After 1000 calls, load balanced across workers
stats = pool.get_pool_stats()
# {"load_balancer": {"total_calls": {0: 250, 1: 250, 2: 250, 3: 250}}}

pool.stop()
pool = MyWorker.options(...).init()  # New pool
# Load balancer reset, starts from zero
```

**Solution**: Don't rely on load balancer state persisting across restarts.

---

This architecture document provides a comprehensive technical overview of the worker and worker pool system in Concurry. For implementation details, see the source code in `src/concurry/core/worker/`.

