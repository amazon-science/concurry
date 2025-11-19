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
6. **No silent fallbacks** - Explicit configuration, noisy failures over silent degradation

#### Principle 6: No Silent Fallbacks

**Core Tenet:** Concurry must **fail noisily** rather than silently degrade performance.

**Why This Matters:**

Silent fallbacks are the bane of concurrency frameworks. When a system automatically falls back to slower implementations without warning, users experience:
- **Mysterious slowdowns** that are hard to diagnose
- **Production surprises** when code suddenly runs 10-100x slower
- **False confidence** in development that breaks in production
- **Debugging nightmares** trying to find why "the same code" performs differently

**Design Policy:**

✅ **DO:** Have multiple implementations for flexibility
```python
# Example: Multiple limit backend implementations
- InMemorySharedLimitSet (fast, thread-safe, single-process)
- MultiprocessSharedLimitSet (slower, multi-process safe)
- RaySharedLimitSet (distributed, Ray cluster)
```

✅ **DO:** Select implementation via explicit configuration
```python
# User explicitly chooses based on their execution mode
limits = LimitSet(
    limits=[...],
    mode="ray"  # ← Explicit choice
)
```

❌ **DON'T:** Auto-fallback when configured implementation fails
```python
# BAD: Silent fallback
try:
    return RaySharedLimitSet(...)
except RayNotAvailableError:
    # ❌ Silently use slower implementation
    return InMemorySharedLimitSet(...)  
```

✅ **DO:** Fail loudly with actionable error
```python
# GOOD: Noisy failure
if mode == "ray" and not ray.is_initialized():
    raise RuntimeError(
        "Ray mode selected but Ray is not initialized. "
        "Call ray.init() before creating workers, or use mode='thread'."
    )
```

**Real-World Example:**

```python
# User configures for Ray (expects distributed performance)
pool = MyWorker.options(
    mode="ray",
    max_workers=100  # Expects 100 Ray actors
).init()

# WRONG: Silent fallback to thread pool
# → User thinks they have 100 distributed workers
# → Actually running 100 threads in one process
# → 10x slower, saturates single machine
# → Production outage with no clear cause

# RIGHT: Noisy failure
# RuntimeError: Ray mode selected but Ray is not initialized.
# → User immediately knows what's wrong
# → Can fix or choose different mode
# → No mysterious slowdowns
```

**Implementation Guidelines:**

1. **Configuration is a contract** - If user specifies `mode="ray"`, they expect Ray
2. **Fail at initialization** - Check requirements during `.init()`, not during execution
3. **Clear error messages** - Tell user exactly what's wrong and how to fix it
4. **No implicit downgrades** - Don't automatically use slower implementation
5. **Document requirements** - Each mode's dependencies must be clear

**Exceptions to This Rule:**

The only acceptable "fallback" is when it's **semantically equivalent and performance-neutral**:

```python
# OK: Fallback with equivalent performance
if len(my_list) == 0:  # ← Could use "not my_list" but explicit is better
    return default_value
```

This is not really a fallback - it's just handling an edge case with no performance implication.

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

**Problem 3: Internal Method Calls Bypassing Retry Logic**

When BaseModel workers have methods that internally call other methods with retry configuration, those internal calls could bypass retry validation:

```python
from concurry import worker, async_gather
from pydantic import BaseModel
from typing import List, Dict, Any

@worker(mode="asyncio")
class LLM(BaseModel):
    model_name: str
    
    async def call_llm(self, prompt: str) -> Dict[str, Any]:
        """Single LLM call with validation."""
        response = await litellm.acompletion(
            model=self.model_name, 
            messages=[{"role": "user", "content": prompt}]
        )
        return {"response": response.choices[0].message.content}
    
    async def call_batch(self, prompts: List[str]) -> List[str]:
        """Batch calls - internally calls call_llm()."""
        tasks = [self.call_llm(prompt) for prompt in prompts]
        results = await async_gather(tasks)
        return [r["response"] for r in results]

# Configure with retry_until for call_llm
llm = LLM.options(
    mode="asyncio",
    num_retries={"*": 0, "call_llm": 0},
    retry_until={
        "*": None, 
        "call_llm": lambda result, **ctx: validate_json(result["response"])
    }
).init(model_name="gpt-4")

# PROBLEM: When call_batch() internally calls call_llm(),
# the validator wasn't being invoked on internal calls!
# Result: Invalid responses slip through validation
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
6. **Internal Call Validation**: Methods called internally by other methods still go through retry/validation logic

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

**Step 5: Internal Method Call Retry Wrapping**:

To ensure internal method calls go through retry logic, the wrapper dynamically modifies `_wrapped_instance`'s class:

```python
# In WorkerWithLimitsAndRetry.__init__:
def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    
    # Cache composition flag for performance (critical in Ray)
    _is_composition = hasattr(self, "_wrapped_instance")
    object.__setattr__(self, "_is_composition_wrapper", _is_composition)
    
    # CRITICAL FIX: Replace _wrapped_instance's class with retry-aware version
    if _is_composition:
        original_instance = self._wrapped_instance
        original_class = type(original_instance)
        
        class WrappedInstanceWithRetry(original_class):
            def __getattribute__(self, name: str):
                """Apply retry wrapping to method calls."""
                attr = super().__getattribute__(name)
                
                # Apply retry logic (same as WorkerWithLimitsAndRetry)
                if (
                    has_retry
                    and not name.startswith("_")
                    and callable(attr)
                    and not isinstance(attr, type)
                ):
                    # Get retry config for this method
                    if name in retry_configs:
                        method_config = retry_configs[name]
                    else:
                        method_config = retry_configs.get("*")
                    
                    if method_config is None:
                        return attr
                    if method_config.num_retries == 0 and method_config.retry_until is None:
                        return attr
                    
                    # Wrap with retry logic
                    wrapped = create_retry_wrapper(
                        attr, method_config, name, original_class.__name__
                    )
                    return wrapped
                
                return attr
        
        # Replace instance's class (Python allows this!)
        original_instance.__class__ = WrappedInstanceWithRetry
```

**Why This Matters:**

When a method like `call_batch()` internally calls `call_llm()`, the call goes through:

1. `CompositionWrapper.call_batch()` → delegates to `_wrapped_instance.call_batch()`
2. Inside `call_batch()`: `self.call_llm()` → goes through `WrappedInstanceWithRetry.__getattribute__`
3. Retry wrapper applied → validation occurs → `RetryValidationError` raised if validation fails

**Without this fix**, internal calls would go directly to the unwrapped method, bypassing retry/validation entirely.

**Example Scenario:**

```python
@worker(mode="asyncio")
class LLM(BaseModel):
    async def call_llm(self, prompt: str) -> dict:
        """Validated method."""
        response = await api_call(prompt)
        return {"response": response}
    
    async def call_batch(self, prompts: List[str]) -> List[dict]:
        """Internally calls call_llm()."""
        tasks = [self.call_llm(p) for p in prompts]  # ← Internal calls
        return await async_gather(tasks)

llm = LLM.options(
    num_retries={"*": 0, "call_llm": 0},
    retry_until={"*": None, "call_llm": validator}
).init()

# When call_batch() calls call_llm() internally:
# ✅ With Step 5: validator runs, RetryValidationError raised if invalid
# ❌ Without Step 5: validator skipped, invalid data returned
```

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

##### Process Mode: Cloudpickle and Multiprocessing Context

**Critical Design Decision**: Process workers use `cloudpickle` for all user-provided objects and `forkserver` as the default multiprocessing context.

**Problem Context**:

Users frequently define functions, classes, and retry filters in Jupyter notebooks or test files:
```python
# Common user workflow in Jupyter notebook
def my_function(x):  # Local function - NOT picklable by standard pickle
    return x * 2

def should_retry(exception, **context):  # Local retry filter
    return isinstance(exception, ValueError)

worker = TaskWorker.options(
    mode="process",
    retry_on=[should_retry]  # Needs cloudpickle!
).init(fn=my_function)  # Needs cloudpickle!
```

Standard `pickle` **cannot** serialize:
- Local functions (defined in function scope)
- Lambda functions
- Functions/classes defined in `__main__` (Jupyter notebooks, test files)
- Closures with local variable capture

This would break a core user workflow, forcing users to define all functions at module level.

**Solution: Cloudpickle for User-Provided Objects**

`ProcessWorkerProxy` uses `cloudpickle` (not standard `pickle`) for:

1. **Worker class** (`worker_cls`): Allows local Worker classes in notebooks/tests
2. **Init arguments** (`init_args`, `init_kwargs`): Allows local functions as parameters
3. **Retry configuration** (`retry_config`): Allows local retry filters
4. **Task functions** (TaskWorker): Allows local functions, lambdas

```python
# In ProcessWorkerProxy.post_initialize()
worker_cls_bytes = cloudpickle.dumps(self.worker_cls)
init_args_bytes = cloudpickle.dumps(self.init_args)
init_kwargs_bytes = cloudpickle.dumps(self.init_kwargs)
retry_config_bytes = cloudpickle.dumps(self.retry_config)

# Later unpickled in worker process
worker_cls = cloudpickle.loads(worker_cls_bytes)
init_args = cloudpickle.loads(init_args_bytes)
init_kwargs = cloudpickle.loads(init_kwargs_bytes)
retry_config = cloudpickle.loads(retry_config_bytes)
```

**What Does NOT Use Cloudpickle:**

- **Multiprocessing primitives** (Queues, Locks, Semaphores): Standard `multiprocessing` serialization
- **Manager proxies** (`MultiprocessSharedLimitSet`): Custom `__getstate__`/`__setstate__`
- **Internal state** (futures dict, threads): Not serialized

**Multiprocessing Context: Fork vs Spawn vs Forkserver**

Python's `multiprocessing` supports three start methods:

| Context | Startup Time | Memory | Thread Safety | Ray Client Compatible |
|---------|-------------|---------|---------------|----------------------|
| `fork` | ~10ms | Shared (copy-on-write) | ❌ **UNSAFE** | ❌ **BREAKS** |
| `spawn` | ~10-20s on Linux, ~1-2s on macOS | Independent | ✅ Safe | ✅ Works |
| `forkserver` | ~200ms | Independent | ✅ Safe | ✅ Works |

**Why Fork is UNSAFE:**

`fork()` copies the entire process memory, including:
- Active threads (which continue running in child!)
- Open file descriptors
- gRPC connections (Ray client mode)
- Mutexes and condition variables (can be in inconsistent state)

**The Ray Client Problem:**

Ray client mode (common user workflow) uses gRPC for communication with remote cluster. gRPC spawns background threads. If you `fork()` while gRPC threads are active:

```
Parent Process              Forked Child Process
gRPC Thread 1 ──fork()──>  gRPC Thread 1 (CORRUPTED - mid-operation)
gRPC Thread 2 ──fork()──>  gRPC Thread 2 (CORRUPTED - holding mutex)
Main Thread   ──fork()──>  Main Thread (tries to use gRPC -> DEADLOCK/SEGFAULT)
```

**Real Error (before fix):**
```
[mutex.cc : 2443] RAW: Check w->waitp->cond == nullptr failed
Check failed: next_worker->state == KICKED
Segmentation fault (core dumped)
```

**Why Spawn is TOO SLOW:**

`spawn` starts a fresh Python interpreter, which must:
1. Initialize Python runtime
2. Import all modules
3. Load dependencies
4. Reconstruct worker state

**Benchmarks:**
- Process mode with `fork`: ~10ms per worker startup
- Process mode with `spawn`: ~10-20s per worker startup on Linux (1000x slower!)
- Process mode with `forkserver`: ~200ms per worker startup (20x slower than fork, 50x faster than spawn)

**Why Forkserver is the Best Balance:**

`forkserver` works by:
1. Starting a clean server process early (before any threads)
2. Server process forks worker processes on demand
3. Forked workers inherit minimal state (no gRPC threads!)

**Benefits:**
- ✅ **Safe**: No active threads in server process when forking
- ✅ **Fast**: ~200ms startup vs 10-20s for spawn
- ✅ **Compatible**: Works with Ray client + process workers concurrently
- ✅ **Common workflow**: Users can have Ray client connected 24/7 and use process workers

**Configuration:**

```python
from concurry.config import global_config

# Default (recommended)
assert global_config.defaults.mp_context == "forkserver"

# Override if needed (not recommended)
with global_config.temp_config(mp_context="spawn"):
    worker = MyWorker.options(mode="process").init()
```

**Historical Approaches (Failed):**

1. **Attempt 1**: Use `fork` with `Manager()` - **FAILED** (segfaults with Ray client)
2. **Attempt 2**: Use `spawn` for everything - **FAILED** (10-20s startup, unusable)
3. **Attempt 3**: Use `spawn` for Manager, `forkserver` for workers - **FAILED** (Manager proxy pickling issues)
4. **Final Solution**: Use `forkserver` for both Manager and workers + cloudpickle for user objects - **SUCCESS**

**Current Architecture (October 2025):**

```python
# ProcessWorkerProxy creates worker process:
ctx = multiprocessing.get_context("forkserver")  # From global_config
process = ctx.Process(
    target=_process_worker_main,
    args=(
        cloudpickle.dumps(worker_cls),      # ← cloudpickle
        cloudpickle.dumps(init_args),        # ← cloudpickle
        cloudpickle.dumps(init_kwargs),      # ← cloudpickle
        limits,                               # ← Manager proxies (standard pickle)
        cloudpickle.dumps(retry_config),     # ← cloudpickle
        command_queue,                        # ← multiprocessing.Queue
        result_queue,                         # ← multiprocessing.Queue
    )
)

# MultiprocessSharedLimitSet creates Manager:
manager_ctx = multiprocessing.get_context("forkserver")  # Same context!
manager = manager_ctx.Manager()
# Manager proxies are pickled using their custom __reduce__ methods
```

**Key Insight**: Manager proxies have custom `__reduce__` methods that handle their own serialization. When workers are created, the Manager proxies are pickled (using their custom methods) and unpickled in the worker process, automatically reconnecting to the Manager server. This works with any `mp_context` as long as **both Manager and workers use the same context**.

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
    
    # 2. Check if stopped
    if self._stopped:
        raise RuntimeError("Worker pool is stopped")
    
    # 3. Record start
    self._load_balancer.record_start(worker_idx)
    
    # 4. Execute method (worker manages its own queue internally)
    result = getattr(self._workers[worker_idx], name)(*args, **kwargs)
    
    # 5. Wrap future to record completion
    return self._wrap_future_with_tracking(result, worker_idx)
```

**Future Wrapping for Load Balancer Tracking:**
```python
def _wrap_future_with_tracking(self, future, worker_idx):
    def on_complete(f):
        self._load_balancer.record_complete(worker_idx)
    
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

### Method Execution (Pool) - Non-Blocking Dispatch

**Critical: Pool dispatch is ALWAYS non-blocking to the user.**

```
user calls pool.method(args, kwargs)  [NON-BLOCKING]
    ↓
WorkerProxyPool.__getattr__("method") intercepts  [INSTANT]
    ↓
Check _method_cache for cached wrapper  [O(1)]
    ↓
If not cached, create method_wrapper:
    ↓
    ├─ If on_demand:
    │   1. Wait for on-demand slot (if limit reached)
    │      - Blocks internally but returns future to user immediately
    │   2. Check if stopped
    │   3. Increment counter, get worker_index  [O(1), instant]
    │   4. Create worker with _create_worker(worker_index)
    │   5. Track in _on_demand_workers
    │   6. Call worker.method(args, kwargs)  [Returns future instantly]
    │   7. Wrap future for cleanup after completion
    │   8. Return future (or result if blocking=True)  [USER NEVER BLOCKS]
    │
    └─ If persistent:
        1. Check workers exist and not stopped  [O(1), instant]
        2. Select worker: idx = load_balancer.select_worker(N)  [O(1) or O(N), instant]
        3. Check if stopped  [O(1), instant]
        4. Record start: load_balancer.record_start(idx)  [O(1), instant]
        5. Call worker.method(args, kwargs)  [Returns future instantly]
           - USER RECEIVES FUTURE IMMEDIATELY (non-blocking!)
           - Worker manages its own submission queue internally
           - Worker's internal semaphore may block proxy→backend flow
           - User is never aware of internal blocking
        6. Wrap future to record completion in load balancer  [Instant]
        7. Return future (or result if blocking=True)  [USER NEVER BLOCKS]

Total user-facing time: ~1-10μs (microseconds)
User code never blocks on submission regardless of queue state!
```

**Key Points:**
- Load balancer selection is O(1) (round-robin) or O(N) where N=max_workers (typically <100)
- Worker method call returns future immediately
- Worker's internal queue manages backpressure transparently
- Pool dispatch layer has NO semaphores or blocking
- All blocking happens inside WorkerProxy, hidden from user

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

### 3. Submission Queue (max_queued_tasks) - Non-Blocking User Submissions

**Critical Design Principle: User submissions are ALWAYS non-blocking.**

The submission architecture has two distinct layers:

**Layer 1: User Code → Worker Proxy** (Always Non-Blocking, Unlimited)
- User calls `worker.method(args)` or `pool.method(args)`
- Returns future **instantly** without blocking
- No limit on number of submissions
- No semaphore at this layer
- **All futures created immediately** (< 1ms per submission)

**Layer 2: Worker Proxy → Execution Backend** (Controlled by `max_queued_tasks`)
- Worker proxy manages internal queue to backend (thread/process/ray)
- Semaphore limits in-flight tasks to backend
- Only `max_queued_tasks` tasks forwarded to backend at once
- Tasks wait in proxy's internal queue when backend is full
- **User code never sees this blocking**

#### Callback-Driven Forwarding Architecture

The implementation uses a **callback-driven forwarding mechanism** to achieve non-blocking submissions:

```python
# In WorkerProxy.post_initialize():
self._submission_semaphore = threading.BoundedSemaphore(max_queued_tasks)
self._pending_submissions = queue.Queue()  # Client-side pending queue

# In WorkerProxy.__getattr__().method_wrapper():
def method_wrapper(*args, **kwargs):
    # Check if stopped
    if self._stopped:
        raise RuntimeError("Worker is stopped")
    
    # Try to forward immediately (NON-BLOCKING check)
    if self._submission_semaphore is None:
        # No rate limiting - forward immediately
        future = self._execute_method(name, *args, **kwargs)
    elif self._submission_semaphore.acquire(blocking=False):  # ← Non-blocking!
        # Got semaphore - forward to backend now
        future = self._execute_method(name, *args, **kwargs)
        # Add callback to release semaphore and forward next pending
        future = self._wrap_future_with_submission_tracking(future)
    else:
        # Semaphore unavailable - create shell future and queue for later
        from concurrent.futures import Future as PyFuture
        py_future = PyFuture()
        future = ConcurrentFuture(future=py_future)
        # Queue: (shell_future, py_future, method_name, args, kwargs)
        self._pending_submissions.put((future, py_future, name, args, kwargs))
    
    # Return future IMMEDIATELY (non-blocking!)
    return future if not self.blocking else future.result()

# Callback-driven forwarding chain:
def _wrap_future_with_submission_tracking(self, future):
    """When a task completes, automatically forward next pending task."""
    
    def on_submission_complete(f):
        # Release semaphore first
        self._submission_semaphore.release()
        
        # Try to forward next pending submission (if any)
        try:
            shell_future, py_future, method_name, args, kwargs = \
                self._pending_submissions.get_nowait()
            
            # Try to acquire semaphore (should succeed since we just released)
            if self._submission_semaphore.acquire(blocking=False):
                # Forward to backend
                backend_future = self._execute_method(method_name, *args, **kwargs)
                
                # Chain backend future to shell future
                def chain_result(bf):
                    try:
                        result = bf.result()
                        py_future.set_result(result)
                    except Exception as e:
                        py_future.set_exception(e)
                
                backend_future.add_done_callback(chain_result)
                # Recursively track this forwarded submission
                backend_future.add_done_callback(on_submission_complete)
        except queue.Empty:
            # No pending submissions - done
            pass
    
    future.add_done_callback(on_submission_complete)
    return future
```

**Key Innovation:** No blocking on semaphore during submission. Instead:
1. Try non-blocking acquire: `semaphore.acquire(blocking=False)`
2. If successful: forward immediately
3. If fails: create "shell" future, queue internally
4. On completion callback: automatically forward next queued task

**Advantages:**
- **Zero blocking** in user code (all futures returned instantly)
- **No dedicated threads** (callbacks drive forwarding)
- **Automatic backpressure** (semaphore limits backend load)
- **Self-perpetuating chain** (each completion triggers next forward)
- **Minimal overhead** (< 1μs per submission when queue not full)
- **Memory efficient** (shell futures are lightweight until backed)

#### Design Rationale

**Why callback-driven instead of background threads?**

We considered several alternatives:

**Alternative 1: Blocking Semaphore (Original Implementation)**
```python
# Simple but BLOCKS user code
self._submission_semaphore.acquire()  # ← User code blocks here!
future = self._execute_method(name, *args, **kwargs)
return future
```
❌ **Problem**: User's submission loop blocks, violating "futures are immediate" principle
❌ **Impact**: Submitting 1000 tasks with `max_queued_tasks=10` would block 990 times

**Alternative 2: Dedicated Forwarding Thread Pool**
```python
# Add thread pool to each worker to forward submissions
self._forwarding_pool = ThreadPoolExecutor(max_workers=1)
future = self._forwarding_pool.submit(
    self._wait_for_semaphore_and_forward, 
    name, args, kwargs
)
```
❌ **Complexity**: Adds 1-N threads per worker (expensive with 100+ workers)
❌ **Resource waste**: Thread sits idle most of the time
❌ **Coordination**: Requires complex shutdown and error handling
❌ **Memory**: Thread stacks consume memory (~8MB per thread on 64-bit systems)

**Alternative 3: Async Queue with Event Loop**
```python
# Use asyncio queue for pending submissions
await self._pending_queue.put((name, args, kwargs))
# Separate async task forwards to backend
```
❌ **Mode limitations**: Only works in asyncio mode, not thread/process/ray
❌ **Mixing concerns**: Forces async into synchronous worker framework
❌ **Complexity**: Requires event loop management in each proxy

**Chosen: Callback-Driven Forwarding** ✅
```python
# Non-blocking semaphore check
if self._submission_semaphore.acquire(blocking=False):
    forward_immediately()
else:
    queue_internally()  # Returns shell future instantly
    
# On completion callback:
future.add_done_callback(forward_next_from_queue)
```
✅ **Zero blocking**: All submissions return immediately
✅ **No threads**: Callbacks run in existing completion threads
✅ **Mode agnostic**: Works identically in thread/process/ray/asyncio
✅ **Simple**: ~100 lines of code, easy to reason about
✅ **Efficient**: Minimal overhead, self-cleaning
✅ **Safe shutdown**: Cancel pending futures in `stop()`, no thread coordination

**Key Insight**: We already have threads for task completion (thread pool workers, process result handlers, Ray monitors). Reusing their callbacks for forwarding is free!

#### Shutdown and Cleanup

**Critical Issue**: What happens to pending futures when `stop()` is called?

**Solution**: Cancel all futures in `_pending_submissions` queue:

```python
def stop(self, timeout: float = 30) -> None:
    self._stopped = True
    
    # Cancel all pending futures that were never forwarded
    if self._pending_submissions is not None:
        while True:
            try:
                shell_future, py_future, method_name, args, kwargs = \
                    self._pending_submissions.get_nowait()
                py_future.cancel()  # Mark as cancelled
            except queue.Empty:
                break  # All pending futures cancelled
```

**Callback Guard**: Prevent forwarding during shutdown:

```python
def on_submission_complete(f):
    # Release semaphore
    self._submission_semaphore.release()
    
    # ⚠️ CRITICAL: Don't forward if stopped
    if self._stopped:
        return  # Exit early
    
    # Forward next task from queue...
```

**Why this matters**: Without the guard, cancelled futures could trigger callbacks that try to forward more tasks to a stopped worker, causing:
- Deadlocks (trying to acquire locks on stopped backend)
- Race conditions (accessing worker state during teardown)
- Timeouts (tasks hang waiting for stopped backend)

**Lifecycle:**
1. User calls `stop()` → Sets `self._stopped = True`
2. Cancel all pending futures in `_pending_submissions` → Users see `CancelledError`
3. Callback guard prevents forwarding new tasks → No new submissions reach backend
4. Existing in-flight tasks complete or timeout → Clean shutdown
5. Backend cleaned up (threads joined, processes terminated, ray actors killed)

#### Two-Queue Architecture

**Why two queues?**
- `_pending_submissions` (client-side, in WorkerProxy): Rate-limits forwarding to backend
- `_command_queue` (backend-side, Thread/Process/Asyncio only): Holds tasks waiting for execution

```
Client Code                    WorkerProxy                   Execution Backend
    │                               │                              │
    │  worker.method(1)             │                              │
    ├──────────────────────────────>│                              │
    │  <return future instantly>    │                              │
    │                               │  Forward task 1              │
    │                               ├─────────────────────────────>│
    │  worker.method(2)             │  (semaphore: 1/10 slots)    │
    ├──────────────────────────────>│                              │
    │  <return future instantly>    │                              │
    │                               │  Forward task 2              │
    │                               ├─────────────────────────────>│
    │  ... submit 1000 more         │  (semaphore: 2/10 slots)    │
    │  (ALL return instantly!)      │                              │
    │                               │  Queue task 11-1000          │
    │                               │  (semaphore full)            │
    │                               │                              │
    │                               │  <task 1 completes>          │
    │                               │<─────────────────────────────│
    │                               │  Release semaphore (1/10)    │
    │                               │  Forward task 11 from queue  │
    │                               ├─────────────────────────────>│
    │                               │  (semaphore: 2/10 slots)    │
```

**Purpose**: Controls how many tasks can be in-flight from worker proxy to underlying execution (thread/process/ray). User submissions are always non-blocking. The proxy manages its internal queue to the underlying execution context. This prevents overloading the execution backend, especially for Ray actors, while keeping user submissions instant.

#### Example Flows

**Single Worker Flow:**
```
User submits 1000 tasks (instant, non-blocking):
├─ Task 1-10: Immediately forwarded to backend (semaphore allows)
│   └─ Future returned instantly
├─ Task 11-1000: Queued in _pending_submissions (semaphore full)
│   └─ "Shell" futures returned instantly (not yet backed by execution)
└─ As tasks complete:
    └─ Callback chain forwards tasks 11-1000 from queue to backend
    
User has all 1000 futures immediately and never blocks!
Submission time: ~0.001s (instant)
Execution time: depends on task duration
```

**Worker Pool Behavior:**
```
Pool with 5 workers, max_queued_tasks=10 each:
├─ User submits 1000 tasks (instant, non-blocking pool dispatch)
├─ Load balancer distributes to 5 workers (instant, O(1))
│   └─ ~200 tasks per worker
├─ Each worker:
│   ├─ Forwards 10 tasks to its backend (semaphore allows)
│   └─ Queues remaining ~190 in _pending_submissions
└─ Total: 50 tasks in-flight to backends, 950 queued in proxies

Total capacity: 5 workers × 10 queue = 50 in-flight to backends
User submission time: ~0.002s for 1000 tasks (instant!)
```

**Key Invariant:** 
`max_queued_tasks` controls the depth of the Worker Proxy → Execution Backend queue, not the User → Worker Proxy submission. The user-facing API is always non-blocking regardless of this setting.

**Bypassed for:**
- **Sync mode**: Immediate execution, no queue needed
- **Asyncio mode**: Event loop handles concurrency, no queue needed  
- **Blocking mode**: Sequential execution, returns results directly
- **On-demand workers**: Ephemeral workers, pool already limits concurrency

**Performance:**
- Future creation: < 1 μs per future (instant)
- Callback overhead: ~5-10 μs per task completion
- Total submission time: O(n) where n = number of tasks, ~1 μs per task
- Example: 10,000 tasks submitted in ~10ms (0.01s)

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

## Enhanced Worker Decorator and Auto-Initialization

### Overview

The `@worker` decorator and `__init_subclass__` mechanism provide three ways to configure workers with pre-defined options:

1. **Decorator Configuration**: `@worker(mode='thread', max_workers=4)`
2. **Inheritance Configuration**: `class LLM(Worker, mode='thread', max_workers=4)`
3. **Auto-Initialization**: `auto_init=True` enables direct class instantiation to create workers

This feature makes worker creation more ergonomic while maintaining backward compatibility.

### Design Goals

1. **Ergonomic API**: Enable `llm = LLM(model='gpt-4')` to create workers directly
2. **Backward Compatible**: `@worker` without parameters still works
3. **Flexible Configuration**: Support decorator, inheritance, and `.options()` override
4. **Clear Precedence**: Explicit `.options()` > Decorator > Inheritance > `global_config`
5. **No Recursion**: Prevent infinite loops when `auto_init=True`

### Implementation Architecture

#### Three Configuration Sources

**1. Decorator Configuration (`_worker_decorator_config`)**

Set by `@worker(...)` decorator:

```python
@worker(mode='thread', max_workers=4, auto_init=True)
class LLM:
    pass

# Stored as: LLM._worker_decorator_config = {'mode': 'thread', 'max_workers': 4, 'auto_init': True}
```

**2. Inheritance Configuration (`_worker_inheritance_config`)**

Set by `__init_subclass__` when subclassing `Worker`:

```python
class LLM(Worker, mode='thread', max_workers=4, auto_init=True):
    pass

# Stored as: LLM._worker_inheritance_config = {'mode': 'thread', 'max_workers': 4, 'auto_init': True}
```

**3. Explicit Configuration (`.options()`)**

Highest priority, overrides both decorator and inheritance:

```python
llm = LLM.options(mode='process', max_workers=8).init(...)
# mode='process', max_workers=8 (overrides decorator/inheritance)
```

#### Configuration Merging in `Worker.options()`

The `Worker.options()` method merges all three sources:

```python
@classmethod
def options(cls, *, mode=_NO_ARG, ...):
    merged_params = {}
    
    # 1. Start with inheritance config (lowest priority)
    if hasattr(cls, '_worker_inheritance_config'):
        merged_params.update(cls._worker_inheritance_config)
    
    # 2. Override with decorator config (medium priority)
    if hasattr(cls, '_worker_decorator_config'):
        merged_params.update(cls._worker_decorator_config)
    
    # 3. Override with explicit parameters (highest priority)
    if mode is not _NO_ARG:
        merged_params['mode'] = mode
    # ... (other parameters)
    
    # 4. Apply global_config defaults for missing values
    # ...
    
    return WorkerBuilder(worker_cls=cls, ...)
```

**Precedence (highest to lowest)**:
1. Explicit `.options()` parameters
2. `@worker` decorator parameters
3. `class Worker(...)` inheritance parameters
4. `global_config` defaults

#### Auto-Initialization via `Worker.__new__`

When `auto_init=True`, direct class instantiation creates a worker:

```python
def __new__(cls, *args, **kwargs):
    # 1. Check for _from_proxy flag (prevents recursion)
    in_proxy_creation = kwargs.pop('_from_proxy', False)
    if in_proxy_creation:
        # Proxy is creating the actual worker instance
        return super().__new__(cls)
    
    # 2. Merge decorator and inheritance configs
    merged_config = {}
    if hasattr(cls, '_worker_inheritance_config'):
        merged_config.update(cls._worker_inheritance_config)
    if hasattr(cls, '_worker_decorator_config'):
        merged_config.update(cls._worker_decorator_config)
    
    # 3. Check if auto_init is enabled
    should_auto_init = merged_config.get('auto_init', False)
    
    if should_auto_init:
        # 4. Create worker via .options().init()
        options_params = {k: v for k, v in merged_config.items() if k != 'auto_init'}
        builder = cls.options(**options_params)
        return builder.init(*args, **kwargs)
    
    # 5. Normal instantiation (plain Python instance)
    return super().__new__(cls)
```

**Key Points**:
- `_from_proxy=True` flag prevents infinite recursion
- `auto_init` defaults to `True` if any config is provided
- `.options().init()` is called automatically when `auto_init=True`

#### Recursion Prevention with `_from_proxy` Flag

**The Problem**: Without recursion prevention, this would loop infinitely:

```python
@worker(mode='thread', auto_init=True)
class LLM:
    pass

llm = LLM(...)  # Calls Worker.__new__
# → sees auto_init=True
# → calls cls.options().init(...)
# → WorkerBuilder creates proxy
# → Proxy calls worker_cls(...) to create worker instance
# → Calls Worker.__new__ again
# → sees auto_init=True again
# → INFINITE RECURSION!
```

**The Solution**: `_from_proxy=True` flag breaks the cycle:

```python
# In all proxy classes (SyncWorkerProxy, ThreadWorkerProxy, etc.):
def post_initialize(self):
    worker_cls = _create_worker_wrapper(self.worker_cls, ...)
    
    # CRITICAL: Pass _from_proxy=True to bypass auto_init
    init_kwargs = dict(self.init_kwargs)
    init_kwargs['_from_proxy'] = True  # ← Prevents recursion
    
    self._worker = worker_cls(*self.init_args, **init_kwargs)
```

When `_from_proxy=True` is present, `Worker.__new__` skips the `auto_init` logic and creates a plain instance directly.

**Flow with `_from_proxy`**:
1. User: `llm = LLM(model='gpt-4')`
2. `Worker.__new__`: sees `auto_init=True`, calls `.options().init(model='gpt-4')`
3. `WorkerBuilder`: creates `SyncWorkerProxy(init_kwargs={'model': 'gpt-4'})`
4. `SyncWorkerProxy.post_initialize()`: adds `_from_proxy=True` to `init_kwargs`
5. Proxy: calls `worker_cls(model='gpt-4', _from_proxy=True)`
6. `Worker.__new__`: sees `_from_proxy=True`, skips `auto_init`, returns plain instance ✅
7. No recursion!

#### Composition Wrapper Compatibility

The composition wrapper (for `Typed`/`BaseModel` workers) must NOT inherit `auto_init`:

```python
def _create_composition_wrapper(worker_cls):
    class CompositionWrapper(Worker):
        def __init__(self, *args, **kwargs):
            # Remove _from_proxy before creating wrapped instance
            kwargs.pop('_from_proxy', None)
            self._wrapped_instance = worker_cls(*args, _from_proxy=True, **kwargs)
    
    # CRITICAL: Remove auto_init from config to prevent recursion
    if hasattr(worker_cls, '_worker_inheritance_config'):
        config = worker_cls._worker_inheritance_config.copy()
        config.pop('auto_init', None)  # ← Remove auto_init
        if len(config) > 0:
            CompositionWrapper._worker_inheritance_config = config
    
    if hasattr(worker_cls, '_worker_decorator_config'):
        config = worker_cls._worker_decorator_config.copy()
        config.pop('auto_init', None)  # ← Remove auto_init
        if len(config) > 0:
            CompositionWrapper._worker_decorator_config = config
    
    return CompositionWrapper
```

**Why**: The composition wrapper is an internal implementation detail. It should never auto-initialize itself when instantiated by the proxy.

#### Worker Wrapper Compatibility

Similarly, worker wrappers (for limits/retries) must NOT inherit `auto_init`:

```python
def _create_worker_wrapper(worker_cls, limits, retry_configs):
    if limits is not None:
        class WorkerWithLimits(worker_cls):
            def __init__(self, *args, **kwargs):
                kwargs.pop('_from_proxy', None)
                super().__init__(*args, **kwargs)
                # ... limits logic
        
        # CRITICAL: Remove auto_init from inherited config
        if hasattr(WorkerWithLimits, '_worker_inheritance_config'):
            config = WorkerWithLimits._worker_inheritance_config.copy()
            config.pop('auto_init', None)
            if len(config) > 0:
                WorkerWithLimits._worker_inheritance_config = config
        
        if hasattr(WorkerWithLimits, '_worker_decorator_config'):
            config = WorkerWithLimits._worker_decorator_config.copy()
            config.pop('auto_init', None)
            if len(config) > 0:
                WorkerWithLimits._worker_decorator_config = config
        
        return WorkerWithLimits
    
    return worker_cls
```

**Why**: Worker wrappers are internal classes created by the proxy. They should never trigger `auto_init` when instantiated.

### Usage Patterns

#### Pattern 1: Decorator with Auto-Init

```python
@worker(mode='thread', max_workers=4, auto_init=True)
class LLM:
    def __init__(self, model_name: str):
        self.model_name = model_name
    
    def call_llm(self, prompt: str) -> str:
        return f"Response from {self.model_name}"

# Direct instantiation creates worker
llm = LLM(model_name='gpt-4')
future = llm.call_llm("Hello")
result = future.result()
llm.stop()
```

#### Pattern 2: Inheritance with Auto-Init

```python
class LLM(Worker, mode='thread', max_workers=4, auto_init=True):
    def __init__(self, model_name: str):
        self.model_name = model_name

# Direct instantiation creates worker
llm = LLM(model_name='gpt-4')
llm.stop()
```

#### Pattern 3: Decorator Without Auto-Init (Backward Compatible)

```python
@worker
class LLM:
    pass

# Must use .options().init() (backward compatible)
llm = LLM.options(mode='thread').init(...)
llm.stop()
```

#### Pattern 4: Override at Instantiation

```python
@worker(mode='thread', max_workers=4, auto_init=True)
class LLM:
    pass

# Use decorator defaults
llm1 = LLM(...)  # mode='thread', max_workers=4

# Override decorator config
llm2 = LLM.options(mode='process', max_workers=8).init(...)
# mode='process', max_workers=8

llm1.stop()
llm2.stop()
```

### Testing and Validation

**Test Coverage**:
- `tests/core/worker/test_enhanced_worker_decorator.py` (comprehensive test suite)
  - Basic decorator functionality
  - Inheritance configuration
  - Auto-initialization behavior
  - Configuration precedence
  - Options override
  - All execution modes
  - Typed/BaseModel workers
  - Context manager support
  - Error cases
  - Recursion prevention

**Key Test Cases**:
1. Decorator with `auto_init=True` creates workers directly
2. Decorator with `auto_init=False` creates plain instances
3. Inheritance with `auto_init=True` creates workers directly
4. `.options()` overrides decorator/inheritance config
5. Mixed decorator + inheritance (decorator wins, with warning)
6. No infinite recursion with `auto_init=True`
7. Composition wrapper doesn't trigger `auto_init`
8. Worker wrapper doesn't trigger `auto_init`
9. Works across all execution modes (sync, thread, process, asyncio, ray)
10. Works with `Typed` and `BaseModel` workers

### Design Rationale

**Why Three Configuration Sources?**
- **Decorator**: Convenient for standalone classes
- **Inheritance**: Natural for class hierarchies
- **`.options()`**: Runtime flexibility and override capability

**Why `auto_init` Defaults to `True`?**
- If user provides any configuration, they likely want auto-initialization
- Makes the API more ergonomic (`llm = LLM(...)` vs `llm = LLM.options().init(...)`)
- Can be explicitly disabled with `auto_init=False`

**Why `_from_proxy` Flag?**
- Simplest solution to prevent recursion
- Minimal performance overhead (one dict lookup)
- Clear intent (flag explicitly indicates proxy creation)
- Alternative approaches (checking call stack, thread-local state) are more complex

**Why Remove `auto_init` from Wrappers?**
- Wrappers are internal implementation details
- They should never auto-initialize themselves
- Prevents infinite recursion when proxy creates worker instance
- Keeps `auto_init` behavior at the user-facing layer only

### Backward Compatibility

**Fully Backward Compatible**:
- `@worker` without parameters still works
- Existing `.options().init()` code unchanged
- No breaking changes to existing APIs
- New features are opt-in via `auto_init=True`

**Migration Path**:
```python
# Old code (still works)
@worker
class LLM:
    pass
llm = LLM.options(mode='thread').init(...)

# New code (more ergonomic)
@worker(mode='thread', auto_init=True)
class LLM:
    pass
llm = LLM(...)
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

