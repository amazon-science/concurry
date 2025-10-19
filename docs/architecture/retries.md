# Retry Mechanism Architecture

This document describes the design, implementation, and maintenance guidelines for Concurry's retry mechanism.

## Design Goals

1. **Actor-side execution** - All retry logic runs inside the worker (actor/process/thread), not client-side
2. **Zero overhead when disabled** - `num_retries=0` should have no performance cost
3. **Framework consistency** - Retry works identically across all execution modes (sync, thread, process, asyncio, Ray)
4. **Flexible filtering** - Support exception types, callable filters, and output validators
5. **Resource awareness** - Coordinate with limits system to release resources between retries
6. **Serialization safety** - Handle multiprocessing and Ray serialization requirements
7. **Async-aware** - Support both sync and async functions/methods seamlessly

## Architecture Overview

### Core Components

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Retry System Architecture                       │
│                                                                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                        RetryConfig                             │  │
│  │  - num_retries, retry_wait, retry_algorithm, retry_jitter     │  │
│  │  - retry_on (exception filters)                                │  │
│  │  - retry_until (output validators)                             │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                               ↓                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │              Worker Method Wrapping Layer                      │  │
│  │                                                                 │  │
│  │  For regular workers (via _create_worker_wrapper):             │  │
│  │    • __getattribute__ intercepts calls (sync/thread/process)  │  │
│  │    • Pre-wrapped methods at class level (Ray)                 │  │
│  │                                                                 │  │
│  │  For TaskWorker (via _execute_task):                          │  │
│  │    • Retry applied directly in proxy's _execute_task method   │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                               ↓                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                  Retry Execution Functions                     │  │
│  │                                                                 │  │
│  │  • execute_with_retry (sync functions)                        │  │
│  │  • execute_with_retry_async (async functions)                 │  │
│  │  • execute_with_retry_auto (dispatcher for TaskWorker)        │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                               ↓                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    Helper Functions                            │  │
│  │                                                                 │  │
│  │  • calculate_retry_wait (backoff + jitter)                    │  │
│  │  • _should_retry_on_exception (filter matching)               │  │
│  │  • _validate_result (output validation)                       │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Execution Flow

#### Regular Worker Methods

```python
# User calls worker method
result = worker.my_method(x=5).result()

# Flow:
1. WorkerProxy.__getattribute__ intercepts "my_method" call
2. _create_worker_wrapper has wrapped the method with retry logic:
   - For sync/thread/process/asyncio: __getattribute__ wraps on access
   - For Ray: method pre-wrapped at class level
3. create_retry_wrapper returns wrapped method that calls:
   - execute_with_retry (sync) or execute_with_retry_async (async)
4. Retry loop executes inside actor/process:
   a. Try executing method
   b. If exception: check retry_on filters → retry or raise
   c. If success: check retry_until validators → retry or return
   d. Between retries: calculate_retry_wait + sleep
5. On success: return result
6. On exhaustion: raise last exception or RetryValidationError
```

#### TaskWorker.submit()

```python
# User submits arbitrary function
future = task_worker.submit(some_function, arg1, arg2)
result = future.result()

# Flow:
1. TaskWorker.submit() calls self._execute_task(fn, args, kwargs)
2. WorkerProxy._execute_task checks if retry_config exists:
   if self.retry_config is not None and self.retry_config.num_retries > 0:
       result = execute_with_retry_auto(fn, args, kwargs, retry_config, context)
   else:
       result = fn(*args, **kwargs)
3. execute_with_retry_auto dispatches based on function type:
   - Sync function → execute_with_retry
   - Async function → asyncio.run(execute_with_retry_async(...))
4. Retry loop executes (same as regular methods)
5. Result wrapped in appropriate Future type
```

**Critical Design Point**: Retry is applied in `_execute_task`, NOT in `submit()`. This prevents double-wrapping, since `submit()` could also be wrapped by `__getattribute__` retry logic if it were a regular method.

## Core Components Deep Dive

### 1. RetryConfig

**Location**: `concurry/core/retry.py`

**Type**: `morphic.Typed` (Pydantic-based)

**Purpose**: Encapsulates all retry configuration parameters with validation.

**Fields**:

```python
class RetryConfig(Typed):
    # Retry attempts
    num_retries: Optional[conint(ge=0)] = None  # Defaults from global_config
    
    # Exception filtering
    retry_on: Union[type, Callable, List[Union[type, Callable]]] 
        = Field(default_factory=lambda: [Exception])
    
    # Backoff strategy
    retry_algorithm: Optional[RetryAlgorithm] = None  # Defaults from global_config
    retry_wait: Optional[confloat(gt=0)] = None       # Defaults from global_config
    retry_jitter: Optional[confloat(ge=0, le=1)] = None  # Defaults from global_config
    
    # Output validation
    retry_until: Optional[Union[Callable, List[Callable]]] = None
```

**Validation Rules**:

1. `retry_on` items must be:
   - Exception classes (subclasses of `BaseException`), OR
   - Callables that accept `(exception, **context)` and return `bool`

2. `retry_until` items must be:
   - Callables that accept `(result, **context)` and return `bool`
   - All validators must return `True` for result to be considered valid

3. Numeric constraints:
   - `num_retries` ≥ 0
   - `retry_wait` > 0
   - `retry_jitter` ∈ [0, 1]

**Default Resolution**:

Defaults are loaded from `global_config` in `post_initialize()`:

```python
def post_initialize(self) -> None:
    from ..config import global_config
    local_config = global_config.clone()  # Thread-safe
    defaults = local_config.defaults
    
    if self.num_retries is None:
        object.__setattr__(self, "num_retries", defaults.num_retries)
    # ... (same for other fields)
```

**Serialization**:

- **Process mode**: Works natively (pickle)
- **Ray mode**: Uses `cloudpickle` for serialization
  - RetryConfig is serialized as bytes: `cloudpickle.dumps(retry_config)`
  - Deserialized inside Ray task: `cloudpickle.loads(retry_config_bytes)`
  - This avoids Pydantic's internal locks causing pickle failures

### 2. Retry Execution Functions

#### execute_with_retry (Sync)

**Signature**:
```python
def execute_with_retry(
    func: Callable,
    args: tuple,
    kwargs: dict,
    config: RetryConfig,
    context: Dict[str, Any],
) -> Any
```

**Purpose**: Execute synchronous function with retry loop.

**Algorithm**:

```python
max_attempts = config.num_retries + 1  # Initial + retries
all_results = []
all_exceptions = []
validation_errors = []

for attempt in 1..max_attempts:
    try:
        result = func(*args, **kwargs)
        
        # Check output validators (if any)
        if config.retry_until is not None:
            is_valid, error_msg = _validate_result(result, config.retry_until, context)
            if is_valid:
                return result  # Success
            else:
                all_results.append(result)
                validation_errors.append(error_msg)
                if attempt == max_attempts:
                    raise RetryValidationError(...)
                wait = calculate_retry_wait(attempt, config)
                time.sleep(wait)
                continue
        else:
            return result  # Success (no validators)
    
    except Exception as e:
        all_exceptions.append(e)
        
        # Check exception filters
        should_retry = _should_retry_on_exception(e, config.retry_on, context)
        if not should_retry:
            raise  # Not retriable
        
        if attempt == max_attempts:
            raise  # Retries exhausted
        
        wait = calculate_retry_wait(attempt, config)
        time.sleep(wait)

# Should never reach here (exceptions raised above)
```

**Context Dict**:

Passed to filters and validators:

```python
context = {
    "method_name": str,        # Function/method name
    "worker_class": str,       # Worker class name (or "TaskWorker")
    "attempt": int,            # Current attempt number (1-indexed)
    "elapsed_time": float,     # Seconds since first attempt
    "args": tuple,             # Original positional arguments
    "kwargs": dict,            # Original keyword arguments
}
```

#### execute_with_retry_async (Async)

**Signature**:
```python
async def execute_with_retry_async(
    async_func: Callable,
    args: tuple,
    kwargs: dict,
    config: RetryConfig,
    context: Dict[str, Any],
) -> Any
```

**Purpose**: Execute async function with retry loop.

**Differences from sync version**:
- Uses `await async_func(*args, **kwargs)` instead of `func(*args, **kwargs)`
- Uses `await asyncio.sleep(wait)` instead of `time.sleep(wait)`
- Otherwise identical algorithm

#### execute_with_retry_auto (TaskWorker Dispatcher)

**Signature**:
```python
def execute_with_retry_auto(
    fn: Callable,
    args: tuple,
    kwargs: dict,
    config: RetryConfig,
    context: Dict[str, Any],
) -> Any
```

**Purpose**: Automatically dispatch to correct retry function based on function type.

**Implementation**:

```python
if inspect.iscoroutinefunction(fn):
    # Async function - run with asyncio.run() for sync contexts
    return asyncio.run(execute_with_retry_async(fn, args, kwargs, config, context))
else:
    # Sync function - use regular retry
    return execute_with_retry(fn, args, kwargs, config, context)
```

**Usage Context**:

- Used ONLY in `_execute_task` methods of worker proxies for TaskWorker
- Simplifies proxy implementations by handling both sync/async in one call
- NOT used for AsyncioWorker (it directly uses `execute_with_retry_async`)

**Design Rationale**:

TaskWorker needs to handle arbitrary user functions that could be sync or async. The proxy doesn't know in advance which type the user will submit. `execute_with_retry_auto` centralizes this dispatch logic.

### 3. Worker Method Wrapping

#### _create_worker_wrapper

**Location**: `concurry/core/worker/base_worker.py`

**Purpose**: Dynamically create a wrapper class that adds retry logic to worker methods.

**Signature**:
```python
def _create_worker_wrapper(
    worker_cls: Type,
    limits: Any,
    retry_config: Optional[Any] = None,
    for_ray: bool = False,
) -> Type
```

**Wrapping Strategies**:

##### Strategy 1: __getattribute__ Interception (Sync/Thread/Process/Asyncio)

```python
class WorkerWithLimitsAndRetry(worker_cls):
    def __getattribute__(self, name: str):
        attr = super().__getattribute__(name)
        
        # Only wrap public methods if retry is configured AND not for Ray
        if (
            has_retry
            and not for_ray
            and not name.startswith("_")
            and callable(attr)
            and not isinstance(attr, type)
        ):
            # Check if already wrapped
            if hasattr(attr, "__wrapped_with_retry__"):
                return attr
            
            # Wrap the method
            wrapped = create_retry_wrapper(
                attr,
                retry_config,
                method_name=name,
                worker_class_name=worker_cls.__name__,
            )
            wrapped.__wrapped_with_retry__ = True
            return wrapped
        
        return attr
```

**Key Points**:
- Lazy wrapping: methods wrapped on first access, cached via `__wrapped_with_retry__` marker
- Only public methods (no `_` prefix) are wrapped
- Callable check excludes classes and non-callable attributes

##### Strategy 2: Pre-Wrapping at Class Level (Ray Only)

```python
if for_ray and has_retry:
    for attr_name in dir(worker_cls):
        if attr_name.startswith("_"):
            continue
        if attr_name not in worker_cls.__dict__:  # Skip inherited
            continue
        
        attr = getattr(worker_cls, attr_name)
        if not callable(attr):
            continue
        
        # Create wrapper that binds self at call time
        def make_wrapped_method(original_method, method_name):
            if inspect.iscoroutinefunction(original_method):
                async def async_method_wrapper(self, *args, **kwargs):
                    context = {
                        "method_name": method_name,
                        "worker_class_name": worker_cls.__name__,
                    }
                    bound_method = original_method.__get__(self, type(self))
                    return await execute_with_retry_async(
                        bound_method, args, kwargs, retry_config, context
                    )
                return async_method_wrapper
            else:
                def sync_method_wrapper(self, *args, **kwargs):
                    context = {
                        "method_name": method_name,
                        "worker_class_name": worker_cls.__name__,
                    }
                    bound_method = original_method.__get__(self, type(self))
                    return execute_with_retry(
                        bound_method, args, kwargs, retry_config, context
                    )
                return sync_method_wrapper
        
        wrapped = make_wrapped_method(attr, attr_name)
        setattr(WorkerWithLimitsAndRetry, attr_name, wrapped)
```

**Why Ray needs pre-wrapping**:

Ray's `ray.remote()` performs signature inspection and method wrapping at actor creation time. When `__getattribute__` is used, Ray's inspection sees a generic callable (the wrapper) instead of the actual method signature, causing `TypeError: too many positional arguments`.

Pre-wrapping at the class level ensures Ray sees the correct signatures during actor creation.

#### create_retry_wrapper

**Location**: `concurry/core/retry.py`

**Purpose**: Create a wrapper function that adds retry logic to a single method.

**Signature**:
```python
def create_retry_wrapper(
    method: Callable,
    config: RetryConfig,
    method_name: str,
    worker_class_name: str,
) -> Callable
```

**Implementation**:

```python
is_async = inspect.iscoroutinefunction(method)

if is_async:
    async def async_wrapper(*args, **kwargs):
        context = {
            "method_name": method_name,
            "worker_class": worker_class_name,
            "args": args,
            "kwargs": kwargs,
        }
        return await execute_with_retry_async(method, args, kwargs, config, context)
    
    async_wrapper.__name__ = method.__name__
    async_wrapper.__doc__ = method.__doc__
    return async_wrapper
else:
    def sync_wrapper(*args, **kwargs):
        context = {
            "method_name": method_name,
            "worker_class": worker_class_name,
            "args": args,
            "kwargs": kwargs,
        }
        return execute_with_retry(method, args, kwargs, config, context)
    
    sync_wrapper.__name__ = method.__name__
    sync_wrapper.__doc__ = method.__doc__
    return sync_wrapper
```

**Key Points**:
- Preserves method name and docstring for debugging
- Detects async methods automatically
- Creates appropriate wrapper (sync or async)

### 4. Backoff Algorithms and Jitter

#### calculate_retry_wait

**Location**: `concurry/core/retry.py`

**Purpose**: Calculate wait time based on attempt number, algorithm, and jitter.

**Signature**:
```python
def calculate_retry_wait(attempt: int, config: RetryConfig) -> float
```

**Algorithms**:

1. **Linear Backoff**:
   ```python
   wait = retry_wait * attempt
   # attempt=1 → 1×wait, attempt=2 → 2×wait, attempt=3 → 3×wait
   ```

2. **Exponential Backoff** (default):
   ```python
   wait = retry_wait * (2 ** (attempt - 1))
   # attempt=1 → 1×wait, attempt=2 → 2×wait, attempt=3 → 4×wait
   ```

3. **Fibonacci Backoff**:
   ```python
   wait = retry_wait * fibonacci(attempt)
   # attempt=1 → 1×wait, attempt=2 → 1×wait, attempt=3 → 2×wait, attempt=4 → 3×wait
   ```

**Fibonacci Sequence**:

```python
def _fibonacci(n: int) -> int:
    if n <= 2:
        return 1
    a, b = 1, 1
    for _ in range(n - 2):
        a, b = b, a + b
    return b
```

Sequence: `[1, 1, 2, 3, 5, 8, 13, 21, ...]`

**Jitter Application** (Full Jitter Algorithm):

After calculating base wait time:

```python
if config.retry_jitter > 0:
    wait = random.uniform(0, calculated_wait)
else:
    wait = calculated_wait

return max(0, wait)
```

**Full Jitter Benefits** (from AWS blog):
- Prevents thundering herd when many workers retry simultaneously
- Distributes retry attempts across time window
- Reduces burst load on failing services
- `jitter=1.0` → maximum randomization: `[0, calculated_wait]`
- `jitter=0.0` → no randomization: `calculated_wait`

**Example**:

```python
config = RetryConfig(
    retry_wait=1.0,
    retry_algorithm=RetryAlgorithm.Exponential,
    retry_jitter=1.0
)

# Attempt 1: [0, 2] seconds (base=1.0 * 2^0 = 1.0, jitter=[0, 1.0])
# Attempt 2: [0, 2] seconds (base=1.0 * 2^1 = 2.0, jitter=[0, 2.0])
# Attempt 3: [0, 4] seconds (base=1.0 * 2^2 = 4.0, jitter=[0, 4.0])
```

### 5. Exception Filtering

#### _should_retry_on_exception

**Location**: `concurry/core/retry.py`

**Purpose**: Determine if an exception should trigger retry based on filters.

**Signature**:
```python
def _should_retry_on_exception(
    exception: BaseException,
    retry_on_filters: List[Union[type, Callable]],
    context: Dict[str, Any],
) -> bool
```

**Algorithm**:

```python
for filter_item in retry_on_filters:
    if isinstance(filter_item, type) and issubclass(filter_item, BaseException):
        # Exception type filter
        if isinstance(exception, filter_item):
            return True
    elif callable(filter_item):
        # Callable filter
        try:
            if filter_item(exception=exception, **context):
                return True
        except Exception:
            # If filter raises, don't retry
            continue

return False
```

**Filter Types**:

1. **Exception Class**:
   ```python
   retry_on=[ValueError, ConnectionError]
   # Retries if exception is instance of ValueError or ConnectionError
   ```

2. **Callable Filter**:
   ```python
   retry_on=lambda exception, **ctx: (
       isinstance(exception, ValueError) and "retry" in str(exception)
   )
   # Custom logic: only retry ValueError if message contains "retry"
   ```

3. **Mixed**:
   ```python
   retry_on=[
       TimeoutError,  # Always retry TimeoutError
       lambda exception, **ctx: (  # Custom logic for ValueError
           isinstance(exception, ValueError) and ctx['attempt'] < 3
       )
   ]
   ```

**Context Available to Filters**:
- `exception`: The exception instance
- `method_name`: Name of method/function
- `worker_class`: Worker class name
- `attempt`: Current attempt number
- `elapsed_time`: Seconds since first attempt
- `args`, `kwargs`: Original call arguments

**Exception Hierarchy**:

```python
retry_on=[Exception]  # Default - catches all exceptions
# vs
retry_on=[ValueError]  # Only catches ValueError and subclasses
```

### 6. Output Validation

#### _validate_result

**Location**: `concurry/core/retry.py`

**Purpose**: Validate function output against validation functions.

**Signature**:
```python
def _validate_result(
    result: Any,
    validators: List[Callable],
    context: Dict[str, Any],
) -> tuple[bool, Optional[str]]
```

**Algorithm**:

```python
for validator in validators:
    try:
        if not validator(result=result, **context):
            validator_name = getattr(validator, "__name__", str(validator))
            return False, f"Validator '{validator_name}' returned False"
    except Exception as e:
        validator_name = getattr(validator, "__name__", str(validator))
        return False, f"Validator '{validator_name}' raised: {e}"

return True, None
```

**Validator Requirements**:

1. Accept `result` as first keyword argument
2. Accept `**context` for additional context
3. Return `bool`: `True` if valid, `False` if should retry
4. May raise exception (treated as validation failure)

**All Validators Must Pass**:

If `retry_until=[validator1, validator2, validator3]`, ALL must return `True` for result to be considered valid. This is AND logic, not OR.

**Example Validators**:

```python
# Simple type check
lambda result, **ctx: isinstance(result, dict)

# Field presence check
lambda result, **ctx: "data" in result and "status" in result

# Content validation
lambda result, **ctx: result.get("status") == "success"

# Complex validation with context
def validate_json_schema(result, **ctx):
    try:
        schema.validate(result)
        return True
    except ValidationError:
        return False
```

**Context Available to Validators**:
- `result`: The function output
- `method_name`: Name of method/function
- `worker_class`: Worker class name
- `attempt`: Current attempt number
- `elapsed_time`: Seconds since first attempt
- `args`, `kwargs`: Original call arguments

### 7. RetryValidationError

**Location**: `concurry/core/retry.py`

**Purpose**: Exception raised when output validation fails after all retry attempts.

**Attributes**:

```python
class RetryValidationError(Exception):
    attempts: int              # Total attempts made
    all_results: List[Any]    # All outputs from each attempt
    validation_errors: List[str]  # Failure reasons
    method_name: str          # Method that was retried
```

**Pickling Support**:

```python
def __reduce__(self):
    """Support pickling for multiprocessing."""
    return (
        self.__class__,
        (self.attempts, self.all_results, self.validation_errors, self.method_name),
    )
```

Required for Process and Ray modes where exceptions are pickled to send across process boundaries.

**User Handling**:

```python
try:
    result = worker.generate_json(prompt).result()
except RetryValidationError as e:
    # Inspect all attempts
    print(f"Failed after {e.attempts} attempts")
    print(f"Validation errors: {e.validation_errors}")
    
    # Use last result anyway (or log for debugging)
    last_output = e.all_results[-1]
    
    # Or inspect all results
    for i, (result, error) in enumerate(zip(e.all_results, e.validation_errors)):
        print(f"Attempt {i+1}: {error}")
        print(f"  Output: {result}")
```

## Integration with Other Systems

### Interaction with Limits

**Key Design Principle**: Retry and limits are **independent but coordinated**.

**Limits Release Pattern**:

```python
class APIWorker(Worker):
    def call_api(self, prompt: str):
        # Limits acquired in context manager
        with self.limits.acquire(requested={"tokens": 100}) as acq:
            result = external_api(prompt)
            acq.update(usage={"tokens": result.tokens})
            return result
```

**What Happens During Retry**:

1. **First attempt**:
   - Acquire limits via `self.limits.acquire()`
   - Execute method body
   - Exception raised before `acq.update()`
   - Context manager `__exit__` releases limits
   
2. **Retry attempt**:
   - Wait for backoff period (`calculate_retry_wait`)
   - Acquire limits again (fresh acquisition)
   - Execute method body again
   - On success: `acq.update()` called, context manager exits normally
   - On failure: limits released again

**Key Point**: Each retry is a **complete new invocation** of the method, including limit acquisition/release. This ensures:
- Failed attempts don't hold resources
- Resources are available for retry
- Other workers can access resources between retries

**Integration Points**:

```
User Method Call
    ↓
Retry Wrapper (execute_with_retry)
    ↓
    LOOP for attempt in 1..max_attempts:
        ↓
        User Method Body
            ↓
            with self.limits.acquire() as acq:  ← Acquire limits
                ↓
                ... method logic ...
                ↓
                acq.update(usage={...})  ← Update usage
            ↓  ← Limits released (success or exception)
        ↓
        If exception: check retry_on → continue loop or raise
        If success: check retry_until → return or continue loop
        ↓
        calculate_retry_wait + sleep
    END LOOP
```

### TaskWorker Integration

**Design Goal**: TaskWorker should support retry for arbitrary functions submitted via `submit()` and `map()`.

**Challenge**: Avoid double-wrapping retry logic.

**Solution**: Apply retry in `_execute_task`, not in `submit()`.

**Reasoning**:

If retry logic were in `submit()`:
```python
class TaskWorkerMixin:
    def submit(self, fn, *args, **kwargs):  # This is a public method
        # If this method has retry logic...
        future = self._execute_task(fn, *args, **kwargs)
        return future
```

Problem: `submit()` is a public method, so `__getattribute__` would ALSO wrap it with retry logic, causing:
- `N` retries at submit level × `M` retries at method level = `N×M` total retries
- Double overhead

**Correct Implementation**:

```python
# In TaskWorkerMixin (task_worker.py)
def submit(self, fn, *args, **kwargs):
    """Public method - does NOT have retry logic."""
    if self._stopped:
        raise RuntimeError("Worker is stopped")
    
    # Delegate to _execute_task (which has retry logic)
    future = self._execute_task(fn, *args, **kwargs)
    
    if self.blocking:
        return future.result()
    return future

# In worker proxy (e.g., sync_worker.py)
def _execute_task(self, fn, *args, **kwargs):
    """Private method - applies retry logic HERE."""
    if self.retry_config is not None and self.retry_config.num_retries > 0:
        context = {
            "method_name": fn.__name__ if hasattr(fn, "__name__") else "anonymous_function",
            "worker_class_name": "TaskWorker",
        }
        result = execute_with_retry_auto(fn, args, kwargs, self.retry_config, context)
    else:
        result = _invoke_function(fn, *args, **kwargs)
    
    return SyncFuture(result_value=result)
```

**Key Points**:
- `submit()` is public → excluded from `__getattribute__` wrapping (no double-wrap)
- `_execute_task()` is private → NOT wrapped by `__getattribute__`
- Retry logic explicitly applied in `_execute_task()`
- Works for both `submit()` and `map()` (both call `_execute_task`)

**Why execute_with_retry_auto?**

TaskWorker can receive ANY function - sync or async:

```python
# Sync function
worker.submit(lambda x: x * 2, 5)

# Async function
async def async_compute(x):
    await asyncio.sleep(0.01)
    return x * 2
worker.submit(async_compute, 5)
```

`execute_with_retry_auto` automatically detects function type and dispatches:
- Sync → `execute_with_retry`
- Async → `asyncio.run(execute_with_retry_async(...))`

## Serialization Concerns

### Process Mode (multiprocessing)

**Requirements**:
- All objects must be pickleable
- Includes: `RetryConfig`, exceptions, user functions

**RetryConfig Serialization**:

`RetryConfig` is a `morphic.Typed` (Pydantic) class. Pydantic models are pickleable by default.

**RetryValidationError Serialization**:

Must implement `__reduce__` for pickle:

```python
def __reduce__(self):
    return (
        self.__class__,
        (self.attempts, self.all_results, self.validation_errors, self.method_name),
    )
```

**User Function Serialization**:

User functions submitted to TaskWorker must be pickleable:
- Top-level functions: ✅ pickleable
- Lambdas: ✅ pickleable (with dill/cloudpickle)
- Closures: ⚠️ Limited (captured variables must be pickleable)
- Local functions: ❌ Not pickleable

### Ray Mode

**Challenge**: Ray uses `cloudpickle` but has specific requirements for actors.

**RetryConfig Serialization for TaskWorker**:

Ray TaskWorker uses special pattern to serialize `RetryConfig`:

```python
# In ray_worker.py _execute_task
if self.retry_config is not None and self.retry_config.num_retries > 0:
    import cloudpickle
    
    original_fn = fn
    retry_config_bytes = cloudpickle.dumps(self.retry_config)  # Serialize to bytes
    
    def ray_retry_wrapper(*inner_args, **inner_kwargs):
        import cloudpickle
        from ..retry import execute_with_retry_auto
        
        # Deserialize inside Ray task
        r_config = cloudpickle.loads(retry_config_bytes)
        context = {
            "method_name": original_fn.__name__ if hasattr(original_fn, "__name__") else "anonymous_function",
            "worker_class_name": "TaskWorker",
        }
        return execute_with_retry_auto(original_fn, inner_args, inner_kwargs, r_config, context)
    
    fn = ray_retry_wrapper
```

**Why this pattern?**

Pydantic models contain internal locks (`_thread.lock`) that are not pickleable. By serializing to bytes upfront with `cloudpickle`, we:
1. Serialize the entire config object at task submission time (client-side)
2. Pass the bytes into the Ray remote function closure
3. Deserialize inside the Ray task (worker-side)
4. Avoid Ray's default pickling attempting to serialize the lock

**Regular Worker Methods**:

For regular worker methods (not TaskWorker), Ray actors are created with pre-wrapped methods at class level. The `retry_config` is passed during actor creation and lives on the actor instance, so no special serialization needed.

## Performance Considerations

### Zero Overhead When Disabled

**Goal**: When `num_retries=0`, retry logic should have **zero performance impact**.

**Implementation**:

1. **WorkerBuilder Short-Circuit**:
   ```python
   def _create_retry_config(self) -> Optional[Any]:
       if self.num_retries == 0:
           return None  # Don't create RetryConfig
       
       return RetryConfig(...)
   ```

2. **Wrapper Creation Short-Circuit**:
   ```python
   def _create_worker_wrapper(worker_cls, limits, retry_config, for_ray):
       has_retry = retry_config is not None and retry_config.num_retries > 0
       
       if not has_retry:
           # Return simple wrapper (only sets limits)
           return WorkerWithLimits  # No __getattribute__ overhead
       
       return WorkerWithLimitsAndRetry  # Has retry wrapping
   ```

3. **TaskWorker Short-Circuit**:
   ```python
   def _execute_task(self, fn, *args, **kwargs):
       if self.retry_config is not None and self.retry_config.num_retries > 0:
           # Apply retry logic
           result = execute_with_retry_auto(...)
       else:
           # Direct execution - no retry overhead
           result = _invoke_function(fn, *args, **kwargs)
   ```

**Performance Impact**:

- `num_retries=0`: No `__getattribute__` override, direct method calls
- `num_retries>0`: One `__getattribute__` intercept + retry wrapper overhead (~1-2 µs per call)

### Caching Wrapped Methods

**Pattern**: Wrapped methods cached to avoid repeated wrapping.

**Implementation**:

```python
def __getattribute__(self, name: str):
    attr = super().__getattribute__(name)
    
    # Check if already wrapped
    if hasattr(attr, "__wrapped_with_retry__"):
        return attr
    
    # Wrap and mark
    wrapped = create_retry_wrapper(...)
    wrapped.__wrapped_with_retry__ = True
    return wrapped
```

**Benefit**: First access pays wrapping cost, subsequent accesses return cached wrapper.

**Caveat**: Cache is per-attribute-access, not per-instance. But since methods are bound at access time, this is negligible overhead.

## Constraints and Limitations

### 1. Closure Capture in Process/Ray Modes

**Limitation**: Closures that capture mutable state don't work across processes.

**Example**:

```python
call_count = [0]  # Mutable closure variable

def flaky_function():
    call_count[0] += 1
    if call_count[0] < 3:
        raise ValueError("Retry me")
    return "success"

# Works in sync/thread/asyncio modes
worker = TaskWorker.options(mode="thread").init()
worker.submit(flaky_function)  # call_count updates correctly

# DOES NOT WORK in process/ray modes
worker = TaskWorker.options(mode="process").init()
worker.submit(flaky_function)  # call_count NOT shared across processes
```

**Workaround**: Use time-based or stateless retry logic:

```python
import time
start_time = time.time()

def flaky_function():
    if time.time() - start_time < 0.1:  # Fail for first 100ms
        raise ValueError("Retry me")
    return "success"
```

### 2. RetryConfig Not Directly Accessible in User Code

**Limitation**: User methods cannot directly access `retry_config` during execution.

**Example**:

```python
class MyWorker(Worker):
    def compute(self, x):
        # Cannot access retry_config here
        # attempt = self.retry_config.attempt  # NO
        return x * 2
```

**Workaround**: Use filters/validators to pass retry context:

```python
config = RetryConfig(
    num_retries=3,
    retry_on=lambda exception, attempt, **ctx: (
        # Can access attempt number in filter
        isinstance(exception, ValueError) and attempt < 3
    )
)
```

### 3. Async Functions in Sync/Thread/Process Modes

**Behavior**: Async functions ARE supported, but executed via `asyncio.run()`.

**Implication**: No concurrency benefit in sync/thread/process modes.

**Example**:

```python
async def async_task(x):
    await asyncio.sleep(1)
    return x * 2

# Works but no concurrency
worker = TaskWorker.options(mode="thread").init()
worker.submit(async_task, 5)  # Uses asyncio.run() internally
```

For true async concurrency, use `mode="asyncio"`:

```python
worker = TaskWorker.options(mode="asyncio").init()
worker.submit(async_task, 5)  # Proper async concurrency
```

### 4. Retry Affects Total Execution Time

**Implication**: User must account for retry delays in timeouts.

**Example**:

```python
config = RetryConfig(
    num_retries=3,
    retry_wait=2.0,
    retry_algorithm=RetryAlgorithm.Exponential
)
# Max wait time: 2^0 + 2^1 + 2^2 = 1+2+4 = 7 seconds (without jitter)
# Total time: execution_time * 4 attempts + 7 seconds wait

worker = MyWorker.options(mode="thread", num_retries=3, retry_wait=2.0).init()
future = worker.long_task()
result = future.result(timeout=30)  # Must account for retries + waits
```

## Extension Points

### Adding New Retry Algorithms

**Location**: `concurry/core/constants.py` + `concurry/core/retry.py`

**Steps**:

1. **Add to RetryAlgorithm enum**:
   ```python
   # In constants.py
   class RetryAlgorithm(str, AutoEnum):
       Linear = auto()
       Exponential = auto()
       Fibonacci = auto()
       Custom = auto()  # New algorithm
   ```

2. **Implement in calculate_retry_wait**:
   ```python
   # In retry.py
   def calculate_retry_wait(attempt: int, config: RetryConfig) -> float:
       base_wait = config.retry_wait
       
       if config.retry_algorithm == RetryAlgorithm.Custom:
           calculated_wait = custom_algorithm(attempt, base_wait)
       elif config.retry_algorithm == RetryAlgorithm.Linear:
           # ... existing logic
       
       # Apply jitter
       if config.retry_jitter > 0:
           wait = random.uniform(0, calculated_wait)
       else:
           wait = calculated_wait
       
       return max(0, wait)
   ```

3. **Add default to GlobalDefaults**:
   ```python
   # In config.py
   class GlobalDefaults(MutableTyped):
       retry_algorithm: RetryAlgorithm = RetryAlgorithm.Exponential
   ```

### Adding Context Information to Filters/Validators

**Pattern**: Add fields to `context` dict in retry execution functions.

**Example**: Adding retry count to context:

```python
# In execute_with_retry
for attempt in range(1, max_attempts + 1):
    current_context = {
        **context,
        "attempt": attempt,
        "elapsed_time": time.time() - start_time,
        "max_attempts": max_attempts,  # NEW FIELD
    }
    
    try:
        result = func(*args, **kwargs)
        if config.retry_until is not None:
            is_valid, error_msg = _validate_result(result, config.retry_until, current_context)
            # ...
```

Now validators can access `max_attempts`:

```python
lambda result, attempt, max_attempts, **ctx: (
    # Only validate strictly on last attempt
    strict_check(result) if attempt == max_attempts else lenient_check(result)
)
```

### Supporting Custom Validation Logic

**Pattern**: Subclass `RetryConfig` to add custom validation.

**Example**:

```python
from concurry.core.retry import RetryConfig
from pydantic import field_validator

class CustomRetryConfig(RetryConfig):
    max_total_time: Optional[float] = None
    
    @field_validator("max_total_time")
    @classmethod
    def validate_max_total_time(cls, v):
        if v is not None and v <= 0:
            raise ValueError("max_total_time must be positive")
        return v
```

Then modify `execute_with_retry` to check `max_total_time`:

```python
start_time = time.time()
for attempt in range(1, max_attempts + 1):
    # Check total time limit
    if hasattr(config, "max_total_time") and config.max_total_time is not None:
        if time.time() - start_time >= config.max_total_time:
            raise TimeoutError(f"Retry exceeded max_total_time {config.max_total_time}")
    
    # ... rest of retry logic
```

## Design Decisions and Rationale

### Why Actor-Side Retry?

**Decision**: All retry logic runs inside the worker (actor/process/thread), not client-side.

**Rationale**:

1. **Efficiency**: Avoid round-trip latency for each retry
   - Client-side: Submit → Execute → Fail → Return to client → Submit again
   - Actor-side: Submit → Execute → Fail → Retry locally → Return when done

2. **Serialization**: Only final result needs to cross process boundary
   - Intermediate failures and retries stay local
   - Reduces serialization overhead

3. **Resource Fairness**: Actor can release resources between retries
   - Local retry loop can call `self.limits.acquire()` multiple times
   - Client-side retry would need to re-submit entire task

4. **Simplicity**: User sees one future per call, not multiple retries
   - Abstraction: user doesn't need to manage retry loop
   - Debugging: retry state lives with execution, not distributed

### Why Pre-Wrap Ray Methods?

**Decision**: Ray actors use pre-wrapped methods at class level, not `__getattribute__`.

**Rationale**:

Ray's `ray.remote()` decorator inspects method signatures at actor creation time. When `__getattribute__` intercepts method access:

```python
class MyActor:
    def method(self, x: int) -> int:
        return x * 2

# Ray sees:
def __getattribute__(self, name):
    # ... returns a wrapper
    pass

# Ray can't inspect actual signature of 'method'
# → TypeError: too many positional arguments
```

Pre-wrapping ensures Ray sees correct signatures:

```python
class MyActor:
    def method(self, x: int) -> int:  # Original signature preserved
        # (wrapped at class level before ray.remote)
        return x * 2

# Ray sees the actual method signature
```

### Why RetryValidationError Contains All Results?

**Decision**: `RetryValidationError` stores all attempt results, not just the last one.

**Rationale**:

1. **Debugging**: Users can inspect all outputs to understand why validation failed
2. **Recovery**: Users can choose any result to use (e.g., "least bad" output)
3. **Logging**: Users can log all attempts for analysis
4. **Transparency**: Full visibility into retry process

**Example Use Case**:

```python
try:
    result = llm_worker.generate_json(prompt).result()
except RetryValidationError as e:
    # Pick result with most valid fields
    best_result = max(e.all_results, key=lambda r: count_valid_fields(r))
    return best_result
```

### Why execute_with_retry_auto for TaskWorker?

**Decision**: Create separate `execute_with_retry_auto` dispatcher instead of having each proxy handle sync/async detection.

**Rationale**:

1. **DRY**: Avoid duplicating sync/async detection across 5 worker proxies
2. **Consistency**: Single source of truth for dispatch logic
3. **Simplicity**: Each proxy just calls `execute_with_retry_auto`, no branching
4. **Maintainability**: Future changes to dispatch logic only need one place

**Alternative Considered**: Have each proxy check `inspect.iscoroutinefunction`:

```python
# REJECTED: duplicated across all proxies
def _execute_task(self, fn, *args, **kwargs):
    if inspect.iscoroutinefunction(fn):
        result = asyncio.run(execute_with_retry_async(...))
    else:
        result = execute_with_retry(...)
```

**Chosen**: Single dispatcher:

```python
# ACCEPTED: centralized logic
def _execute_task(self, fn, *args, **kwargs):
    result = execute_with_retry_auto(fn, args, kwargs, ...)
```

## Testing Strategy

### Test Categories

1. **Unit Tests** (`tests/core/retry/test_retry.py`):
   - `RetryConfig` validation
   - `calculate_retry_wait` algorithms
   - `_should_retry_on_exception` filtering
   - `_validate_result` validation
   - `execute_with_retry` / `execute_with_retry_async` logic

2. **Integration Tests** (`tests/core/retry/test_worker_retry.py`):
   - Retry with regular worker methods
   - Retry with exception filters
   - Retry with output validators
   - Retry + limits interaction
   - Retry + worker pools
   - Retry + Pydantic validation
   - TaskWorker + retry

### Cross-Execution-Mode Testing

**Pattern**: Use `worker_mode` fixture from `conftest.py`:

```python
def test_basic_retry(self, worker_mode):
    """Test retry across all execution modes."""
    worker = MyWorker.options(
        mode=worker_mode,
        num_retries=3
    ).init()
    
    result = worker.flaky_method().result()
    assert result == expected
    
    worker.stop()
```

**Modes Tested**: sync, thread, process, asyncio, ray (if installed)

### Edge Cases to Test

1. **Zero retries**: `num_retries=0` → no retry overhead
2. **Max retries exhausted**: Correct exception propagation
3. **Successful retry**: Return correct result
4. **Mixed filters**: Exception types + callables
5. **All validators**: AND logic for multiple `retry_until`
6. **Validation error**: `RetryValidationError` with all results
7. **Async methods**: Both sync and async retried correctly
8. **Limits + retry**: Limits released between retries
9. **Pool + retry**: Each worker retries independently
10. **Serialization**: Process/Ray modes handle `RetryConfig` and exceptions

## Common Pitfalls

### 1. Forgetting to Set num_retries

**Problem**: Creating `RetryConfig` but not setting `num_retries > 0`.

```python
config = RetryConfig(retry_algorithm=RetryAlgorithm.Exponential)
# num_retries defaults to 0 → no retries!
```

**Solution**: Always explicitly set `num_retries`:

```python
config = RetryConfig(
    num_retries=3,
    retry_algorithm=RetryAlgorithm.Exponential
)
```

### 2. Closure Capture in Process/Ray

**Problem**: Using mutable closures for retry logic in process/ray modes.

```python
attempts = [0]
def task():
    attempts[0] += 1  # Won't work across processes
    if attempts[0] < 3:
        raise ValueError()
```

**Solution**: Use stateless logic:

```python
import time
start = time.time()

def task():
    if time.time() - start < 0.1:  # Fails for first 100ms
        raise ValueError()
```

### 3. Timeout Too Short for Retries

**Problem**: Future timeout doesn't account for retry delays.

```python
worker = MyWorker.options(num_retries=5, retry_wait=2.0).init()
future = worker.slow_task()
result = future.result(timeout=5)  # TOO SHORT!
# Max wait: 2 + 4 + 8 + 16 + 32 = 62 seconds + execution time
```

**Solution**: Calculate max retry time:

```python
# Exponential: sum of 2^0, 2^1, ..., 2^(n-1)
# = 2^n - 1
max_retry_wait = (2 ** config.num_retries - 1) * config.retry_wait
timeout = execution_time + max_retry_wait + buffer
```

### 4. Validator Side Effects

**Problem**: Validators modifying state or having side effects.

```python
results_log = []

config = RetryConfig(
    retry_until=lambda result, **ctx: (
        results_log.append(result),  # SIDE EFFECT
        result["valid"]
    )[1]
)
```

**Solution**: Keep validators pure:

```python
def is_valid(result, **ctx):
    return result.get("valid", False)

config = RetryConfig(retry_until=is_valid)
```

### 5. Not Checking RetryValidationError

**Problem**: Not handling `RetryValidationError` when using `retry_until`.

```python
# Missing try/except
result = worker.generate_json(prompt).result()
# If validation fails → RetryValidationError raised
```

**Solution**: Always handle validation errors:

```python
try:
    result = worker.generate_json(prompt).result()
except RetryValidationError as e:
    # Handle validation failure
    logger.error(f"Validation failed: {e.validation_errors}")
    result = fallback_value
```

## Summary

The retry mechanism in Concurry is designed as an actor-side, zero-overhead-when-disabled system that seamlessly integrates with all execution modes and worker types. Key architectural principles:

1. **Actor-side execution**: Retries run locally within workers for efficiency
2. **Dynamic wrapping**: Methods wrapped at runtime (or class-level for Ray)
3. **TaskWorker integration**: Retry applied in `_execute_task` to avoid double-wrapping
4. **Algorithm flexibility**: Linear, exponential, Fibonacci backoff with full jitter
5. **Filter composability**: Exception types and callables for retry decisions
6. **Output validation**: `retry_until` validators for LLM/API responses
7. **Serialization handling**: Special patterns for Process/Ray modes
8. **Limits coordination**: Independent but coordinated resource management

The architecture enables robust, production-ready retry behavior while maintaining zero overhead when retries are disabled.

