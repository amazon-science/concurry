# Configuration System Architecture

This document describes the design, implementation, and maintenance guidelines for Concurry's global configuration system.

## Design Goals

1. **Eliminate hardcoded defaults** - All timeouts, intervals, and algorithm choices should be configurable
2. **Hierarchical configuration** - Support global defaults with mode-specific overrides
3. **Type safety** - Leverage Pydantic for validation and type checking
4. **Thread safety** - Allow safe concurrent access from multiple threads
5. **Explicit over implicit** - Make configuration flow transparent and predictable
6. **Maintainability** - Clear rules for where defaults should live

## Architecture Overview

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                      GlobalConfig                            │
│  ┌────────────────┐  ┌────────────────────────────────────┐ │
│  │ GlobalDefaults │  │  ExecutionModeDefaults (per mode)  │ │
│  │   (base)       │  │   - sync                            │ │
│  └────────────────┘  │   - asyncio                         │ │
│                      │   - thread                          │ │
│                      │   - process                         │ │
│                      │   - ray                             │ │
│                      └────────────────────────────────────┘ │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │         ResolvedDefaults (runtime view)                 │ │
│  │  Falls back: mode -> global                             │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Class Hierarchy

1. **`GlobalDefaults`** (`MutableTyped`)
   - Contains all base default values
   - Every configurable parameter starts here
   - Mutable to allow runtime updates

2. **`ExecutionModeDefaults`** (`MutableTyped`)
   - Contains mode-specific overrides
   - All fields are `Optional[...]` (default to `None`)
   - When `None`, falls back to `GlobalDefaults`

3. **`GlobalConfig`** (`MutableTyped`)
   - Top-level configuration container
   - Has one `GlobalDefaults` instance
   - Has one `ExecutionModeDefaults` per mode (sync, asyncio, thread, process, ray)
   - Provides `get_defaults(mode)` to return `ResolvedDefaults`

4. **`ResolvedDefaults`** (read-only proxy)
   - Runtime view that resolves mode → global fallback
   - Each property checks mode-specific value first, then global
   - Not mutable; exists only for reading resolved values

### Global Instance

```python
# In concurry/config.py
global_config = GlobalConfig()

# Users access via:
from concurry import global_config
```

## Configuration Flow

### User-Facing Classes (Public API)

**Rule**: Public API classes should **NOT** have hardcoded defaults in their fields.

**Implementation Pattern**:

```python
from concurry.core.base import Typed
from concurry.config import global_config

class RetryConfig(Typed):
    """Public API class for retry configuration."""
    
    # Fields default to None
    num_retries: Optional[int] = None
    retry_wait: Optional[float] = None
    retry_algorithm: Optional[RetryAlgorithm] = None
    
    def post_initialize(self) -> None:
        """Load defaults from global config if not provided."""
        # Clone for thread safety (user code may be multi-threaded)
        local_config = global_config.clone()
        
        if self.num_retries is None:
            self.num_retries = local_config.defaults.num_retries
        if self.retry_wait is None:
            self.retry_wait = local_config.defaults.retry_wait
        if self.retry_algorithm is None:
            self.retry_algorithm = local_config.defaults.retry_algorithm
```

**Why**:
- Users can override any parameter explicitly
- If not overridden, uses current global configuration
- Allows runtime configuration changes to affect new instances
- Thread-safe via `clone()`

**Examples of public API classes**:
- `Worker` (via `Worker.options()`)
- `RetryConfig`
- `RateLimit`
- `LimitPool`
- `Limit` (call, resource)
- `PollingStrategy` classes (when instantiated by users)
- `ProgressBar`

### Internal/Private Classes

**Rule**: Internal classes should **NOT** have any defaults. All values must be explicitly passed.

**Implementation Pattern**:

```python
class ThreadWorkerProxy:
    """Internal class - not part of public API."""
    
    def __init__(
        self,
        command_queue_timeout: float,  # No default!
        stop_timeout: float,            # No default!
        # ... other params
    ):
        self._command_queue_timeout = command_queue_timeout
        self._stop_timeout = stop_timeout
```

**Why**:
- Internal classes are only instantiated by Concurry's own code
- The calling code (e.g., `WorkerBuilder`) is responsible for reading from `global_config`
- Prevents "default drift" where the same default is specified in multiple places
- Makes configuration flow explicit and easier to trace
- Catches bugs if a parameter is not being passed

**Examples of internal classes**:
- All `*WorkerProxy` classes (Thread, Process, Asyncio, Ray, Sync)
- All `*WorkerProxyPool` classes
- `RateLimiter` implementations (TokenBucket, LeakyBucket, etc.)
- `LoadBalancer` implementations
- Internal helper classes

### Where Defaults Are Applied

Defaults are read from `global_config` and passed to internal classes at these locations:

1. **`WorkerBuilder._create_worker_proxy_pool()`**
   - Reads mode-specific timeouts and pool settings
   - Passes them to `*WorkerProxyPool` constructors
   
2. **`*WorkerProxyPool._create_worker()`**
   - Reads worker-specific timeouts
   - Passes them to `*WorkerProxy` constructors

3. **`Worker.options()` / `Worker.init()`**
   - Reads worker options (num_retries, blocking, etc.)
   - Stores in `WorkerOptions`, which has defaults applied in `post_initialize()`

4. **Public API class `post_initialize()` methods**
   - Each public API class applies its own defaults
   - E.g., `RetryConfig`, `RateLimit`, `LimitPool`

5. **Factory functions**
   - `Poller()` function reads defaults for polling strategies
   - `LimitSet()` function reads defaults before creating limit set

6. **Algorithm implementations at usage time**
   - Rate limiters read `rate_limiter_min_wait_time` in `acquire()`
   - Limit sets read `limit_set_acquire_sleep` in `acquire()`
   - Worker pools read `worker_pool_cleanup_sleep` in cleanup loops

## Thread Safety

### `global_config.clone()`

Used for reading configuration in **user-facing code** (code that might be called from multiple threads):

```python
def some_public_api_method(self):
    # Clone for thread safety
    local_config = global_config.clone()
    timeout = local_config.defaults.stop_timeout
    # Use timeout...
```

**Why clone**:
- Users might modify `global_config` from another thread
- Cloning creates a snapshot at a point in time
- Prevents race conditions where config changes mid-operation

### Direct Access

Internal library code (single-threaded, deterministic flow) can access directly:

```python
# In WorkerBuilder (called during Worker.init(), which is synchronous)
def _create_worker_proxy_pool(self, mode: ExecutionMode):
    # Direct access is safe here - we're in a controlled context
    defaults = global_config.get_defaults(mode)
    timeout = defaults.command_queue_timeout
    # ...
```

**When direct access is safe**:
- Code runs synchronously (e.g., `Worker.init()`)
- Code runs in a single worker's context
- Code is not exposed to user threads

## Adding New Configuration Parameters

### Step-by-Step Guide

1. **Add to `GlobalDefaults`**:

```python
class GlobalDefaults(MutableTyped):
    # ... existing fields ...
    
    # New parameter (with concrete default value)
    my_new_timeout: float = 10.0
```

2. **Add to `ExecutionModeDefaults`**:

```python
class ExecutionModeDefaults(MutableTyped):
    # ... existing fields ...
    
    # Optional override (None = use global default)
    my_new_timeout: Optional[float] = None
```

3. **Add to `ResolvedDefaults`**:

```python
class ResolvedDefaults:
    # ... existing properties ...
    
    @property
    def my_new_timeout(self) -> float:
        """
        Timeout for my new feature.
        Falls back to global_my_new_timeout if mode-specific value is None.
        """
        if self._mode_defaults.my_new_timeout is not None:
            return self._mode_defaults.my_new_timeout
        return self._global_defaults.my_new_timeout
```

4. **Update `temp_config()` validation**:

```python
# In temp_config() function
valid_attributes = {
    # Global
    'global_my_new_timeout',
    # Mode-specific
    'thread_my_new_timeout',
    'process_my_new_timeout',
    'asyncio_my_new_timeout',
    'ray_my_new_timeout',
    'sync_my_new_timeout',
    # ... existing attributes ...
}
```

5. **Update calling code**:

If the parameter is for an internal class, update the code that instantiates it:

```python
# In some factory or builder
local_config = global_config.clone()  # or direct access if safe
my_timeout = local_config.defaults.my_new_timeout

internal_obj = InternalClass(
    my_timeout=my_timeout,  # Pass explicitly
    # ...
)
```

If the parameter is for a public API class, update its `post_initialize()`:

```python
class MyPublicClass(Typed):
    my_timeout: Optional[float] = None
    
    def post_initialize(self) -> None:
        local_config = global_config.clone()
        if self.my_timeout is None:
            self.my_timeout = local_config.defaults.my_new_timeout
```

6. **Update docstrings**:

Update any docstrings that mention default values:

```python
def __init__(self, my_timeout: Optional[float] = None):
    """
    Args:
        my_timeout: Timeout in seconds. Defaults to global_config.defaults.my_new_timeout.
    """
```

7. **Add tests**:

Add test coverage in `tests/test_global_config.py`:

```python
def test_my_new_timeout_global():
    with temp_config(global_my_new_timeout=20.0):
        obj = MyPublicClass()
        assert obj.my_timeout == 20.0

def test_my_new_timeout_mode_specific():
    with temp_config(thread_my_new_timeout=30.0):
        # Verify mode-specific override works
        pass
```

## Common Patterns

### Pattern 1: Simple Timeout

**Scenario**: Adding a new timeout to an internal class.

```python
# 1. Add to GlobalDefaults
class GlobalDefaults(MutableTyped):
    new_operation_timeout: float = 5.0

# 2. Add to ExecutionModeDefaults  
class ExecutionModeDefaults(MutableTyped):
    new_operation_timeout: Optional[float] = None

# 3. Add to ResolvedDefaults
class ResolvedDefaults:
    @property
    def new_operation_timeout(self) -> float:
        if self._mode_defaults.new_operation_timeout is not None:
            return self._mode_defaults.new_operation_timeout
        return self._global_defaults.new_operation_timeout

# 4. Update internal class (no default!)
class InternalClass:
    def __init__(self, operation_timeout: float):
        self._timeout = operation_timeout

# 5. Update caller
defaults = global_config.get_defaults(mode)
obj = InternalClass(operation_timeout=defaults.new_operation_timeout)
```

### Pattern 2: Algorithm Choice

**Scenario**: Adding a configurable algorithm default.

```python
# 1. Add to GlobalDefaults (with concrete enum default)
class GlobalDefaults(MutableTyped):
    my_algorithm: MyAlgorithm = MyAlgorithm.DefaultOption

# 2-3. Add to ExecutionModeDefaults and ResolvedDefaults (as above)

# 4. Update public API class
class MyPublicClass(Typed):
    algorithm: Optional[MyAlgorithm] = None
    
    def post_initialize(self) -> None:
        local_config = global_config.clone()
        if self.algorithm is None:
            self.algorithm = local_config.defaults.my_algorithm
```

### Pattern 3: Mode-Specific Only

**Scenario**: A parameter that only makes sense for specific modes.

```python
# 1. Add to GlobalDefaults (even if only some modes use it)
class GlobalDefaults(MutableTyped):
    thread_pool_specific_option: int = 100

# 2-3. Add to ExecutionModeDefaults and ResolvedDefaults

# 4. Use in mode-specific code
# In ThreadWorkerProxyPool
defaults = global_config.get_defaults(ExecutionMode.Threads)
option = defaults.thread_pool_specific_option
# Process mode can ignore this, or have its own override
```

## Gotchas and Limitations

### 1. Circular Import Issues

**Problem**: `config.py` imports constants, which might import modules that need config.

**Solution**: Import `global_config` inside functions, not at module level:

```python
# BAD (at module level)
from concurry.config import global_config

class MyClass:
    def method(self):
        timeout = global_config.defaults.timeout

# GOOD (inside function/method)
class MyClass:
    def method(self):
        from concurry.config import global_config
        timeout = global_config.defaults.timeout
```

### 2. Forgetting `post_initialize()`

**Problem**: Adding fields to a `Typed` class without calling `post_initialize()`.

**Solution**: Pydantic automatically calls `post_initialize()` after `__init__`, but only if it's defined. Always define it for public API classes:

```python
class MyPublicClass(Typed):
    my_field: Optional[int] = None
    
    def post_initialize(self) -> None:
        # MUST call parent if it exists
        super().post_initialize()
        
        # Then set defaults
        if self.my_field is None:
            local_config = global_config.clone()
            self.my_field = local_config.defaults.my_field
```

### 3. Default Values in Internal Classes

**Problem**: Accidentally adding a default to an internal class parameter.

```python
# BAD
class InternalWorkerProxy:
    def __init__(self, timeout: float = 5.0):  # ❌ Has default!
        pass

# GOOD
class InternalWorkerProxy:
    def __init__(self, timeout: float):  # ✅ No default!
        pass
```

**Detection**: Run regex search `=\s*\d+\.?\d*` in internal class constructors.

### 4. Docstring Staleness

**Problem**: Docstrings mention hardcoded values instead of config keys.

```python
# BAD
def __init__(self, retries: Optional[int] = None):
    """
    Args:
        retries: Number of retries. Defaults to 3.  # ❌ Hardcoded
    """

# GOOD
def __init__(self, retries: Optional[int] = None):
    """
    Args:
        retries: Number of retries. Defaults to global_config.defaults.num_retries.  # ✅
    """
```

### 5. Forgetting Mode-Specific Fields

**Problem**: Adding to `GlobalDefaults` but not `ExecutionModeDefaults`.

**Impact**: Users can't override per-mode.

**Solution**: Always add to both, even if you think only one mode needs it.

### 6. Missing `temp_config()` Attributes

**Problem**: Adding a config field but forgetting to update `temp_config()` validation.

**Impact**: `temp_config(my_new_field=...)` raises "Invalid attribute" error.

**Solution**: After adding fields, update the `valid_attributes` set in `temp_config()`.

## Verification and Testing

### Manual Verification Steps

1. **Search for numeric defaults in internal classes**:
   ```bash
   # Look for hardcoded defaults
   grep -rn "=\s*[0-9]" src/concurry/core/worker/*.py
   grep -rn "=\s*[0-9]" src/concurry/core/algorithms/*.py
   ```

2. **Search for enum defaults**:
   ```bash
   # Look for enum defaults
   grep -rn "=\s*LoadBalancingAlgorithm\." src/concurry/
   grep -rn "=\s*RateLimitAlgorithm\." src/concurry/
   grep -rn "=\s*PollingAlgorithm\." src/concurry/
   ```

3. **Search for time.sleep() calls**:
   ```bash
   # Check for hardcoded sleep times
   grep -rn "time.sleep(" src/concurry/core/
   ```

### Automated Testing

Add tests for each new config parameter:

```python
def test_new_config_global():
    """Test global default is applied."""
    with temp_config(global_my_param=123):
        obj = MyClass()
        assert obj.my_param == 123

def test_new_config_mode_specific():
    """Test mode-specific override."""
    with temp_config(
        global_my_param=100,
        thread_my_param=200
    ):
        # Thread mode uses override
        thread_defaults = global_config.get_defaults(ExecutionMode.Threads)
        assert thread_defaults.my_param == 200
        
        # Ray mode uses global
        ray_defaults = global_config.get_defaults(ExecutionMode.Ray)
        assert ray_defaults.my_param == 100

def test_new_config_explicit_override():
    """Test explicit parameter wins."""
    with temp_config(global_my_param=100):
        obj = MyClass(my_param=999)
        assert obj.my_param == 999
```

## Migration Guide

If you find hardcoded defaults in existing code:

1. **Identify the value type** (timeout, interval, algorithm, etc.)
2. **Add to config** (GlobalDefaults, ExecutionModeDefaults, ResolvedDefaults)
3. **Remove the hardcoded default** from the parameter
4. **Update callers** to pass the value from config
5. **Update docstrings** to reference config key
6. **Add tests** for the new config parameter
7. **Update user guide** if it's a user-facing setting

## Maintenance Checklist

When reviewing PRs that touch defaults:

- [ ] Are all defaults in `GlobalDefaults`?
- [ ] Are all optional overrides in `ExecutionModeDefaults`?
- [ ] Are all properties in `ResolvedDefaults` with fallback logic?
- [ ] Are internal classes free of default values?
- [ ] Do public API classes use `post_initialize()` to apply defaults?
- [ ] Are docstrings updated to reference config keys?
- [ ] Is `temp_config()` validation updated?
- [ ] Are tests added for the new config?
- [ ] Is the user guide updated if needed?

## Related Documentation

- [User Guide: Configuration](../user-guide/configuration.md) - End-user documentation
- [Cursor Rules: Configuration](../../.cursorrules) - Quick reference for maintainers and LLM tools

