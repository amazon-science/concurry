# Limits

Limits in concurry provide flexible, composable resource protection and rate limiting. They enable you to control resource usage, enforce rate limits, and track consumption across different dimensions simultaneously.

## Overview

The limit system has two layers:

### Layer 1: Limit Definitions (Data Containers)

1. **RateLimit** - Time-based rate limiting with multiple algorithms
2. **CallLimit** - Call counting (special case of RateLimit)
3. **ResourceLimit** - Semaphore-based resource limiting

**Important**: `Limit` objects are simple data containers that define constraints. They are **NOT thread-safe** and cannot be acquired directly.

### Layer 2: LimitSet (Thread-Safe Executor)

**LimitSet** is a factory function that creates thread-safe limit executors. It handles:
- Thread-safe acquisition and release
- Atomic multi-limit acquisition
- Partial acquisition (nested patterns)
- Backend selection based on execution mode

**LimitSet returns**:
- `InMemorySharedLimitSet` - For sync, thread, asyncio (uses `threading.Lock`)
- `MultiprocessSharedLimitSet` - For process mode (uses `multiprocessing.Manager`)
- `RaySharedLimitSet` - For Ray mode (uses Ray actor)

## Quick Reference

### Basic Pattern

```python
from concurry import LimitSet, RateLimit, CallLimit, ResourceLimit, RateLimiterAlgorithm

# 1. Define limits (data containers)
limits = LimitSet(limits=[
    CallLimit(window_seconds=60, capacity=100),
    RateLimit(key="tokens", window_seconds=60, capacity=1000),
    ResourceLimit(key="connections", capacity=10)
])

# 2. Acquire limits (thread-safe)
with limits.acquire(requested={"tokens": 50, "connections": 2}) as acq:
    result = do_work()
    # 3. Update RateLimit usage
    acq.update(usage={"tokens": result.actual_tokens})
    # CallLimit and ResourceLimit auto-handled
```

### Key Behaviors

| Feature | Behavior |
|---------|----------|
| **Limit objects** | Data containers only, NOT thread-safe |
| **LimitSet** | Factory function, creates thread-safe executor |
| **CallLimit** | Always acquired with default of 1, no update needed |
| **ResourceLimit** | Always acquired with default if not specified, no update needed |
| **RateLimit** | Must be in `requested` dict, requires `update()` call |
| **Partial acquisition** | Specify only what you need, CallLimit/ResourceLimit auto-included |
| **Nested acquisition** | Supported, enables fine-grained resource management |
| **Shared limits** | `shared=True` (default) shares limits across workers |
| **Mode matching** | `mode` parameter must match worker execution mode |

## Basic Usage

### Creating a LimitSet

Always use `LimitSet` to create thread-safe limit executors:

```python
from concurry import LimitSet, RateLimit, RateLimiterAlgorithm

# Define limit constraints (data containers)
rate_limit = RateLimit(
    key="api_tokens",
    window_seconds=60,
    algorithm=RateLimiterAlgorithm.TokenBucket,
    capacity=1000
)

# Create thread-safe LimitSet
limits = LimitSet(
    limits=[rate_limit],
    shared=True,  # Default: share across workers
    mode="sync"   # Default: for sync/thread/asyncio
)

# Acquire and use tokens (thread-safe)
with limits.acquire(requested={"api_tokens": 100}) as acq:
    result = call_api()
    # Update with actual usage
    acq.update(usage={"api_tokens": result.actual_tokens})
```

**Key points:**
- `Limit` objects are data containers - use `LimitSet` for thread-safe operations
- `LimitSet` is a factory that creates appropriate backend implementations
- Always call `acq.update()` for RateLimits to report actual usage
- Unused tokens may be refunded (algorithm-specific)
- Usage must not exceed requested amount

### RateLimit

RateLimits enforce time-based constraints on resource usage, such as API tokens, bandwidth, or request rates.

```python
from concurry import LimitSet, RateLimit, RateLimiterAlgorithm

# Define rate limit
rate_limit = RateLimit(
    key="api_tokens",
    window_seconds=60,
    algorithm=RateLimiterAlgorithm.TokenBucket,
    capacity=1000
)

# Create LimitSet for thread-safe usage
limits = LimitSet(limits=[rate_limit])

# Use it
with limits.acquire(requested={"api_tokens": 100}) as acq:
    result = call_api()
    acq.update(usage={"api_tokens": result.actual_tokens})
```

### CallLimit

CallLimit is a special RateLimit for counting calls, where usage is always 1.

```python
from concurry import LimitSet, CallLimit, RateLimiterAlgorithm

# Define call limit
call_limit = CallLimit(
    window_seconds=60,
    algorithm=RateLimiterAlgorithm.SlidingWindow,
    capacity=100
)

# Create LimitSet
limits = LimitSet(limits=[call_limit])

# Each acquisition counts as 1 call (automatic)
with limits.acquire():
    make_api_call()
```

**Key points:**
- Fixed key: `"call_count"`
- Usage is always 1 (validated)
- No need to call `update()` - handled automatically
- Perfect for call rate limits independent of resource usage

### ResourceLimit

ResourceLimits provide simple counting for finite resources like database connections or file handles.

```python
from concurry import LimitSet, ResourceLimit

# Define resource limit
resource_limit = ResourceLimit(
    key="db_connections",
    capacity=10
)

# Create LimitSet
limits = LimitSet(limits=[resource_limit])

# Acquire 2 connections
with limits.acquire(requested={"db_connections": 2}):
    conn1 = get_connection()
    conn2 = get_connection()
    execute_queries(conn1, conn2)
# Connections automatically released
```

**Key points:**
- No time component (unlike RateLimit)
- Automatic release on context exit
- No need to call `update()` - handled automatically
- Thread-safe semaphore logic handled by LimitSet

## Rate Limiting Algorithms

RateLimit supports five algorithms with different characteristics:

### TokenBucket

Allows bursts up to capacity while maintaining average rate. Tokens refill continuously.

```python
limit = RateLimit(
    key="tokens",
    window_seconds=60,
    algorithm=RateLimiterAlgorithm.TokenBucket,
    capacity=1000
)
```

**Best for:** APIs that allow occasional bursts

**Characteristics:**
- Burst handling: Excellent
- Precision: Good
- Memory: Low
- Refunding: Yes

### LeakyBucket

Processes requests at fixed rate, smoothing traffic.

```python
limit = RateLimit(
    key="tokens",
    window_seconds=60,
    algorithm=RateLimiterAlgorithm.LeakyBucket,
    capacity=1000
)
```

**Best for:** Predictable, steady-state traffic

**Characteristics:**
- Burst handling: Poor (by design)
- Precision: Excellent
- Memory: Low
- Refunding: No

### SlidingWindow

Precise rate limiting with rolling time window. More accurate than fixed window.

```python
limit = RateLimit(
    key="tokens",
    window_seconds=60,
    algorithm=RateLimiterAlgorithm.SlidingWindow,
    capacity=1000
)
```

**Best for:** Precise rate limiting without fixed window edge cases

**Characteristics:**
- Burst handling: Good
- Precision: Excellent
- Memory: Higher (stores timestamps)
- Refunding: No

### FixedWindow

Simple rate limiting with fixed time buckets. Fast but can allow 2x burst at window boundaries.

```python
limit = RateLimit(
    key="tokens",
    window_seconds=60,
    algorithm=RateLimiterAlgorithm.FixedWindow,
    capacity=1000
)
```

**Best for:** Simple rate limiting where edge cases are acceptable

**Characteristics:**
- Burst handling: Poor (2x burst at boundaries)
- Precision: Moderate
- Memory: Lowest
- Refunding: No

### GCRA (Generic Cell Rate Algorithm)

Most precise rate limiting using theoretical arrival time tracking.

```python
limit = RateLimit(
    key="tokens",
    window_seconds=60,
    algorithm=RateLimiterAlgorithm.GCRA,
    capacity=1000
)
```

**Best for:** Strict rate control with precise timing

**Characteristics:**
- Burst handling: Excellent
- Precision: Best
- Memory: Low
- Refunding: Yes

## LimitSet: Multi-Dimensional Limiting

LimitSet enables atomic acquisition of multiple limits simultaneously with full thread-safety.

### Basic Multi-Dimensional Limiting

```python
from concurry import (
    LimitSet, RateLimit, CallLimit, ResourceLimit,
    RateLimiterAlgorithm
)

# Create LimitSet with multiple limit types
limits = LimitSet(limits=[
    CallLimit(
        window_seconds=60,
        algorithm=RateLimiterAlgorithm.SlidingWindow,
        capacity=100
    ),
    RateLimit(
        key="input_tokens",
        window_seconds=60,
        algorithm=RateLimiterAlgorithm.GCRA,
        capacity=10_000
    ),
    RateLimit(
        key="output_tokens",
        window_seconds=60,
        algorithm=RateLimiterAlgorithm.TokenBucket,
        capacity=1_000
    ),
    ResourceLimit(
        key="db_connections",
        capacity=10
    )
])

# Acquire specific limits atomically
# CallLimit is automatically acquired with default of 1
with limits.acquire(requested={
    "input_tokens": 500,
    "output_tokens": 50,
    "db_connections": 2
}) as acq:
    result = process_data()
    
    # Update RateLimits with actual usage
    acq.update(usage={
        "input_tokens": result.input_used,
        "output_tokens": result.output_used
    })
    # CallLimit and ResourceLimit handled automatically
```

**Key behavior:**
- When `requested` is specified, CallLimit and ResourceLimit are **automatically included** with default of 1
- RateLimits must be explicitly specified in `requested`
- All limits are acquired atomically (all-or-nothing)

### Nested Acquisition Pattern

LimitSet supports **partial acquisition**, enabling powerful nested patterns:

```python
# Level 1: Acquire long-lived resources
with limits.acquire(requested={"db_connections": 2}):
    # Do setup with connections
    
    # Level 2: Acquire rate limits for operations
    # Note: CallLimit still automatically acquired here
    with limits.acquire(requested={
        "input_tokens": 100,
        "output_tokens": 50
    }) as rate_acq:
        result = call_api()
        rate_acq.update(usage={
            "input_tokens": result.input_used,
            "output_tokens": result.output_used
        })
    
    # Connections still held here, but tokens released
    
    # Another rate-limited operation
    with limits.acquire(requested={
        "input_tokens": 200,
        "output_tokens": 20
    }) as rate_acq2:
        result2 = call_api()
        rate_acq2.update(usage={
            "input_tokens": result2.input_used,
            "output_tokens": result2.output_used
        })
    
    # Connections released at end of outer context
```

**Benefits of nested acquisition:**
- Hold resources only as long as needed
- Reduces resource contention
- More efficient limit utilization
- Better granular control

### Non-Blocking try_acquire

```python
acq = limits.try_acquire(requested={
    "input_tokens": 1000,
    "db_connections": 1
})

if acq.successful:
    with acq:
        # All limits acquired
        result = expensive_operation()
        acq.update(usage={"input_tokens": result.tokens})
else:
    # Could not acquire all limits immediately
    print("Resources not available, will retry later")
```

## Worker Integration

Limits integrate seamlessly with Workers via the `limits` parameter. You can pass either a `LimitSet` or a list of `Limit` objects.

### Option 1: Pass LimitSet (Recommended for Sharing)

```python
from concurry import Worker, LimitSet, RateLimit, ResourceLimit, RateLimiterAlgorithm

# Create shared LimitSet
shared_limits = LimitSet(
    limits=[
        RateLimit(
            key="api_tokens",
            window_seconds=60,
            algorithm=RateLimiterAlgorithm.TokenBucket,
            capacity=1000
        ),
        ResourceLimit(
            key="db_connections",
            capacity=5
        )
    ],
    shared=True,  # Shared across workers (default)
    mode="thread"  # Match worker mode
)

class LLMWorker(Worker):
    def __init__(self, model: str):
        self.model = model
    
    def process(self, prompt: str) -> str:
        # Nested acquisition pattern
        with self.limits.acquire(requested={"db_connections": 1}):
            context = get_context_from_db()
            
            with self.limits.acquire(requested={"api_tokens": 500}) as acq:
                result = call_llm(self.model, prompt, context)
                acq.update(usage={"api_tokens": result.tokens_used})
                return result.text

# Multiple workers share the same limits
workers = [
    LLMWorker.options(mode="thread", limits=shared_limits).init("gpt-4")
    for _ in range(5)
]
```

### Option 2: Pass List of Limits (Private Per Worker)

```python
# Define limits as list
limit_definitions = [
    RateLimit(
        key="api_tokens",
        window_seconds=60,
        algorithm=RateLimiterAlgorithm.TokenBucket,
        capacity=1000
    ),
    ResourceLimit(key="db_connections", capacity=5)
]

# Each worker creates its own private LimitSet
worker = LLMWorker.options(
    mode="thread",
    limits=limit_definitions  # List, not LimitSet
).init("gpt-4")
```

**Behavior:**
- Passing a `LimitSet`: Workers share the same limits
- Passing a `List[Limit]`: Each worker gets its own private `LimitSet`

### Execution Modes

Limits work with all execution modes, with appropriate backend selection:

| Mode | LimitSet Backend | Shared Across |
|------|------------------|---------------|
| `sync` | `InMemorySharedLimitSet` | Same process |
| `thread` | `InMemorySharedLimitSet` | Same process |
| `asyncio` | `InMemorySharedLimitSet` | Same process |
| `process` | `MultiprocessSharedLimitSet` | Multiple processes |
| `ray` | `RaySharedLimitSet` | Ray cluster |

```python
# For process workers
process_limits = LimitSet(
    limits=[...],
    shared=True,
    mode="process"  # Uses multiprocessing.Manager
)

worker = MyWorker.options(mode="process", limits=process_limits).init()

# For Ray workers
ray_limits = LimitSet(
    limits=[...],
    shared=True,
    mode="ray"  # Uses Ray actor
)

worker = MyWorker.options(mode="ray", limits=ray_limits).init()
```

## Shared vs Non-Shared LimitSets

`LimitSet` supports both shared and non-shared modes via the `shared` parameter (defaults to `True`).

### Shared LimitSets (shared=True, default)

Multiple workers share the same limit pool:

```python
# Create shared LimitSet
shared_limits = LimitSet(
    limits=[
        RateLimit(
            key="api_tokens",
            window_seconds=60,
            algorithm=RateLimiterAlgorithm.TokenBucket,
            capacity=1000
        )
    ],
    shared=True,  # Default
    mode="thread"
)

# All workers share the 1000 token/minute limit
workers = [
    APIWorker.options(mode="thread", limits=shared_limits).init()
    for _ in range(5)
]

# If worker 1 uses 600 tokens, only 400 remain for all workers
```

### Non-Shared LimitSets (shared=False)

Each worker gets its own independent limit pool:

```python
# Create non-shared LimitSet (less common)
non_shared_limits = LimitSet(
    limits=[...],
    shared=False,
    mode="sync"  # Must be "sync" for non-shared
)

# Each worker gets its own copy with separate limits
worker1 = APIWorker.options(mode="sync", limits=non_shared_limits).init()
worker2 = APIWorker.options(mode="sync", limits=non_shared_limits).init()

# worker1's usage doesn't affect worker2's limits
```

**Note**: Non-shared mode only works with `mode="sync"`. For most use cases, you want `shared=True` (the default).

### Backend Types and Performance

LimitSet automatically selects the appropriate backend based on `mode`:

| Backend | Modes | Synchronization | Overhead |
|---------|-------|----------------|----------|
| `InMemorySharedLimitSet` | sync, thread, asyncio | `threading.Lock` | 1-5 μs |
| `MultiprocessSharedLimitSet` | process | `multiprocessing.Manager` | 50-100 μs |
| `RaySharedLimitSet` | ray | Ray actor (0.01 CPU) | 500-1000 μs |

```python
# InMemorySharedLimitSet: Fast, in-process
thread_limits = LimitSet(limits=[...], shared=True, mode="thread")

# MultiprocessSharedLimitSet: Cross-process
process_limits = LimitSet(limits=[...], shared=True, mode="process")

# RaySharedLimitSet: Distributed
ray_limits = LimitSet(limits=[...], shared=True, mode="ray")
```

## Advanced Patterns

### Conditional Limiting

```python
def process_with_priority(priority: str, data):
    # High priority gets more tokens
    requested = {"api_tokens": 1000 if priority == "high" else 100}
    
    with limits.acquire(requested=requested) as acq:
        result = process(data)
        acq.update(usage={"api_tokens": result.actual_tokens})
        return result
```

### Graceful Degradation

```python
def process_with_fallback(data):
    # Try premium service first
    acq = premium_limits.try_acquire(requested={"premium_tokens": 100})
    
    if acq.successful:
        with acq:
            result = premium_service(data)
            acq.update(usage={"premium_tokens": result.tokens})
            return result
    else:
        # Fall back to basic service
        with basic_limits.acquire(requested={"basic_tokens": 10}) as acq:
            result = basic_service(data)
            acq.update(usage={"basic_tokens": result.tokens})
            return result
```

### Monitoring and Observability

```python
def monitor_limits(limits: LimitSet):
    """Print current limit statistics."""
    stats = limits.get_stats()
    
    for key, limit_stats in stats.items():
        print(f"\nLimit: {key}")
        for stat_name, value in limit_stats.items():
            print(f"  {stat_name}: {value}")

# Get individual limit stats
token_limit = limits["api_tokens"]
token_stats = token_limit.get_stats()
print(f"Available tokens: {token_stats['available_tokens']}")
print(f"Utilization: {token_stats['utilization']:.2%}")
```

### Timeout Handling

```python
try:
    with limits.acquire(
        requested={"api_tokens": 1000},
        timeout=5.0
    ) as acq:
        result = expensive_operation()
        acq.update(usage={"api_tokens": result.tokens})
except TimeoutError:
    print("Could not acquire tokens within 5 seconds")
    # Handle timeout - queue for later, use cached result, etc.
```

## Best Practices

### 1. Choose the Right Algorithm

- **TokenBucket**: For APIs with burst tolerance (most common)
- **GCRA**: For strict rate control with precise timing
- **SlidingWindow**: When you need precision without burst issues
- **LeakyBucket**: For smooth, predictable traffic
- **FixedWindow**: When simplicity matters more than edge cases

### 2. Always Use LimitSet, Not Limit Directly

```python
# ✅ Good: Use LimitSet for thread-safe operations
limits = LimitSet(limits=[
    RateLimit(key="tokens", window_seconds=60, capacity=1000)
])
with limits.acquire(requested={"tokens": 100}) as acq:
    result = operation()
    acq.update(usage={"tokens": result.actual_cost})

# ❌ Bad: Don't use Limit directly (not thread-safe!)
limit = RateLimit(key="tokens", window_seconds=60, capacity=1000)
# limit.acquire() doesn't exist!
```

### 3. Always Update RateLimit Usage

```python
# ✅ Good: Report actual usage for RateLimits
with limits.acquire(requested={"tokens": 100}) as acq:
    result = operation()
    acq.update(usage={"tokens": result.actual_cost})

# ❌ Bad: Missing update for RateLimit (raises RuntimeError)
with limits.acquire(requested={"tokens": 100}) as acq:
    result = operation()
    # Missing acq.update()! Will raise error on context exit
```

**Note**: CallLimit and ResourceLimit are automatic and don't need `update()`.

### 4. Use Nested Acquisition for Better Resource Management

```python
# ✅ Good: Nest resources and rate limits
with limits.acquire(requested={"db_connections": 1}):
    # Setup
    with limits.acquire(requested={"tokens": 100}) as acq:
        result = do_work()
        acq.update(usage={"tokens": result.tokens})
    # Connection still held, tokens released

# ❌ Avoid: Acquiring everything at once for long operations
with limits.acquire(requested={"db_connections": 1, "tokens": 100}) as acq:
    # Connection AND tokens held for entire duration
    long_running_operation()
    acq.update(usage={"tokens": 100})
```

### 5. Handle Timeouts Gracefully

```python
# ✅ Good: Handle timeout and provide feedback
try:
    with limits.acquire(requested={"tokens": 1000}, timeout=3.0) as acq:
        result = operation()
        acq.update(usage={"tokens": result.tokens})
except TimeoutError:
    logger.warning("Rate limit timeout, queueing for later")
    queue.put(task)
```

### 6. Monitor Limit Utilization

```python
# ✅ Good: Regular monitoring
def check_limit_health():
    stats = limits.get_stats()
    for key, limit_stats in stats.items():
        if limit_stats.get('utilization', 0) > 0.9:
            alert(f"Limit {key} at {limit_stats['utilization']:.0%}")
```

### 7. Match LimitSet Mode to Worker Mode

```python
# ✅ Good: Match modes for shared limits
thread_limits = LimitSet(limits=[...], shared=True, mode="thread")
workers = [
    Worker.options(mode="thread", limits=thread_limits).init()
    for _ in range(5)
]

# ❌ Don't: Mix execution modes
process_limits = LimitSet(limits=[...], shared=True, mode="process")
worker = Worker.options(mode="thread", limits=process_limits).init()
# ^ Won't work! Mode mismatch
```

### 8. Use Partial Acquisition (CallLimit/ResourceLimit Auto-Included)

```python
# ✅ Good: Specify only what you need - CallLimit auto-acquired
limits = LimitSet(limits=[
    CallLimit(window_seconds=60, capacity=100),
    RateLimit(key="tokens", window_seconds=60, capacity=1000)
])

# CallLimit automatically acquired with default of 1
with limits.acquire(requested={"tokens": 50}) as acq:
    result = operation()
    acq.update(usage={"tokens": result.tokens})
    # CallLimit was automatically acquired and released
```

## Error Handling

### Common Errors

**ValueError: Usage exceeds requested**
```python
# Cause: Trying to use more than requested
acq = limit.acquire(requested=100)
acq.update(used=150)  # Error!

# Solution: Request sufficient amount upfront
acq = limit.acquire(requested=200)
acq.update(used=150)  # OK
```

**RuntimeError: Not all limits updated**
```python
# Cause: Missing update() call for RateLimit
with limits.acquire(requested={"tokens": 100}) as acq:
    pass  # Error on exit - no update!

# Solution: Always update RateLimits
with limits.acquire(requested={"tokens": 100}) as acq:
    result = operation()
    acq.update(usage={"tokens": result.tokens})
```

**TimeoutError: Failed to acquire**
```python
# Cause: Could not acquire within timeout
limits.acquire(requested={"tokens": 1000}, timeout=1.0)

# Solution: Handle timeout or increase timeout
try:
    limits.acquire(requested={"tokens": 1000}, timeout=5.0)
except TimeoutError:
    # Queue for later, use cached result, etc.
    pass
```

## Performance Considerations

### Acquisition Overhead

| Backend | Overhead | Use Case |
|---------|----------|----------|
| InMemory | 1-5 μs | Single process |
| Multiprocess | 50-100 μs | Multi-process |
| Ray | 500-1000 μs | Distributed |

### Algorithm Performance

| Algorithm | Memory | CPU | Precision |
|-----------|--------|-----|-----------|
| FixedWindow | Lowest | Lowest | Moderate |
| TokenBucket | Low | Low | Good |
| GCRA | Low | Low | Best |
| LeakyBucket | Low | Medium | Excellent |
| SlidingWindow | Higher | Medium | Excellent |

### Optimization Tips

1. **Batch operations** when possible to reduce acquire/release cycles
2. **Use try_acquire** for non-critical operations
3. **Monitor utilization** to right-size limits
4. **Choose simpler algorithms** (FixedWindow, TokenBucket) for high-throughput scenarios
5. **Use nested acquisition** to minimize resource holding time

## See Also

- [Workers Guide](workers.md) - Integrating limits with Workers
- [API Reference](../api/limits.md) - Detailed API documentation
- [Examples](../examples.md) - More limit usage examples


