# Global Configuration

Concurry provides a powerful global configuration system that allows you to customize default behavior across all execution modes. This eliminates the need to repeatedly specify the same parameters and makes it easy to tune performance for your specific use case.

## Overview

The configuration system has two levels:

1. **Global defaults** - Apply to all execution modes unless overridden
2. **Mode-specific overrides** - Take precedence for a specific execution mode (sync, asyncio, thread, process, ray)

## Quick Start

### Accessing the Global Configuration

```python
from concurry import global_config

# View current defaults
print(global_config.defaults.num_retries)  # 0
print(global_config.defaults.retry_wait)   # 1.0

# View mode-specific settings
print(global_config.thread.max_workers)     # 30
print(global_config.ray.max_queued_tasks)   # 3
```

### Modifying Defaults Permanently

```python
from concurry import global_config

# Change global defaults (affects all modes)
global_config.defaults.num_retries = 3
global_config.defaults.retry_wait = 2.0

# Change mode-specific settings
global_config.thread.max_workers = 50
global_config.ray.max_queued_tasks = 10

# Reset to library defaults
global_config.reset_to_defaults()
```

### Temporary Configuration with `temp_config()`

The recommended approach is using the `temp_config()` context manager:

```python
from concurry import temp_config, Worker

class MyWorker(Worker):
    def process(self, data):
        return data * 2

# Temporary global override
with temp_config(global_num_retries=5, global_retry_wait=3.0):
    # All workers created here use these retry settings
    worker1 = MyWorker.options(mode="thread").init()
    worker2 = MyWorker.options(mode="ray").init()
    # Both have num_retries=5, retry_wait=3.0
    
    worker1.stop()
    worker2.stop()
# Original settings restored after context

# Mode-specific overrides
with temp_config(
    thread_max_workers=100,
    ray_max_queued_tasks=50
):
    # Only affects thread and ray modes respectively
    pass
```

## Configuration Categories

### Worker & Pool Configuration

Configure worker pool behavior:

```python
with temp_config(
    thread_max_workers=50,          # Max workers in thread pool
    thread_max_queued_tasks=2000,   # Max queued tasks
    process_max_workers=8,          # Max workers in process pool
    ray_max_workers=0               # 0 = unlimited for Ray
):
    pass
```

### Execution Configuration

Control blocking behavior and future unwrapping:

```python
with temp_config(
    global_blocking=True,           # Return results directly, not futures
    global_unwrap_futures=False     # Don't auto-unwrap nested futures
):
    worker = MyWorker.options(mode="thread").init()
    result = worker.process(10)  # Returns result directly (blocking=True)
    worker.stop()
```

### Retry Configuration

Configure retry behavior globally:

```python
from concurry import temp_config
from concurry.core.retry import RetryAlgorithm

with temp_config(
    global_num_retries=3,                          # Retry up to 3 times
    global_retry_algorithm=RetryAlgorithm.Exponential,  # Exponential backoff
    global_retry_wait=2.0,                         # Min 2s wait
    global_retry_jitter=0.5                        # 50% jitter
):
    pass
```

### Load Balancing Configuration

Configure how tasks are distributed across workers:

```python
from concurry import temp_config
from concurry.core.constants import LoadBalancingAlgorithm

with temp_config(
    thread_load_balancing=LoadBalancingAlgorithm.LeastActiveLoad,
    process_load_balancing=LoadBalancingAlgorithm.RoundRobin
):
    pass
```

### Rate Limiting Configuration

Configure rate limiter behavior:

```python
from concurry import temp_config
from concurry.core.constants import RateLimitAlgorithm

with temp_config(
    global_rate_limit_algorithm=RateLimitAlgorithm.TokenBucket,
    global_rate_limiter_min_wait_time=0.05  # Min 50ms between checks
):
    pass
```

### Polling Strategy Configuration

Configure how `wait()` and `gather()` poll for results:

```python
with temp_config(
    # Fixed polling
    global_polling_fixed_interval=0.02,  # Check every 20ms
    
    # Adaptive polling
    global_polling_adaptive_min_interval=0.0005,   # 0.5ms min
    global_polling_adaptive_max_interval=0.3,      # 300ms max
    global_polling_adaptive_initial_interval=0.02, # Start at 20ms
    
    # Exponential polling
    global_polling_exponential_initial_interval=0.02,  # Start at 20ms
    global_polling_exponential_max_interval=3.0,       # Max 3s
    
    # Progressive polling
    global_polling_progressive_min_interval=0.0005,    # 0.5ms min
    global_polling_progressive_max_interval=1.0        # 1s max
):
    pass
```

### Internal Timeouts

Configure internal timeouts (advanced):

```python
with temp_config(
    # Thread mode timeouts
    thread_worker_command_queue_timeout=0.2,
    
    # Process mode timeouts
    process_worker_result_queue_timeout=60.0,
    process_worker_result_queue_cleanup_timeout=2.0,
    
    # Asyncio mode timeouts
    asyncio_worker_loop_ready_timeout=60.0,
    asyncio_worker_thread_ready_timeout=60.0,
    asyncio_worker_sync_queue_timeout=0.2,
    
    # Pool timeouts (all modes)
    thread_pool_on_demand_cleanup_timeout=10.0,
    thread_pool_on_demand_slot_max_wait=120.0
):
    pass
```

### Progress Bar Configuration

Configure progress bar display:

```python
with temp_config(
    global_progress_bar_ncols=120,      # Wider progress bars
    global_progress_bar_smoothing=0.2,  # More smoothing
    global_progress_bar_miniters=10     # Update every 10 iterations
):
    from concurry.utils.progress import ProgressBar
    
    for item in ProgressBar(range(1000)):
        # Uses custom settings
        pass
```

## Hierarchical Resolution

When looking up a configuration value, Concurry follows this resolution order:

1. **Explicit parameter** passed to the function/method (highest priority)
2. **Mode-specific override** from `global_config.<mode>.*`
3. **Global default** from `global_config.defaults.*` (lowest priority)

Example:

```python
from concurry import temp_config, Worker

# Set global default
with temp_config(global_num_retries=3):
    # Set thread-specific override
    with temp_config(thread_num_retries=10):
        # Explicit parameter wins
        worker = Worker.options(
            mode="thread",
            num_retries=5  # This value is used
        ).init()
        
        # Without explicit parameter, mode-specific wins
        worker2 = Worker.options(mode="thread").init()
        # Uses num_retries=10 (thread override)
        
        # Different mode uses global default
        worker3 = Worker.options(mode="ray").init()
        # Uses num_retries=3 (global default)
```

## Best Practices

### 1. Use `temp_config()` for Tests

```python
import pytest
from concurry import temp_config

def test_with_custom_config():
    with temp_config(
        global_num_retries=0,  # No retries in tests
        thread_max_workers=2   # Fewer workers for testing
    ):
        # Test code here
        pass
```

### 2. Set Global Defaults at Application Startup

```python
from concurry import global_config

def configure_concurry():
    """Configure Concurry for production environment."""
    # Aggressive retries
    global_config.defaults.num_retries = 3
    global_config.defaults.retry_wait = 2.0
    
    # Larger thread pools
    global_config.thread.max_workers = 100
    global_config.thread.max_queued_tasks = 5000
    
    # Ray configuration
    global_config.ray.max_workers = 0  # Unlimited
    
if __name__ == "__main__":
    configure_concurry()
    # Start application
```

### 3. Prefer Mode-Specific Overrides Over Global Changes

When you need different behavior per mode:

```python
# Good: Specific overrides
with temp_config(
    thread_max_workers=100,
    process_max_workers=8,
    ray_max_workers=0
):
    pass

# Less ideal: Global setting that might not fit all modes
with temp_config(global_max_workers=100):
    pass
```

### 4. Document Configuration Dependencies

If your code relies on specific configuration:

```python
def heavy_processing():
    """
    Process heavy workloads.
    
    Configuration:
        Recommended to increase thread pool size:
        >>> with temp_config(thread_max_workers=200):
        ...     heavy_processing()
    """
    pass
```

## Common Patterns

### Performance Tuning

```python
# High-throughput configuration
with temp_config(
    thread_max_workers=200,
    thread_max_queued_tasks=10000,
    global_polling_fixed_interval=0.001,  # Poll more frequently
    global_rate_limiter_min_wait_time=0.001
):
    # High-performance processing
    pass
```

### Conservative/Reliable Configuration

```python
# Conservative settings for reliability
with temp_config(
    global_num_retries=5,
    global_retry_wait=5.0,
    thread_max_workers=10,        # Fewer workers
    thread_max_queued_tasks=100,  # Smaller queues
    global_stop_timeout=60.0      # Longer stop timeout
):
    # Reliable processing
    pass
```

### Development/Debug Configuration

```python
# Debug-friendly settings
with temp_config(
    thread_max_workers=1,          # Single worker for easier debugging
    global_num_retries=0,          # No retries to see failures immediately
    global_blocking=True,          # Synchronous for simpler stack traces
    global_stop_timeout=5.0        # Quick shutdown
):
    # Development code
    pass
```

## Configuration Reference

For a complete list of all configuration options, see:

- `GlobalDefaults` class in `concurry/config.py` for global settings
- `ExecutionModeDefaults` class in `concurry/config.py` for mode-specific settings

You can also inspect the configuration at runtime:

```python
from concurry import global_config
import pprint

# See all global defaults
pprint.pprint(global_config.defaults.model_dump())

# See all thread mode settings
pprint.pprint(global_config.thread.model_dump())

# Get resolved defaults for a specific mode
from concurry.core.constants import ExecutionMode
resolved = global_config.get_defaults(ExecutionMode.Threads)
print(resolved.num_retries)  # Falls back to global if thread-specific is None
```

## Related Documentation

- [Architecture: Configuration System](../architecture/configuration.md) - Detailed design and implementation
- [Workers](workers.md) - Worker configuration options
- [Retries](retries.md) - Retry configuration
- [Limits](limits.md) - Rate limiting configuration

