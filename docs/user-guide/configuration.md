# Global Configuration

**Configure Concurry to adapt to your environment—from local debugging to production scaling.**

## The Problem: "It works on my machine"

Hardcoding values like `max_workers=10` or `num_retries=3` scattered throughout your codebase creates a mess:
*   **Dev**: You want verbose logging and synchronous execution for debugging.
*   **Test**: You want deterministic behavior and zero retries to catch bugs fast.
*   **Prod**: You want massive parallelism, aggressive retries, and quiet logging.

If you hardcode these values, you have to change code to switch environments.

## The Solution: Hierarchical Configuration

Concurry solves this with a **Global Configuration System** that separates *what* your code does from *how* it executes.

### Configuration Hierarchy

Concurry resolves settings in this order (Specificity wins):

```text
1. Explicit Argument (Highest Priority)
   worker = Worker.options(num_retries=5).init()
      |
      v
2. Mode-Specific Config
   global_config.thread.num_retries = 3
      |
      v
3. Global Default (Lowest Priority)
   global_config.defaults.num_retries = 0
```

This means you can set safe defaults globally, tune them for specific execution modes (like Thread vs Ray), and override them for specific critical workers.

---

## Quick Start

### 1. Accessing Defaults
All defaults live in `concurry.global_config`.

```python
from concurry import global_config

# Check defaults
print(global_config.defaults.num_retries)      # 0
print(global_config.thread.max_workers)        # 30
print(global_config.ray.max_queued_tasks)      # 3
```

### 2. The `temp_config` Context Manager (Recommended)
The safest way to change config is temporarily using a context manager. This is thread-safe and scopes changes to a block.

```python
from concurry import worker, temp_config

@worker(mode="thread")
class MyWorker:
    def process(self, data):
        return data * 2

# "Production Mode" block
with temp_config(
    global_num_retries=3,           # Retry everything 3 times
    thread_max_workers=100,         # Scale up threads
    global_retry_wait=1.0           # Wait 1s between retries
):
    # This worker inherits the config automatically!
    w = MyWorker.init()
    
    # ... do work ...
    w.stop()

# Settings revert here
```

---

## Cookbook: Configuration Patterns

### Pattern 1: "Debug Mode" (Local Development)
When debugging, concurrency is your enemy. You want things to run one at a time, fail fast, and show you exactly what happened.

```python
# config/dev.py
from concurry import global_config

def configure_dev():
    """Setup for local debugging."""
    # Force synchronous execution (no threads/processes)
    # This makes stack traces clean and readable.
    global_config.defaults.blocking = True 
    
    # Fail immediately on error (don't hide bugs with retries)
    global_config.defaults.num_retries = 0
    
    # Use fixed polling for predictable behavior
    global_config.defaults.polling_algorithm = "fixed"
```

### Pattern 2: "Production Mode" (High Throughput)
In production, you want to maximize resource usage and handle transient failures gracefully.

```python
# config/prod.py
from concurry import global_config
import multiprocessing as mp

def configure_prod():
    """Setup for high-throughput production."""
    # Aggressive parallelism
    global_config.thread.max_workers = 50
    global_config.process.max_workers = mp.cpu_count()  # Matches CPU cores
    global_config.ray.max_workers = 0      # Unlimited (auto-scaling using Ray)
    
    # Robustness
    global_config.defaults.num_retries = 3
    global_config.defaults.retry_algorithm = "exponential"
    
    # Optimization
    global_config.defaults.polling_algorithm = "adaptive"
```

### Pattern 3: "Test Mode" (CI/CD)
In tests, you want deterministic behavior and speed.

```python
# tests/conftest.py
import pytest
from concurry import temp_config

@pytest.fixture(autouse=True)
def concurry_config():
    """Automatically apply test config for every test."""
    with temp_config(
        global_num_retries=0,        # Don't mask flakes
        thread_max_workers=4,        # Don't blow up CI memory
        process_max_workers=2
    ):
        yield
```

---

## Configuration Reference

### Categories

| Category | Key Prefix | Example | Description |
| :--- | :--- | :--- | :--- |
| **Execution** | `global_` | `global_blocking` | Controls async vs sync behavior. |
| **Retries** | `global_` | `global_num_retries` | Default retry behavior. |
| **Limits** | `global_` | `global_rate_limiter_algorithm` | Rate limiting defaults. |
| **Worker** | `<mode>_` | `thread_max_workers` | Pool sizes and queue limits per mode. |
| **Polling** | `global_` | `global_polling_fixed_interval` | `wait()` and `gather()` tuning. |

### Common Options

| Option | Default | Description |
| :--- | :--- | :--- |
| `defaults.num_retries` | `0` | Number of retries for failed tasks. |
| `defaults.retry_wait` | `1.0` | Seconds to wait before retry. |
| `thread.max_workers` | `1` | Max threads in the pool. |
| `process.max_workers` | `1` | Max processes in the pool. |
| `ray.max_workers` | `0` | Max Ray actors (0 = unlimited). |
| `defaults.blocking` | `False` | If True, `future.result()` is called immediately. |

### Inspecting Configuration
You can print the current configuration state at any time:

```python
from concurry import global_config
import pprint

# Dump current state
pprint.pprint(global_config.defaults.model_dump())
```
