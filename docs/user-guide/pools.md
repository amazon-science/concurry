# Worker Pools

Worker pools allow you to distribute work across multiple worker instances automatically. Instead of managing multiple workers manually, a pool provides a single interface that dispatches method calls to available workers using configurable load balancing strategies.

## Overview

Worker pools provide:

- **Automatic Load Balancing**: Distribute work across workers using different algorithms
- **Shared Resource Limits**: Enforce rate limits and resource constraints across the entire pool
- **On-Demand Workers**: Create workers dynamically for bursty workloads
- **Transparent API**: Use pools exactly like single workers
- **Pool Statistics**: Monitor worker utilization and load distribution

## Basic Usage

### Creating a Pool

Create a worker pool by specifying `max_workers` when calling `.options()`:

```python
from concurry import Worker

class DataProcessor(Worker):
    def __init__(self, multiplier: int):
        self.multiplier = multiplier
        self.processed = 0
    
    def process(self, value: int) -> int:
        self.processed += 1
        return value * self.multiplier

# Create a pool with 5 workers
pool = DataProcessor.options(
    mode="thread",
    max_workers=5
).init(multiplier=10)

# Use exactly like a single worker
future = pool.process(42)
result = future.result()  # 420

# Check pool statistics
stats = pool.get_pool_stats()
print(f"Pool has {stats['total_workers']} workers")

pool.stop()
```

### Context Manager (Recommended)

Pools support the context manager protocol for automatic cleanup of all workers:

```python
# Context manager automatically stops all workers
with DataProcessor.options(
    mode="thread",
    max_workers=5
).init(multiplier=10) as pool:
    future = pool.process(42)
    result = future.result()  # 420
# All 5 workers automatically stopped here

# Works with blocking mode
with DataProcessor.options(
    mode="thread",
    max_workers=5,
    blocking=True
).init(multiplier=10) as pool:
    results = [pool.process(i) for i in range(10)]
# Pool automatically stopped

# Cleanup happens even on exceptions
with DataProcessor.options(mode="thread", max_workers=3).init(multiplier=2) as pool:
    if some_error:
        raise ValueError("Error occurred")
# All workers still stopped despite exception
```

**Benefits:**
- ✅ Automatic cleanup of all workers - no need to remember `.stop()`
- ✅ Exception safe - all workers stopped even on errors
- ✅ Cleaner code - follows Python best practices
- ✅ Works with all pool types (thread, process, ray)
- ✅ Works with on-demand pools

### Supported Modes

Different execution modes support different pool configurations:

| Mode | Default max_workers | Supports Pools | Notes |
|------|---------------------|----------------|-------|
| `sync` | 1 (fixed) | ❌ No | Single-threaded execution only |
| `asyncio` | 1 (fixed) | ❌ No | Single event loop only |
| `thread` | 24 | ✅ Yes | Thread-based concurrency |
| `process` | 4 | ✅ Yes | Process-based concurrency |
| `ray` | 0 (unlimited) | ✅ Yes | Distributed execution |

```python
# Thread pool - good for I/O-bound tasks
thread_pool = MyWorker.options(mode="thread", max_workers=10).init()

# Process pool - good for CPU-bound tasks
process_pool = MyWorker.options(mode="process", max_workers=4).init()

# Ray pool - good for distributed computing
import ray
ray.init()
ray_pool = MyWorker.options(
    mode="ray",
    max_workers=20,
    actor_options={"num_cpus": 0.5}  # Each worker uses 0.5 CPU
).init()
```

## Load Balancing Algorithms

Worker pools use load balancing algorithms to decide which worker handles each request.

### Round Robin (Default)

Distributes requests evenly in circular fashion. Simple and fair for homogeneous workers.

```python
pool = MyWorker.options(
    mode="thread",
    max_workers=5,
    load_balancing="round_robin"  # or "rr"
).init()

# Calls go to: worker 0, 1, 2, 3, 4, 0, 1, 2, ...
for i in range(10):
    pool.process(i)
```

**Best for:**
- Workers with similar capabilities
- Tasks with similar execution times
- When simplicity is preferred

### Least Active Load

Selects the worker with the fewest currently active (in-flight) requests. Adapts dynamically to worker load.

```python
pool = MyWorker.options(
    mode="thread",
    max_workers=5,
    load_balancing="active"  # or "least_active"
).init()

# Always selects the worker with fewest active calls
# Good for tasks with varying execution times
```

**Best for:**
- Tasks with variable execution times
- Heterogeneous workers
- Avoiding overloading slow workers

### Least Total Load

Selects the worker with the fewest total calls over its lifetime. Ensures even distribution of total work.

```python
pool = MyWorker.options(
    mode="thread",
    max_workers=5,
    load_balancing="total"  # or "least_total"
).init()

# Ensures all workers get equal number of tasks long-term
```

**Best for:**
- Monitoring total work distribution
- Ensuring even wear on workers
- Tasks with similar execution times

### Random

Randomly selects a worker for each request. Simple and effective for stateless workers.

```python
pool = MyWorker.options(
    mode="thread",
    max_workers=5,
    load_balancing="random"  # or "rand"
).init()

# Each request goes to a random worker
# Default for on-demand pools
```

**Best for:**
- Stateless workers
- On-demand pools
- High-throughput scenarios

### Comparing Load Balancers

```python
import time
from concurry import Worker

class SlowWorker(Worker):
    def process(self, duration: float) -> str:
        time.sleep(duration)
        return f"Processed for {duration}s"

# With round-robin, all workers might be busy
rr_pool = SlowWorker.options(
    mode="thread",
    max_workers=3,
    load_balancing="round_robin"
).init()

# With least-active, new calls go to idle workers
la_pool = SlowWorker.options(
    mode="thread",
    max_workers=3,
    load_balancing="active"
).init()

# Submit mixed workload
for duration in [5.0, 0.1, 0.1, 0.1]:  # 1 slow, 3 fast
    rr_pool.process(duration)  # May queue behind slow task
    la_pool.process(duration)  # Fast tasks avoid slow worker

rr_pool.stop()
la_pool.stop()
```

## Resource Limits with Pools

Worker pools can enforce shared resource limits across all workers, ensuring the entire pool respects rate limits and resource constraints.

### Shared Rate Limiting

```python
from concurry import Worker, CallLimit, RateLimit

class APIWorker(Worker):
    def __init__(self, api_key: str):
        self.api_key = api_key
    
    def call_api(self, prompt: str) -> str:
        # Automatically rate-limited across all workers in pool
        with self.limits.acquire(requested={"tokens": 100}):
            response = external_api_call(prompt)
            return response

# Create pool with shared limits
# All 10 workers share the same 1000 tokens/min budget
pool = APIWorker.options(
    mode="thread",
    max_workers=10,
    limits=[
        CallLimit(window_seconds=60, capacity=100),  # 100 calls/min
        RateLimit(
            key="tokens",
            window_seconds=60,
            capacity=1000  # 1000 tokens/min shared across pool
        )
    ]
).init(api_key="my-key")

# All workers share the limit pool
futures = [pool.call_api(f"Request {i}") for i in range(200)]
results = [f.result() for f in futures]

pool.stop()
```

### Resource Pooling

```python
from concurry import Worker, ResourceLimit

class DatabaseWorker(Worker):
    def __init__(self, db_config: dict):
        self.db_config = db_config
    
    def query(self, sql: str) -> list:
        # Limit concurrent database connections across all workers
        with self.limits.acquire(requested={"connections": 1}):
            return execute_query(sql)

# Pool of 20 workers sharing 5 database connections
pool = DatabaseWorker.options(
    mode="thread",
    max_workers=20,
    limits=[
        ResourceLimit(key="connections", capacity=5)
    ]
).init(db_config={...})

# Even with 20 workers, only 5 queries run concurrently
```

### Per-Worker vs Shared Limits

```python
from concurry import Worker, LimitSet, CallLimit

# Per-worker limits: Each worker has its own 10 calls/sec
# Total pool capacity: 50 calls/sec (5 workers × 10)
pool1 = MyWorker.options(
    mode="thread",
    max_workers=5,
    limits=[CallLimit(window_seconds=1, capacity=10)]  # List creates shared LimitSet
).init()

# Pre-create a shared LimitSet for explicit sharing
shared_limits = LimitSet(
    limits=[CallLimit(window_seconds=1, capacity=10)],
    shared=True,
    mode="thread"
)

# Shared limits: All workers share 10 calls/sec
# Total pool capacity: 10 calls/sec (shared across all workers)
pool2 = MyWorker.options(
    mode="thread",
    max_workers=5,
    limits=shared_limits  # Pass LimitSet instance
).init()
```

## On-Demand Workers

On-demand pools create workers dynamically for each request and destroy them after completion. Useful for bursty workloads or resource-constrained environments.

### Basic On-Demand Pool

```python
from concurry import Worker

class BatchProcessor(Worker):
    def process_batch(self, data: list) -> dict:
        # Heavy processing
        return {"processed": len(data), "result": sum(data)}

# Create on-demand pool
pool = BatchProcessor.options(
    mode="thread",
    on_demand=True,
    max_workers=0  # Unlimited (up to cpu_count()-1 for threads)
).init()

# Each call creates a new worker
future1 = pool.process_batch([1, 2, 3])
future2 = pool.process_batch([4, 5, 6])

# Workers are automatically cleaned up after results are retrieved
result1 = future1.result()
result2 = future2.result()

pool.stop()
```

### On-Demand with Limits

```python
# Limit concurrent on-demand workers
pool = MyWorker.options(
    mode="ray",
    on_demand=True,
    max_workers=10  # Max 10 concurrent on-demand workers
).init()

# On-demand pools use 'random' load balancing by default
stats = pool.get_pool_stats()
print(stats["load_balancer"]["algorithm"])  # "Random"
```

### When to Use On-Demand

**Use on-demand for:**
- Bursty workloads with idle periods
- Resource-constrained environments
- Cold-start is acceptable
- Workers hold significant memory

**Use persistent pools for:**
- Steady workload
- Warm start is important
- Low per-request overhead needed
- Workers are lightweight

## Worker Composition

You can use workers and pools inside other workers, enabling powerful composition patterns.

### Pool Inside Worker

```python
from concurry import Worker

class ComputeWorker(Worker):
    """Worker that does heavy computation."""
    def compute(self, x: int) -> int:
        return x ** 2

class CoordinatorWorker(Worker):
    """Coordinator that manages a pool of compute workers."""
    def __init__(self):
        # Create internal pool
        self.compute_pool = ComputeWorker.options(
            mode="process",  # CPU-bound
            max_workers=4
        ).init()
    
    def process_batch(self, values: list) -> list:
        # Distribute work across internal pool
        futures = [self.compute_pool.compute(x) for x in values]
        return [f.result() for f in futures]
    
    def __del__(self):
        # Cleanup internal pool
        if hasattr(self, 'compute_pool'):
            self.compute_pool.stop()

# Use coordinator in thread mode
coordinator = CoordinatorWorker.options(mode="thread").init()
results = coordinator.process_batch([1, 2, 3, 4, 5]).result()
print(results)  # [1, 4, 9, 16, 25]

coordinator.stop()
```

### Pipeline with Multiple Pools

```python
from concurry import Worker

class Fetcher(Worker):
    """Fetch data from external sources."""
    def fetch(self, url: str) -> bytes:
        return download(url)

class Processor(Worker):
    """Process fetched data."""
    def process(self, data: bytes) -> dict:
        return parse_and_transform(data)

class Storer(Worker):
    """Store processed data."""
    def store(self, data: dict) -> str:
        return save_to_database(data)

# Create pipeline with three pools
fetcher_pool = Fetcher.options(mode="thread", max_workers=10).init()
processor_pool = Processor.options(mode="process", max_workers=4).init()
storer_pool = Storer.options(mode="thread", max_workers=5).init()

# Process pipeline with automatic future unwrapping
urls = ["http://example.com/1", "http://example.com/2"]
for url in urls:
    # Chain workers - futures are automatically unwrapped
    fetched = fetcher_pool.fetch(url)
    processed = processor_pool.process(fetched)  # Auto-unwraps future
    stored = storer_pool.store(processed)  # Auto-unwraps future
    print(f"Stored: {stored.result()}")

# Cleanup
fetcher_pool.stop()
processor_pool.stop()
storer_pool.stop()
```

### Nested Pools with Ray

```python
from concurry import Worker
import ray

ray.init()

class LeafWorker(Worker):
    """Leaf worker that does actual work."""
    def work(self, x: int) -> int:
        return x * 2

class BranchWorker(Worker):
    """Branch worker that manages leaf workers."""
    def __init__(self):
        # Each branch manages its own leaf pool
        self.leaf_pool = LeafWorker.options(
            mode="ray",
            max_workers=5,
            actor_options={"num_cpus": 0.1}
        ).init()
    
    def process_group(self, values: list) -> list:
        futures = [self.leaf_pool.work(x) for x in values]
        return [f.result() for f in futures]

# Create pool of branch workers (each with internal leaf pool)
branch_pool = BranchWorker.options(
    mode="ray",
    max_workers=3,
    actor_options={"num_cpus": 0.2}
).init()

# Distribute work across branch pool
# Each branch distributes to its leaf pool
result = branch_pool.process_group([1, 2, 3, 4, 5]).result()
print(result)  # [2, 4, 6, 8, 10]

branch_pool.stop()
```

## Handling Exceptions

Worker pools handle exceptions gracefully, allowing you to catch and handle errors from any worker in the pool.

### Basic Exception Handling

```python
from concurry import Worker

class RiskyWorker(Worker):
    def risky_operation(self, value: int) -> int:
        if value < 0:
            raise ValueError(f"Negative value not allowed: {value}")
        return value * 2

pool = RiskyWorker.options(mode="thread", max_workers=5).init()

# Submit mixed good/bad values
values = [1, 2, -3, 4, -5]
futures = [pool.risky_operation(v) for v in values]

# Handle each result
for i, future in enumerate(futures):
    try:
        result = future.result()
        print(f"Success: {values[i]} -> {result}")
    except ValueError as e:
        print(f"Error: {values[i]} -> {e}")

pool.stop()
```

### Partial Failure Handling

```python
from concurry import Worker
from concurrent.futures import TimeoutError

class UnreliableWorker(Worker):
    def process(self, item: dict) -> dict:
        if item.get("fail"):
            raise RuntimeError("Processing failed")
        return {"result": item["value"] * 2}

pool = UnreliableWorker.options(
    mode="process",
    max_workers=4
).init()

# Process batch with some failures
items = [
    {"value": 1},
    {"value": 2, "fail": True},  # This will fail
    {"value": 3},
    {"value": 4, "fail": True},  # This will fail
    {"value": 5},
]

futures = [pool.process(item) for item in items]

# Collect results, handling failures
results = []
errors = []

for i, future in enumerate(futures):
    try:
        result = future.result(timeout=5)
        results.append(result)
    except RuntimeError as e:
        errors.append((i, str(e)))
    except TimeoutError:
        errors.append((i, "Timeout"))

print(f"Successful: {len(results)}")
print(f"Failed: {len(errors)}")

pool.stop()
```

### Retry Logic with Pools

```python
from concurry import Worker
import random

class RetryableWorker(Worker):
    def flaky_operation(self, data: str) -> str:
        if random.random() < 0.3:  # 30% failure rate
            raise ConnectionError("Temporary failure")
        return data.upper()

pool = RetryableWorker.options(mode="thread", max_workers=5).init()

def process_with_retry(item: str, max_retries: int = 3) -> str:
    """Process item with automatic retries."""
    for attempt in range(max_retries):
        try:
            future = pool.flaky_operation(item)
            return future.result(timeout=5)
        except ConnectionError as e:
            if attempt == max_retries - 1:
                raise  # Last attempt, give up
            print(f"Attempt {attempt + 1} failed, retrying...")
    
# Process with retries
items = ["hello", "world", "foo", "bar"]
results = [process_with_retry(item) for item in items]
print(results)

pool.stop()
```

## Long-Running Tasks

Worker pools can handle long-running tasks efficiently with proper timeout and cancellation handling.

### Timeout Handling

```python
from concurry import Worker
from concurrent.futures import TimeoutError
import time

class SlowWorker(Worker):
    def slow_task(self, duration: float) -> str:
        time.sleep(duration)
        return f"Completed after {duration}s"

pool = SlowWorker.options(mode="thread", max_workers=3).init()

# Submit mix of fast and slow tasks
tasks = [
    pool.slow_task(0.5),
    pool.slow_task(10.0),  # This will timeout
    pool.slow_task(0.5),
]

# Get results with timeout
for i, future in enumerate(tasks):
    try:
        result = future.result(timeout=2.0)  # 2 second timeout
        print(f"Task {i}: {result}")
    except TimeoutError:
        print(f"Task {i}: Timed out (worker continues in background)")

pool.stop(timeout=15)  # Wait for workers to finish
```

### Progress Tracking

```python
from concurry import Worker, ProgressBar
import time

class ProgressWorker(Worker):
    def process_items(self, items: list) -> list:
        results = []
        # Track progress across workers
        for item in ProgressBar(items, desc="Processing", style="ray"):
            time.sleep(0.1)  # Simulate work
            results.append(item * 2)
        return results

pool = ProgressWorker.options(
    mode="ray",
    max_workers=4,
    actor_options={"num_cpus": 0.25}
).init()

# Submit multiple batches (each tracked separately)
batches = [list(range(10)) for _ in range(4)]
futures = [pool.process_items(batch) for batch in batches]

# Wait for all to complete
results = [f.result() for f in futures]

pool.stop()
```

### Graceful Shutdown

```python
from concurry import Worker
import time
import signal

class GracefulWorker(Worker):
    def __init__(self):
        self.should_stop = False
    
    def long_task(self, items: list) -> list:
        results = []
        for item in items:
            if self.should_stop:
                break
            time.sleep(0.5)
            results.append(item * 2)
        return results
    
    def shutdown(self):
        self.should_stop = True

pool = GracefulWorker.options(mode="thread", max_workers=5).init()

# Submit long-running tasks
futures = [pool.long_task(list(range(100))) for _ in range(5)]

# Setup signal handler for graceful shutdown
def signal_handler(sig, frame):
    print("Shutting down gracefully...")
    # Tell workers to stop
    for _ in range(5):  # For each worker
        pool.shutdown()
    # Wait for completion
    pool.stop(timeout=10)
    print("Shutdown complete")

signal.signal(signal.SIGINT, signal_handler)

# Wait for results or interrupt
try:
    results = [f.result() for f in futures]
except KeyboardInterrupt:
    pass
```

## Pool Statistics and Monitoring

Worker pools provide detailed statistics for monitoring performance and debugging.

### Basic Statistics

```python
from concurry import Worker

class MonitoredWorker(Worker):
    def process(self, x: int) -> int:
        return x * 2

pool = MonitoredWorker.options(
    mode="thread",
    max_workers=5,
    load_balancing="active"
).init()

# Submit some work
futures = [pool.process(i) for i in range(100)]
results = [f.result() for f in futures]

# Get pool statistics
stats = pool.get_pool_stats()

print(f"Total workers: {stats['total_workers']}")
print(f"Max workers: {stats['max_workers']}")
print(f"On-demand: {stats['on_demand']}")
print(f"Stopped: {stats['stopped']}")

# Load balancer statistics
lb_stats = stats['load_balancer']
print(f"Algorithm: {lb_stats['algorithm']}")
print(f"Total dispatched: {lb_stats['total_dispatched']}")

if lb_stats['algorithm'] == 'LeastActiveLoad':
    print(f"Active calls: {lb_stats['active_calls']}")
    print(f"Total active: {lb_stats['total_active']}")

pool.stop()
```

### Monitoring Load Distribution

```python
from concurry import Worker
import time

class StatefulWorker(Worker):
    def __init__(self):
        self.processed_count = 0
    
    def process(self, x: int) -> int:
        self.processed_count += 1
        time.sleep(0.01)
        return x * 2
    
    def get_count(self) -> int:
        return self.processed_count

# Create pool with different algorithms
for algorithm in ["round_robin", "active", "total", "random"]:
    pool = StatefulWorker.options(
        mode="thread",
        max_workers=3,
        load_balancing=algorithm
    ).init()
    
    # Submit work
    futures = [pool.process(i) for i in range(30)]
    results = [f.result() for f in futures]
    
    # Check statistics
    stats = pool.get_pool_stats()
    print(f"\nAlgorithm: {algorithm}")
    print(f"Total dispatched: {stats['load_balancer']['total_dispatched']}")
    
    if algorithm == "total":
        total_calls = stats['load_balancer']['total_calls']
        print(f"Per-worker calls: {total_calls}")
    
    pool.stop()
```

### Custom Metrics

```python
from concurry import Worker
import time
from collections import defaultdict

class MetricsWorker(Worker):
    def __init__(self):
        self.metrics = defaultdict(int)
        self.start_time = time.time()
    
    def process(self, task_type: str, data: any) -> any:
        start = time.time()
        
        # Process based on type
        if task_type == "fast":
            result = data * 2
        elif task_type == "slow":
            time.sleep(0.1)
            result = data ** 2
        else:
            result = None
        
        # Record metrics
        duration = time.time() - start
        self.metrics[f"{task_type}_count"] += 1
        self.metrics[f"{task_type}_total_time"] += duration
        
        return result
    
    def get_metrics(self) -> dict:
        uptime = time.time() - self.start_time
        return {
            "metrics": dict(self.metrics),
            "uptime": uptime
        }

pool = MetricsWorker.options(mode="thread", max_workers=3).init()

# Submit mixed workload
tasks = [("fast", i) for i in range(50)] + [("slow", i) for i in range(10)]
futures = [pool.process(task_type, data) for task_type, data in tasks]
results = [f.result() for f in futures]

# Aggregate metrics from all workers (not directly accessible in pool)
# Pool stats give load balancer info, individual worker metrics require
# special handling or aggregation logic

pool.stop()
```

## Model Inheritance with Pools

Worker pools support the same model inheritance and validation features as single workers. All the patterns from the Workers guide apply to pools as well.

### Typed/BaseModel Pools (Not Ray-Compatible)

```python
from concurry import Worker
from morphic import Typed
from pydantic import BaseModel, Field
from typing import List

# Typed worker pool (works with thread, process, asyncio)
class TypedWorker(Worker, Typed):
    name: str
    multiplier: int = Field(default=2, ge=1)
    
    def process(self, x: int) -> int:
        return x * self.multiplier

# ✅ Works with thread, process, asyncio
pool = TypedWorker.options(
    mode="thread",
    max_workers=5
).init(name="processor", multiplier=3)

# All workers in pool share the same validated configuration
futures = [pool.process(i) for i in range(10)]
results = [f.result() for f in futures]
print(results)  # [0, 3, 6, 9, 12, 15, 18, 21, 24, 27]
pool.stop()

# ❌ Does NOT work with Ray
try:
    pool = TypedWorker.options(
        mode="ray",
        max_workers=5
    ).init(name="processor", multiplier=3)
except ValueError as e:
    print("Ray mode not supported with Typed workers")
    # ValueError: Cannot create Ray worker with Pydantic-based class
```

### Validation Decorators with Ray Pools

Use `@validate` or `@validate_call` decorators for Ray-compatible validation:

```python
from concurry import Worker
from morphic import validate
from pydantic import validate_call

# Option 1: @validate decorator (Ray-compatible)
class ValidatedWorker(Worker):
    @validate
    def __init__(self, multiplier: int = 2):
        self.multiplier = multiplier
    
    @validate
    def process(self, x: int, offset: float = 0.0) -> float:
        return (x * self.multiplier) + offset

# ✅ Works with Ray!
pool = ValidatedWorker.options(
    mode="ray",
    max_workers=10
).init(multiplier="5")  # String coerced to int

# Strings are coerced for all method calls
futures = [pool.process(str(i), offset=str(i * 0.5)) for i in range(5)]
results = [f.result() for f in futures]
print(results)  # [0.0, 5.5, 11.0, 16.5, 22.0]
pool.stop()

# Option 2: @validate_call decorator (Ray-compatible)
class PydanticValidatedWorker(Worker):
    @validate_call
    def __init__(self, base: int):
        self.base = base
        self.call_count = 0
    
    @validate_call
    def compute(self, x: int, y: int = 0) -> int:
        self.call_count += 1
        return (x + y) * self.base

# ✅ Also works with Ray!
pool = PydanticValidatedWorker.options(
    mode="ray",
    max_workers=10
).init(base=3)

futures = [pool.compute("10", y=str(i)) for i in range(5)]
results = [f.result() for f in futures]
print(results)  # [30, 33, 36, 39, 42]
pool.stop()
```

### Pool-Specific Considerations

**State Isolation:**
Each worker in a pool maintains its own state, even with validation:

```python
from morphic import validate

class StatefulWorker(Worker):
    @validate
    def __init__(self, multiplier: int):
        self.multiplier = multiplier
        self.count = 0
    
    @validate
    def process(self, x: int) -> dict:
        self.count += 1
        return {"result": x * self.multiplier, "count": self.count}

# Create pool of 3 workers
pool = StatefulWorker.options(
    mode="thread",
    max_workers=3,
    load_balancing="round_robin"
).init(multiplier=2)

# Each worker maintains separate count
results = [pool.process(10).result() for _ in range(9)]

# With round-robin, each worker processes 3 times
# results[0], [3], [6]: worker 0 (count: 1, 2, 3)
# results[1], [4], [7]: worker 1 (count: 1, 2, 3)
# results[2], [5], [8]: worker 2 (count: 1, 2, 3)
for r in results:
    print(r)  # {'result': 20, 'count': 1|2|3}

pool.stop()
```

**Shared Limits with Validated Workers:**

```python
from concurry import Worker, RateLimit
from morphic import validate

class APIWorker(Worker):
    @validate
    def __init__(self, api_key: str):
        self.api_key = api_key
    
    @validate
    def call_api(self, endpoint: str, tokens: int = 100) -> dict:
        # Use limits.acquire() to enforce rate limits
        with self.limits.acquire(requested={"tokens": tokens}) as acq:
            response = {"endpoint": endpoint, "tokens": tokens}
            acq.update(usage={"tokens": tokens})
            return response

# Pool of 10 workers sharing 1000 tokens/min
pool = APIWorker.options(
    mode="thread",
    max_workers=10,
    limits=[
        RateLimit(key="tokens", window_seconds=60, capacity=1000)
    ]
).init(api_key="my-key")

# All workers share the token budget
futures = [pool.call_api("/users", tokens=100) for _ in range(20)]
# Only 10 complete immediately, rest wait for token refresh
results = [f.result() for f in futures]

pool.stop()
```

### Ray Pool Compatibility Summary

| Worker Type | Thread Pool | Process Pool | Asyncio Pool | Ray Pool |
|-------------|-------------|--------------|--------------|----------|
| Plain Worker | ✅ | ✅ | ✅ | ✅ |
| Worker + Typed | ✅ | ✅ | ✅ | ❌ |
| Worker + BaseModel | ✅ | ✅ | ✅ | ❌ |
| Worker + @validate | ✅ | ✅ | ✅ | ✅ |
| Worker + @validate_call | ✅ | ✅ | ✅ | ✅ |

**For Ray pools:**
- ✅ Use plain Worker classes
- ✅ Use @validate or @validate_call decorators for validation
- ❌ Don't inherit from Typed or BaseModel

**Example: Ray Pool with Validation**

```python
import ray
from concurry import Worker
from morphic import validate

ray.init()

class DistributedWorker(Worker):
    """Ray-compatible worker with validation."""
    
    @validate
    def __init__(self, model_name: str, batch_size: int = 32):
        self.model_name = model_name
        self.batch_size = batch_size
        # Load model, etc.
    
    @validate
    def predict(self, data: list, threshold: float = 0.5) -> list:
        # Process batch with validation
        return [x * threshold for x in data]

# Create Ray pool with validated workers
pool = DistributedWorker.options(
    mode="ray",
    max_workers=20,
    actor_options={"num_cpus": 0.5}
).init(model_name="bert-base", batch_size="64")  # Coerced to int

# Distribute work across Ray cluster
futures = [
    pool.predict([1.0, 2.0, 3.0], threshold=str(0.8 + i*0.1))  # Strings coerced
    for i in range(10)
]
results = [f.result() for f in futures]

pool.stop()
ray.shutdown()
```

### When to Use Each Approach

**For Non-Ray Pools (thread, process, asyncio):**

Use **Typed/BaseModel** when:
- You want full model validation and lifecycle hooks
- You need immutable configuration
- You want the richest feature set

Use **@validate/@validate_call** when:
- You want flexibility to switch to Ray later
- You only need validation on specific methods
- You prefer decorator-based validation

**For Ray Pools:**

Use **@validate decorator** when:
- You want morphic's validation style
- You need type coercion (strings → numbers)
- You want minimal overhead

Use **@validate_call decorator** when:
- You want Pydantic's validation features
- You need Field constraints
- You prefer strict validation

Use **plain Worker** when:
- You don't need validation
- You want maximum performance

## Retry Mechanisms with Pools

Worker pools fully support retry configuration, with each worker in the pool using the same retry settings.

### Basic Pool with Retry

```python
from concurry import Worker

class APIWorker(Worker):
    def fetch(self, id: int) -> dict:
        return requests.get(f"https://api.example.com/{id}").json()

# Pool of 10 workers, each with retry configuration
pool = APIWorker.options(
    mode="thread",
    max_workers=10,
    num_retries=3,
    retry_algorithm="exponential",
    retry_on=[ConnectionError, TimeoutError]
).init()

# Each request to the pool will retry on failure
futures = [pool.fetch(i) for i in range(100)]
results = [f.result() for f in futures]

pool.stop()
```

### How Retries Work in Pools

**Key behaviors**:

1. **Per-Worker Configuration**: Each worker in the pool has the same retry configuration
2. **Worker-Side Retries**: Retries happen on the worker that received the request
3. **Load Balancing Before Retry**: Load balancer selects a worker once; retries stay on that worker
4. **No Retry Statistics**: Pool statistics track successful dispatches, not retry attempts

```python
# Example: Pool with 5 workers and retries
pool = MyWorker.options(
    mode="thread",
    max_workers=5,
    num_retries=3,
    load_balancing="round_robin"
).init()

# Request goes to worker 0, retries happen on worker 0
future1 = pool.process(1)

# Request goes to worker 1, retries happen on worker 1
future2 = pool.process(2)
```

### Pool with Shared Limits and Retry

Retries automatically coordinate with shared limits:

```python
from concurry import LimitSet, ResourceLimit

# Create shared limit
shared_limits = LimitSet(
    limits=[ResourceLimit(key="db_connections", capacity=10)],
    shared=True,
    mode="thread"
)

# Pool shares the limit across all workers
pool = DatabaseWorker.options(
    mode="thread",
    max_workers=20,  # 20 workers share 10 connections
    num_retries=3,
    retry_on=[DatabaseError],
    limits=shared_limits
).init()

# Each worker's retries properly release/acquire shared limits
# No deadlocks - limits are released between retry attempts
```

### On-Demand Pools with Retry

On-demand pools create and destroy workers dynamically, with retry configuration:

```python
pool = MyWorker.options(
    mode="thread",
    on_demand=True,
    max_workers=10,
    num_retries=3,
    retry_algorithm="exponential"
).init()

# Each on-demand worker is created with retry configuration
future = pool.process(data)
result = future.result()  # Worker retries if needed, then is destroyed

pool.stop()
```

### TaskWorker Pools with Retry

```python
from concurry import TaskWorker

def flaky_function(x):
    if random.random() < 0.5:
        raise ConnectionError("Transient error")
    return x * 2

# Pool of task workers with retry
pool = TaskWorker.options(
    mode="process",
    max_workers=4,
    num_retries=3,
    retry_on=[ConnectionError]
).init()

# Each submit/map call can retry
results = list(pool.map(flaky_function, range(100)))

pool.stop()
```

### Retry with Different Load Balancing

Retry behavior is independent of load balancing:

```python
# Least Active Load with Retry
pool = MyWorker.options(
    mode="thread",
    max_workers=10,
    load_balancing="active",  # Routes to least busy worker
    num_retries=3  # Each worker retries its own tasks
).init()

# If a worker receives a task and fails:
# - It retries locally (doesn't re-dispatch to a different worker)
# - Load balancer only selects worker for initial dispatch
```

### Best Practices for Pool Retries

**1. Use Retries for Transient Errors**

```python
# ✅ Good: Retry on network errors
pool = APIWorker.options(
    mode="thread",
    max_workers=10,
    num_retries=3,
    retry_on=[ConnectionError, TimeoutError]
).init()

# ❌ Bad: Retry on all exceptions (including bugs)
pool = APIWorker.options(
    mode="thread",
    max_workers=10,
    num_retries=3,
    retry_on=[Exception]  # Too broad
).init()
```

**2. Consider Pool Size vs Retry Count**

```python
# For high-availability: More workers, fewer retries
pool = MyWorker.options(
    mode="thread",
    max_workers=20,  # More workers available
    num_retries=2  # Quick failover
).init()

# For resource-constrained: Fewer workers, more retries
pool = MyWorker.options(
    mode="process",
    max_workers=4,  # Limited workers
    num_retries=5  # More retries per worker
).init()
```

**3. Combine with Shared Limits**

```python
# Ensure fair resource distribution across pool
from concurry import RateLimit

pool = APIWorker.options(
    mode="thread",
    max_workers=10,
    num_retries=3,
    limits=[RateLimit(key="requests", window_seconds=60, capacity=100)]
).init()

# All workers share 100 requests/min budget
# Retries count against the budget but are auto-managed
```

**4. Monitor Retry Behavior**

```python
import logging

def retry_logger(exception, attempt, worker_class, **ctx):
    logging.warning(
        f"Worker {worker_class} retry {attempt}: {exception}"
    )
    return isinstance(exception, (ConnectionError, TimeoutError))

pool = MyWorker.options(
    mode="thread",
    max_workers=5,
    num_retries=3,
    retry_on=retry_logger
).init()
```

For comprehensive retry documentation, see the [Retry Mechanisms Guide](retries.md).

## Best Practices

### Choosing Pool Size

```python
import multiprocessing as mp

# For CPU-bound tasks (process mode)
cpu_pool_size = mp.cpu_count()

# For I/O-bound tasks (thread mode)
io_pool_size = mp.cpu_count() * 4  # Or higher

# For Ray distributed tasks
ray_pool_size = 100  # Based on cluster size

pool = MyWorker.options(
    mode="process",
    max_workers=cpu_pool_size
).init()
```

### Initialization Costs

```python
from concurry import Worker

class ExpensiveInitWorker(Worker):
    def __init__(self, model_path: str):
        # Expensive: Load ML model
        self.model = load_model(model_path)
    
    def predict(self, data: list) -> list:
        return self.model.predict(data)

# Use persistent pool - initialization happens once per worker
pool = ExpensiveInitWorker.options(
    mode="process",
    max_workers=4  # Init 4 times total
).init(model_path="/path/to/model")

# DON'T use on-demand for expensive init
# Each call would reload the model!
```

### Resource Cleanup

```python
from concurry import Worker
import contextlib

class ResourceWorker(Worker):
    def __init__(self):
        self.connection = create_connection()
    
    def process(self, data: any) -> any:
        return self.connection.query(data)
    
    def __del__(self):
        # Cleanup connection
        if hasattr(self, 'connection'):
            self.connection.close()

# Use context manager for automatic cleanup
with contextlib.closing(
    ResourceWorker.options(mode="thread", max_workers=5).init()
) as pool:
    results = [pool.process(i).result() for i in range(10)]
# Pool automatically stopped and resources cleaned
```

### Error Isolation

```python
from concurry import Worker

# Bad: Shared mutable state
bad_shared = {"counter": 0}

class BadWorker(Worker):
    def process(self, x: int) -> int:
        # Race condition!
        bad_shared["counter"] += x
        return bad_shared["counter"]

# Good: Worker-local state
class GoodWorker(Worker):
    def __init__(self):
        self.counter = 0  # Each worker has its own
    
    def process(self, x: int) -> int:
        self.counter += x
        return self.counter

pool = GoodWorker.options(mode="thread", max_workers=5).init()
```

## Advanced Patterns

### Dynamic Pool Resizing (Future)

```python
# TODO: Not yet implemented
# Future API for dynamic resizing
# pool.resize(new_size=10)
```

### Priority Queues (Future)

```python
# TODO: Not yet implemented  
# Future API for priority-based dispatch
# pool.process(data, priority=10)
```

### Health Checking

```python
from concurry import Worker

class HealthCheckedWorker(Worker):
    def __init__(self):
        self.healthy = True
    
    def process(self, data: any) -> any:
        if not self.healthy:
            raise RuntimeError("Worker unhealthy")
        return data * 2
    
    def health_check(self) -> bool:
        return self.healthy

# Periodically check worker health
# (Manual implementation - not built-in)
```

## See Also

- [Workers Guide](workers.md) - Detailed worker documentation
- [Retry Mechanisms Guide](retries.md) - Using retries with pools
- [Limits Guide](limits.md) - Resource limits and rate limiting
- [Futures Guide](futures.md) - Working with futures
- [Getting Started](getting-started.md) - Basic concepts

