# Workers

Workers in concurry implement the actor pattern, allowing you to run stateful operations across different execution backends (sync, thread, process, asyncio, ray) with a unified API.

## Overview

A Worker is a class that:
- Maintains its own isolated state
- Executes methods in a specific execution context
- Returns Futures for all method calls (or results directly in blocking mode)
- Can be stopped to clean up resources

## Basic Usage

### Creating a Worker

Create a worker by inheriting from `Worker`:

```python
from concurry import Worker

class DataProcessor(Worker):
    def __init__(self, multiplier: int):
        self.multiplier = multiplier
        self.count = 0

    def process(self, value: int) -> int:
        self.count += 1
        return value * self.multiplier

    def get_count(self) -> int:
        return self.count
```

### Using a Worker

Create a worker instance with `.options().create()`:

```python
# Create worker with thread execution
worker = DataProcessor.options(mode="thread").create(3)

# Call methods (returns futures)
future = worker.process(10)
result = future.result()  # 30

# Check state
count = worker.get_count().result()  # 1

# Clean up
worker.stop()
```

## Execution Modes

Workers support multiple execution modes:

### Sync Mode

Executes synchronously in the current thread (useful for testing):

```python
worker = DataProcessor.options(mode="sync").create(2)
future = worker.process(10)
result = future.result()  # 20 (already computed)
worker.stop()
```

### Thread Mode

Executes in a dedicated thread (good for I/O-bound tasks):

```python
worker = DataProcessor.options(mode="thread").create(2)
future = worker.process(10)
result = future.result()  # Blocks until complete
worker.stop()
```

### Process Mode

Executes in a separate process (good for CPU-bound tasks):

```python
worker = DataProcessor.options(
    mode="process",
    mp_context="fork"  # or "spawn", "forkserver"
).create(2)
future = worker.process(10)
result = future.result()
worker.stop()
```

### Asyncio Mode

Executes in an asyncio event loop in a dedicated thread:

```python
worker = DataProcessor.options(mode="asyncio").create(2)
future = worker.process(10)
result = future.result()
worker.stop()
```

### Ray Mode

Executes using Ray actors for distributed computing:

```python
import ray
ray.init()

# Uses default resource allocation (num_cpus=1, num_gpus=0)
worker = DataProcessor.options(mode="ray").create(2)
future = worker.process(10)
result = future.result()
worker.stop()

# Explicitly specify resources
worker2 = DataProcessor.options(
    mode="ray",
    num_cpus=2,
    num_gpus=1,
    resources={"special_hardware": 1}
).create(2)
future2 = worker2.process(20)
result2 = future2.result()
worker2.stop()

ray.shutdown()
```

**Ray Default Resources:**
- `num_cpus=1`: Each Ray actor is allocated 1 CPU by default
- `num_gpus=0`: No GPU allocation by default  
- These defaults allow Ray workers to be created without explicit resource specifications

## Blocking Mode

By default, worker methods return Futures. Use `blocking=True` to get results directly:

```python
# Non-blocking (default)
worker = DataProcessor.options(mode="thread").create(5)
future = worker.process(10)  # Returns future
result = future.result()  # Wait for result

# Blocking mode
worker = DataProcessor.options(mode="thread", blocking=True).create(5)
result = worker.process(10)  # Returns 50 directly
```

## Submitting Arbitrary Functions

Use `submit_task()` to execute arbitrary functions in the worker's context:

```python
def complex_computation(x, y):
    return (x ** 2 + y ** 2) ** 0.5

worker = DataProcessor.options(mode="process").create(1)

# Submit function that's not a worker method
future = worker.submit_task(complex_computation, 3, 4)
result = future.result()  # 5.0

# Also works with lambdas
future2 = worker.submit_task(lambda x: x * 100, 5)
result2 = future2.result()  # 500

# Mix method calls and task submission
result3 = worker.process(10).result()  # Uses worker method

worker.stop()
```

## State Management

Each worker instance maintains its own isolated state:

```python
class Counter(Worker):
    def __init__(self):
        self.count = 0

    def increment(self) -> int:
        self.count += 1
        return self.count

# Each worker has separate state
worker1 = Counter.options(mode="thread").create()
worker2 = Counter.options(mode="thread").create()

print(worker1.increment().result())  # 1
print(worker1.increment().result())  # 2
print(worker2.increment().result())  # 1 (separate state)

worker1.stop()
worker2.stop()
```

## Using the @worker Decorator

You can also use the `@worker` decorator instead of inheriting from `Worker`:

```python
from concurry import worker

@worker
class Calculator:
    def __init__(self, base: int):
        self.base = base

    def add(self, x: int) -> int:
        return self.base + x

# Use exactly like a Worker
calc = Calculator.options(mode="thread").create(10)
result = calc.add(5).result()  # 15
calc.stop()
```

## Type Safety and Validation

Workers in concurry leverage [morphic's Typed](https://github.com/yourusername/morphic) for enhanced type safety and validation. While the `Worker` class itself does NOT inherit from `Typed` (to allow flexible `__init__` definitions), the internal `WorkerProxy` classes do, providing automatic validation and type checking.

### Automatic Type Validation

Worker configuration methods use the `@validate` decorator for automatic type checking and conversion:

```python
from concurry import Worker

class DataProcessor(Worker):
    def __init__(self, multiplier: int):
        self.multiplier = multiplier

# String booleans are automatically coerced
worker = DataProcessor.options(mode="thread", blocking="true").create(3)
assert worker.blocking is True  # Converted from string to bool

# ExecutionMode values are validated
worker = DataProcessor.options(mode="thread").create(3)  # Valid
# worker = DataProcessor.options(mode="invalid").create(3)  # Would raise error

worker.stop()
```

### Immutable Configuration

Once a worker is created, its configuration fields are immutable:

```python
worker = DataProcessor.options(mode="thread", blocking=False).create(3)

# These fields cannot be modified after creation
# worker.blocking = True  # Raises error
# worker.worker_cls = SomeOtherClass  # Raises error

# Internal state tracking (private attributes) can be updated
worker._stopped = True  # Allowed (with type checking)

worker.stop()
```

### Type Checking on Internal State

Private attributes in worker proxies support automatic type checking:

```python
worker = DataProcessor.options(mode="thread").create(3)

# Internal state is type-checked
worker._stopped = False  # Valid (bool)
# worker._stopped = "not a bool"  # Would raise ValidationError

worker.stop()
```

### Benefits of Typed Integration

1. **Automatic Validation**: Configuration options are validated at creation time
2. **Type Coercion**: String values are automatically converted (e.g., `"true"` → `True`)
3. **Immutability**: Public configuration fields cannot be accidentally modified
4. **Type Safety**: Private attributes are type-checked on updates
5. **Better Error Messages**: Clear validation errors with detailed context

### Worker Class Flexibility

The `Worker` class itself does NOT inherit from `Typed`, giving you complete freedom in defining `__init__`:

```python
# You can use any signature you want
class FlexibleWorker(Worker):
    def __init__(self, a, b, c=10, *args, **kwargs):
        self.a = a
        self.b = b
        self.c = c
        self.args = args
        self.kwargs = kwargs
    
    def process(self):
        return self.a + self.b + self.c

# Works with any initialization pattern
worker = FlexibleWorker.options(mode="sync").create(
    1, 2, c=3, extra1="x", extra2="y"
)
result = worker.process().result()  # 6
worker.stop()
```

This design allows you to use Pydantic, dataclasses, attrs, or plain Python classes for your worker implementations while still benefiting from Typed's validation on the worker proxy layer.

## Multiple Workers

You can create and use multiple workers in parallel:

```python
# Create multiple workers
workers = [
    DataProcessor.options(mode="thread").create(i)
    for i in range(1, 4)
]

# Submit tasks to all workers
futures = [w.process(10) for w in workers]

# Collect results
results = [f.result() for f in futures]
print(results)  # [10, 20, 30]

# Clean up
for w in workers:
    w.stop()
```

## Architecture and Implementation

### Common Fields in Base Class

The worker implementation has been refactored for better maintainability and consistency:

**Base `WorkerProxy` Fields:**
- `worker_cls`: The worker class to instantiate
- `blocking`: Whether method calls return results directly
- `init_args`: Positional arguments for worker initialization  
- `init_kwargs`: Keyword arguments for worker initialization

**Subclass-Specific Fields:**
- `RayWorkerProxy`: `num_cpus`, `num_gpus`, `resources`
- `ProcessWorkerProxy`: `mp_context`
- Other proxies have no additional public fields

This design eliminates redundancy - common fields are defined once in the base class, and worker proxy implementations access them directly without copying to private attributes.

### Consistent Exception Propagation

All worker proxy implementations follow a consistent pattern for exception handling:

1. **Validation errors** (setup, configuration) fail fast
2. **Execution errors** are stored in futures and raised on `.result()`
3. **Original exception types** are preserved across all modes
4. **Exception messages** and tracebacks are maintained

This consistency makes it easier to switch between execution modes without changing error handling code.

## Best Practices

### Choose the Right Execution Mode

- **sync**: Testing and debugging
- **thread**: I/O-bound operations (network requests, file I/O)
- **process**: CPU-bound operations (data processing, computation)
- **asyncio**: Async I/O operations (async libraries, coroutines)
- **ray**: Distributed computing (large-scale parallel processing)

### Resource Management

Always call `stop()` to clean up resources:

```python
worker = DataProcessor.options(mode="process").create(2)
try:
    result = worker.process(10).result()
    # ... use result
finally:
    worker.stop()
```

Or use a context manager pattern:

```python
class ManagedWorker:
    def __init__(self, worker):
        self.worker = worker
    
    def __enter__(self):
        return self.worker
    
    def __exit__(self, *args):
        self.worker.stop()

# Usage
with ManagedWorker(DataProcessor.options(mode="thread").create(2)) as worker:
    result = worker.process(10).result()
    # worker.stop() called automatically
```

### Exception Handling

Exceptions in worker methods are consistently propagated across all execution modes, preserving the original exception type and message.

#### Consistent Exception Behavior

All worker implementations now raise the **original exception** when `.result()` is called:

```python
class Validator(Worker):
    def validate(self, value: int) -> int:
        if value < 0:
            raise ValueError("Value must be positive")
        return value
    
    def divide(self, a: int, b: int) -> float:
        return a / b

worker = Validator.options(mode="process").create()

# ValueError is raised as-is (not wrapped)
try:
    result = worker.validate(-5).result()
except ValueError as e:
    print(f"Got ValueError: {e}")  # Original exception type

# ZeroDivisionError is raised as-is
try:
    result = worker.divide(10, 0).result()
except ZeroDivisionError as e:
    print(f"Got ZeroDivisionError: {e}")  # Original exception type

worker.stop()
```

#### Exception Handling by Mode

| Mode | Setup Errors | Execution Errors |
|------|--------------|------------------|
| **sync** | Immediate | In `SyncFuture`, raised on `result()` |
| **thread** | Via future | Original exception raised on `result()` |
| **process** | Via future | **Original exception** raised on `result()` |
| **asyncio** | Immediate | Original exception raised on `result()` |
| **ray** | Immediate | Wrapped in `RayTaskError` (Ray's behavior) |

**Key Improvement:** Process mode now raises the original exception instead of wrapping it in `RuntimeError`, making debugging easier and behavior consistent across all modes.

#### Non-Existent Method Errors

Configuration errors (like calling non-existent methods) are handled consistently:

```python
worker = DataProcessor.options(mode="thread").create(2)

# Sync and Ray modes: fail immediately
try:
    worker.nonexistent_method()  # AttributeError raised immediately
except AttributeError as e:
    print(f"Method not found: {e}")

# Thread/Process/Asyncio modes: fail when calling result()
try:
    future = worker.nonexistent_method()
    future.result()  # AttributeError raised here
except AttributeError as e:
    print(f"Method not found: {e}")

worker.stop()
```

## TaskWorker

`TaskWorker` is a concrete worker implementation designed specifically for submitting arbitrary tasks without defining custom methods. It's useful when you just need to execute functions in different execution contexts without creating a custom worker class.

### Basic Usage

```python
from concurry import TaskWorker

# Create a task worker
worker = TaskWorker.options(mode="thread").create()

# Submit arbitrary functions
def compute(x, y):
    return x ** 2 + y ** 2

future = worker.submit_task(compute, 3, 4)
result = future.result()  # 25

worker.stop()
```

### Use Cases

TaskWorker is particularly useful for:

- Quick prototyping without defining custom worker classes
- Building higher-level abstractions like WorkerExecutor or WorkerPool
- Submitting one-off tasks to different execution contexts
- Testing worker functionality without custom methods

### Example: Processing Multiple Tasks

```python
from concurry import TaskWorker

# Create a process-based task worker for CPU-intensive work
worker = TaskWorker.options(mode="process").create()

# Submit multiple computational tasks
def factorial(n):
    if n <= 1:
        return 1
    return n * factorial(n - 1)

futures = [worker.submit_task(factorial, i) for i in range(1, 11)]
results = [f.result() for f in futures]

print(results)  # [1, 2, 6, 24, 120, 720, 5040, 40320, 362880, 3628800]

worker.stop()
```

### Comparison with Custom Workers

**Use TaskWorker when:**
- You don't need custom methods
- You're just submitting arbitrary functions
- You want a quick solution without boilerplate

**Use Custom Worker when:**
- You need stateful operations
- You want named, documented methods
- Your worker has complex initialization
- You're building a reusable component

### Example: TaskWorker vs Custom Worker

```python
# Using TaskWorker (simpler, but less structured)
task_worker = TaskWorker.options(mode="thread").create()
result = task_worker.submit_task(lambda x: x * 2, 10).result()
task_worker.stop()

# Using Custom Worker (more structure, better for complex logic)
class Calculator(Worker):
    def __init__(self, multiplier):
        self.multiplier = multiplier
        self.count = 0
    
    def compute(self, x):
        self.count += 1
        return x * self.multiplier

calc_worker = Calculator.options(mode="thread").create(2)
result = calc_worker.compute(10).result()  # 20
count = calc_worker.count  # State is maintained
calc_worker.stop()
```

## Performance Considerations

### Startup Overhead

Different execution modes have different startup costs:

- **sync**: Instant (no overhead)
- **thread**: ~1ms (thread creation)
- **process**: ~20ms (fork) or ~7s (spawn on macOS)
- **asyncio**: ~10ms (event loop setup)
- **ray**: Variable (depends on Ray cluster)

### Method Call Overhead

- **sync**: None (direct call)
- **thread**: Low (queue communication)
- **process**: Moderate (serialization + IPC)
- **asyncio**: Low (event loop scheduling)
- **ray**: Higher (network + serialization)

### When to Use Workers

Workers are best for:
- Long-running stateful services
- Tasks that benefit from isolation (processes)
- Operations that need resource control (Ray)
- Maintaining state across many operations

For one-off tasks, consider using regular Executors instead.

