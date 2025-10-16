# Concurry

![Concurry](concurry-landscape.png)

Welcome to **Concurry** - a unified, delightful Python concurrency library that simplifies parallel and asynchronous programming.

## What is Concurry?

Concurry provides a consistent, framework-agnostic interface for working with concurrent operations in Python. Whether you're using threading, multiprocessing, asyncio, or Ray, Concurry gives you a unified API.

## Key Features

- 🔄 **Unified Future Interface**: Work with futures from any framework (threading, asyncio, Ray) through a single, consistent API
- 🎭 **Actor Pattern (Workers)**: Stateful, isolated workers that run across sync, thread, process, asyncio, and Ray backends with a unified interface
- 🔁 **Automatic Retries**: Built-in retry mechanisms with exponential backoff, exception filtering, and output validation
- 🚦 **Resource Limits**: Flexible rate limiting and resource management with shared limits across worker pools
- 📊 **Beautiful Progress Bars**: Feature-rich progress tracking with tqdm integration, including success/failure states and customizable styling
- 🎯 **Framework Agnostic**: Write code once, run it with any execution backend
- 🚀 **High Performance**: Optimized implementation with < 2.5 µs initialization, minimal overhead (~1-2 µs wrapping), and efficient actor-side retries
- 💡 **Intuitive API**: Clean, Pythonic interface that's easy to learn and use
- 🛡️ **Type Safe**: Runtime validation ensures correct types at construction with clear error messages

## Quick Start

### Unified Futures

```python
from concurry.core.future import wrap_future
import concurrent.futures

# Works with any future type
with concurrent.futures.ThreadPoolExecutor() as executor:
    future = executor.submit(lambda: 42)
    
    # Wrap it in the unified interface
    unified_future = wrap_future(future)
    
    # Consistent API across all future types
    result = unified_future.result(timeout=5)
    print(f"Result: {result}")
```

### Progress Tracking

```python
from concurry.utils.progress import ProgressBar
import time

# Create a progress bar
items = range(100)
for item in ProgressBar(items, desc="Processing"):
    time.sleep(0.01)  # Simulate work
# Automatically shows success state when complete!

# Or create a manual progress bar
pbar = ProgressBar(total=100, desc="Manual Progress")
for i in range(100):
    # Do some work
    time.sleep(0.01)
    pbar.update(1)
pbar.success("All done!")
```

### Worker Pattern

```python
from concurry import Worker

class DataProcessor(Worker):
    def __init__(self, multiplier: int):
        self.multiplier = multiplier
        self.count = 0
    
    def process(self, value: int) -> int:
        self.count += 1
        return value * self.multiplier

# Create worker in any execution mode
worker = DataProcessor.options(mode="thread").init(multiplier=3)

# Call methods (returns futures)
future = worker.process(10)
result = future.result()  # 30

# State is maintained across calls
future2 = worker.process(5)
print(f"Processed {worker.count} items")  # Tracks state

worker.stop()

# Or use TaskWorker for quick task execution
from concurry import TaskWorker

task_worker = TaskWorker.options(mode="process").init()
result = task_worker.submit(lambda x: x ** 2, 5).result()  # 25
task_worker.stop()
```

## Why Choose Concurry?

### Unified Future Interface

Stop writing different code for different concurrency frameworks. Concurry's `BaseFuture` provides a consistent interface whether you're using:

- `concurrent.futures.Future`
- `asyncio.Future`
- Ray's `ObjectRef`
- Custom futures

Built on frozen dataclasses for optimal performance:
- **Fast**: < 2.5 µs initialization
- **Type-safe**: Runtime validation at construction
- **Thread-safe**: Lock-based synchronization where needed
- **API-compatible**: Matches `concurrent.futures.Future` exactly

### Beautiful Progress Tracking

Get beautiful, informative progress bars with:

- Automatic success/failure/stop indicators with color coding
- Multiple styles (auto, notebook, standard, Ray)
- Iterable wrapping for easy integration
- Fine-grained control over updates
- Customizable appearance

### Clean Architecture

Concurry follows best practices:

- Type hints throughout
- Comprehensive documentation
- Well-tested codebase
- Minimal dependencies

## Next Steps

- [Installation Guide](installation.md) - Get started with Concurry
- [Getting Started](user-guide/getting-started.md) - Learn the basics
- [Workers Guide](user-guide/workers.md) - Learn the actor pattern with Workers
- [Worker Pools Guide](user-guide/pools.md) - Scale with worker pools
- [Limits Guide](user-guide/limits.md) - Resource and rate limiting
- [Retry Mechanisms Guide](user-guide/retries.md) - Automatic retry with backoff
- [Futures Guide](user-guide/futures.md) - Master the unified future interface
- [Progress Guide](user-guide/progress.md) - Learn about progress tracking
- [API Reference](api/index.md) - Detailed API documentation
- [Examples](examples.md) - Real-world usage examples

## Community and Support

- 🐛 [Report Issues](https://github.com/amazon-science/concurry/issues)
- 💬 [Discussions](https://github.com/amazon-science/concurry/discussions)
- 📖 [Documentation](https://amazon-science.github.io/concurry/)

!!! tip "Pro Tip"
    Check out the [Futures Guide](user-guide/futures.md) to see how Concurry can unify your concurrency code across different frameworks!

