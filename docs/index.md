# Concurry

Welcome to **Concurry** - a unified, delightful Python concurrency library that simplifies parallel and asynchronous programming.

## What is Concurry?

Concurry provides a consistent, framework-agnostic interface for working with concurrent operations in Python. Whether you're using threading, multiprocessing, asyncio, or Ray, Concurry gives you a unified API.

## Key Features

- 🔄 **Unified Future Interface**: Work with futures from any framework (threading, asyncio, Ray) through a single, consistent API
- 📊 **Beautiful Progress Bars**: Feature-rich progress tracking with tqdm integration, including success/failure states and customizable styling
- 🎯 **Framework Agnostic**: Write code once, run it with any execution backend
- 🚀 **High Performance**: Minimal overhead with optimized implementations
- 💡 **Intuitive API**: Clean, Pythonic interface that's easy to learn and use

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

## Why Choose Concurry?

### Unified Future Interface

Stop writing different code for different concurrency frameworks. Concurry's `BaseFuture` provides a consistent interface whether you're using:

- `concurrent.futures.Future`
- `asyncio.Future`
- Ray's `ObjectRef`
- Custom futures

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
- [Futures Guide](user-guide/futures.md) - Master the unified future interface
- [Progress Guide](user-guide/progress.md) - Learn about progress tracking
- [API Reference](api/index.md) - Detailed API documentation
- [Examples](examples.md) - Real-world usage examples

## Community and Support

- 🐛 [Report Issues](https://github.com/adivekar-utexas/concurry/issues)
- 💬 [Discussions](https://github.com/adivekar-utexas/concurry/discussions)
- 📖 [Documentation](https://adivekar-utexas.github.io/concurry/)

!!! tip "Pro Tip"
    Check out the [Futures Guide](user-guide/futures.md) to see how Concurry can unify your concurrency code across different frameworks!

