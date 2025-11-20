# Concurry

![Concurry](concurry-landscape.png)

Welcome to **Concurry** — the Python concurrency library designed to be **delightful**. 

Concurry provides a unified, framework-agnostic interface for parallel and asynchronous programming. Whether you're using threading, multiprocessing, asyncio, or Ray, Concurry gives you a single, consistent API that just works.

## Concept Map: What do you want to build?

| I want to... | Use... | Why? |
| :--- | :--- | :--- |
| **Run API calls** in parallel | [**Workers**](user-guide/workers.md) (Thread mode) | I/O bound operations don't block the GIL. |
| **Process data** (heavy CPU) | [**Workers**](user-guide/workers.md) (Process mode) | Bypasses the GIL to use all CPU cores. |
| **Scale across machines** | [**Workers**](user-guide/workers.md) (Ray mode) | Seamlessly moves your local code to a cluster. |
| **Process 1000s of items** | [**Worker Pools**](user-guide/pools.md) | Automatically load-balances tasks across workers. |
| **Respect API Rate Limits** | [**Limits**](user-guide/limits.md) | Thread-safe, distributed rate limiting built-in. |
| **Handle Flaky APIs** | [**Retries**](user-guide/retries.md) | Automatic exponential backoff and error handling. |
| **Wait for multiple results** | [**Synchronization**](user-guide/synchronization.md) | Powerful `wait()` and `gather()` primitives. |

## Why Concurry?

### 1. Unified Future Interface
Stop writing different code for `asyncio.Future`, `concurrent.futures.Future`, and Ray `ObjectRef`. Concurry wraps them all in a single, consistent **[Unified Future](user-guide/futures.md)** interface.

```python
# Works with Thread, Process, Asyncio, AND Ray backends!
result = future.result(timeout=5)
```

### 2. The Actor Pattern made Simple
Define a class, inherit from `Worker`, and you have a stateful actor that can run anywhere.

```python
from concurry import Worker

class DataProcessor(Worker):
    def process(self, data):
        return data * 2

# Run efficiently on threads...
worker = DataProcessor.options(mode="thread").init()
# ...or move to a separate process...
worker = DataProcessor.options(mode="process").init()
# ...or scale to a cluster!
worker = DataProcessor.options(mode="ray").init()
```

### 3. Production-Ready Features
Concurry comes batteries-included with the tools you need for robust systems:
*   🚦 **[Limits](user-guide/limits.md)**: Distributed rate limiting and resource semaphores.
*   🔁 **[Retries](user-guide/retries.md)**: Smart retry logic with jitter and validation.
*   📊 **[Progress](user-guide/progress.md)**: Beautiful, zero-config progress bars.

## Where to Start?

*   🚀 **[Getting Started](user-guide/getting-started.md)**: Build a high-performance LLM pipeline in 5 minutes.
*   📚 **[User Guide](user-guide/workers.md)**: Deep dive into Workers, Pools, and more.
*   🖼️ **[Gallery](user-guide/gallery/index.md)**: Real-world examples and recipes.

---

### Community and Support
*   🐛 [Report Issues](https://github.com/amazon-science/concurry/issues)
*   💬 [Discussions](https://github.com/amazon-science/concurry/discussions)
