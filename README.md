# Concurry

<p align="center">
  <img src="docs/concurry-landscape.png" alt="Concurry" width="800">
</p>

<p align="center">
  <a href="https://amazon-science.github.io/concurry/"><img src="https://img.shields.io/badge/docs-latest-blue.svg" alt="Documentation"></a>
  <a href="https://pypi.org/project/concurry/"><img src="https://img.shields.io/pypi/v/concurry.svg" alt="PyPI Version"></a>
  <a href="https://pypi.org/project/concurry/"><img src="https://img.shields.io/pypi/pyversions/concurry.svg" alt="Python Versions"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://github.com/amazon-science/concurry/actions"><img src="https://img.shields.io/github/actions/workflow/status/amazon-science/concurry/tests.yml?branch=main" alt="Build Status"></a>
</p>

## **Parallelism made simple, both for humans and AI agents.**

Concurry is a unified, delightful concurrency library for Python. It replaces the fragmented landscape of `threading`, `multiprocessing`, `asyncio`, and `Ray` with a single, elegant API. Write your code once, and run it on a single thread, multiple cores, or a distributed cluster—without changing a line of business logic.

---

## 🚀 Quickstart: 50x Speedup in 3 Lines of Code

Calling LLMs sequentially is painfully slow. With Concurry, you can parallelize your existing code instantly.

**Prerequisites:** `pip install concurry litellm`

```python
from pydantic import BaseModel
import litellm
# Line 1. Import concurry
from concurry import worker, gather

# Line 2. Add the @worker decorator to an existing class
@worker(mode="thread", max_workers=50)
class LLM(BaseModel):
    model: str
    
    def call(self, prompt: str) -> str:
        # This runs in a separate thread!
        return litellm.completion(
            model=self.model,
            messages=[{"role": "user", "content": prompt}]
        ).choices[0].message.content

# Initialize your worker (looks just like a normal class)
llm = LLM(model="gpt-3.5-turbo")

prompts = [f"What is {i} + {i}?" for i in range(100)]
results = [llm.call(prompt) for prompt in prompts]  # Returns futures instantly, runs in parallel
# Line 3. gather futures
results = gather(results, progress=True)            # Waits for all results

print(f"Processed {len(results)} prompts!")
llm.stop()
```

**The Result:**
- **Sequential:** ~780 seconds
- **Concurry:** ~16 seconds (**50x faster**)

No refactoring. No `concurrent.futures`. No `async def` virus. No ray.remote.
Just your code, parallelized. 
We think that's delicious 🤤

---

## 📦 Installation

```bash
pip install concurry
```

For distributed computing support:
```bash
pip install "concurry[ray]"
```

For all features:
```bash
pip install "concurry[all]"
```

---

## 💡 Why Concurry?

### The Problem: Fragmentation
Python's concurrency tools are scattered.
- **Threading**: Good for I/O, bad API (`concurrent.futures`).
- **Multiprocessing**: Good for CPU, hard to debug, pickling errors.
- **Asyncio**: High throughput, but requires rewriting everything (`async`/`await`).
- **Ray**: Powerful for clusters, but heavyweight for scripts.

### The Solution: Unified API
Concurry abstracts all of these into a single interface.

#### Without Concurry (The Old Way)
You have to learn 4 different APIs to do the same thing.

```python
# ❌ Threading API
with ThreadPoolExecutor() as executor:
    future = executor.submit(task, arg)

# ❌ Multiprocessing API (Different behavior!)
with ProcessPoolExecutor() as executor:
    future = executor.submit(task, arg)

# ❌ Asyncio API (Rewrite everything!)
async def main():
    await asyncio.create_task(async_task(arg))

# ❌ Ray API (Another new API!)
ray.get(ray_task.remote(arg))
```

#### With Concurry (The Delightful Way)
One API, any backend.

```python
from concurry import worker, gather

@worker
class MyWorker:
    def do_work(self, x: int) -> int:
        return x * 2

# Run on threads?
w = MyWorker.options(mode="thread", max_workers=10).init()

# Run on processes? Uncomment below.
# w = MyWorker.options(mode="process", max_workers=10).init()

# Run on a ray cluster? Uncomment below.
# w = MyWorker.options(mode="ray", max_workers=10).init()

# Run on asyncio? Uncomment below.
# w = MyWorker.options(mode="asyncio").init()

# The submission code NEVER changes:
futures = [w.do_work(i) for i in range(1000)]
# The collection code NEVER changes:
results = gather(futures, progress=True)
w.stop()
```

---

## ✨ Key Features

### 🎭 Actor-Based Workers
Stateful workers that persist across calls. Perfect for database connections, model weights, or session management.

```python
from concurry import worker

@worker(mode="thread")
class Counter:
    def __init__(self):
        self.count = 0
    
    def increment(self) -> int:
        self.count += 1
        return self.count

# State is preserved!
counter = Counter()
print(counter.increment().result())  # 1
print(counter.increment().result())  # 2
counter.stop()
```

### 🚦 Rate Limiting
Built-in rate limiting for APIs. Token buckets, sliding windows, and more, enforced globally across all workers.

```python
from concurry import worker, gather, CallLimit

@worker(
    mode="thread",
    max_workers=20,
    # Limit to 100 calls per minute across ALL 20 threads
    limits=[CallLimit(window=60, capacity=100)]
)
class APIWorker:
    def fetch(self, url: str):
        # Rate limit is automatically checked here
        return f"Fetched {url}"

pool = APIWorker()
futures = [pool.fetch(f"url_{i}") for i in range(200)]
results = gather(futures, progress=True)  # Smoothly throttled!
pool.stop()
```

### 🔁 Intelligent Retries
Don't let flaky networks break your batch jobs. Configure retries declaratively.

```python
from concurry import worker, RetryConfig

@worker(
    mode="thread",
    retry_config=RetryConfig(
        max_retries=5,
        retry_on=(ConnectionError, TimeoutError),
        backoff_factor=2.0  # Exponential backoff: 1s, 2s, 4s, ...
    )
)
class FlakyWorker:
    def fetch(self):
        # Automatically retried on failure!
        pass
```

### ✅ Pydantic Integration
Full support for Pydantic models. Arguments are validated and coerced before they even reach the worker.

```python
from concurry import worker
from pydantic import BaseModel, Field

@worker(mode="process")
class DataWorker(BaseModel):
    db_url: str = Field(..., pattern=r"^postgres://")
    
    def process(self, data: dict):
        return data

# Validated at initialization!
try:
    w = DataWorker(db_url="invalid-url")
except Exception as e:
    print(f"Validation failed!: {e}")  # Caught before worker starts
```

### 🎬 The `@task` Decorator
Just want to run a function in parallel? You don't need a class.

```python
from concurry import task, gather
import time

@task(mode="process", max_workers=4)
def heavy_computation(x: int) -> int:
    time.sleep(1)  ## Example heavy computation
    return x

# Run 100 heavy computations in parallel
futures = [heavy_computation(i) for i in range(100)]
results = gather(futures, progress=True)
heavy_computation.stop()
```

---

## 📚 Documentation

- **[User Guide](https://amazon-science.github.io/concurry/user-guide/getting-started/)**: Tutorials and best practices.
- **[API Reference](https://amazon-science.github.io/concurry/api/)**: Detailed API specs.
- **[Gallery](https://amazon-science.github.io/concurry/user-guide/gallery/)**: Production-ready examples (LLM pipelines, web scrapers).

---

## 🤝 Contributing

We love contributions! Check out [CONTRIBUTING.md](CONTRIBUTING.md) to get started.

---

## 📄 License

Apache 2.0 - See [LICENSE](LICENSE) for details.

---

<p align="center">
  <strong>Made with ❤️ by <a href="https://github.com/amazon-science">Amazon Scientists</a></strong>
</p>
