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

**A unified, delightful Python concurrency library** that makes parallel and distributed computing feel like writing sequential code. Built on the actor model, concurry provides workers, pools, rate limiting, retries, and seamless integration with Ray for distributed execution.

---

## Why Concurry?

Python's concurrency landscape is fragmented. Threading, multiprocessing, asyncio, and Ray all have different APIs, behaviors, and gotchas. **Concurry unifies them** with a consistent, elegant interface that works the same way everywhere.

### The Problem

```python
# Different APIs for different backends
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import asyncio
import ray

# Thread pool - one API
with ThreadPoolExecutor() as executor:
    future = executor.submit(task, arg)
    result = future.result()

# Process pool - same API, different behavior
with ProcessPoolExecutor() as executor:
    future = executor.submit(task, arg)
    result = future.result()

# Asyncio - completely different API
async def main():
    result = await asyncio.create_task(async_task(arg))

# Ray - yet another API
@ray.remote
def ray_task(arg):
    return result
future = ray_task.remote(arg)
result = ray.get(future)
```

### The Solution

```python
from concurry import Worker

class DataProcessor(Worker):
    def __init__(self, multiplier: int):
        self.multiplier = multiplier
    
    def process(self, value: int) -> int:
        return value * self.multiplier

# Same code, different backends - just change one parameter!
worker = DataProcessor.options(mode="thread").init(10)      # Thread
# worker = DataProcessor.options(mode="process").init(10)   # Process
# worker = DataProcessor.options(mode="asyncio").init(10)   # Asyncio
# worker = DataProcessor.options(mode="ray").init(10)       # Ray (distributed!)

result = worker.process(42).result()  # 420
worker.stop()
```

**One interface. Five execution modes. Zero headaches.**

---

## ✨ Key Features

### 🎭 **Actor-Based Workers**
Stateful workers that run across sync, thread, process, asyncio, and Ray backends with a unified API.

### 🔄 **Worker Pools with Load Balancing**
Distribute work across multiple workers with pluggable load balancing strategies (round-robin, least-active, random).

### 🚦 **Resource Limits & Rate Limiting**
Token bucket, leaky bucket, sliding window, and more. Enforce rate limits across workers with atomic multi-resource acquisition.

### 🔁 **Intelligent Retry Mechanisms**
Exponential backoff, exception filtering, output validation, and automatic resource release between retries.

### 🎯 **Automatic Future Unwrapping**
Pass futures between workers seamlessly. Concurry automatically unwraps them - even with zero-copy optimization for Ray.

### 📊 **Progress Tracking**
Beautiful progress bars with state indicators, automatic style detection, and rich customization.

### ✅ **Pydantic Integration**
Full validation support with both model inheritance and decorators (Ray-compatible `@validate` decorator included).

### ⚡ **Async First-Class Support**
AsyncIO workers route async methods to an event loop and sync methods to a dedicated thread for optimal performance.

---

## 🚀 Installation

```bash
# Basic installation
pip install concurry

# With Ray support for distributed computing
pip install concurry[ray]

# Development installation with all extras
pip install concurry[all]
```

**Requirements:** Python 3.10+

---

## 💡 Quick Start

### Simple Worker Example

```python
from concurry import Worker

class Counter(Worker):
    def __init__(self):
        self.count = 0
    
    def increment(self) -> int:
        self.count += 1
        return self.count

# Create a thread-based worker
counter = Counter.options(mode="thread").init()

# Call methods (returns futures)
print(counter.increment().result())  # 1
print(counter.increment().result())  # 2
print(counter.increment().result())  # 3

counter.stop()
```

### Worker Pool with Load Balancing

```python
from concurry import Worker

class DataProcessor(Worker):
    def process(self, x: int) -> int:
        return x ** 2

# Create a pool of 5 workers with round-robin load balancing
pool = DataProcessor.options(
    mode="thread",
    max_workers=5,
    load_balancing="round_robin"
).init()

# Distribute work across the pool
futures = [pool.process(i) for i in range(100)]
results = [f.result() for f in futures]

pool.stop()
```

---

## 🎯 Real-World Example: LLM API with Rate Limits and Retries

Here's a production-ready example that calls an LLM API at scale with token-level rate limiting, automatic retries, and validation:

```python
from concurry import Worker, RateLimit, CallLimit
from morphic import validate
import openai

class LLMWorker(Worker):
    """Production-grade LLM worker with rate limiting, retries, and validation."""
    
    @validate
    def __init__(self, model: str = "gpt-4", temperature: float = 0.7):
        self.model = model
        self.temperature = temperature
        self.client = openai.OpenAI()
    
    @validate
    def generate(self, prompt: str, max_tokens: int = 500) -> dict:
        """Generate text with automatic rate limiting."""
        # Rate limits are automatically enforced by the pool
        with self.limits.acquire(requested={"tokens": max_tokens}) as acq:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=self.temperature
            )
            
            result = {
                "text": response.choices[0].message.content,
                "tokens": response.usage.total_tokens,
                "model": self.model
            }
            
            # Report actual token usage for accurate rate limiting
            acq.update(usage={"tokens": result["tokens"]})
            return result

# Validation function for output quality
def validate_output(result, **ctx):
    """Ensure generated text meets quality requirements."""
    return (
        isinstance(result, dict) 
        and len(result.get("text", "")) > 50
        and "error" not in result.get("text", "").lower()
    )

# Create a pool of 10 LLM workers with rate limits and retries
pool = LLMWorker.options(
    mode="thread",
    max_workers=10,
    
    # Rate limiting: 1000 tokens/min and 50 calls/min shared across pool
    limits=[
        RateLimit(key="tokens", window_seconds=60, capacity=1000),
        CallLimit(window_seconds=60, capacity=50)
    ],
    
    # Retry configuration for transient failures
    num_retries=3,
    retry_algorithm="exponential",  # Exponential backoff
    retry_wait=1.0,                  # Base wait of 1 second
    retry_on=[openai.RateLimitError, openai.APIConnectionError],
    retry_until=validate_output      # Retry until validation passes
).init(model="gpt-4", temperature=0.7)

# Process 100 prompts with automatic rate limiting and retries
prompts = [f"Summarize topic {i}" for i in range(100)]
futures = [pool.generate(prompt, max_tokens=200) for prompt in prompts]

# Collect results (blocks until all complete)
results = [f.result() for f in futures]

print(f"Processed {len(results)} prompts")
print(f"Total tokens used: {sum(r['tokens'] for r in results)}")

pool.stop()
```

**What just happened?**

- ✅ 10 workers processing prompts in parallel
- ✅ Shared token budget (1000 tokens/min) across all workers
- ✅ Automatic retries on rate limit errors with exponential backoff
- ✅ Output validation to ensure quality responses
- ✅ Automatic resource release between retry attempts
- ✅ Type validation with `@validate` decorator

---

## 🌐 Distributed Computing with Ray

Scale to hundreds of machines with Ray - same API, just change `mode`:

```python
import ray
from concurry import Worker

ray.init()  # Connect to Ray cluster

class DistributedProcessor(Worker):
    def __init__(self, model_name: str):
        # Load model once per worker
        self.model = load_large_model(model_name)
    
    def predict(self, data: list) -> list:
        return self.model.predict(data)

# Create a pool of 50 Ray actors across the cluster
pool = DistributedProcessor.options(
    mode="ray",
    max_workers=50,
    num_cpus=2,              # Each worker gets 2 CPUs
    num_gpus=0.5,            # Each worker gets 0.5 GPU
).init(model_name="bert-large")

# Distribute work across the entire cluster
batches = [data[i:i+32] for i in range(0, len(data), 32)]
futures = [pool.predict(batch) for batch in batches]
results = [f.result() for f in futures]

pool.stop()
ray.shutdown()
```

**Zero-copy optimization:** When passing futures between Ray workers, Concurry passes `ObjectRef`s directly without serialization!

---

## 🔄 Async Functions with AsyncIO Workers

AsyncIO workers provide 10-50x speedup for concurrent I/O operations:

```python
from concurry import Worker
import aiohttp
import asyncio

class AsyncAPIWorker(Worker):
    def __init__(self, base_url: str):
        self.base_url = base_url
    
    async def fetch(self, endpoint: str) -> dict:
        """Async method - runs in event loop."""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/{endpoint}") as resp:
                return await resp.json()
    
    async def fetch_many(self, endpoints: list) -> list:
        """Fetch multiple URLs concurrently."""
        tasks = [self.fetch(ep) for ep in endpoints]
        return await asyncio.gather(*tasks)

worker = AsyncAPIWorker.options(mode="asyncio").init("https://api.example.com")

# All 100 requests execute concurrently!
result = worker.fetch_many([f"data/{i}" for i in range(100)]).result()

worker.stop()
```

**Architecture:** AsyncIO workers route async methods to an event loop thread and sync methods to a dedicated sync thread, giving you the best of both worlds.

---

## ✅ Pydantic & Morphic Integration

Concurry integrates seamlessly with [Pydantic](https://pydantic.dev/) and [Morphic](https://github.com/adivekar/morphic) for validation:

### Option 1: Model Inheritance (Non-Ray)

```python
from concurry import Worker
from morphic import Typed
from pydantic import Field

class ValidatedWorker(Worker, Typed):
    """Worker with full Pydantic validation and lifecycle hooks."""
    
    name: str = Field(..., min_length=1)
    multiplier: int = Field(default=2, ge=1)
    
    def process(self, x: int) -> int:
        return x * self.multiplier

# Automatic validation
worker = ValidatedWorker.options(mode="thread").init(
    name="processor",
    multiplier=5
)

result = worker.process(10).result()  # 50
worker.stop()
```

### Option 2: Validation Decorators (Ray-Compatible!)

```python
from concurry import Worker
from morphic import validate
import ray

ray.init()

class RayValidatedWorker(Worker):
    """Ray-compatible worker with validation."""
    
    @validate
    def __init__(self, name: str, multiplier: int = 2):
        self.name = name
        self.multiplier = multiplier
    
    @validate
    def process(self, x: int, scale: float = 1.0) -> float:
        return (x * self.multiplier) * scale

# Works with Ray! (Typed/BaseModel don't work with Ray)
worker = RayValidatedWorker.options(mode="ray").init(
    name="ray_processor",
    multiplier="5"  # String coerced to int
)

# Automatic type coercion
result = worker.process("10", scale="2.0").result()  # 100.0
worker.stop()

ray.shutdown()
```

---

## 🎨 More Features

### TaskWorker for Arbitrary Functions

```python
from concurry import TaskWorker

worker = TaskWorker.options(mode="process").init()

# Submit any function
future = worker.submit(lambda x: x ** 2, 42)
print(future.result())  # 1764

# Use map() for batch processing
results = list(worker.map(lambda x: x * 2, range(10)))
print(results)  # [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

worker.stop()
```

### Progress Tracking

```python
from concurry.utils.progress import ProgressBar
import time

# Beautiful progress bars with state indicators
for item in ProgressBar(range(100), desc="Processing"):
    time.sleep(0.01)
    # Automatic success indicator when complete!

# Manual progress bar with failure handling
pbar = ProgressBar(total=100, desc="Complex Task")
try:
    for i in range(100):
        if error_condition:
            raise ValueError("Something went wrong")
        pbar.update(1)
    pbar.success("All done!")
except Exception as e:
    pbar.failure(f"Failed: {e}")
```

### Automatic Future Unwrapping

```python
# Pass futures between workers - they're automatically unwrapped!
producer = DataSource.options(mode="thread").init()
consumer = DataProcessor.options(mode="process").init()

# Producer returns a future
data_future = producer.get_data()

# Consumer automatically unwraps it
result = consumer.process(data_future).result()

# Works with nested structures too!
futures = [producer.get_data() for _ in range(10)]
result = consumer.process_batch(futures).result()  # All unwrapped!
```

### Resource Limits

```python
from concurry import Worker, ResourceLimit

class DatabaseWorker(Worker):
    def query(self, sql: str) -> list:
        # Limit concurrent database connections
        with self.limits.acquire(requested={"connections": 1}):
            return execute_query(sql)

# Pool of 20 workers sharing 5 database connections
pool = DatabaseWorker.options(
    mode="thread",
    max_workers=20,
    limits=[ResourceLimit(key="connections", capacity=5)]
).init()

# Only 5 queries run concurrently, even with 20 workers
```

---

## 📚 Documentation

- **[User Guide](https://amazon-science.github.io/concurry/user-guide/getting-started/)** - Comprehensive tutorials and examples
  - [Workers](https://amazon-science.github.io/concurry/user-guide/workers/) - Actor-based workers
  - [Worker Pools](https://amazon-science.github.io/concurry/user-guide/pools/) - Load balancing and pooling
  - [Limits](https://amazon-science.github.io/concurry/user-guide/limits/) - Rate limiting and resource management
  - [Retries](https://amazon-science.github.io/concurry/user-guide/retries/) - Retry mechanisms
  - [Futures](https://amazon-science.github.io/concurry/user-guide/futures/) - Unified future interface
  - [Progress](https://amazon-science.github.io/concurry/user-guide/progress/) - Progress tracking
- **[API Reference](https://amazon-science.github.io/concurry/api/)** - Detailed API documentation
- **[Examples](https://amazon-science.github.io/concurry/examples/)** - Real-world usage patterns
- **[Contributing](CONTRIBUTING.md)** - How to contribute

---

## 🏗️ Design Principles

1. **Unified API**: One interface for all concurrency paradigms
2. **Actor Model**: Stateful workers with isolated state
3. **Production-Ready**: Rate limiting, retries, validation, monitoring
4. **Performance**: Zero-copy optimizations where possible
5. **Developer Experience**: Intuitive API, rich documentation, great error messages

---

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## 📄 License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- Built on top of [morphic](https://github.com/adivekar/morphic) for validation
- Inspired by [Ray](https://ray.io/), [Pydantic](https://pydantic.dev/), and the actor model
- Progress bars powered by [tqdm](https://github.com/tqdm/tqdm)

---

<p align="center">
  <strong>Made with ❤️ by the <a href="https://github.com/amazon-science">Amazon Science</a> team</strong>
</p>
