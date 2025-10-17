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

## 🚀 Quick Example: 50x Speedup for Batch LLM Calls

Calling LLMs in a loop is painfully slow. With concurry, get 50x faster batch processing with just 3 lines of code change:

```python
from pydantic import BaseModel
from concurry import Worker
from tqdm import tqdm
import litellm

# Define your LLM worker
class LLM(Worker, BaseModel):
    temperature: float
    top_p: float
    model: str

    def call_llm(self, prompt: str) -> str:
        response = litellm.completion(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=self.temperature,
            top_p=self.top_p,
        )
        return response

# Load prompts (e.g., evaluating AI-generated responses for harmfulness)
prompts = [...] # 1000 prompts

# ❌ Sequential: ~775 seconds
llm = LLM(temperature=0.1, top_p=0.9, model="meta-llama/llama-3.1-8b-instruct")
responses = [llm.call_llm(prompt) for prompt in tqdm(prompts)]

# ✅ Concurrent with concurry: ~16 seconds (50x faster!)
llm = LLM.options(
    mode='thread',
    max_workers=100
).init(temperature=0.1, top_p=0.9, model="meta-llama/llama-3.1-8b-instruct")

futures = [llm.call_llm(prompt) for prompt in tqdm(prompts, desc="Submitting")]
responses = [f.result() for f in tqdm(futures, desc="Collecting results")]
```

**What changed?** Just added `.options(mode='thread', max_workers=100).init(...)` and called `.result()` on futures. That's it.

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

### 🎭 Actor-Based Workers
Stateful workers that run across sync, thread, process, asyncio, and Ray backends with a unified API.

```python
class Counter(Worker):
    def __init__(self):
        self.count = 0
    
    def increment(self) -> int:
        self.count += 1
        return self.count

# State is isolated per worker
counter = Counter.options(mode="thread").init()
print(counter.increment().result())  # 1
print(counter.increment().result())  # 2
```

### 🔄 Worker Pools with Load Balancing
Distribute work across multiple workers with pluggable strategies (round-robin, least-active, random).

```python
# Pool of 10 workers with round-robin load balancing
pool = DataProcessor.options(
    mode="thread",
    max_workers=10,
    load_balancing="round_robin"
).init()

# Work automatically distributed across all workers
futures = [pool.process(i) for i in range(1000)]
results = [f.result() for f in futures]
```

### 🚦 Resource Limits & Rate Limiting
Token bucket, leaky bucket, sliding window algorithms. Enforce rate limits across workers with atomic multi-resource acquisition.

```python
from concurry import RateLimit, CallLimit

# Limit to 100 API calls and 10k tokens per minute
pool = APIWorker.options(
    mode="thread",
    max_workers=20,
    limits=[
        CallLimit(window_seconds=60, capacity=100),
        RateLimit(key="tokens", window_seconds=60, capacity=10_000)
    ]
).init()

# Limits automatically enforced across all 20 workers
```

### 🔁 Intelligent Retry Mechanisms
Exponential backoff, exception filtering, output validation, and automatic resource release between retries.

```python
# Retry on transient errors with exponential backoff
worker = APIWorker.options(
    mode="thread",
    num_retries=5,
    retry_algorithm="exponential",
    retry_on=[ConnectionError, TimeoutError],
    retry_until=lambda result: result.get("status") == "ok"
).init()

# Automatically retries up to 5 times on failure
```

### 🎯 Automatic Future Unwrapping
Pass futures between workers seamlessly. Concurry automatically unwraps them - even with zero-copy optimization for Ray.

```python
# Producer creates futures
producer = DataSource.options(mode="thread").init()
data_future = producer.get_data()

# Consumer automatically unwraps the future
consumer = DataProcessor.options(mode="process").init()
result = consumer.process(data_future).result()  # Auto-unwrapped!
```

### 📊 Progress Tracking
Beautiful progress bars with state indicators, automatic style detection, and rich customization.

```python
from concurry.utils.progress import ProgressBar

for item in ProgressBar(items, desc="Processing"):
    process(item)
# Shows: Processing: 100%|██████████| 1000/1000 [00:05<00:00] ✓ Complete
```

### ✅ Pydantic Integration
Full validation support with both model inheritance and decorators (Ray-compatible `@validate` decorator included).

```python
from morphic import validate

class ValidatedWorker(Worker):
    @validate
    def __init__(self, multiplier: int):
        self.multiplier = multiplier
    
    @validate
    def process(self, x: int) -> int:
        return x * self.multiplier

# Automatic type coercion and validation
worker = ValidatedWorker.options(mode="ray").init(multiplier="5")  # str→int
```

### ⚡ Async First-Class Support
AsyncIO workers route async methods to an event loop and sync methods to a dedicated thread for optimal performance (10-50x speedup for I/O).

```python
class AsyncAPIWorker(Worker):
    async def fetch(self, url: str) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                return await resp.json()

worker = AsyncAPIWorker.options(mode="asyncio").init()
# 100 concurrent requests instead of sequential!
result = worker.fetch_many(urls).result()
```

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

## 💡 More Examples

### Worker Pool with Context Manager

```python
from concurry import Worker

class DataProcessor(Worker):
    def process(self, x: int) -> int:
        return x ** 2

# Context manager automatically cleans up all workers
with DataProcessor.options(mode="thread", max_workers=5).init() as pool:
    futures = [pool.process(i) for i in range(100)]
    results = [f.result() for f in futures]
# All workers automatically stopped here
```

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

### Distributed Computing with Ray

```python
import ray
from concurry import Worker

ray.init()

class DistributedProcessor(Worker):
    def __init__(self, model_name: str):
        self.model = load_large_model(model_name)
    
    def predict(self, data: list) -> list:
        return self.model.predict(data)

# 50 Ray actors across your cluster
pool = DistributedProcessor.options(
    mode="ray",
    max_workers=50,
    num_cpus=2,
    num_gpus=0.5
).init(model_name="bert-large")

# Distribute work across entire cluster
batches = [data[i:i+32] for i in range(0, len(data), 32)]
futures = [pool.predict(batch) for batch in batches]
results = [f.result() for f in futures]

pool.stop()
ray.shutdown()
```

### Production-Ready LLM with Rate Limits and Retries

```python
from concurry import Worker, RateLimit, CallLimit
from morphic import validate
import openai

class LLMWorker(Worker):
    @validate
    def __init__(self, model: str = "gpt-4", temperature: float = 0.7):
        self.model = model
        self.temperature = temperature
        self.client = openai.OpenAI()
    
    @validate
    def generate(self, prompt: str, max_tokens: int = 500) -> dict:
        # Rate limits automatically enforced
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
            }
            
            # Report actual usage for accurate rate limiting
            acq.update(usage={"tokens": result["tokens"]})
            return result

# Pool of 10 workers with shared rate limits and automatic retries
pool = LLMWorker.options(
    mode="thread",
    max_workers=10,
    
    # Shared rate limits across all workers
    limits=[
        RateLimit(key="tokens", window_seconds=60, capacity=10_000),
        CallLimit(window_seconds=60, capacity=100)
    ],
    
    # Automatic retry with exponential backoff
    num_retries=3,
    retry_algorithm="exponential",
    retry_on=[openai.RateLimitError, openai.APIConnectionError],
    retry_until=lambda r: len(r.get("text", "")) > 50
).init(model="gpt-4")

# Process 100 prompts with automatic rate limiting and retries
prompts = [f"Summarize topic {i}" for i in range(100)]
futures = [pool.generate(prompt, max_tokens=200) for prompt in prompts]
results = [f.result() for f in futures]

print(f"Processed {len(results)} prompts")
print(f"Total tokens: {sum(r['tokens'] for r in results)}")

pool.stop()
```

### Async I/O with 10-50x Speedup

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

# All 100 requests execute concurrently (10-50x faster)!
result = worker.fetch_many([f"data/{i}" for i in range(100)]).result()

worker.stop()
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
