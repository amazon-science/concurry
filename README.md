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

**A unified, delightful Python concurrency library** that delivers production-grade parallelism with zero architectural changes. 
Built on the actor model, `concurry` provides worker pools with rate limiting, load-balancing, retries. It seamlesslt integrates with Ray, enabling 10-100x speedups on real workloads while preserving your existing code structure. 
A delicious bowl of parallelism, served instantly.


## 🚀 Quickstart: 50x Speedup for Batch LLM calls with 2 Lines of Code

Calling LLMs in a loop is painfully slow. With concurry's `@worker` decorator, transform your existing sequential code to parallel with just **2 lines of changes**:

```diff
from pydantic import BaseModel
+ from concurry import worker, gather
import litellm

# Your existing LLM class - just add @worker decorator
+ @worker(mode='thread', max_workers=100)
class LLM(BaseModel):
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

# Load 1000 prompts for batch evaluation
prompts = [...]

# Create worker instance (same initialization as before)
llm = LLM(temperature=0.1, top_p=0.9, model="meta-llama/llama-3.1-8b-instruct")

# Submit tasks and collect results
- responses = [llm.call_llm(prompt) for prompt in prompts]
+ futures   = [llm.call_llm(prompt) for prompt in prompts]
+ responses = gather(futures)
```

**Performance:**
- **Sequential (before):** ~775 seconds
- **Parallel (after):** ~16 seconds (48x faster)

**What changed?** Just 2 lines:
1. Add `@worker(mode='thread', max_workers=100)` decorator to your class.
2. Replace direct result collection with `gather(futures)`

Your existing code structure, class design, and method signatures stay exactly the same. 
No refactoring. No architectural changes.


## 🚀 Installation
**Requires:** Python 3.10+

```bash
pip install concurry

pip install "concurry[ray]"  # Ray support for distributed workers

pip install "concurry[all]"  # Development install
```

---

## Why Concurry?


#### The Problem

Python's concurrency landscape is fragmented. Threading, asyncio, multiprocessing, and Ray all have different APIs, behaviors, and gotchas. 
**Concurry translates all execution modes** with a consistent, elegant interface that works the same way everywhere.

**Before concurry:**
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

With concurry: One interface. Multiple execution modes. Zero headaches.
```diff
import time
import random
from concurry import worker, gather

+ @worker
class DataProcessor:
    def __init__(self, multiplier: int):
        self.multiplier = multiplier
    
    def compute(self, value: int) -> int:
        time.sleep(random.randint(1,3))  # Simulate calculation 
        return value * self.multiplier

# Same code, different backends - just change one parameter!
# worker = DataProcessor.options(mode="thread", max_workers=1).init(10)   # Thread
# worker = DataProcessor.options(mode="thread", max_workers=100).init(10)  # Thread Pool
# worker = DataProcessor.options(mode="process", max_workers=10).init(10) # Process Pool
# worker = DataProcessor.options(mode="ray", max_workers=10).init(10)     # Ray (distributed!)
# worker = DataProcessor.options(mode="asyncio").init(10)                 # Asyncio
# worker = DataProcessor.options(mode="sync").init(10)                    # Sync mode (for testing)

# Instant submission, non-blocking:
futures = []
for i in range(1_000):
    futures.append(worker.compute(i))
- results = [future.result() for future in futures]  # Boring! 🥱 
+ # gather() blocks till all results are fetched. Progress bars are included.
+ results = gather(futures, progress=True)

+ # ALTERNATE: use gather(iter=True) to stream results as they finish. 
+ for result in gather(futures, iter=True, progress=True):
+     print(result)
worker.stop()
```

---

## ✨ Key Features

### 🎭 Actor-Based Workers
Stateful workers that run across all backends with a unified API.

```diff
+ @worker(mode="thread", max_workers=1)
class Counter:
    def __init__(self):
        self.count = 0
    
    def increment(self) -> int:
        self.count += 1
        return self.count

# State is isolated per worker
+ counter1 = Counter()
+ counter2 = Counter()
+ counter3 = Counter.options(mode="process")
print(counter1.increment().result())  # 1
print(counter1.increment().result())  # 2
print(counter2.increment().result())  # 1
print(counter1.increment().result())  # 3
print(counter2.increment().result())  # 2
print(counter3.increment().result())  # 1
```

### 🔄 Worker Pools with Load Balancing
Distribute work across multiple workers with inbuilt load-balancing strategies (round-robin, least-active, random).

```python
# Pool of 10 workers with round-robin load balancing
pool = DataProcessor.options(
    mode="thread",
    max_workers=10,
    load_balancing="round_robin"
).init()

# Work automatically distributed across all workers
futures = [pool.process(i) for i in range(1_000)]  # Instant, non-blocking submission
results = gather(futures)
```

### ✅ Pydantic Integration
Full validation support with Pydantic BaseModel inheritance and decorators.

```python
from pydantic import BaseModel, validate_call

@worker
class ValidatedWorker:
    multiplier: int 
    
    @validate_call
    def compute(self, x: int) -> int:
        return x * self.multiplier

# Automatic type coercion and validation
worker = ValidatedWorker.options(mode="ray").init(multiplier="5")  # str→int coercion
```

### 🚦 Rate Limiting
Token bucket, leaky bucket and sliding window algorithms enforce rate limits across workers with atomic multi-resource acquisition.

```python
from concurry import worker, gather, RateLimit, CallLimit
import litellm

@worker(
    mode="thread",
    limits=[
        CallLimit(window_seconds=60, capacity=100),      # Max 100 calls/min
        RateLimit(key="tokens", window_seconds=60, capacity=100_000)  # Max 100k tokens/min
    ]
)
class LLMWorker:
    def __init__(self, model: str, temperature: float):
        self.model = model
        self.temperature = temperature
    
    def generate(self, prompt: str, max_tokens: int = 500) -> dict:
        # Acquire limits before making API call
        with self.limits.acquire(requested={"tokens": max_tokens}) as acq:
            response = litellm.completion(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=self.temperature
            )
            result = {
                "text": response.choices[0].message.content,
                "tokens": response.usage.total_tokens
            }
            
            # Report actual token usage for accurate rate limiting
            acq.update(usage={"tokens": result["tokens"]})
            return result

# Pool of 20 workers with shared rate limits
pool = LLMWorker.options(max_workers=20).init(model="gpt-4o-mini", temperature=0.7)

# Limits automatically enforced across all 20 workers
prompts = [f"What is {i} + {i}?" for i in range(1000)]
futures = [pool.generate(prompt, max_tokens=16) for prompt in prompts]
results = gather(futures)

print(f"Total tokens used: {sum(r['tokens'] for r in results)}")
pool.stop()
```

### 🔁 Intelligent Retry Mechanisms
Exponential backoff, exception filtering, output validation, and automatic resource release between retries.

```diff
# Retry on transient errors with exponential backoff
worker = LLMWorker.options(
    max_workers=20,
+    num_retries=5,  
+    retry_algorithm="exponential",
+    retry_on=[ConnectionError, TimeoutError],
+    retry_until=lambda result: result.get("status") == "ok"
).init(model="gpt-4o-mini", temperature=0.7)

# Automatically retries up to 5 times on failure
```

### ⚡ First-Class Async Support
AsyncIO workers route async methods to an event loop and sync methods to a dedicated thread for optimal performance (10-50x speedup for I/O).

```python
@worker
class AsyncAPIWorker:
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

worker = AsyncAPIWorker.options(mode="asyncio").init()
# concurrent requests instead of sequential!
result = worker.fetch_many(urls).result()
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
from concurry import ProgressBar

for item in ProgressBar(items, desc="Processing"):
    process(item)
# Shows: Processing: 100%|██████████| 1000/1000 [00:05<00:00] ✓ Complete
```

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

### Distributed Computing on a Ray cluster
Here's an example of running 96 BERT models in just a few lines of code:

```python
import ray
from concurry import worker, gather

ray.init()

@worker
class DistributedProcessor:
    def __init__(self, model_name: str):
        self.model = load_large_model(model_name)
    
    def predict(self, data: list) -> list:
        return self.model.predict(data)

# 96 Ray actors across your cluster, each using half a GPU
pool = DistributedProcessor.options(
    mode="ray",
    max_workers=96,
    actor_options=dict(
        num_cpus=2,
        num_gpus=0.5
    )
).init(model_name="bert-large")

# Distribute work across entire cluster
batches = [data[i:i+32] for i in range(0, len(data), 32)]
futures = [pool.predict(batch) for batch in batches]  # Instant submission 
results = gather(futures)

pool.stop()
ray.shutdown()
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
  <strong>Made with ❤️ by <a href="https://github.com/amazon-science">Amazon Scientists</a></strong>
</p>
