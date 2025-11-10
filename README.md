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


## 🚀 Quickstart: 50x Speedup for Batch LLM calls with 3 Lines of Code

Calling LLMs in a loop is painfully slow. With concurry's `@worker` decorator, transform your existing sequential code to parallel with just **3 lines of changes**:

```diff
from pydantic import BaseModel
+ from concurry import worker, gather
import litellm

# Your existing sequential class - just add @worker decorator
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

# Load 10k prompts for batch evaluation
prompts = [f"What is {i} + {i}?" for i in range(10_000)]

# Create worker instance (same initialization as before)
llm = LLM(temperature=0.1, top_p=0.9, model="meta-llama/llama-3.1-8b-instruct")

# Submit tasks: llm.call_llm(...) now returns a future!
responses = [llm.call_llm(prompt) for prompt in prompts]  
# Collect results
+ responses = gather(responses, progress=True)
```

**Performance gap:**
- **Sequential (before concurry):** ~775 seconds
- **Parallel (after concurry):** ~16 seconds (48x faster)

**What changed?** Just 3 lines:
1. Import concurry modules: `from concurry import worker, gather`
2. Add `@worker(mode='thread', max_workers=100)` decorator to your class. All calls now return futures.
3. Replace direct result collection with `gather(futures)`

No refactoring. No architectural changes.
Your existing code structure, class design, and method signatures stay exactly the same. 


## 🚀 Installation
**Requires:** Python 3.10+

```bash
pip install concurry
pip install "concurry[ray]"  # Ray support for distributed workers
pip install "concurry[all]"  # Install all dependencies
```

---

## Why Concurry?

Python's concurrency landscape is fragmented. Threading, asyncio, multiprocessing, and Ray all have different APIs, behaviors, and gotchas. 
**Concurry translates all execution modes** with a consistent, elegant interface that works the same way everywhere.

#### Without concurry

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

#### With concurry

```python
import time
import random
from concurry import worker, gather

@worker
class DataProcessor:
    def __init__(self, multiplier: int):
        self.multiplier = multiplier
    
    def compute(self, value: int) -> int:
        time.sleep(random.randint(1,3))  # Simulate calculation 
        return value * self.multiplier

## Same code, different backends - just change one parameter!
# worker = DataProcessor.options(mode="thread", max_workers=1).init(10)    # Thread
worker = DataProcessor.options(mode="thread", max_workers=100).init(10)    # Thread Pool
# worker = DataProcessor.options(mode="process", max_workers=10).init(10)  # Process Pool
# worker = DataProcessor.options(mode="ray", max_workers=10).init(10)      # Ray (distributed!)
# worker = DataProcessor.options(mode="asyncio").init(10)                  # Asyncio
# worker = DataProcessor.options(mode="sync").init(10)                     # Sync mode (for testing)

# Instant submission, non-blocking:
futures = []
for i in range(1_000):  # 1000 tasks
    futures.append(worker.compute(i))
# gather(...) blocks till all results are fetched. Progress bars are included.
results = gather(futures, progress=True)
worker.stop()
```

One interface. Multiple execution modes. Zero headaches.

---

## ✨ Key Features

### 🎭 Actor-Based Workers
Stateful workers that run across all backends with a unified API.

```python
@worker(mode="thread", max_workers=1)
class Counter:
    def __init__(self):
        self.count = 0
    
    def increment(self) -> int:
        self.count += 1
        return self.count

# State is isolated per worker
counter1 = Counter()  # Create a stateful thread
counter2 = Counter()  # Create a stateful thread
counter3 = Counter.options(mode="process").init()  # Create a stateful process
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
class ValidatedWorker(BaseModel):
    multiplier: int 
    
    @validate_call
    def compute(self, x: int) -> int:
        return x * self.multiplier

# Automatic type coercion and validation
# Automatic type coercion and validation
worker = ValidatedWorker.options(mode="thread").init(multiplier="5")  # str→int coercion
print(worker.compute(1).result())      # 5
print(worker.compute("a string").result())  # ValidationError
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
pool = LLMWorker.options(max_workers=20).init(model="gpt-5-nano", temperature=0.7)

# Limits automatically enforced across all 20 workers
prompts = [f"What is {i} + {i}?" for i in range(1000)]
futures = [pool.generate(prompt, max_tokens=16) for prompt in prompts]
results = gather(futures)

print(f"Total tokens used: {sum(r['tokens'] for r in results)}")
pool.stop()
```

### 🔁 Intelligent Retry Mechanisms
Exponential backoff, exception filtering, output validation, and automatic resource release between retries.

```python
# Retry on transient errors with exponential backoff
worker = LLMWorker.options(
    max_workers=20,
    # Automatically retries up to 5 times on ConnectionError or TimeoutError:
    num_retries=5,  
    retry_algorithm="exponential",
    retry_on=[ConnectionError, TimeoutError],
).init(model="gpt-5-nano", temperature=0.7)
```

### ⚡ First-Class Async Support
AsyncIO workers route async methods to an event loop and sync methods to a dedicated thread for optimal performance (10-50x speedup for I/O).

```python
import asyncio
import aiohttp
from concurry import worker, async_gather

@worker(mode="asyncio")
class AsyncAPIWorker:
    def __init__(self, base_url: str):
        self.base_url = base_url
    
    async def fetch(self, endpoint: str) -> bytes:
        """Async method - runs in event loop."""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/{endpoint}") as resp:
                return await resp.read()  # Read binary content
    
    async def fetch_many(self, endpoints: list) -> list:
        """Fetch multiple URLs concurrently."""
        tasks = [self.fetch(ep) for ep in endpoints]
        return await async_gather(*tasks)

worker = AsyncAPIWorker(base_url="https://picsum.photos")
# Fetch 50 random 256x256 images
urls = [
    '256' for i in range(50)
]
# Concurrent requests instead of sequential!
images = worker.fetch_many(urls).result()

# Show the images
from IPython.display import display, Image
for image in images:
    display(Image(image))
```

### 🎯 Automatic DAG-like Pipelines with Mixed Execution Modes
Chain workers with different execution modes seamlessly. Futures are automatically unwrapped, enabling heterogeneous pipelines without blocking.

```python
from concurry import worker, gather
import pandas as pd

@worker(mode="thread", max_workers=100)  # I/O-bound: read from disk/network
class DataLoader:
    def load_csv(self, path: str) -> pd.DataFrame:
        return pd.read_csv(path)

@worker(mode="process", max_workers=10)  # CPU-bound: heavy computation
class DataProcessor:
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        # Expensive operations: feature engineering, aggregations, etc.
        return df.groupby('category').agg({'value': ['mean', 'std', 'count']})

@worker(mode="thread", max_workers=100)  # I/O-bound: write to disk/database  
class DataWriter:
    def save(self, df: pd.DataFrame, path: str) -> str:
        df.to_parquet(path)
        return f"Saved to {path}"

# Create workers with different execution modes
loader = DataLoader()
processor = DataProcessor()
writer = DataWriter()

# Chain operations: each step automatically unwraps the previous future
files = [
    f'data_{i}.csv' for i in range(1000)
]
results = []
for i, file in enumerate(files):
    df_future = loader.load_csv(file)                # Returns future immediately
    processed_future = processor.process(df_future)  # Auto-unwraps df_future 
    result_future = writer.save(processed_future, f'output_{i}.parquet')  # Auto-unwraps processed_future
    results.append(result_future)

# All 1000 files processed in parallel, each through the full pipeline
outputs = gather(results)  # ['Saved to output_0.parquet', 'Saved to output_1.parquet', ...]
# Cleanup
loader.stop()
processor.stop()
writer.stop()
```

### Distributed Computing on a Ray cluster
Here's an example of running inference on 96 BERT models in just a few lines of code:

```python
import ray
from concurry import worker, gather

ray.init(ignore_reinit_error=True)

@worker
class DistributedProcessor:
    def __init__(self, model_name: str):
        self.model = load_large_model(model_name)
    
    def predict(self, data: list) -> list:
        return self.model.predict(data)

# 96 Ray actors across your cluster, each using 0.5 GPU and 2 CPUs
pool = DistributedProcessor.options(
    mode="ray",
    max_workers=96,
    actor_options=dict(
        num_cpus=2,
        num_gpus=0.5
    )
).init(model_name="bert-base-uncased")

# Distribute work across entire cluster
batch_size = 32
batches = [data[i:i+batch_size] for i in range(0, len(data), batch_size)]
futures = [pool.predict(batch) for batch in batches]  # Instant submission, non-blocking
results = gather(futures)
# Cleanup Ray actors
pool.stop()
```


### 🎬 @task Decorator for Quick Parallelization

Parallelize any function with a single decorator; no worker class needed:

```python
from concurry import task, gather
import numpy as np

@task(mode="process", max_workers=4)  # CPU-bound funcion: use processes or ray
def matrix_multiply(matrix_size: int) -> float:
    """Heavy computation: matrix multiplication."""
    A = np.random.rand(matrix_size, matrix_size)
    B = np.random.rand(matrix_size, matrix_size)
    C = np.dot(A, B)  # Expensive operation
    return np.sum(C)
# matrix_multiply is now a worker instance!

# Process 20 matrices in parallel (each 3000x3000)
matrix_sizes = [3000] * 20
futures = [matrix_multiply(size) for size in matrix_sizes] 
results = gather(futures, progress=True)  # Parallel execution across 4 CPUs

print(f"Computed {len(results)} matrix multiplications")
matrix_multiply.stop()  # Cleanup worker pool
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
