# Getting Started

This guide will walk you through Concurry's core concepts using a practical example: **making batch LLM calls 50x faster**.

## The Problem: Sequential Code is Slow

Let's say you need to call an LLM API 1,000 times (e.g., evaluating AI-generated responses for safety). Sequential code is painfully slow:

```python
import litellm
from tqdm import tqdm

def call_llm(prompt: str, model: str, temperature: float) -> str:
    """Call LLM API with a prompt."""
    response = litellm.completion(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=temperature,
    )
    return response.choices[0].message.content

# Load 1,000 prompts
prompts = [...]  # Your prompts here

# ❌ Sequential: Call LLM one at a time
model = "gpt-4o-mini"
responses = []
for prompt in tqdm(prompts, desc="Processing"):
    response = call_llm(prompt, model, temperature=0.1)
    responses.append(response)

# Time: ~775 seconds (12+ minutes!) 😱
```

**Why is this slow?** Each API call waits for the previous one to complete. Your CPU sits idle while waiting for network I/O.

## The Solution: Concurry Workers

With Concurry, make all calls concurrently with just **3 lines of code changed**:

```python
from concurry import Worker
from tqdm import tqdm
import litellm

# 1. Wrap your logic in a Worker class
class LLM(Worker):
    def __init__(self, model: str, temperature: float):
        self.model = model
        self.temperature = temperature
    
    def call_llm(self, prompt: str) -> str:
        """Call LLM API with a prompt."""
        response = litellm.completion(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=self.temperature,
        )
        return response.choices[0].message.content

# 2. Create a pool of workers instead of a single instance
llm = LLM.options(
    mode='thread',      # Use thread-based concurrency (great for I/O)
    max_workers=100     # 100 concurrent calls
).init(model="gpt-4o-mini", temperature=0.1)

# 3. Submit all tasks and collect results using futures
futures = [llm.call_llm(prompt) for prompt in tqdm(prompts, desc="Submitting")]
responses = [f.result() for f in tqdm(futures, desc="Collecting")]

# Time: ~16 seconds (50x faster!) 🚀
```

**What changed?**
- Added `.options(mode='thread', max_workers=100).init(...)` → Creates a pool of 100 workers
- Called `.result()` on futures → Waits for each task to complete
- That's it! 50x speedup with minimal code changes.

## Installation

First, install Concurry:

```bash
pip install concurry
```

For distributed computing with Ray:

```bash
pip install concurry[ray]
```

## What Just Happened?

Let's break down the key concepts:

### 1. Workers: Stateful Concurrent Actors

A **Worker** is a class that runs concurrently in the background. Think of it as a dedicated assistant that handles tasks for you:

```python
class LLM(Worker):
    def __init__(self, model: str, temperature: float):
        self.model = model          # Worker state
        self.temperature = temperature
    
    def call_llm(self, prompt: str) -> str:
        # This method runs in the background
        return litellm.completion(...)
```

**Key points:**
- Workers maintain state (e.g., `self.model`, `self.temperature`)
- Each method call runs in the background
- Workers are isolated - one worker's state doesn't affect another

### 2. Worker Pools: Parallel Execution

When you use `.options(max_workers=100)`, Concurry creates a **pool** of 100 workers:

```python
llm = LLM.options(
    mode='thread',      # How workers run (thread, process, asyncio, ray)
    max_workers=100     # How many workers in the pool
).init(model="gpt-4o-mini", temperature=0.1)
```

**What happens:**
- Concurry creates 100 worker threads
- Each worker can handle one API call at a time
- 100 API calls can run concurrently
- Load balancing automatically distributes work across workers

### 3. Futures: Asynchronous Results

When you call a worker method, you get a **future** - a placeholder for a result that will arrive later:

```python
# Submit a task - returns immediately with a future
future = llm.call_llm("What is AI?")

# Do other work here...

# Get the result when you need it (blocks until complete)
response = future.result()
```

**Common pattern:**
```python
# Submit all tasks first (fast - just queuing work)
futures = [llm.call_llm(prompt) for prompt in prompts]

# Collect results later (blocks until each completes)
responses = [f.result() for f in futures]
```

This is why Concurry is fast: you submit all 1,000 tasks at once, and 100 workers process them concurrently!

### 4. Unified Interface: One API, Multiple Backends

The same code works across different execution modes:

```python
# Thread-based (great for I/O like API calls)
llm = LLM.options(mode='thread', max_workers=100).init(...)

# Process-based (great for CPU-heavy work)
llm = LLM.options(mode='process', max_workers=8).init(...)

# Async-based (even more I/O efficiency)
llm = LLM.options(mode='asyncio').init(...)

# Distributed with Ray (scale across machines!)
llm = LLM.options(mode='ray', max_workers=1000).init(...)
```

**Just change one parameter**, and your code runs on different backends. No need to learn ThreadPoolExecutor, ProcessPoolExecutor, asyncio, and Ray separately!

## Core Concepts

Concurry provides powerful building blocks for production-grade concurrent systems:

### 1. **Workers** - Stateful Concurrent Actors
The core abstraction. Workers run in the background across sync, thread, process, asyncio, and Ray modes.

### 2. **Worker Pools** - Automatic Load Balancing
Scale to hundreds of workers with automatic work distribution and configurable load balancing strategies.

### 3. **Limits** - Rate Limiting & Resource Control
Enforce API rate limits, token budgets, and resource constraints across all workers with atomic multi-resource acquisition.

### 4. **Retry Mechanisms** - Automatic Fault Tolerance
Built-in exponential backoff, exception filtering, and output validation. Automatically retries failed tasks.

### 5. **Unified Future Interface** - Framework-Agnostic Results
Consistent API for working with futures from any framework (threading, asyncio, Ray, etc.)

### 6. **Progress Tracking** - Beautiful Progress Bars
Rich, color-coded progress bars with success/failure states that work in terminals and notebooks

## Best Practices

### 1. Choose the Right Execution Mode

```python
# I/O-bound (API calls, database queries, file I/O)
# → Use 'thread' mode with many workers
llm = LLM.options(mode='thread', max_workers=100).init(...)

# CPU-bound (data processing, ML inference)
# → Use 'process' mode with workers ≈ CPU cores
processor = DataProcessor.options(mode='process', max_workers=8).init(...)

# Heavy I/O with async libraries (aiohttp, httpx)
# → Use 'asyncio' mode for even better performance
api = AsyncAPI.options(mode='asyncio').init(...)

# Distributed across machines
# → Use 'ray' mode for cluster computing
model = LargeModel.options(mode='ray', max_workers=1000).init(...)
```

### 2. Always Clean Up Workers

```python
# ✅ Good: Use context managers for automatic cleanup
with LLM.options(mode='thread', max_workers=100).init(...) as llm:
    futures = [llm.call_llm(prompt) for prompt in prompts]
    responses = [f.result() for f in futures]
# Workers automatically stopped here

# ⚠️ Or manually call stop()
llm = LLM.options(mode='thread', max_workers=100).init(...)
try:
    futures = [llm.call_llm(prompt) for prompt in prompts]
    responses = [f.result() for f in futures]
finally:
    llm.stop()  # Always clean up!
```

### 3. Handle Errors in Parallel Execution

```python
from concurrent.futures import TimeoutError

# Collect results with error handling
results = []
errors = []

for i, future in enumerate(futures):
    try:
        result = future.result(timeout=30)  # Set reasonable timeout
        results.append(result)
    except TimeoutError:
        errors.append((i, "Timeout"))
    except Exception as e:
        errors.append((i, str(e)))

print(f"Success: {len(results)}, Failed: {len(errors)}")
```

### 4. Use Worker State for Configuration

```python
# ✅ Good: Store configuration in worker state
class LLM(Worker):
    def __init__(self, model: str, temperature: float, max_tokens: int):
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens  # Reused across all calls
    
    def call_llm(self, prompt: str) -> str:
        return litellm.completion(
            model=self.model,
            temperature=self.temperature,
            max_tokens=self.max_tokens
        )

# ❌ Bad: Pass same config every time
class BadLLM(Worker):
    def call_llm(self, prompt: str, model: str, temperature: float) -> str:
        # Wasteful - passing same values repeatedly
        return litellm.completion(...)
```

### 5. Submit All Tasks Before Collecting Results

```python
# ✅ Good: Submit all tasks first, then collect
futures = [llm.call_llm(prompt) for prompt in prompts]  # Fast - just queuing
responses = [f.result() for f in futures]  # Blocks as needed

# ❌ Bad: Submit and wait one at a time
responses = []
for prompt in prompts:
    future = llm.call_llm(prompt)
    response = future.result()  # Blocks immediately - no parallelism!
    responses.append(response)
```

## Adding Production Features

Once you have the basics working, Concurry makes it easy to add production-grade features with minimal code:

### Rate Limiting

Protect your API from rate limit errors by enforcing limits across all workers:

```python
from concurry import Worker, RateLimit, CallLimit

class LLM(Worker):
    def __init__(self, model: str, temperature: float):
        self.model = model
        self.temperature = temperature
    
    def call_llm(self, prompt: str) -> str:
        # Rate limits automatically enforced
        with self.limits.acquire(requested={"tokens": 1000}) as acq:
            response = litellm.completion(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=self.temperature,
            )
            
            # Report actual token usage for accurate limiting
            tokens_used = response.usage.total_tokens
            acq.update(usage={"tokens": tokens_used})
            
            return response.choices[0].message.content

# Create pool with shared rate limits
llm = LLM.options(
    mode='thread',
    max_workers=100,
    limits=[
        CallLimit(window_seconds=60, capacity=500),     # 500 calls/minute
        RateLimit(key="tokens", window_seconds=60, capacity=50_000)  # 50k tokens/min
    ]
).init(model="gpt-4o-mini", temperature=0.1)

# All 100 workers share the same rate limits
futures = [llm.call_llm(prompt) for prompt in prompts]
responses = [f.result() for f in futures]
```

### Automatic Retries

Handle transient errors automatically with exponential backoff:

```python
import openai

llm = LLM.options(
    mode='thread',
    max_workers=100,
    
    # Retry configuration
    num_retries=5,                                      # Try up to 5 times
    retry_algorithm="exponential",                       # Exponential backoff
    retry_wait=1.0,                                      # Start with 1s wait
    retry_on=[openai.RateLimitError, openai.APIConnectionError],  # Which errors to retry
    retry_until=lambda r: len(r) > 10                   # Retry until output is valid
).init(model="gpt-4o-mini", temperature=0.1)

# Automatically retries on rate limits or connection errors
futures = [llm.call_llm(prompt) for prompt in prompts]
responses = [f.result() for f in futures]
```

### Progress Tracking

Add beautiful progress bars to track your batch processing:

```python
from concurry.utils.progress import ProgressBar

# Submit tasks with progress
futures = []
for prompt in ProgressBar(prompts, desc="Submitting"):
    futures.append(llm.call_llm(prompt))

# Collect results with progress
responses = []
for future in ProgressBar(futures, desc="Processing"):
    responses.append(future.result())
```

### All Together: Production-Ready LLM Worker

Combine all features for a robust production system:

```python
from concurry import Worker, RateLimit, CallLimit
from concurry.utils.progress import ProgressBar
import openai
import litellm

class ProductionLLM(Worker):
    def __init__(self, model: str, temperature: float):
        self.model = model
        self.temperature = temperature
    
    def call_llm(self, prompt: str) -> dict:
        """Call LLM with rate limiting and error handling."""
        with self.limits.acquire(requested={"tokens": 2000}) as acq:
            response = litellm.completion(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=self.temperature,
            )
            
            # Report actual usage
            tokens_used = response.usage.total_tokens
            acq.update(usage={"tokens": tokens_used})
            
            return {
                "text": response.choices[0].message.content,
                "tokens": tokens_used
            }

# Production configuration
llm = ProductionLLM.options(
    # Execution
    mode='thread',
    max_workers=100,
    
    # Rate limiting (shared across all workers)
    limits=[
        CallLimit(window_seconds=60, capacity=500),
        RateLimit(key="tokens", window_seconds=60, capacity=50_000)
    ],
    
    # Automatic retries
    num_retries=5,
    retry_algorithm="exponential",
    retry_wait=1.0,
    retry_on=[openai.RateLimitError, openai.APIConnectionError]
).init(model="gpt-4o-mini", temperature=0.1)

# Process with progress tracking
with llm:  # Auto-cleanup with context manager
    futures = [llm.call_llm(p) for p in ProgressBar(prompts, desc="Submitting")]
    responses = [f.result() for f in ProgressBar(futures, desc="Processing")]

print(f"Processed {len(responses)} prompts")
print(f"Total tokens: {sum(r['tokens'] for r in responses)}")
```

**What you get:**
- 🚀 **50x faster** than sequential code
- 🚦 **Rate limiting** prevents API errors
- 🔁 **Automatic retries** on transient failures
- 📊 **Progress tracking** for visibility
- 🧹 **Automatic cleanup** with context managers
- ⚡ **Production-ready** with minimal code

## Next Steps

Now that you understand the basics, continue your journey with:

- [Workers Guide](workers.md) - **Start here** to learn the actor pattern and build stateful concurrent operations
- [Worker Pools Guide](pools.md) - Scale workers with pools and load balancing
- [Limits Guide](limits.md) - Add resource and rate limiting to your workers
- [Retry Mechanisms Guide](retries.md) - Make your workers fault-tolerant with automatic retries
- [Futures Guide](futures.md) - Learn advanced future patterns
- [Progress Guide](progress.md) - Master progress bar customization
- [Examples](../examples.md) - See real-world usage patterns
- [API Reference](../api/index.md) - Detailed API documentation

