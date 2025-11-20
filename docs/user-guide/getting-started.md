# Getting Started

This guide will walk you through Concurry's core concepts by solving a real-world problem: **making batch LLM calls 50x faster**.

## The Problem: Sequential Code is Slow

Imagine you have 1,000 prompts to send to an LLM API. A standard Python loop processes them one by one. If each call takes 1 second, your script takes **~16 minutes** to run.

### ❌ The Slow Way (Sequential)

```python
import time
from tqdm import tqdm

# Mock LLM client for demonstration
class LLMClient:
    def completion(self, prompt: str) -> str:
        time.sleep(0.8)  # Simulate network latency
        return f"Response to: {prompt}"

client = LLMClient()
prompts = [f"Prompt {i}" for i in range(100)]

# Sequential execution: CPU sits idle 99% of the time waiting for network
results = []
for prompt in tqdm(prompts, desc="Sequential"):
    results.append(client.completion(prompt))

# Total time: ~80 seconds
```

**Why is this bad?** Your CPU is doing nothing while waiting for the API to respond. We need **concurrency** to send multiple requests at once.

---

## The Solution: Concurry Workers

With Concurry, we can turn this into a parallel pipeline with minimal changes. We wrap our logic in a `Worker` and run it in a **Thread Pool**.

### ✅ The Fast Way (Concurrent)

```python
from concurry import worker
from tqdm import tqdm
import time

# 1. Decorate your class with @worker
@worker(mode="thread")
class LLMWorker:
    def completion(self, prompt: str) -> str:
        time.sleep(0.8)  # Simulate network latency
        return f"Response to: {prompt}"

# 2. Initialize a POOL of workers
# 'thread' mode is perfect for I/O tasks like API calls
pool = LLMWorker.options(
    max_workers=20  # Run 20 requests at once
).init()

prompts = [f"Prompt {i}" for i in range(100)]

# 3. Submit tasks (Returns "Futures" instantly)
# This loop finishes in milliseconds because it's just queuing work
futures = [pool.completion(prompt) for prompt in prompts]

# 4. Collect results (Waits for completion)
results = [f.result() for f in tqdm(futures, desc="Concurrent")]

# Total time: ~4 seconds (20x speedup!)
pool.stop()
```

### What Just Happened?

1.  **Worker Definition**: We defined `LLMWorker` using the `@worker` decorator. In Concurry, a **Worker** is an independent "actor" that runs in its own context (thread, process, or remote machine).
2.  **Pool Creation**: `max_workers=20` spun up 20 threads.
3.  **Submission**: Calling `pool.completion()` didn't block. It instantly returned a **Future** (a promise of a result).
4.  **Collection**: `f.result()` blocked only until that specific task was done. Since 20 ran at once, the total time was slashed.

---

## Mental Model: The Actor Pattern

To use Concurry effectively, think in terms of **Actors**:

*   **Stateful**: Unlike simple functions, a Worker can hold state (database connections, loaded models) that persists across calls.
*   **Isolated**: Each worker runs independently. A crash in one doesn't necessarily crash your main program.
*   **Message Passing**: When you call `worker.method()`, you aren't running code immediately. You are sending a **message** to the worker's queue. The worker picks it up, processes it, and puts the result in the **Future**.

```text
[Your Code]  --message (args)-->  [Queue]  -->  [Worker]
     ^                                             |
     |                                             |
     └-------------<-- result via Future --<-------┘
```

---

## Installation

```bash
pip install concurry
```

For distributed computing with Ray support:

```bash
pip install "concurry[ray]"
```

---

## Roadmap: Choose Your Adventure

Now that you've seen the basics, where should you go next?

| I want to... | Go to... |
| :--- | :--- |
| **Understand how Workers operate** (Thread vs Process) | [**Workers Guide**](workers.md) |
| **Scale to thousands of tasks** using Pools | [**Worker Pools**](pools.md) |
| **Prevent API rate limit errors** | [**Limits Guide**](limits.md) |
| **Handle crashes and network glitches** | [**Retries Guide**](retries.md) |
| **Learn about the Unified Future** | [**Futures Guide**](futures.md) |

### Quick Recipes

**Heavy CPU Calculation?**
Use `mode="process"` to bypass the Python GIL.
```python
@worker(mode="process")
class MathWorker: ...

w = MathWorker.options(max_workers=4).init()
```

**Strict API Limits?**
Add a shared Rate Limit.
```python
from concurry import worker, RateLimit

@worker(mode="thread")
class APIWorker: ...

pool = APIWorker.options(
    limits=[RateLimit(key="api", capacity=5, window_seconds=1)]
).init()
```

**Running on a Cluster?**
Just switch the mode.
```python
@worker(mode="ray")
class DataWorker: ...

w = DataWorker.init()
```
