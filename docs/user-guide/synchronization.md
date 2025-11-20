# Synchronization

**Orchestrate chaos. Coordinate thousands of concurrent futures with simple, powerful primitives.**

## The Problem: "Herding Cats"

Launching 1000 tasks is easy. Managing them is hard.
*   How do you know when they are all done?
*   What if you only care about the *first* one to finish?
*   What if one fails? Do you stop everything or keep going?

## The Solution: Two Powerful Primitives

Concurry gives you two main tools: `wait()` and `gather()`.

### Decision Matrix: Which one do I use?

| I want to... | Use... | Why? |
| :--- | :--- | :--- |
| **Collect all results** in order | `gather(futures)` | Simple list of results `[r1, r2, r3]`. |
| **Process results as they finish** | `gather(futures, iter=True)` | Streaming iterator. Low memory usage. |
| **Race multiple tasks** | `wait(futures, return_when="FIRST_COMPLETED")` | Returns as soon as *one* wins. |
| **Handle failures immediately** | `wait(futures, return_when="FIRST_EXCEPTION")` | Fail-fast orchestration. |

---

## Quick Start

```python
from concurry import worker, wait, ReturnWhen

@worker(mode="thread")
class DataWorker:
    def fetch_data(self, id: int) -> dict:
        return {"id": id, "data": f"result_{id}"}

# Create worker and submit tasks
worker = DataWorker.init()
futures = [worker.fetch_data(i) for i in range(10)]

# Wait for all to complete
done, not_done = wait(futures, timeout=30.0)

print(f"Completed: {len(done)}, Pending: {len(not_done)}")
worker.stop()
```

---

## Pattern 1: The "Batch" (Gather)
You have a list of tasks and you want all the answers.

```python
# 1. Launch tasks
futures = [worker.calculate(i) for i in range(100)]

# 2. Wait for all of them
results = gather(futures, progress=True)

# results is [0, 2, 4, ...]
```

### Dictionary Support
If you lose track of which result belongs to which task, use a dictionary!

```python
tasks = {
    "users": worker.get_users(),
    "posts": worker.get_posts(),
    "comments": worker.get_comments()
}

# Results preserve keys
results = gather(tasks)
print(results["users"])
```

---

## Pattern 2: The "Stream" (Iterator)
Waiting for *all* 1000 tasks to finish before processing anything is slow. Use `iter=True` to process them as they complete.

```python
futures = [worker.slow_task(i) for i in range(1000)]

# Yields results as soon as they arrive!
for i, result in gather(futures, iter=True):
    save_to_db(result)
```

**Why this delights users:** It makes your application feel faster and uses less memory.

---

## Pattern 3: The "Race" (Wait)
You want to query 3 mirrors and use the fastest one.

```python
from concurry import wait, ReturnWhen

sources = [mirror1.get(), mirror2.get(), mirror3.get()]

# Return as soon as ONE finishes
done, not_done = wait(
    sources, 
    return_when=ReturnWhen.FIRST_COMPLETED
)

fastest_result = done.pop().result()

# Optional: Cancel the losers
for f in not_done:
    f.cancel()
```

---

## Async Support
If you are using `async def` functions, use the async-native versions:

```python
from concurry import async_gather

async def main():
    # Works exactly like gather(), but awaitable
    results = await async_gather(futures)
```

## Deep Dive: Polling Strategies
By default, Concurry uses **Adaptive Polling**. It checks frequently at first, then backs off if tasks are slow, then speeds up again when they start finishing.

You can tune this in `global_config`, but the default "Just Works" for 99% of cases.
