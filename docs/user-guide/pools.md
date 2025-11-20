# Worker Pools

Worker Pools allow you to scale your processing by distributing tasks across multiple worker instances. Instead of managing a single worker, a Pool gives you a single interface to dispatch tasks to `N` workers automatically.

## The Problem: One Worker isn't Enough

If you have a `process` worker doing CPU tasks, it uses 1 CPU core. If your machine has 8 cores, you are wasting 87% of your potential performance.

## The Solution: The Worker Pool

A Pool creates multiple instances of your worker and uses a **Load Balancer** to distribute tasks among them.

### Basic Usage

Just add `max_workers=N` to your options.

!!! tip "Functions vs Classes"
    If you just want to parallelize a simple function (no state), you don't need a class! Use the [`@task` decorator](task-decorator.md) with `max_workers=N` instead.
```python
from concurry import worker

@worker(mode="process")
class CPUWorker:
    def compute(self, x):
        return x * x

# Create a pool of 4 processes
pool = CPUWorker.options(
    max_workers=4
).init()
    
# Submit 100 tasks
# Concurry automatically distributes these 100 tasks across the 4 workers
futures = [pool.compute(i) for i in range(100)]

results = [f.result() for f in futures]
pool.stop()
```

## Load Balancing Strategies

How does the pool decide which worker gets the next task? You can choose the algorithm.

### 1. Round Robin (Default)
Distributes tasks sequentially. Fair and simple.

```text
Task 1 -> Worker A
Task 2 -> Worker B
Task 3 -> Worker C
Task 4 -> Worker A
Task 5 -> Worker B ...
```

**Best For**: Tasks that take roughly the same amount of time.

### 2. Least Active (`load_balancing="active"`)
Sends the task to the worker with the *fewest* tasks currently in its queue.

```text
Worker A: [Task 1 (Busy)]
Worker B: [Idle]          <- Task 2 goes here!
Worker C: [Task 3 (Busy)]
```

**Best For**: Tasks with highly variable duration (e.g., one task takes 1s, another takes 10s). Prevents backing up behind a "slow" task.

### 3. Random (`load_balancing="random"`)
Selects a worker randomly.

**Best For**: High-throughput scenarios where checking queue depth is overhead, or for **On-Demand** pools.

## Queue Mechanics: Non-Blocking by Design

A common confusion is: *"Does `pool.compute()` block if all workers are busy?"*

**Answer: NO.**

Concurry uses a two-layer queuing system to ensure your main thread **never blocks** on submission.

```text
User Code
   |
   v  (1) Submit Task (Instant)
   |
[ Global Pool Dispatcher ]
   |
   +--- (2) Load Balancer selects Worker A ---+
                                              |
                                     [ Worker A Internal Queue ]
                                              |
                                     (3) Only `max_queued_tasks` 
                                         are sent to Backend
                                              |
                                     [ Actual Execution Backend ]
                                     (Thread / Process / Ray Actor)
```

1.  **Submission**: Returns a `Future` immediately.
2.  **Dispatch**: The pool assigns the task to a specific worker instance.
3.  **Backpressure**: Each worker has a `max_queued_tasks` setting (default varies by mode). If Worker A's backend is full, the task sits in Worker A's *internal* queue. It does **not** block your main Python thread.

### Configuring Queues

You can tune the backpressure behavior:

```python
pool = MyWorker.options(
    mode="thread",
    max_workers=5,
    max_queued_tasks=10  # Each worker holds max 10 active tasks in backend
).init()
```

*   **Increase** for tiny, fast tasks (keep workers fed).
*   **Decrease** for huge, memory-intensive tasks (prevent OOM).

## When to use a Pool?

| Scenario | Recommendation | Why? |
| :--- | :--- | :--- |
| **Simple API calls** | `Worker` (Thread mode) | One thread usually handles simple sequential requests fine. |
| **High-Volume API calls** | **`Pool`** (Thread mode) | Need concurrency to saturate network bandwidth. |
| **Heavy Computation** | **`Pool`** (Process mode) | Need multiple cores to speed up total time. |
| **Simple Function** | [`@task`](task-decorator.md) | No need for a custom class if there is no state. |

## On-Demand Pools

Sometimes you don't want workers sitting idle. **On-Demand** pools create workers when a task arrives and kill them when it's done.

```python
# Use @worker(mode="process") or override in options
pool = MyWorker.options(
    mode="process",
    on_demand=True,  # Workers created JIT
    max_workers=10   # Max concurrent workers
).init()
```

**Use Case**: Bursty traffic or expensive resources that shouldn't stay allocated.

## Shared Limits in Pools

One of the most powerful features of Concurry pools is **Shared Limits**. If you have 20 workers but only 5 database connections allowed, you can enforce that globally.

See the [Limits Guide](limits.md) for details.
