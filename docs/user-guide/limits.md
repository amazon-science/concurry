# Limits

Limits in Concurry provide flexible, composable protection for your resources. They allow you to control concurrency (how many things run at once) and rate (how many things run over time).

## The Problem: Overloading Resources

*   **Scenario A**: You spin up 100 threads to call an API, but the API rate limits you to 500 requests/minute. You get 429 errors.
*   **Scenario B**: You have 8 processes for CPU work, but they all try to open a database connection. Your DB only allows 5 connections.

## The Solution: Concurry Limits

Concurry provides a thread-safe, distributed limiting system.

### The Three Layers of Limiting

Understanding these layers helps you build complex configurations.

```text
Layer 1: Data Container (The "What")
[ RateLimit(capacity=100) ]  <-- Just a definition. Not thread-safe.

        |
        v

Layer 2: LimitSet (The "Enforcer")
[ LimitSet(...) ]            <-- The Engine. Manages locks/atomic counters.
                                 Can be In-Memory, Multiprocess, or Ray.

        |
        v

Layer 3: LimitPool (The "Distributor")
[ LimitPool(...) ]           <-- The Load Balancer. Splits load across 
                                 multiple LimitSets (e.g., for Multi-Region).
```

## Quick Start: Common Scenarios

### Scenario 1: The "OpenAI Tier 1" Rate Limit
You want to respect a rate limit of 500 requests per minute (RPM) and 10,000 tokens per minute (TPM).

```python
from concurry import worker, RateLimit, LimitSet

# Define the constraints
limits = LimitSet(
    limits=[
        # 500 requests / 60 seconds
        RateLimit(key="requests", capacity=500, window_seconds=60),
        # 10,000 tokens / 60 seconds
        RateLimit(key="tokens", capacity=10000, window_seconds=60)
    ],
    shared=True,   # Share this across all workers in the pool!
    mode="thread"  # Match your worker mode
)

@worker(mode="thread")
class AIWorker:
    def generate(self, prompt):
        # Acquire 1 request + estimated 100 tokens
        with self.limits.acquire(requested={"requests": 1, "tokens": 100}) as acq:
            response = call_openai(prompt)
            
            # Update with ACTUAL token usage (e.g., maybe it was 120 tokens)
            acq.update(usage={"tokens": response.usage.total_tokens})
            return response

# 10 threads, but they will collectively respect the limits
pool = AIWorker.options(max_workers=10, limits=limits).init()
```

**Why `update()`?**
Rate limits often depend on the *result* (like token usage). Concurry allows you to reserve an estimate and then correct it with the actual value.

### Scenario 2: The Database Connection Pool (Resource Limit)
You have a pool of 20 workers, but your database only supports 5 concurrent connections.

```python
from concurry import worker, ResourceLimit

# ResourceLimit behaves like a Semaphore
db_limit = LimitSet(
    limits=[ResourceLimit(key="db_conn", capacity=5)], 
    shared=True,
    mode="process"
)

@worker(mode="process")
class DBWorker:
    def query(self, sql):
        # Blocks here if 5 workers are already using the DB
        with self.limits.acquire(requested={"db_conn": 1}):
            return run_query(sql)
        # Connection released automatically on exit

# 20 workers, but max 5 running query() at the same time
pool = DBWorker.options(max_workers=20, limits=db_limit).init()
```

## Advanced: Multi-Region Limiting with LimitPool

For high-scale systems, you might have quotas in multiple regions (e.g., AWS US-East and EU-West). A `LimitPool` can load-balance your workers across these independent quotas.

```python
from concurry import worker, LimitPool

# Define independent limits for each region
us_east = LimitSet(
    limits=[RateLimit(key="tpm", capacity=1000, window_seconds=60)],
    config={"region": "us-east-1"}  # Metadata attached to the limit
)

eu_west = LimitSet(
    limits=[RateLimit(key="tpm", capacity=1000, window_seconds=60)],
    config={"region": "eu-west-1"}
    )

# Create a pool of limits
limit_pool = LimitPool(limit_sets=[us_east, eu_west], load_balancing="round_robin")

@worker
class GlobalWorker:
    def process(self):
        # Acquire from the pool. It picks US-East or EU-West automatically.
        with self.limits.acquire(requested={"tpm": 10}) as acq:
            # The config tells us WHICH region we got!
            region = acq.config["region"]
            print(f"Processing in {region}")
            call_api(region=region)

worker = GlobalWorker.options(limits=limit_pool).init()
```

## Best Practices

### 1. Always Share Limits for Pools
If you create a pool of workers, you almost always want `shared=True` in your `LimitSet`. If `shared=False` (default), each worker gets its *own* private quota (e.g., 10 workers * 500 RPM = 5000 RPM total), which is usually not what you want for API limits.

### 2. Match the Mode
The `mode` of your `LimitSet` must match your Worker's mode.
*   Worker `thread` -> LimitSet `thread` (Uses `threading.Lock`)
*   Worker `process` -> LimitSet `process` (Uses `multiprocessing.Manager`)
*   Worker `ray` -> LimitSet `ray` (Uses a Ray Actor)

### 3. Nested Limits
You can nest acquisitions for fine-grained control.

```python
# Acquire DB connection
with self.limits.acquire(requested={"db": 1}):
    data = fetch_data()
    # Now acquire API rate limit
    with self.limits.acquire(requested={"api": 1}):
        send_data(data)
```
This holds the DB connection while waiting for the API limit, which might be inefficient. Prefer acquiring only what you need, when you need it.
