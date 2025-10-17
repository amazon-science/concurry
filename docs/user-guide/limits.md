# Limits

Limits in concurry provide flexible, composable resource protection and rate limiting. They enable you to control resource usage, enforce rate limits, and track consumption across different dimensions simultaneously.

## Overview

The limit system has three layers:

### Layer 1: Limit Definitions (Data Containers)

1. **RateLimit** - Time-based rate limiting with multiple algorithms
2. **CallLimit** - Call counting (special case of RateLimit)
3. **ResourceLimit** - Semaphore-based resource limiting

**Important**: `Limit` objects are simple data containers that define constraints. They are **NOT thread-safe** and cannot be acquired directly.

### Layer 2: LimitSet (Thread-Safe Executor)

**LimitSet** is a factory function that creates thread-safe limit executors. It handles:
- Thread-safe acquisition and release
- Atomic multi-limit acquisition
- Partial acquisition (nested patterns)
- Backend selection based on execution mode
- Optional `config` dict for metadata (e.g., region, account)

**LimitSet returns**:
- `InMemorySharedLimitSet` - For sync, thread, asyncio (uses `threading.Lock`)
- `MultiprocessSharedLimitSet` - For process mode (uses `multiprocessing.Manager`)
- `RaySharedLimitSet` - For Ray mode (uses Ray actor)

### Layer 3: LimitPool (Multi-LimitSet Load Balancing)

**LimitPool** aggregates multiple independent `LimitSets` with load balancing. It provides:
- **Reduced contention** - Workers acquire from different `LimitSets`
- **Higher throughput** - More concurrent acquisitions without blocking
- **Multi-region support** - Each `LimitSet` can represent a different region/account
- **Scalability** - Add more `LimitSets` to increase capacity

**Use cases**:
- Multi-region API endpoints
- Multiple API accounts/keys
- Service tiers (premium vs. standard)
- High-contention workload distribution

## Quick Reference

### Basic Pattern

```python
from concurry import LimitSet, RateLimit, CallLimit, ResourceLimit, RateLimitAlgorithm

# 1. Define limits (data containers)
limits = LimitSet(limits=[
    CallLimit(window_seconds=60, capacity=100),
    RateLimit(key="tokens", window_seconds=60, capacity=1000),
    ResourceLimit(key="connections", capacity=10)
])

# 2. Acquire limits (thread-safe)
with limits.acquire(requested={"tokens": 50, "connections": 2}) as acq:
    result = do_work()
    # 3. Update RateLimit usage
    acq.update(usage={"tokens": result.actual_tokens})
    # CallLimit and ResourceLimit auto-handled
```

### Key Behaviors

| Feature | Behavior |
|---------|----------|
| **Limit objects** | Data containers only, NOT thread-safe |
| **LimitSet** | Factory function, creates thread-safe executor |
| **LimitPool** | Aggregates multiple LimitSets with load balancing |
| **Config** | Each LimitSet can have metadata accessible via acquisition |
| **Empty LimitSet** | Workers always have `self.limits`, even without configuration |
| **No limits** | Empty LimitSet always allows acquisition, zero overhead |
| **CallLimit** | Always acquired with default of 1, no update needed |
| **ResourceLimit** | Always acquired with default if not specified, no update needed |
| **RateLimit** | Must be in `requested` dict, requires `update()` call |
| **Partial acquisition** | Specify only what you need, CallLimit/ResourceLimit auto-included |
| **Nested acquisition** | Supported, enables fine-grained resource management |
| **Shared limits** | `shared=True` (default) shares limits across workers |
| **Mode matching** | `mode` parameter must match worker execution mode |

## Basic Usage

### Creating a LimitSet

Always use `LimitSet` to create thread-safe limit executors:

```python
from concurry import LimitSet, RateLimit, RateLimitAlgorithm

# Define limit constraints (data containers)
rate_limit = RateLimit(
    key="api_tokens",
    window_seconds=60,
    algorithm=RateLimitAlgorithm.TokenBucket,
    capacity=1000
)

# Create thread-safe LimitSet
limits = LimitSet(
    limits=[rate_limit],
    shared=True,  # Default: share across workers
    mode="sync"   # Default: for sync/thread/asyncio
)

# Acquire and use tokens (thread-safe)
with limits.acquire(requested={"api_tokens": 100}) as acq:
    result = call_api()
    # Update with actual usage
    acq.update(usage={"api_tokens": result.actual_tokens})
```

**Key points:**
- `Limit` objects are data containers - use `LimitSet` for thread-safe operations
- `LimitSet` is a factory that creates appropriate backend implementations
- Always call `acq.update()` for RateLimits to report actual usage
- Unused tokens may be refunded (algorithm-specific)
- Usage must not exceed requested amount

### Empty LimitSet (No Limits)

Workers **always** have `self.limits` available, even when no limits are configured. If you create a worker without passing limits, it automatically gets an empty LimitSet that always allows acquisition without blocking.

```python
from concurry import Worker

class APIWorker(Worker):
    def __init__(self):
        pass
    
    def process(self, data):
        # self.limits is always available
        with self.limits.acquire():
            # Always succeeds immediately, no blocking
            return do_work(data)

# Worker without limits - self.limits.acquire() always succeeds
worker = APIWorker.options(mode="thread").init()
result = worker.process(data).result()
worker.stop()
```

**Key benefits:**
- Write code once, conditionally enforce limits
- No need to check `if self.limits is not None`
- Zero overhead when no limits configured
- Enables gradual adoption of limits

**Creating empty LimitSet directly:**

```python
from concurry import LimitSet

# Create empty LimitSet - always allows acquisition
empty_limits = LimitSet(limits=[], shared=False, mode="sync")

with empty_limits.acquire():
    # Always succeeds immediately
    do_work()
```

**Use cases:**
- Development/testing without limit enforcement
- Conditional limit enforcement based on environment
- Gradual rollout of rate limiting
- Code that optionally uses limits

### RateLimit

RateLimits enforce time-based constraints on resource usage, such as API tokens, bandwidth, or request rates.

```python
from concurry import LimitSet, RateLimit, RateLimitAlgorithm

# Define rate limit
rate_limit = RateLimit(
    key="api_tokens",
    window_seconds=60,
    algorithm=RateLimitAlgorithm.TokenBucket,
    capacity=1000
)

# Create LimitSet for thread-safe usage
limits = LimitSet(limits=[rate_limit])

# Use it
with limits.acquire(requested={"api_tokens": 100}) as acq:
    result = call_api()
    acq.update(usage={"api_tokens": result.actual_tokens})
```

### CallLimit

CallLimit is a special RateLimit for counting calls, where usage is always 1.

```python
from concurry import LimitSet, CallLimit, RateLimitAlgorithm

# Define call limit
call_limit = CallLimit(
    window_seconds=60,
    algorithm=RateLimitAlgorithm.SlidingWindow,
    capacity=100
)

# Create LimitSet
limits = LimitSet(limits=[call_limit])

# Each acquisition counts as 1 call (automatic)
with limits.acquire():
    make_api_call()
```

**Key points:**
- Fixed key: `"call_count"`
- Usage is always 1 (validated)
- No need to call `update()` - handled automatically
- Perfect for call rate limits independent of resource usage

### ResourceLimit

ResourceLimits provide simple counting for finite resources like database connections or file handles.

```python
from concurry import LimitSet, ResourceLimit

# Define resource limit
resource_limit = ResourceLimit(
    key="db_connections",
    capacity=10
)

# Create LimitSet
limits = LimitSet(limits=[resource_limit])

# Acquire 2 connections
with limits.acquire(requested={"db_connections": 2}):
    conn1 = get_connection()
    conn2 = get_connection()
    execute_queries(conn1, conn2)
# Connections automatically released
```

**Key points:**
- No time component (unlike RateLimit)
- Automatic release on context exit
- No need to call `update()` - handled automatically
- Thread-safe semaphore logic handled by LimitSet

## Rate Limiting Algorithms

RateLimit supports five algorithms with different characteristics:

### TokenBucket

Allows bursts up to capacity while maintaining average rate. Tokens refill continuously.

```python
limit = RateLimit(
    key="tokens",
    window_seconds=60,
    algorithm=RateLimitAlgorithm.TokenBucket,
    capacity=1000
)
```

**Best for:** APIs that allow occasional bursts

**Characteristics:**
- Burst handling: Excellent
- Precision: Good
- Memory: Low
- Refunding: Yes

### LeakyBucket

Processes requests at fixed rate, smoothing traffic.

```python
limit = RateLimit(
    key="tokens",
    window_seconds=60,
    algorithm=RateLimitAlgorithm.LeakyBucket,
    capacity=1000
)
```

**Best for:** Predictable, steady-state traffic

**Characteristics:**
- Burst handling: Poor (by design)
- Precision: Excellent
- Memory: Low
- Refunding: No

### SlidingWindow

Precise rate limiting with rolling time window. More accurate than fixed window.

```python
limit = RateLimit(
    key="tokens",
    window_seconds=60,
    algorithm=RateLimitAlgorithm.SlidingWindow,
    capacity=1000
)
```

**Best for:** Precise rate limiting without fixed window edge cases

**Characteristics:**
- Burst handling: Good
- Precision: Excellent
- Memory: Higher (stores timestamps)
- Refunding: No

### FixedWindow

Simple rate limiting with fixed time buckets. Fast but can allow 2x burst at window boundaries.

```python
limit = RateLimit(
    key="tokens",
    window_seconds=60,
    algorithm=RateLimitAlgorithm.FixedWindow,
    capacity=1000
)
```

**Best for:** Simple rate limiting where edge cases are acceptable

**Characteristics:**
- Burst handling: Poor (2x burst at boundaries)
- Precision: Moderate
- Memory: Lowest
- Refunding: No

### GCRA (Generic Cell Rate Algorithm)

Most precise rate limiting using theoretical arrival time tracking.

```python
limit = RateLimit(
    key="tokens",
    window_seconds=60,
    algorithm=RateLimitAlgorithm.GCRA,
    capacity=1000
)
```

**Best for:** Strict rate control with precise timing

**Characteristics:**
- Burst handling: Excellent
- Precision: Best
- Memory: Low
- Refunding: Yes

## LimitSet: Multi-Dimensional Limiting

LimitSet enables atomic acquisition of multiple limits simultaneously with full thread-safety.

### Basic Multi-Dimensional Limiting

```python
from concurry import (
    LimitSet, RateLimit, CallLimit, ResourceLimit,
    RateLimitAlgorithm
)

# Create LimitSet with multiple limit types
limits = LimitSet(limits=[
    CallLimit(
        window_seconds=60,
        algorithm=RateLimitAlgorithm.SlidingWindow,
        capacity=100
    ),
    RateLimit(
        key="input_tokens",
        window_seconds=60,
        algorithm=RateLimitAlgorithm.GCRA,
        capacity=10_000
    ),
    RateLimit(
        key="output_tokens",
        window_seconds=60,
        algorithm=RateLimitAlgorithm.TokenBucket,
        capacity=1_000
    ),
    ResourceLimit(
        key="db_connections",
        capacity=10
    )
])

# Acquire specific limits atomically
# CallLimit is automatically acquired with default of 1
with limits.acquire(requested={
    "input_tokens": 500,
    "output_tokens": 50,
    "db_connections": 2
}) as acq:
    result = process_data()
    
    # Update RateLimits with actual usage
    acq.update(usage={
        "input_tokens": result.input_used,
        "output_tokens": result.output_used
    })
    # CallLimit and ResourceLimit handled automatically
```

**Key behavior:**
- When `requested` is specified, CallLimit and ResourceLimit are **automatically included** with default of 1
- RateLimits must be explicitly specified in `requested`
- All limits are acquired atomically (all-or-nothing)

### Nested Acquisition Pattern

LimitSet supports **partial acquisition**, enabling powerful nested patterns:

```python
# Level 1: Acquire long-lived resources
with limits.acquire(requested={"db_connections": 2}):
    # Do setup with connections
    
    # Level 2: Acquire rate limits for operations
    # Note: CallLimit still automatically acquired here
    with limits.acquire(requested={
        "input_tokens": 100,
        "output_tokens": 50
    }) as rate_acq:
        result = call_api()
        rate_acq.update(usage={
            "input_tokens": result.input_used,
            "output_tokens": result.output_used
        })
    
    # Connections still held here, but tokens released
    
    # Another rate-limited operation
    with limits.acquire(requested={
        "input_tokens": 200,
        "output_tokens": 20
    }) as rate_acq2:
        result2 = call_api()
        rate_acq2.update(usage={
            "input_tokens": result2.input_used,
            "output_tokens": result2.output_used
        })
    
    # Connections released at end of outer context
```

**Benefits of nested acquisition:**
- Hold resources only as long as needed
- Reduces resource contention
- More efficient limit utilization
- Better granular control

### Non-Blocking try_acquire

```python
acq = limits.try_acquire(requested={
    "input_tokens": 1000,
    "db_connections": 1
})

if acq.successful:
    with acq:
        # All limits acquired
        result = expensive_operation()
        acq.update(usage={"input_tokens": result.tokens})
else:
    # Could not acquire all limits immediately
    print("Resources not available, will retry later")
```

## LimitPool and Config for Multi-Region/Multi-Account Scenarios

For high-scale production scenarios with multiple API endpoints (e.g., different AWS regions, multiple accounts, or different service tiers), **LimitPool** provides a powerful way to distribute load across multiple independent `LimitSets` while reducing contention.

### Config Parameter on LimitSet

Each `LimitSet` can have an associated `config` dictionary containing metadata that's accessible via the acquisition object. This is useful for passing contextual information (like region, account ID, or API endpoint) when making external API calls.

```python
from concurry import LimitSet, RateLimit, RateLimitAlgorithm

# Create LimitSet with config metadata
us_east_limits = LimitSet(
    limits=[
        RateLimit(key="tokens", window_seconds=60, capacity=1000)
    ],
    shared=True,
    mode="thread",
    config={"region": "us-east-1", "account": "12345"}
)

# Access config during acquisition
with us_east_limits.acquire(requested={"tokens": 100}) as acq:
    # Get region from config
    region = acq.config["region"]
    account = acq.config["account"]
    
    # Make API call to specific region/account
    result = call_api(region=region, account=account)
    
    # Update usage
    acq.update(usage={"tokens": result.tokens_used})
```

**Key Features:**
- Config is immutable after acquisition (copies are made)
- Empty dict by default (no config needed for simple cases)
- Accessible via `acquisition.config`
- Perfect for multi-region/multi-account routing

### LimitPool: High-Performance Load Balancing

`LimitPool` aggregates multiple independent `LimitSets` and uses load balancing to distribute acquisitions across them. This dramatically reduces contention when many workers are competing for the same limits.

**Benefits:**
- **Reduced Contention**: Workers acquire from different `LimitSets` in round-robin fashion
- **Higher Throughput**: More concurrent acquisitions without blocking
- **Multi-Region Support**: Each `LimitSet` can represent a different region/account
- **Scalability**: Add more `LimitSets` to increase capacity

```python
from concurry import LimitSet, LimitPool, RateLimit, RateLimitAlgorithm

# Create multiple independent LimitSets with different configs
us_east = LimitSet(
    limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
    shared=True,
    mode="thread",
    config={"region": "us-east-1", "endpoint": "https://api-us-east-1.example.com"}
)

us_west = LimitSet(
    limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
    shared=True,
    mode="thread",
    config={"region": "us-west-2", "endpoint": "https://api-us-west-2.example.com"}
)

eu_west = LimitSet(
    limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
    shared=True,
    mode="thread",
    config={"region": "eu-west-1", "endpoint": "https://api-eu-west-1.example.com"}
)

# Create LimitPool with load balancing
pool = LimitPool(
    limit_sets=[us_east, us_west, eu_west],
    load_balancing="round_robin",
    worker_index=0  # Starting offset for round-robin
)

# Acquire from pool - automatically selects a LimitSet
with pool.acquire(requested={"tokens": 100}) as acq:
    # Get region and endpoint from selected LimitSet's config
    region = acq.config["region"]
    endpoint = acq.config["endpoint"]
    
    # Make API call to the selected region
    result = requests.post(endpoint, json={"prompt": "Hello"})
    
    # Update usage
    acq.update(usage={"tokens": result.json()["tokens_used"]})
```

### Load Balancing Strategies

`LimitPool` supports two load balancing algorithms:

**1. Round-Robin (Default)**
- Workers select `LimitSets` in sequential order
- Each worker starts at a different offset to minimize overlap
- Best for persistent worker pools
- Provides even distribution across all `LimitSets`

```python
from concurry import LimitPool

pool = LimitPool(
    limit_sets=[limitset1, limitset2, limitset3],
    load_balancing="round_robin",  # Default
    worker_index=0  # Worker's starting offset
)
```

**2. Random**
- Randomly selects a `LimitSet` for each acquisition
- Best for on-demand workers or bursty workloads
- Provides good distribution with minimal coordination

```python
pool = LimitPool(
    limit_sets=[limitset1, limitset2, limitset3],
    load_balancing="random",
    worker_index=0  # Not used for random
)
```

### Worker Integration with LimitPool

Workers automatically handle `LimitPool` through `self.limits`. When you pass a `LimitPool` to a worker, it's used seamlessly for all limit acquisitions.

```python
from concurry import Worker, LimitSet, LimitPool, RateLimit, RateLimitAlgorithm

# Create multiple LimitSets for different regions
limitsets = []
for region, endpoint in [
    ("us-east-1", "https://api-us-east-1.example.com"),
    ("us-west-2", "https://api-us-west-2.example.com"),
    ("eu-west-1", "https://api-eu-west-1.example.com"),
]:
    ls = LimitSet(
        limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
        shared=True,
        mode="thread",
        config={"region": region, "endpoint": endpoint}
    )
    limitsets.append(ls)

# Create LimitPool
limit_pool = LimitPool(
    limit_sets=limitsets,
    load_balancing="round_robin",
    worker_index=0
)

class APIWorker(Worker):
    def __init__(self):
        pass
    
    def call_api(self, prompt: str):
        # Acquire from pool - automatically selects a region
        with self.limits.acquire(requested={"tokens": 100}) as acq:
            # Get region and endpoint from selected LimitSet's config
            region = acq.config["region"]
            endpoint = acq.config["endpoint"]
            
            # Make API call
            result = requests.post(
                endpoint,
                json={"prompt": prompt, "max_tokens": 100}
            )
            
            # Update actual usage
            acq.update(usage={"tokens": result.json()["tokens_used"]})
            
            return {"region": region, "response": result.json()["text"]}

# Create worker with LimitPool
worker = APIWorker.options(mode="thread", limits=limit_pool).init()

# Each call automatically load-balances across regions
result1 = worker.call_api("Hello").result()  # Might use us-east-1
result2 = worker.call_api("World").result()  # Might use us-west-2
result3 = worker.call_api("AI").result()     # Might use eu-west-1

worker.stop()
```

### Worker Pools with LimitPool

When using worker pools, each worker gets its own `LimitPool` instance with a unique `worker_index` for proper round-robin distribution:

```python
from concurry import Worker, LimitSet, LimitPool, RateLimit

# Create multiple LimitSets (one per region)
limitsets = [
    LimitSet(
        limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
        shared=True,
        mode="thread",
        config={"region": f"region-{i}", "account": f"account-{i}"}
    )
    for i in range(5)  # 5 regions
]

# Pass list of LimitSets directly - workers get LimitPool automatically
pool = APIWorker.options(
    mode="thread",
    max_workers=10,
    limits=limitsets  # List of LimitSets creates LimitPool per worker
).init()

# Each worker has its own LimitPool with proper worker_index offset
# Worker 0: starts at index 0, 5, 10, ...
# Worker 1: starts at index 1, 6, 11, ...
# Worker 2: starts at index 2, 7, 12, ...
# ... minimizes contention through staggered starting points

futures = [pool.call_api(f"prompt-{i}") for i in range(100)]
results = [f.result() for f in futures]

pool.stop()
```

### Use Cases for LimitPool

**1. Multi-Region API Access**

Distribute load across multiple regional API endpoints to maximize throughput and reduce latency:

```python
regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
limitsets = [
    LimitSet(
        limits=[RateLimit(key="tokens", window_seconds=60, capacity=10000)],
        shared=True,
        mode="thread",
        config={"region": region, "endpoint": f"https://api-{region}.example.com"}
    )
    for region in regions
]

pool = LimitPool(limit_sets=limitsets, load_balancing="round_robin")
```

**2. Multiple API Accounts**

Use separate accounts to increase overall rate limits:

```python
accounts = ["account-1", "account-2", "account-3"]
limitsets = [
    LimitSet(
        limits=[RateLimit(key="requests", window_seconds=60, capacity=1000)],
        shared=True,
        mode="thread",
        config={"account": account, "api_key": get_api_key(account)}
    )
    for account in accounts
]

pool = LimitPool(limit_sets=limitsets, load_balancing="random")
```

**3. Service Tiers**

Differentiate between premium and standard tiers:

```python
premium = LimitSet(
    limits=[RateLimit(key="tokens", window_seconds=60, capacity=10000)],
    shared=True,
    mode="thread",
    config={"tier": "premium", "priority": "high"}
)

standard = LimitSet(
    limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
    shared=True,
    mode="thread",
    config={"tier": "standard", "priority": "normal"}
)

pool = LimitPool(limit_sets=[premium, standard], load_balancing="round_robin")
```

**4. High-Contention Workloads**

Reduce lock contention by splitting a single limit pool into multiple independent pools:

```python
# Instead of 1 LimitSet with 10,000 capacity (high contention)
# Use 10 LimitSets with 1,000 capacity each (low contention per LimitSet)
limitsets = [
    LimitSet(
        limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
        shared=True,
        mode="thread",
        config={"pool_id": i}
    )
    for i in range(10)
]

pool = LimitPool(limit_sets=limitsets, load_balancing="round_robin")
# Workers distribute acquisitions across 10 independent LimitSets
# Dramatically reduces lock contention for high-throughput scenarios
```

**5. Real-World Example: AWS Bedrock Multi-Region, Multi-Account Claude 3.7 Sonnet**

This comprehensive example demonstrates using `LimitPool` with AWS Bedrock's Claude 3.7 Sonnet across multiple regions and accounts, with realistic rate limits based on AWS quotas:

```python
import boto3
import base64
import json
from concurry import Worker, LimitSet, LimitPool, RateLimit, CallLimit, RateLimitAlgorithm

# AWS Bedrock default quotas for Claude 3.7 (as of 2025)
# - Input tokens per minute (TPM): 400,000
# - Output tokens per minute (TPM): 100,000
# - Requests per minute (RPM): 500

# Configuration for multiple AWS accounts and regions
# Each region has a different model ID for Claude 3.7 Sonnet
bedrock_configs = [
    # Region 1: US East (N. Virginia), 2 accounts
    {
        "region": "us-east-1",
        "account_id": "123456789012",
        "model_id": "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
        "profile": "account1",
    },
    {
        "region": "us-east-1",
        "account_id": "234567890123",
        "model_id": "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
        "profile": "account2",
    },
    # Region 2: Europe (Paris), 2 accounts
    {
        "region": "eu-west-3",
        "account_id": "123456789012",
        "model_id": "eu.anthropic.claude-3-7-sonnet-20250219-v1:0",
        "profile": "account1",
    },
    {
        "region": "eu-west-3",
        "account_id": "234567890123",
        "model_id": "eu.anthropic.claude-3-7-sonnet-20250219-v1:0",
        "profile": "account2",
    },
    # Region 3: Asia Pacific (Mumbai), 2 accounts
    {
        "region": "ap-south-1",
        "account_id": "123456789012",
        "model_id": "apac.anthropic.claude-3-7-sonnet-20250219-v1:0",
        "profile": "account1",
    },
    {
        "region": "ap-south-1",
        "account_id": "234567890123",
        "model_id": "apac.anthropic.claude-3-7-sonnet-20250219-v1:0",
        "profile": "account2",
    },
]

# Create LimitSets for each account/region combination
# Each LimitSet enforces AWS Bedrock's default quotas
limitsets = []
for config in bedrock_configs:
    limitset = LimitSet(
        limits=[
            # AWS Bedrock: 500 requests per minute per account/region
            CallLimit(
                window_seconds=60,
                algorithm=RateLimitAlgorithm.SlidingWindow,
                capacity=500
            ),
            # AWS Bedrock: 400,000 input tokens per minute
            RateLimit(
                key="input_tokens",
                window_seconds=60,
                algorithm=RateLimitAlgorithm.TokenBucket,
                capacity=400_000
            ),
            # AWS Bedrock: 100,000 output tokens per minute
            RateLimit(
                key="output_tokens",
                window_seconds=60,
                algorithm=RateLimitAlgorithm.TokenBucket,
                capacity=100_000
            ),
        ],
        shared=True,
        mode="thread",
        config=config,  # Store region, account, and model info
    )
    limitsets.append(limitset)

# Create LimitPool to distribute load across all account/region combinations
limit_pool = LimitPool(
    limit_sets=limitsets,
    load_balancing="round_robin",
    worker_index=0
)

## Create worker which invokes Claude 3.7 Sonnet with vision capabilities:
class ClaudeWorker(Worker):
    """Worker for calling Claude 3.7 Sonnet on AWS Bedrock with images."""
    
    def __init__(self):
        # Bedrock clients will be created per-request based on config
        self._clients = {}
    
    def _get_client(self, region: str, profile: str):
        """Get or create a Bedrock client for the given region and profile."""
        key = (region, profile)
        if key not in self._clients:
            session = boto3.Session(profile_name=profile)
            self._clients[key] = session.client(
                service_name="bedrock-runtime",
                region_name=region
            )
        return self._clients[key]
    
    def analyze_image(
        self,
        image_path: str,
        prompt: str,
        max_tokens: int = 1024
    ) -> dict:
        """Analyze an image using Claude 3.7 Sonnet with vision capabilities.
        
        Args:
            image_path: Path to the image file
            prompt: Text prompt for image analysis
            max_tokens: Maximum output tokens
            
        Returns:
            Dictionary with analysis results and metadata
        """
        # Acquire limits from the pool
        # LimitPool automatically selects an account/region using round-robin
        with self.limits.acquire(requested={
            "input_tokens": 10_000,  # Estimate for prompt + image
            "output_tokens": max_tokens
        }) as acq:
            # Get config from the selected LimitSet
            config = acq.config
            region = config["region"]
            account_id = config["account_id"]
            model_id = config["model_id"]
            profile = config["profile"]
            
            # Get Bedrock client for this region/account
            client = self._get_client(region, profile)
            
            # Read and encode image
            with open(image_path, "rb") as f:
                image_bytes = f.read()
            
            # Determine image format
            import mimetypes
            mime_type, _ = mimetypes.guess_type(image_path)
            if mime_type not in ["image/jpeg", "image/png", "image/gif", "image/webp"]:
                mime_type = "image/jpeg"  # Default
            
            # Construct request for Claude 3.7 with vision
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": mime_type,
                                    "data": base64.b64encode(image_bytes).decode("utf-8")
                                }
                            },
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ]
                    }
                ]
            }
            
            # Invoke Claude 3.7 Sonnet
            try:
                response = client.invoke_model(
                    modelId=model_id,
                    body=json.dumps(request_body)
                )
                
                # Parse response
                response_body = json.loads(response["body"].read())
                
                # Extract token usage from response
                chars_per_token = 4  # Rough estimate
                usage = response_body.get("usage", {})
                input_tokens_used = usage.get("input_tokens", 10_000)  # Fallback estimate
                output_tokens_used = usage.get("output_tokens", len(response_body.get("content", [{}])[0].get("text", "")) // chars_per_token)
                
                # Update actual token usage
                acq.update(usage={
                    "input_tokens": input_tokens_used,
                    "output_tokens": output_tokens_used
                })
                
                # Return results with metadata
                return {
                    "text": response_body["content"][0]["text"],
                    "region": region,
                    "account_id": account_id,
                    "model_id": model_id,
                    "input_tokens": input_tokens_used,
                    "output_tokens": output_tokens_used,
                    "stop_reason": response_body.get("stop_reason"),
                }
                
            except Exception as e:
                # If error, still update usage with estimates to avoid deadlock
                acq.update(usage={
                    "input_tokens": 10_000,
                    "output_tokens": 0
                })
                raise e

# Create worker pool with LimitPool
# 10 workers will distribute load across 6 account/region combinations
with ClaudeWorker.options(
    mode="thread",
    max_workers=1000,
    limits=limit_pool,
    num_retries=3,
    retry_on=[Exception],  # Retry on any Bedrock errors
    retry_algorithm="exponential",
    retry_wait=1.0,
).init() as pool:
    # Process multiple images concurrently
    images = [
        ("image1.jpg", "Describe this image in detail"),
        ("image2.png", "What objects do you see?"),
        ("image3.jpg", "Analyze the composition"),
        # ... more images
    ]
    
    # Submit all tasks
    futures = [
        pool.analyze_image(image_path, prompt)
        for image_path, prompt in images
    ]
    
    # Collect results
    results = [f.result() for f in futures]
    
    # Results show which region/account was used for each request
    for i, result in enumerate(results):
        print(f"Image {i+1}:")
        print(f"  Region: {result['region']}")
        print(f"  Account: {result['account_id']}")
        print(f"  Tokens: {result['input_tokens']} in | {result['output_tokens']} out")
        print(f"  Response: {result['text'][:100]}...")
        print()

# Pool automatically stopped by context manager
# All limits properly tracked across all account/region combinations
```

**Key Benefits of this Approach:**

1. **Maximum Throughput**: 6 account/region combinations × 500 RPM = 3,000 requests/minute capacity
2. **Automatic Failover**: If one region/account hits limits, requests automatically go to others
3. **Cost Optimization**: Distribute load across accounts and region to maximize throughput
4. **Geographic Distribution**: Use closest regions for lower latency
5. **Quota Management**: Each LimitSet enforces AWS Bedrock's per-region quotas in each account
6. **Zero Lock Contention**: Workers acquire from different LimitSets, minimizing blocking
7. **Automatic Retries**: Built-in retry logic for transient Bedrock errors (using exponential backoff and minimum wait time of 1 second between retries)
8. **Token Tracking**: Precise tracking of actual token usage vs requested

**Monitoring and Scaling:**

```python
# Get comprehensive stats across all account/region combinations
stats = limit_pool.get_stats()

print(f"Total LimitSets: {stats['num_limit_sets']}")
print(f"Load Balancing: {stats['load_balancing']}")
print()

for i, ls_stats in enumerate(stats['limit_sets']):
    config = limitsets[i].config
    print(f"LimitSet {i}: {config['region']} ({config['account_id']})")
    print(f"  Input Tokens: {ls_stats['input_tokens']}")
    print(f"  Output Tokens: {ls_stats['output_tokens']}")
    print(f"  Calls: {ls_stats['call_count']}")
    print()
```

**Cost Considerations:**

AWS Bedrock pricing varies by region. With this setup, you can:
- Route traffic to cheaper regions during off-peak times
- Maximize utilization of committed throughput contracts
- Implement smart routing based on token costs per region
- Track costs per account/region for chargeback

### LimitPool Statistics

Monitor usage across all `LimitSets` in the pool:

```python
# Get comprehensive stats
stats = pool.get_stats()
print(f"Number of LimitSets: {stats['num_limit_sets']}")
print(f"Load balancing: {stats['load_balancing']}")

# Per-LimitSet stats
for i, ls_stats in enumerate(stats['limit_sets']):
    print(f"LimitSet {i}:")
    for key, limit_stats in ls_stats.items():
        print(f"  {key}: {limit_stats}")
```

### Accessing Individual LimitSets

You can access individual `LimitSets` in the pool by index:

```python
pool = LimitPool(limit_sets=[ls1, ls2, ls3])

# Access by index
first_limitset = pool[0]
second_limitset = pool[1]

# Get stats for specific LimitSet
stats = first_limitset.get_stats()

# Direct acquisition from specific LimitSet
with first_limitset.acquire(requested={"tokens": 100}) as acq:
    # Use acq as normal
    pass
```

**Backward Compatibility:**

For pools with a single `LimitSet`, you can also access `Limit` objects by key:

```python
# Single LimitSet pool
pool = LimitPool(limit_sets=[limitset])

# Access Limit by key (only works for single-LimitSet pools)
token_limit = pool["tokens"]
stats = token_limit.get_stats()
```

## Worker Integration

Limits integrate seamlessly with Workers via the `limits` parameter. You can pass a `LimitSet`, a `LimitPool`, or a list of `Limit` objects.

### Option 1: Pass LimitSet (Recommended for Sharing)

```python
from concurry import Worker, LimitSet, RateLimit, ResourceLimit, RateLimitAlgorithm

# Create shared LimitSet
shared_limits = LimitSet(
    limits=[
        RateLimit(
            key="api_tokens",
            window_seconds=60,
            algorithm=RateLimitAlgorithm.TokenBucket,
            capacity=1000
        ),
        ResourceLimit(
            key="db_connections",
            capacity=5
        )
    ],
    shared=True,  # Shared across workers (default)
    mode="thread"  # Match worker mode
)

class LLMWorker(Worker):
    def __init__(self, model: str):
        self.model = model
    
    def process(self, prompt: str) -> str:
        # Nested acquisition pattern
        with self.limits.acquire(requested={"db_connections": 1}):
            context = get_context_from_db()
            
            with self.limits.acquire(requested={"api_tokens": 500}) as acq:
                result = call_llm(self.model, prompt, context)
                acq.update(usage={"api_tokens": result.tokens_used})
                return result.text

# Multiple workers share the same limits
workers = [
    LLMWorker.options(mode="thread", limits=shared_limits).init("gpt-4")
    for _ in range(5)
]
```

### Option 2: Pass List of Limits (Private Per Worker)

```python
# Define limits as list
limit_definitions = [
    RateLimit(
        key="api_tokens",
        window_seconds=60,
        algorithm=RateLimitAlgorithm.TokenBucket,
        capacity=1000
    ),
    ResourceLimit(key="db_connections", capacity=5)
]

# Each worker creates its own private LimitSet
worker = LLMWorker.options(
    mode="thread",
    limits=limit_definitions  # List, not LimitSet
).init("gpt-4")
```

**Behavior:**
- Passing a `LimitSet`: Workers share the same limits
- Passing a `List[Limit]`: Each worker gets its own private `LimitSet`
- Omitting `limits` parameter: Workers get empty LimitSet (always succeeds)

### Option 3: No Limits (Default)

Workers **always** have `self.limits` available. If you don't pass the `limits` parameter, workers automatically get an empty LimitSet that always allows acquisition.

```python
from concurry import Worker

class SimpleWorker(Worker):
    def process(self, data):
        # self.limits is always available, even without configuration
        with self.limits.acquire():
            # Always succeeds immediately, no blocking
            return do_work(data)

# Worker without limits - self.limits.acquire() always succeeds
worker = SimpleWorker.options(mode="thread").init()
result = worker.process(data).result()
worker.stop()
```

**Key benefits:**
- Write limit-aware code once
- Conditionally enable limits based on environment
- No runtime checks needed (`if self.limits is not None`)
- Zero overhead when limits not configured
- Enables gradual adoption and testing

**Example: Conditional limits based on environment**

```python
import os
from concurry import Worker, LimitSet, RateLimit, RateLimitAlgorithm

# Define limits only in production
limits = None
if os.getenv("ENV") == "production":
    limits = LimitSet(
        limits=[
            RateLimit(
                key="api_tokens",
                window_seconds=60,
                algorithm=RateLimitAlgorithm.TokenBucket,
                capacity=1000
            )
        ],
        shared=True,
        mode="thread"
    )

class APIWorker(Worker):
    def call_api(self, prompt: str):
        # Code works the same regardless of whether limits are configured
        with self.limits.acquire(requested={"api_tokens": 100}) as acq:
            result = external_api(prompt)
            acq.update(usage={"api_tokens": result.tokens})
            return result.text

# Production: limits enforced
# Development: limits always succeed, no blocking
worker = APIWorker.options(mode="thread", limits=limits).init()
```

### Execution Modes

Limits work with all execution modes, with appropriate backend selection:

| Mode | LimitSet Backend | Shared Across |
|------|------------------|---------------|
| `sync` | `InMemorySharedLimitSet` | Same process |
| `thread` | `InMemorySharedLimitSet` | Same process |
| `asyncio` | `InMemorySharedLimitSet` | Same process |
| `process` | `MultiprocessSharedLimitSet` | Multiple processes |
| `ray` | `RaySharedLimitSet` | Ray cluster |

```python
# For process workers
process_limits = LimitSet(
    limits=[...],
    shared=True,
    mode="process"  # Uses multiprocessing.Manager
)

worker = MyWorker.options(mode="process", limits=process_limits).init()

# For Ray workers
ray_limits = LimitSet(
    limits=[...],
    shared=True,
    mode="ray"  # Uses Ray actor
)

worker = MyWorker.options(mode="ray", limits=ray_limits).init()
```

## Shared vs Non-Shared LimitSets

`LimitSet` supports both shared and non-shared modes via the `shared` parameter (defaults to `True`).

### Shared LimitSets (shared=True, default)

Multiple workers share the same limit pool:

```python
# Create shared LimitSet
shared_limits = LimitSet(
    limits=[
        RateLimit(
            key="api_tokens",
            window_seconds=60,
            algorithm=RateLimitAlgorithm.TokenBucket,
            capacity=1000
        )
    ],
    shared=True,  # Default
    mode="thread"
)

# All workers share the 1000 token/minute limit
workers = [
    APIWorker.options(mode="thread", limits=shared_limits).init()
    for _ in range(5)
]

# If worker 1 uses 600 tokens, only 400 remain for all workers
```

### Non-Shared LimitSets (shared=False)

Each worker gets its own independent limit pool:

```python
# Create non-shared LimitSet (less common)
non_shared_limits = LimitSet(
    limits=[...],
    shared=False,
    mode="sync"  # Must be "sync" for non-shared
)

# Each worker gets its own copy with separate limits
worker1 = APIWorker.options(mode="sync", limits=non_shared_limits).init()
worker2 = APIWorker.options(mode="sync", limits=non_shared_limits).init()

# worker1's usage doesn't affect worker2's limits
```

**Note**: Non-shared mode only works with `mode="sync"`. For most use cases, you want `shared=True` (the default).

### Backend Types and Performance

LimitSet automatically selects the appropriate backend based on `mode`:

| Backend | Modes | Synchronization | Overhead |
|---------|-------|----------------|----------|
| `InMemorySharedLimitSet` | sync, thread, asyncio | `threading.Lock` | 1-5 μs |
| `MultiprocessSharedLimitSet` | process | `multiprocessing.Manager` | 50-100 μs |
| `RaySharedLimitSet` | ray | Ray actor (0.01 CPU) | 500-1000 μs |

```python
# InMemorySharedLimitSet: Fast, in-process
thread_limits = LimitSet(limits=[...], shared=True, mode="thread")

# MultiprocessSharedLimitSet: Cross-process
process_limits = LimitSet(limits=[...], shared=True, mode="process")

# RaySharedLimitSet: Distributed
ray_limits = LimitSet(limits=[...], shared=True, mode="ray")
```

## Advanced Patterns

### Conditional Limiting

```python
def process_with_priority(priority: str, data):
    # High priority gets more tokens
    requested = {"api_tokens": 1000 if priority == "high" else 100}
    
    with limits.acquire(requested=requested) as acq:
        result = process(data)
        acq.update(usage={"api_tokens": result.actual_tokens})
        return result
```

### Graceful Degradation

```python
def process_with_fallback(data):
    # Try premium service first
    acq = premium_limits.try_acquire(requested={"premium_tokens": 100})
    
    if acq.successful:
        with acq:
            result = premium_service(data)
            acq.update(usage={"premium_tokens": result.tokens})
            return result
    else:
        # Fall back to basic service
        with basic_limits.acquire(requested={"basic_tokens": 10}) as acq:
            result = basic_service(data)
            acq.update(usage={"basic_tokens": result.tokens})
            return result
```

### Monitoring and Observability

```python
def monitor_limits(limits: LimitSet):
    """Print current limit statistics."""
    stats = limits.get_stats()
    
    for key, limit_stats in stats.items():
        print(f"\nLimit: {key}")
        for stat_name, value in limit_stats.items():
            print(f"  {stat_name}: {value}")

# Get individual limit stats
token_limit = limits["api_tokens"]
token_stats = token_limit.get_stats()
print(f"Available tokens: {token_stats['available_tokens']}")
print(f"Utilization: {token_stats['utilization']:.2%}")
```

### Timeout Handling

```python
try:
    with limits.acquire(
        requested={"api_tokens": 1000},
        timeout=5.0
    ) as acq:
        result = expensive_operation()
        acq.update(usage={"api_tokens": result.tokens})
except TimeoutError:
    print("Could not acquire tokens within 5 seconds")
    # Handle timeout - queue for later, use cached result, etc.
```

## Best Practices

### 1. Choose the Right Algorithm

- **TokenBucket**: For APIs with burst tolerance (most common)
- **GCRA**: For strict rate control with precise timing
- **SlidingWindow**: When you need precision without burst issues
- **LeakyBucket**: For smooth, predictable traffic
- **FixedWindow**: When simplicity matters more than edge cases

### 2. Always Use LimitSet, Not Limit Directly

```python
# ✅ Good: Use LimitSet for thread-safe operations
limits = LimitSet(limits=[
    RateLimit(key="tokens", window_seconds=60, capacity=1000)
])
with limits.acquire(requested={"tokens": 100}) as acq:
    result = operation()
    acq.update(usage={"tokens": result.actual_cost})

# ❌ Bad: Don't use Limit directly (not thread-safe!)
limit = RateLimit(key="tokens", window_seconds=60, capacity=1000)
# limit.acquire() doesn't exist!
```

### 3. Always Update RateLimit Usage

```python
# ✅ Good: Report actual usage for RateLimits
with limits.acquire(requested={"tokens": 100}) as acq:
    result = operation()
    acq.update(usage={"tokens": result.actual_cost})

# ❌ Bad: Missing update for RateLimit (raises RuntimeError)
with limits.acquire(requested={"tokens": 100}) as acq:
    result = operation()
    # Missing acq.update()! Will raise error on context exit
```

**Note**: CallLimit and ResourceLimit are automatic and don't need `update()`.

### 4. Use Nested Acquisition for Better Resource Management

```python
# ✅ Good: Nest resources and rate limits
with limits.acquire(requested={"db_connections": 1}):
    # Setup
    with limits.acquire(requested={"tokens": 100}) as acq:
        result = do_work()
        acq.update(usage={"tokens": result.tokens})
    # Connection still held, tokens released

# ❌ Avoid: Acquiring everything at once for long operations
with limits.acquire(requested={"db_connections": 1, "tokens": 100}) as acq:
    # Connection AND tokens held for entire duration
    long_running_operation()
    acq.update(usage={"tokens": 100})
```

### 5. Handle Timeouts Gracefully

```python
# ✅ Good: Handle timeout and provide feedback
try:
    with limits.acquire(requested={"tokens": 1000}, timeout=3.0) as acq:
        result = operation()
        acq.update(usage={"tokens": result.tokens})
except TimeoutError:
    logger.warning("Rate limit timeout, queueing for later")
    queue.put(task)
```

### 6. Monitor Limit Utilization

```python
# ✅ Good: Regular monitoring
def check_limit_health():
    stats = limits.get_stats()
    for key, limit_stats in stats.items():
        if limit_stats.get('utilization', 0) > 0.9:
            alert(f"Limit {key} at {limit_stats['utilization']:.0%}")
```

### 7. Match LimitSet Mode to Worker Mode

```python
# ✅ Good: Match modes for shared limits
thread_limits = LimitSet(limits=[...], shared=True, mode="thread")
workers = [
    Worker.options(mode="thread", limits=thread_limits).init()
    for _ in range(5)
]

# ❌ Don't: Mix execution modes
process_limits = LimitSet(limits=[...], shared=True, mode="process")
worker = Worker.options(mode="thread", limits=process_limits).init()
# ^ Won't work! Mode mismatch
```

### 8. Use Partial Acquisition (CallLimit/ResourceLimit Auto-Included)

```python
# ✅ Good: Specify only what you need - CallLimit auto-acquired
limits = LimitSet(limits=[
    CallLimit(window_seconds=60, capacity=100),
    RateLimit(key="tokens", window_seconds=60, capacity=1000)
])

# CallLimit automatically acquired with default of 1
with limits.acquire(requested={"tokens": 50}) as acq:
    result = operation()
    acq.update(usage={"tokens": result.tokens})
    # CallLimit was automatically acquired and released
```

## Error Handling

### Common Errors

**ValueError: Usage exceeds requested**
```python
# Cause: Trying to use more than requested
acq = limit.acquire(requested=100)
acq.update(used=150)  # Error!

# Solution: Request sufficient amount upfront
acq = limit.acquire(requested=200)
acq.update(used=150)  # OK
```

**RuntimeError: Not all limits updated**
```python
# Cause: Missing update() call for RateLimit
with limits.acquire(requested={"tokens": 100}) as acq:
    pass  # Error on exit - no update!

# Solution: Always update RateLimits
with limits.acquire(requested={"tokens": 100}) as acq:
    result = operation()
    acq.update(usage={"tokens": result.tokens})
```

**TimeoutError: Failed to acquire**
```python
# Cause: Could not acquire within timeout
limits.acquire(requested={"tokens": 1000}, timeout=1.0)

# Solution: Handle timeout or increase timeout
try:
    limits.acquire(requested={"tokens": 1000}, timeout=5.0)
except TimeoutError:
    # Queue for later, use cached result, etc.
    pass
```

## Integration with Retry Mechanisms

Limits work seamlessly with Concurry's retry mechanism. When a method is retried, limits are automatically released between attempts to prevent deadlocks and ensure fair resource usage.

### Automatic Limit Release on Retry

When using limits with retry configuration, the system automatically:

1. Acquires limits before method execution
2. If method fails and should retry:
   - Releases all acquired limits
   - Waits for retry delay
   - Reacquires limits for next attempt
3. Releases limits after final success or failure

```python
from concurry import Worker, ResourceLimit

class DatabaseWorker(Worker):
    def query(self, sql: str) -> list:
        # Acquire database connection
        with self.limits.acquire(requested={"connections": 1}) as acq:
            result = execute_query(sql)
            acq.update(usage={"connections": 1})
            return result

worker = DatabaseWorker.options(
    mode="thread",
    num_retries=3,
    retry_on=[DatabaseError],
    limits=[ResourceLimit(key="connections", capacity=5)]
).init()

# If query fails:
# - Connection is automatically released
# - Wait for retry delay
# - Connection is reacquired for retry
# - No deadlocks!
```

### Rate Limits with Retry

Rate limits are properly managed across retries:

```python
from concurry import RateLimit

class APIWorker(Worker):
    def call_api(self, endpoint: str) -> dict:
        with self.limits.acquire(requested={"requests": 1}) as acq:
            response = requests.get(f"{self.base_url}/{endpoint}")
            acq.update(usage={"requests": 1})
            return response.json()

worker = APIWorker.options(
    mode="thread",
    num_retries=5,
    retry_algorithm="exponential",
    limits=[RateLimit(key="requests", window_seconds=60, capacity=100)]
).init()

# Each retry attempt counts as a separate request
# Limits are released between attempts
# Total budget is respected across all attempts
```

### Shared Limits with Retry

When using shared limits across a pool, retries coordinate properly:

```python
from concurry import LimitSet, ResourceLimit

# Create shared limit
shared_limits = LimitSet(
    limits=[ResourceLimit(key="db_connections", capacity=10)],
    shared=True,
    mode="thread"
)

# Pool with shared limits and retry
pool = DatabaseWorker.options(
    mode="thread",
    max_workers=20,  # 20 workers share 10 connections
    num_retries=3,
    retry_on=[DatabaseError],
    limits=shared_limits
).init()

# Benefits:
# - Each worker's retries properly release/acquire shared limits
# - No starvation - limits are freed between attempts
# - Fair resource distribution across all workers
```

### Call Limits with Retry

`CallLimit` automatically tracks retry attempts:

```python
from concurry import CallLimit

worker = MyWorker.options(
    mode="thread",
    num_retries=3,
    limits=[CallLimit(window_seconds=1, capacity=10)]
).init()

# CallLimit counts each attempt (initial + retries)
# Each retry attempt is a separate "call" for limit purposes
# Automatically managed - no manual update needed
```

### Best Practices for Limits with Retry

**1. Size Limits for Worst-Case Retry Scenarios**

```python
# If each request can retry 3 times, and you have 10 workers:
# Worst case: 10 * (1 + 3) = 40 total attempts

# Size rate limits accordingly
worker = MyWorker.options(
    mode="thread",
    max_workers=10,
    num_retries=3,
    limits=[RateLimit(
        key="requests",
        window_seconds=60,
        capacity=100  # Accounts for retries
    )]
).init()
```

**2. Use Resource Limits to Prevent Resource Exhaustion**

```python
# Limit concurrent database connections
worker = DatabaseWorker.options(
    mode="thread",
    num_retries=3,
    limits=[ResourceLimit(key="connections", capacity=10)]
).init()

# Even with retries, never exceeds 10 concurrent connections
```

**3. Combine with Exponential Backoff**

```python
# Retry with backoff reduces rate limit pressure
worker = APIWorker.options(
    mode="thread",
    num_retries=5,
    retry_algorithm="exponential",  # Increases wait time
    retry_wait=1.0,
    limits=[RateLimit(key="requests", window_seconds=60, capacity=100)]
).init()

# Later retries have longer delays, spreading out rate limit usage
```

**4. Monitor Limit Utilization with Retries**

```python
def should_retry_with_limit_check(exception, attempt, **ctx):
    """Smart retry that backs off if limits are tight."""
    if attempt > 3:
        return False  # Don't retry too many times
    
    # Check if we should retry based on exception
    return isinstance(exception, (ConnectionError, TimeoutError))

worker = MyWorker.options(
    mode="thread",
    num_retries=5,
    retry_on=should_retry_with_limit_check,
    limits=[RateLimit(key="requests", window_seconds=60, capacity=100)]
).init()
```

**5. Use Shared Limits for Pool-Wide Retry Coordination**

```python
# Shared limits ensure fair resource distribution even with retries
shared_limits = LimitSet(
    limits=[
        ResourceLimit(key="resources", capacity=20),
        RateLimit(key="requests", window_seconds=60, capacity=200)
    ],
    shared=True,
    mode="thread"
)

pool = MyWorker.options(
    mode="thread",
    max_workers=10,
    num_retries=3,
    limits=shared_limits
).init()

# All workers share the limits
# Retries don't cause resource starvation
```

For comprehensive retry documentation, see the [Retry Mechanisms Guide](retries.md).

## Performance Considerations

### Acquisition Overhead

| Backend | Overhead | Use Case |
|---------|----------|----------|
| InMemory | 1-5 μs | Single process |
| Multiprocess | 50-100 μs | Multi-process |
| Ray | 500-1000 μs | Distributed |

### Algorithm Performance

| Algorithm | Memory | CPU | Precision |
|-----------|--------|-----|-----------|
| FixedWindow | Lowest | Lowest | Moderate |
| TokenBucket | Low | Low | Good |
| GCRA | Low | Low | Best |
| LeakyBucket | Low | Medium | Excellent |
| SlidingWindow | Higher | Medium | Excellent |

### Optimization Tips

1. **Batch operations** when possible to reduce acquire/release cycles
2. **Use try_acquire** for non-critical operations
3. **Monitor utilization** to right-size limits
4. **Choose simpler algorithms** (FixedWindow, TokenBucket) for high-throughput scenarios
5. **Use nested acquisition** to minimize resource holding time

## See Also

- [Workers Guide](workers.md) - Integrating limits with Workers
- [Retry Mechanisms Guide](retries.md) - Using retries with limits
- [Worker Pools Guide](pools.md) - Shared limits across pools
- [API Reference](../api/limits.md) - Detailed API documentation
- [Examples](../examples.md) - More limit usage examples


