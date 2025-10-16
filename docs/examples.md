# Examples

This page provides practical examples of using Concurry in real-world scenarios.

## Example 1: API Worker with Retry and Rate Limiting

Build a robust API client that automatically retries on transient errors and respects rate limits:

```python
from concurry import Worker, RateLimit
import requests

class APIWorker(Worker):
    def __init__(self, base_url: str):
        self.base_url = base_url
    
    def fetch_data(self, endpoint: str) -> dict:
        """Fetch data from API with automatic limit handling."""
        # Rate limit automatically enforced
        with self.limits.acquire(requested={"requests": 1}) as acq:
            response = requests.get(f"{self.base_url}/{endpoint}")
            response.raise_for_status()
            acq.update(usage={"requests": 1})
            return response.json()

# Create worker with retry and rate limiting
worker = APIWorker.options(
    mode="thread",
    num_retries=3,
    retry_algorithm="exponential",
    retry_on=[requests.ConnectionError, requests.Timeout],
    limits=[RateLimit(key="requests", window_seconds=60, capacity=100)]
).init(base_url="https://api.example.com")

# Fetch data - automatically retries on failure, respects rate limit
data = worker.fetch_data("users/123").result()
print(f"Fetched user: {data['name']}")

worker.stop()
```

## Example 2: Worker Pool for Parallel Data Processing

Process large datasets in parallel with load balancing and progress tracking:

```python
from concurry import Worker, TaskWorker, ProgressBar
import time

class DataProcessor(Worker):
    def __init__(self, multiplier: int):
        self.multiplier = multiplier
        self.processed = 0
    
    def process(self, value: int) -> int:
        """Process a single value."""
        time.sleep(0.01)  # Simulate work
        self.processed += 1
        return value * self.multiplier

# Create a pool of 10 workers
pool = DataProcessor.options(
    mode="process",  # Use multiprocessing for CPU-bound work
    max_workers=10,
    load_balancing="least_active"
).init(multiplier=2)

# Process 1000 items in parallel
data = range(1000)
futures = []

for item in ProgressBar(data, desc="Submitting tasks"):
    futures.append(pool.process(item))

# Collect results
results = []
for future in ProgressBar(futures, desc="Collecting results"):
    results.append(future.result())

print(f"Processed {len(results)} items")
pool.stop()
```

## Example 3: Async API Scraper with Concurrency

Scrape multiple URLs concurrently using async/await:

```python
from concurry import Worker
import asyncio
import aiohttp

class AsyncWebScraper(Worker):
    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self.scraped_count = 0
    
    async def fetch_url(self, url: str) -> dict:
        """Fetch a single URL asynchronously."""
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=self.timeout) as response:
                self.scraped_count += 1
                return {
                    'url': url,
                    'status': response.status,
                    'content': await response.text()
                }
    
    async def fetch_multiple(self, urls: list) -> list:
        """Fetch multiple URLs concurrently."""
        tasks = [self.fetch_url(url) for url in urls]
        return await asyncio.gather(*tasks, return_exceptions=True)

# Use asyncio mode for maximum async performance
scraper = AsyncWebScraper.options(mode="asyncio").init(timeout=30)

urls = [
    'https://example.com/page1',
    'https://example.com/page2',
    'https://example.com/page3',
]

# All URLs are fetched concurrently
results = scraper.fetch_multiple(urls).result()
print(f"Scraped {len(results)} pages")

scraper.stop()
```

## Example 4: LLM Worker with Output Validation

Use retry with output validation for LLM API calls:

```python
from concurry import Worker, RetryValidationError
import json
import openai

class LLMWorker(Worker):
    def __init__(self, model: str = "gpt-4"):
        self.model = model
        self.api_calls = 0
    
    def generate_json(self, prompt: str) -> dict:
        """Generate JSON from LLM with automatic retry on invalid output."""
        self.api_calls += 1
        response = openai.ChatCompletion.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}]
        )
        result = response['choices'][0]['message']['content']
        return json.loads(result)  # May raise JSONDecodeError

def is_valid_response(result, **ctx):
    """Validate LLM response has required fields."""
    return isinstance(result, dict) and "data" in result and "status" in result

# Configure worker with retry and validation
worker = LLMWorker.options(
    mode="thread",
    num_retries=5,
    retry_on=[json.JSONDecodeError, KeyError],  # Retry on JSON errors
    retry_until=is_valid_response,  # Retry until valid output
    retry_algorithm="linear",
    retry_wait=2.0
).init()

try:
    result = worker.generate_json("Generate user data as JSON").result()
    print(f"Generated: {result}")
except RetryValidationError as e:
    print(f"Failed after {e.attempts} attempts")
    print(f"All results: {e.all_results}")
    # Use the best result even though validation failed
    last_output = e.all_results[-1]

worker.stop()
```

## Example 5: Database Worker with Resource Limits

Manage database connections with resource limits:

```python
from concurry import Worker, ResourceLimit
import psycopg2

class DatabaseWorker(Worker):
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
    
    def query(self, sql: str) -> list:
        """Execute SQL query with connection pooling."""
        # Acquire a connection from the pool
        with self.limits.acquire(requested={"connections": 1}) as acq:
            conn = psycopg2.connect(self.connection_string)
            try:
                cursor = conn.cursor()
                cursor.execute(sql)
                result = cursor.fetchall()
                acq.update(usage={"connections": 1})
                return result
            finally:
                conn.close()

# Pool of 20 workers sharing 10 database connections
pool = DatabaseWorker.options(
    mode="thread",
    max_workers=20,
    num_retries=3,
    retry_on=[psycopg2.OperationalError],
    limits=[ResourceLimit(key="connections", capacity=10)]
).init(connection_string="postgresql://localhost/mydb")

# Submit 100 queries - only 10 concurrent connections max
queries = [f"SELECT * FROM users WHERE id = {i}" for i in range(100)]
futures = [pool.query(q) for q in queries]

results = [f.result() for f in futures]
print(f"Executed {len(results)} queries")

pool.stop()
```

## Example 6: TaskWorker for Quick Parallel Execution

Use `TaskWorker` for quick task execution without defining custom workers:

```python
from concurry import TaskWorker
import time

def expensive_computation(x: int) -> int:
    """Simulate expensive computation."""
    time.sleep(0.1)
    return x ** 2 + x ** 3

# Create a task worker
worker = TaskWorker.options(mode="process").init()

# Submit multiple tasks
futures = [worker.submit(expensive_computation, i) for i in range(10)]

# Wait for results
results = [f.result() for f in futures]
print(f"Results: {results}")

# Or use map() for simpler batch processing
results = list(worker.map(expensive_computation, range(10)))
print(f"Map results: {results}")

worker.stop()
```

## Example 7: Distributed Computing with Ray

Scale to distributed computing with Ray workers:

```python
from concurry import Worker
import ray

ray.init()

class DistributedProcessor(Worker):
    def __init__(self, config: dict):
        self.config = config
        self.processed = 0
    
    def process_batch(self, batch: list) -> dict:
        """Process a batch of data."""
        results = [self.process_item(item) for item in batch]
        self.processed += len(batch)
        return {
            "processed": len(results),
            "results": results,
            "total": self.processed
        }
    
    def process_item(self, item):
        # Heavy computation
        return item * 2

# Create a pool of Ray actors across the cluster
pool = DistributedProcessor.options(
    mode="ray",
    max_workers=50,  # 50 actors across cluster
    actor_options={"num_cpus": 0.5}
).init(config={"version": "1.0"})

# Process large dataset across cluster
batches = [list(range(i*100, (i+1)*100)) for i in range(100)]
futures = [pool.process_batch(batch) for batch in batches]

results = [f.result() for f in futures]
print(f"Processed {sum(r['processed'] for r in results)} items across cluster")

pool.stop()
ray.shutdown()
```

## Example 8: Mixed Sync/Async Worker Methods

Combine sync and async methods in a single worker:

```python
from concurry import Worker
import asyncio
import aiohttp

class HybridWorker(Worker):
    def __init__(self):
        self.results = []
    
    async def fetch_data(self, url: str) -> dict:
        """Async method for I/O-bound operations."""
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                return await response.json()
    
    def process_data(self, data: dict) -> str:
        """Sync method for CPU-bound operations."""
        # Heavy processing
        result = json.dumps(data, indent=2)
        self.results.append(result)
        return result
    
    async def fetch_and_process(self, url: str) -> str:
        """Combine async and sync operations."""
        data = await self.fetch_data(url)
        # Can call sync method from async context
        return self.process_data(data)

# Use asyncio mode for best async performance
worker = HybridWorker.options(mode="asyncio").init()

# Call both async and sync methods
data = worker.fetch_data("https://api.example.com/data").result()
processed = worker.process_data(data).result()
combined = worker.fetch_and_process("https://api.example.com/data").result()

worker.stop()
```

## Example 9: Progress Tracking with Workers

Combine workers with progress bars for real-time feedback:

```python
from concurry import Worker, ProgressBar
import time

class BatchProcessor(Worker):
    def __init__(self):
        self.processed = 0
    
    def process_batch(self, items: list) -> list:
        """Process a batch of items with progress tracking."""
        results = []
        # Track progress within worker
        for item in ProgressBar(items, desc="Processing batch", leave=False):
            time.sleep(0.01)
            results.append(item * 2)
            self.processed += 1
        return results

# Create a pool
pool = BatchProcessor.options(
    mode="thread",
    max_workers=5
).init()

# Submit multiple batches
batches = [list(range(i*20, (i+1)*20)) for i in range(10)]

# Track overall progress
futures = []
for batch in ProgressBar(batches, desc="Submitting batches"):
    futures.append(pool.process_batch(batch))

# Collect results with progress
results = []
for future in ProgressBar(futures, desc="Collecting results"):
    results.append(future.result())

print(f"Processed {sum(len(r) for r in results)} items total")
pool.stop()
```

## Example 10: Advanced Retry with Context

Use retry context for intelligent retry decisions:

```python
from concurry import Worker
import requests
import time

class SmartAPIWorker(Worker):
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.failures = []
    
    def call_api(self, endpoint: str) -> dict:
        """API call that logs failures."""
        try:
            response = requests.get(f"{self.base_url}/{endpoint}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.failures.append({
                "endpoint": endpoint,
                "error": str(e),
                "time": time.time()
            })
            raise

def smart_retry_filter(exception, attempt, elapsed_time, **ctx):
    """Intelligent retry logic based on context."""
    # Don't retry after 30 seconds total
    if elapsed_time > 30:
        return False
    
    # Give up on auth errors
    if isinstance(exception, requests.HTTPError):
        if exception.response.status_code in [401, 403]:
            return False
    
    # Retry network errors with backoff
    if isinstance(exception, (requests.ConnectionError, requests.Timeout)):
        return True
    
    # Retry rate limit errors
    if isinstance(exception, requests.HTTPError):
        if exception.response.status_code == 429:
            return True
    
    return False

# Create worker with smart retry
worker = SmartAPIWorker.options(
    mode="thread",
    num_retries=5,
    retry_on=smart_retry_filter,
    retry_algorithm="fibonacci",
    retry_wait=1.0
).init(base_url="https://api.example.com")

result = worker.call_api("data").result()
print(f"Success: {result}")
print(f"Failed attempts: {len(worker.failures)}")

worker.stop()
```

## Next Steps

- [Workers Guide](user-guide/workers.md) - Learn more about the Worker pattern
- [Worker Pools Guide](user-guide/pools.md) - Scale with worker pools
- [Limits Guide](user-guide/limits.md) - Resource and rate limiting
- [Retry Mechanisms Guide](user-guide/retries.md) - Automatic retry strategies
- [API Reference](api/index.md) - Complete API documentation
