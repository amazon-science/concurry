# Examples

This page provides practical examples of using Concurry in real-world scenarios.

## Example 1: Parallel Data Processing with Progress

Process a large dataset in parallel with beautiful progress tracking:

```python
from concurry.core.future import wrap_future
from concurry.utils.progress import ProgressBar
from concurrent.futures import ThreadPoolExecutor
import time

def process_item(item):
    """Simulate processing an item."""
    time.sleep(0.1)  # Simulate work
    return item * 2

def process_dataset_parallel(data):
    """Process dataset in parallel with progress tracking."""
    results = []
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Submit all tasks
        futures = [wrap_future(executor.submit(process_item, item)) for item in data]
        
        # Track progress
        pbar = ProgressBar(total=len(futures), desc="Processing items")
        
        for future in futures:
            try:
                result = future.result(timeout=10)
                results.append(result)
                pbar.update(1)
            except Exception as e:
                pbar.failure(f"Error: {e}")
                raise
        
        pbar.success("All items processed!")
    
    return results

# Usage
data = range(20)
results = process_dataset_parallel(data)
print(f"Processed {len(results)} items")
```

## Example 2: Framework-Agnostic Future Handling

Write code that works with any future type:

```python
from concurry.core.future import wrap_future, BaseFuture
from concurrent.futures import ThreadPoolExecutor
import asyncio
from typing import Any, List

def handle_future(future: Any) -> Any:
    """Handle any type of future using the unified interface."""
    # Wrap in unified interface
    unified_future = wrap_future(future)
    
    # Consistent API regardless of the original future type
    if not unified_future.done():
        print("Waiting for result...")
    
    try:
        result = unified_future.result(timeout=5)
        print(f"Success: {result}")
        return result
    except TimeoutError:
        print("Timeout!")
        unified_future.cancel()
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None

# Works with threading
with ThreadPoolExecutor() as executor:
    thread_future = executor.submit(lambda: 42)
    handle_future(thread_future)

# Works with asyncio
async def async_example():
    loop = asyncio.get_event_loop()
    async_future = loop.create_future()
    async_future.set_result(100)
    handle_future(async_future)

asyncio.run(async_example())
```

## Example 3: Progress Bar with Different States

Use different progress bar states to indicate success, failure, or cancellation:

```python
from concurry.utils.progress import ProgressBar
import time
import random

def process_with_possible_failure(items):
    """Process items with possible failure."""
    pbar = ProgressBar(total=len(items), desc="Processing", color="#0288d1")
    
    for i, item in enumerate(items):
        try:
            # Simulate work
            time.sleep(0.1)
            
            # Simulate random failures
            if random.random() < 0.1:  # 10% failure rate
                raise ValueError(f"Failed to process item {item}")
            
            pbar.update(1)
            
        except ValueError as e:
            pbar.failure(f"Failed at item {i+1}")
            raise
    
    pbar.success("All done!")

# Success case
try:
    items = range(10)
    process_with_possible_failure(items)
except ValueError:
    print("Processing failed")
```

## Example 4: Custom Progress Bar Styling

Customize the progress bar appearance:

```python
from concurry.utils.progress import ProgressBar
import time

# Custom colors and styling
pbar = ProgressBar(
    total=100,
    desc="Custom Progress",
    unit="files",
    color="#9c27b0",  # Purple
    ncols=120,
    smoothing=0.1,
    miniters=5  # Update every 5 iterations
)

for i in range(100):
    time.sleep(0.02)
    pbar.update(1)
    
    # Update description dynamically
    if i % 25 == 0:
        pbar.set_description(f"Phase {i//25 + 1}")

pbar.success("Complete!")
```

## Example 5: Wrapping Iterables with Progress

Easily wrap any iterable with a progress bar:

```python
from concurry.utils.progress import ProgressBar
import time

# Automatically wraps any iterable
data = range(50)

for item in ProgressBar(data, desc="Processing items", unit="item"):
    # Process each item
    time.sleep(0.05)
    
    # Progress bar automatically updates
    # Automatically shows success when iteration completes
```

## Example 6: Async/Await with Futures

Use the unified future interface with async/await:

```python
from concurry.core.future import wrap_future
from concurrent.futures import ThreadPoolExecutor
import asyncio

async def async_process():
    """Process tasks using async/await with unified futures."""
    
    def cpu_bound_task(n):
        """Simulate CPU-bound work."""
        total = sum(i * i for i in range(n))
        return total
    
    # Create futures from different sources
    with ThreadPoolExecutor() as executor:
        future1 = wrap_future(executor.submit(cpu_bound_task, 1000000))
        future2 = wrap_future(executor.submit(cpu_bound_task, 2000000))
        
        # Use await syntax with unified interface
        result1 = await future1
        result2 = await future2
        
        print(f"Results: {result1}, {result2}")

# Run the async function
asyncio.run(async_process())
```

## Example 7: Ray Integration (Optional)

Use Concurry with Ray for distributed computing:

```python
# Requires: pip install concurry[ray]
try:
    import ray
    from concurry.core.future import wrap_future
    from concurry.utils.progress import ProgressBar
    
    # Initialize Ray
    ray.init()
    
    @ray.remote
    def compute_task(x):
        """Remote computation task."""
        return x ** 2
    
    def ray_example():
        """Example using Ray with Concurry."""
        # Submit tasks to Ray
        tasks = [compute_task.remote(i) for i in range(100)]
        
        # Wrap Ray ObjectRefs in unified interface
        unified_futures = [wrap_future(task) for task in tasks]
        
        # Track progress
        results = []
        for future in ProgressBar(unified_futures, desc="Computing"):
            result = future.result()
            results.append(result)
        
        return results
    
    results = ray_example()
    print(f"Computed {len(results)} results")
    
    ray.shutdown()
    
except ImportError:
    print("Ray not installed. Install with: pip install concurry[ray]")
```

## Example 8: Batch Processing with Progress Updates

Process data in batches with fine-grained progress tracking:

```python
from concurry.utils.progress import ProgressBar
from concurrent.futures import ThreadPoolExecutor
from typing import List
import time

def process_batch(batch: List[int]) -> List[int]:
    """Process a batch of items."""
    time.sleep(0.2)  # Simulate batch processing
    return [x * 2 for x in batch]

def batch_process_with_progress(data: List[int], batch_size: int = 10):
    """Process data in batches with progress tracking."""
    batches = [data[i:i+batch_size] for i in range(0, len(data), batch_size)]
    
    pbar = ProgressBar(
        total=len(data),
        desc="Batch processing",
        unit="items"
    )
    
    results = []
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Process batches in parallel
        futures = [executor.submit(process_batch, batch) for batch in batches]
        
        for future in futures:
            batch_results = future.result()
            results.extend(batch_results)
            pbar.update(len(batch_results))
    
    pbar.success(f"Processed {len(results)} items in {len(batches)} batches")
    return results

# Usage
data = list(range(100))
results = batch_process_with_progress(data, batch_size=10)
```

## Example 9: Combining Multiple Progress Bars

Use multiple progress bars for different stages:

```python
from concurry.utils.progress import ProgressBar
import time

def multi_stage_processing(data):
    """Process data through multiple stages."""
    
    # Stage 1: Loading
    pbar1 = ProgressBar(total=len(data), desc="Stage 1: Loading", color="#2196f3")
    loaded_data = []
    for item in data:
        time.sleep(0.05)
        loaded_data.append(item)
        pbar1.update(1)
    pbar1.success("Loading complete")
    
    # Stage 2: Processing
    pbar2 = ProgressBar(total=len(loaded_data), desc="Stage 2: Processing", color="#ff9800")
    processed_data = []
    for item in loaded_data:
        time.sleep(0.05)
        processed_data.append(item * 2)
        pbar2.update(1)
    pbar2.success("Processing complete")
    
    # Stage 3: Saving
    pbar3 = ProgressBar(total=len(processed_data), desc="Stage 3: Saving", color="#4caf50")
    for item in processed_data:
        time.sleep(0.05)
        pbar3.update(1)
    pbar3.success("Saving complete")
    
    return processed_data

# Usage
data = range(20)
results = multi_stage_processing(data)
```

## Next Steps

- [API Reference](api/index.md) - Detailed API documentation
- [Futures Guide](user-guide/futures.md) - Deep dive into futures
- [Progress Guide](user-guide/progress.md) - Advanced progress tracking

