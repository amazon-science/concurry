# Progress Tracking

Concurry's `ProgressBar` provides beautiful, informative progress tracking with rich features and customization options.

## The Problem

Basic progress tracking in Python is often:

- **Ugly**: Plain text counters or basic progress bars
- **Uninformative**: No indication of success/failure
- **Inflexible**: Hard to customize appearance
- **Inconsistent**: Different behavior in notebooks vs terminals

## The Solution: ProgressBar

Concurry's `ProgressBar` provides:

- **Beautiful Display**: Color-coded, smooth progress bars
- **State Indicators**: Success, failure, and stop states with automatic coloring
- **Style Detection**: Automatically adapts to terminal, notebook, or Ray environments
- **Rich Customization**: Colors, units, descriptions, update frequency, and more
- **Iterable Wrapping**: Easy integration with any iterable

## Basic Usage

### Wrapping Iterables

The simplest way to use ProgressBar:

```python
from concurry.utils.progress import ProgressBar
import time

# Automatically wraps any iterable
items = range(100)
for item in ProgressBar(items, desc="Processing"):
    time.sleep(0.01)
    # Progress automatically updates
# Automatically shows success when complete!
```

Works with any iterable:

```python
# Lists
for item in ProgressBar([1, 2, 3, 4, 5], desc="List"):
    process(item)

# Dictionaries
for key, value in ProgressBar(my_dict, desc="Dict"):
    process(key, value)

# Generators
def generate_items():
    for i in range(100):
        yield i

for item in ProgressBar(generate_items(), desc="Generator", total=100):
    process(item)
```

### Manual Progress Bar

For more control, create a manual progress bar:

```python
from concurry.utils.progress import ProgressBar

pbar = ProgressBar(total=100, desc="Manual Progress")

for i in range(100):
    # Do some work
    result = process_item(i)
    
    # Update progress
    pbar.update(1)

# Mark as complete
pbar.success("All done!")
```

## Progress States

### Success State

Mark successful completion:

```python
pbar = ProgressBar(total=100, desc="Processing")

for i in range(100):
    process(i)
    pbar.update(1)

# Green color with success message
pbar.success("Complete!")
```

### Failure State

Indicate failures:

```python
pbar = ProgressBar(total=100, desc="Processing")

try:
    for i in range(100):
        if error_occurred(i):
            raise ValueError("Processing failed")
        process(i)
        pbar.update(1)
    pbar.success()
except Exception as e:
    # Red color with failure message
    pbar.failure(f"Failed: {e}")
    raise
```

### Stop State

Indicate early stopping:

```python
pbar = ProgressBar(total=100, desc="Processing")

for i in range(100):
    if should_stop(i):
        # Grey color with stop message
        pbar.stop("Stopped early")
        break
    process(i)
    pbar.update(1)
```

## Customization

### Colors

Customize progress bar colors:

```python
from concurry.utils.progress import ProgressBar

# Custom color (hex code)
pbar = ProgressBar(
    total=100,
    desc="Custom Color",
    color="#9c27b0"  # Purple
)

# Color changes with state
for i in range(100):
    pbar.update(1)

pbar.success()  # Automatically turns green
```

### Units

Specify what you're tracking:

```python
# Default is "row"
pbar = ProgressBar(total=100, unit="row")

# Custom units
pbar = ProgressBar(total=100, unit="file")
pbar = ProgressBar(total=100, unit="item")
pbar = ProgressBar(total=100, unit="MB")
```

### Progress Bar Width

Control the width:

```python
# Default width
pbar = ProgressBar(total=100, ncols=100)

# Wider progress bar
pbar = ProgressBar(total=100, ncols=150)

# Auto-fit to terminal
pbar = ProgressBar(total=100, ncols=None)  # In notebook mode
```

### Update Frequency

Control how often the progress bar updates:

```python
# Update every iteration (default)
pbar = ProgressBar(total=1000, miniters=1)

# Update every 10 iterations (better performance)
pbar = ProgressBar(total=1000, miniters=10)

# Update every 100 iterations
pbar = ProgressBar(total=1000, miniters=100)

# Note: Updates are batched automatically
for i in range(1000):
    pbar.update(1)  # Only actually updates every miniters iterations
```

### Smoothing

Control progress smoothing:

```python
# More smoothing (default)
pbar = ProgressBar(total=100, smoothing=0.15)

# Less smoothing (more responsive)
pbar = ProgressBar(total=100, smoothing=0.05)

# No smoothing
pbar = ProgressBar(total=100, smoothing=0)
```

## Styles

### Auto Style (Default)

Automatically detects the environment:

```python
pbar = ProgressBar(total=100, style="auto")
# Uses notebook style in Jupyter
# Uses terminal style otherwise
```

### Notebook Style

Optimized for Jupyter notebooks:

```python
pbar = ProgressBar(total=100, style="notebook")
# Rich HTML widgets in notebooks
```

### Standard Style

Terminal-based progress bar:

```python
pbar = ProgressBar(total=100, style="std")
# Standard terminal output
```

### Ray Style

Integrates with Ray's progress tracking:

```python
# Requires: pip install concurry[ray]
try:
    import ray
    ray.init()
    
    pbar = ProgressBar(total=100, style="ray")
    # Uses Ray's distributed progress tracking
    
    ray.shutdown()
except ImportError:
    print("Ray not installed")
```

## Advanced Features

### Dynamic Descriptions

Update the description during progress:

```python
pbar = ProgressBar(total=100, desc="Phase 1")

for i in range(100):
    # Change description at different phases
    if i == 33:
        pbar.set_description("Phase 2")
    elif i == 66:
        pbar.set_description("Phase 3")
    
    pbar.update(1)

pbar.success()
```

### Changing Total

Update the total dynamically:

```python
pbar = ProgressBar(total=100, desc="Processing")

for i in range(50):
    pbar.update(1)

# More work discovered!
pbar.set_total(150)

for i in range(50, 150):
    pbar.update(1)

pbar.success()
```

### Setting Progress Directly

Set the current progress value:

```python
pbar = ProgressBar(total=100, desc="Processing")

# Jump to 50% complete
pbar.set_n(50)

# Continue from there
for i in range(50, 100):
    pbar.update(1)

pbar.success()
```

### Changing Units

Update units dynamically:

```python
pbar = ProgressBar(total=100, desc="Processing", unit="file")

for i in range(50):
    pbar.update(1)

# Switch to different unit
pbar.set_unit("MB")

for i in range(50, 100):
    pbar.update(1)

pbar.success()
```

## Practical Patterns

### Pattern 1: Try-Finally with Progress

Ensure progress bar closes properly:

```python
from concurry.utils.progress import ProgressBar

pbar = ProgressBar(total=100, desc="Work")
try:
    for i in range(100):
        process(i)
        pbar.update(1)
    pbar.success()
except Exception as e:
    pbar.failure(str(e))
    raise
finally:
    pbar.close()  # Ensure cleanup
```

### Pattern 2: Nested Progress Bars

Track multiple stages:

```python
from concurry.utils.progress import ProgressBar

stages = ["Loading", "Processing", "Saving"]
data = load_data()

for stage in stages:
    pbar = ProgressBar(
        total=len(data),
        desc=stage,
        color="#0288d1" if stage == "Processing" else "#607d8b"
    )
    
    for item in data:
        process_item(item, stage)
        pbar.update(1)
    
    pbar.success(f"{stage} complete")
```

### Pattern 3: Conditional Progress

Only show progress when needed:

```python
from concurry.utils.progress import ProgressBar

def process_items(items, show_progress=True):
    """Process items with optional progress tracking."""
    pbar = ProgressBar(
        total=len(items),
        desc="Processing",
        disable=not show_progress  # Hide when disabled
    )
    
    results = []
    for item in items:
        result = process(item)
        results.append(result)
        pbar.update(1)
    
    pbar.success()
    return results

# With progress
results = process_items(data, show_progress=True)

# Without progress (silent mode)
results = process_items(data, show_progress=False)
```

### Pattern 4: Batch Updates

Update progress in batches:

```python
from concurry.utils.progress import ProgressBar

pbar = ProgressBar(total=1000, desc="Processing", miniters=10)

batch_size = 10
for i in range(0, 1000, batch_size):
    batch = items[i:i+batch_size]
    process_batch(batch)
    
    # Update by batch size
    pbar.update(batch_size)

pbar.success()
```

### Pattern 5: Progress with Results

Track both progress and collect results:

```python
from concurry.utils.progress import ProgressBar
from typing import List

def process_with_progress(items) -> List:
    """Process items and track progress."""
    results = []
    failed = 0
    
    pbar = ProgressBar(total=len(items), desc="Processing")
    
    for item in items:
        try:
            result = process(item)
            results.append(result)
        except Exception as e:
            failed += 1
            pbar.set_description(f"Processing ({failed} failed)")
        
        pbar.update(1)
    
    if failed > 0:
        pbar.failure(f"Complete with {failed} failures")
    else:
        pbar.success("All successful")
    
    return results
```

### Pattern 6: Parallel Processing with Progress

Combine with concurrent execution:

```python
from concurry.utils.progress import ProgressBar
from concurrent.futures import ThreadPoolExecutor

def parallel_process_with_progress(items):
    """Process items in parallel with progress tracking."""
    pbar = ProgressBar(total=len(items), desc="Processing")
    results = []
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process, item) for item in items]
        
        for future in futures:
            result = future.result()
            results.append(result)
            pbar.update(1)
    
    pbar.success()
    return results
```

## Configuration via Dictionary

Pass configuration as a dictionary:

```python
from concurry.utils.progress import ProgressBar

config = {
    "total": 100,
    "desc": "Processing",
    "unit": "file",
    "color": "#9c27b0",
    "ncols": 120,
    "smoothing": 0.1
}

pbar = ProgressBar(**config)

for i in range(100):
    pbar.update(1)

pbar.success()
```

## Progress Bar Reuse

Reuse an existing progress bar:

```python
from concurry.utils.progress import ProgressBar

# Create base progress bar
base_pbar = ProgressBar(total=100, desc="Base", unit="item")

# Create new progress bar from existing one
pbar = ProgressBar(
    progress_bar=base_pbar,
    total=200,  # Override total
    desc="New"  # Override description
)

# pbar now uses base_pbar's underlying tqdm instance
# but with updated settings
```

## Best Practices

### 1. Always Indicate Final State

```python
# Good - clear final state
pbar = ProgressBar(total=100)
try:
    for i in range(100):
        pbar.update(1)
    pbar.success()
except Exception as e:
    pbar.failure(str(e))
    raise

# Less ideal - no final state indication
pbar = ProgressBar(total=100)
for i in range(100):
    pbar.update(1)
# No success/failure indication
```

### 2. Use Appropriate miniters

```python
# Good - reduce updates for large iterations
pbar = ProgressBar(total=1000000, miniters=1000)

# Less efficient - too many updates
pbar = ProgressBar(total=1000000, miniters=1)
```

### 3. Set Meaningful Descriptions

```python
# Good - clear description
pbar = ProgressBar(total=100, desc="Processing customer records")

# Less helpful - vague description
pbar = ProgressBar(total=100, desc="Processing")
```

### 4. Use Iterable Wrapping When Possible

```python
# Good - concise and automatic
for item in ProgressBar(items, desc="Processing"):
    process(item)

# More verbose - manual updates
pbar = ProgressBar(total=len(items), desc="Processing")
for item in items:
    process(item)
    pbar.update(1)
pbar.success()
```

## Performance Considerations

### Update Frequency

Adjust `miniters` for better performance with large iterations:

```python
# For 1M iterations, update every 1000
pbar = ProgressBar(total=1000000, miniters=1000)
```

### Disable When Not Needed

Disable progress in non-interactive environments:

```python
import sys

show_progress = sys.stdout.isatty()  # Only show in terminal

pbar = ProgressBar(
    total=100,
    desc="Processing",
    disable=not show_progress
)
```

## Next Steps

Now that you can track progress, combine it with other Concurry features:

- [Workers Guide](workers.md) - Add progress tracking to stateful workers
- [Worker Pools Guide](pools.md) - Track progress across worker pools
- [Futures Guide](futures.md) - Combine progress bars with unified futures
- [Examples](../examples.md) - See real-world usage patterns with progress
- [API Reference](../api/progress.md) - Detailed progress API documentation

