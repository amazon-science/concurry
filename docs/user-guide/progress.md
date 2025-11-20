# Progress Tracking

**Stop staring at a blank screen. Get instant, beautiful visibility into your concurrent tasks.**

## The Problem: "Is it stuck?"

Launching 10,000 tasks into the background is terrifying.
*   Is it working?
*   How fast is it going?
*   Will it finish today or next week?
*   Did it hang?

## The Solution: Zero-Config Visibility

Concurry wraps the excellent `tqdm` library to give you state-aware, multi-environment progress bars that just work.

---

## Quick Start

### 1. The Wrapper (Simplest)
Just wrap your iterable.

```python
from concurry.utils.progress import ProgressBar
import time

items = range(100)

# Wraps any iterable (list, range, generator)
for item in ProgressBar(items, desc="Processing"):
    time.sleep(0.1)
```

### 2. With Workers (Automatic)
Both `gather` and `map` have built-in support.

```python
# "Show me progress for these 1000 tasks"
results = gather(futures, progress=True)
```

### 3. With Metadata
Add units and colors to make it meaningful.

```python
results = gather(
    futures,
    progress={
        "desc": "Downloading",
        "unit": "MB",
        "color": "green"
    }
)
```

---

## Smart Features

### Environment Detection
Concurry automatically detects where it is running and renders the appropriate bar:
*   **Terminal**: Standard ASCII/Unicode bar.
*   **Jupyter Notebook**: Beautiful HTML/JS widget.
*   **Ray Cluster**: Aggregated distributed progress (if configured).

### State Colors
The bar changes color to communicate status instantly:
*   **Blue**: Running
*   **Green**: Success
*   **Red**: Exception/Failure
*   **Grey**: Stopped/Cancelled

---

## Advanced Patterns

### 1. Manual Updates
For when you aren't iterating over a list.

```python
pbar = ProgressBar(total=100)

while keep_running:
    do_work()
    pbar.update(1)  # Increment by 1

pbar.success()  # Turn green!
```

### 2. Dynamic Descriptions
Tell the user *what* is happening right now.

```python
pbar = ProgressBar(items)
for item in pbar:
    # Updates the text next to the bar
    pbar.set_description(f"Processing {item.name}")
    process(item)
```

### 3. Nested Progress
Track batches and items simultaneously.

```python
for batch in ProgressBar(batches, desc="Overall"):
    for item in ProgressBar(batch, desc="Current Batch", leave=False):
        process(item)
```

## See Also
*   [**Synchronization**](synchronization.md) for using progress with `gather()`.
