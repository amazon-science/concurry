# Progress API Reference

Complete API reference for Concurry's progress tracking.

## Module: `concurry.utils.progress`

### Classes

#### ProgressBar

::: concurry.utils.progress.ProgressBar
    options:
      show_source: true
      members:
        - __init__
        - update
        - set_n
        - set_total
        - set_description
        - set_unit
        - success
        - stop
        - failure
        - refresh
        - close

---

## Usage Examples

### Basic Usage

```python
from concurry.utils.progress import ProgressBar
import time

# Wrap an iterable
for item in ProgressBar(range(100), desc="Processing"):
    time.sleep(0.01)
# Automatically shows success!

# Manual progress bar
pbar = ProgressBar(total=100, desc="Manual")
for i in range(100):
    time.sleep(0.01)
    pbar.update(1)
pbar.success("Complete!")
```

### Customization

```python
from concurry.utils.progress import ProgressBar

# Custom colors and settings
pbar = ProgressBar(
    total=100,
    desc="Processing",
    unit="files",
    color="#9c27b0",  # Purple
    ncols=120,
    smoothing=0.1,
    miniters=5
)

for i in range(100):
    pbar.update(1)

pbar.success()
```

### State Management

```python
from concurry.utils.progress import ProgressBar

pbar = ProgressBar(total=100, desc="Processing")

try:
    for i in range(100):
        if error_condition:
            raise ValueError("Error occurred")
        process(i)
        pbar.update(1)
    pbar.success("All done!")  # Green
except Exception as e:
    pbar.failure(f"Failed: {e}")  # Red
    raise
```

### Dynamic Updates

```python
from concurry.utils.progress import ProgressBar

pbar = ProgressBar(total=100, desc="Phase 1")

for i in range(100):
    # Update description dynamically
    if i == 50:
        pbar.set_description("Phase 2")
    
    # Update total if needed
    if i == 75 and more_work_discovered:
        pbar.set_total(150)
    
    pbar.update(1)

pbar.success()
```

## Constructor Parameters

### Required Parameters

- `total` (int, optional): Total number of items to process. Required for manual progress bars.

### Optional Parameters

#### Display Options
- `desc` (str): Description shown before the progress bar
- `unit` (str): Unit name for items being processed (default: "row")
- `color` (str): Hex color code for the progress bar (default: "#0288d1")
- `ncols` (int): Width of the progress bar in characters (default: 100)

#### Behavior Options
- `style` (Literal["auto", "notebook", "std", "ray"]): Progress bar style (default: "auto")
- `smoothing` (float): Smoothing factor for progress updates (default: 0.15)
- `disable` (bool): Whether to disable the progress bar (default: False)
- `miniters` (int): Minimum iterations between updates (default: 1)

#### Advanced Options
- `pbar` (TqdmProgressBar, optional): Existing tqdm progress bar instance
- `progress_bar` (Union[bool, dict, ProgressBar]): Progress bar configuration or instance
- `prefer_kwargs` (bool): Whether to prefer kwargs over progress_bar dict (default: True)

## Methods

### update()

Update the progress bar by n steps.

```python
pbar.update(n=1)  # Increment by 1
pbar.update(10)   # Increment by 10
```

**Parameters:**
- `n` (int): Number of steps to increment (default: 1)

**Returns:**
- `Optional[bool]`: True if display was updated, None if buffered

---

### set_n()

Set the current progress value directly.

```python
pbar.set_n(50)  # Set to 50
```

**Parameters:**
- `new_n` (int): New progress value

---

### set_total()

Set the total number of items.

```python
pbar.set_total(200)  # Change total to 200
```

**Parameters:**
- `new_total` (int): New total value

---

### set_description()

Set the description text.

```python
pbar.set_description("Phase 2")
pbar.set_description("Processing files", refresh=True)
```

**Parameters:**
- `desc` (str, optional): New description
- `refresh` (bool, optional): Whether to refresh display (default: True)

**Returns:**
- `Optional[str]`: The previous description

---

### set_unit()

Set the unit name.

```python
pbar.set_unit("files")
pbar.set_unit("MB")
```

**Parameters:**
- `new_unit` (str): New unit name

---

### success()

Mark the progress bar as successful (green color).

```python
pbar.success()  # Default success message
pbar.success("All done!")  # Custom message
pbar.success("Complete", close=False)  # Keep open
```

**Parameters:**
- `desc` (str, optional): Success message
- `close` (bool): Whether to close the progress bar (default: True)
- `append_desc` (bool): Whether to append to existing description (default: True)

---

### failure()

Mark the progress bar as failed (red color).

```python
pbar.failure()  # Default failure message
pbar.failure("Error occurred!")  # Custom message
```

**Parameters:**
- `desc` (str, optional): Failure message
- `close` (bool): Whether to close the progress bar (default: True)
- `append_desc` (bool): Whether to append to existing description (default: True)

---

### stop()

Mark the progress bar as stopped (grey color).

```python
pbar.stop()  # Default stop message
pbar.stop("Cancelled by user")  # Custom message
```

**Parameters:**
- `desc` (str, optional): Stop message
- `close` (bool): Whether to close the progress bar (default: True)
- `append_desc` (bool): Whether to append to existing description (default: True)

---

### refresh()

Manually refresh the progress bar display.

```python
pbar.refresh()
```

---

### close()

Close and clean up the progress bar.

```python
pbar.close()
```

## Type Signatures

### ProgressBar Class

```python
from concurry.utils.progress import ProgressBar
from typing import Literal, Optional, Union

class ProgressBar:
    def __init__(
        self,
        *args,
        pbar: Optional[TqdmProgressBar] = None,
        style: Literal["auto", "notebook", "std", "ray"] = "auto",
        unit: str = "row",
        color: str = "#0288d1",
        ncols: int = 100,
        smoothing: float = 0.15,
        total: Optional[int] = None,
        disable: bool = False,
        miniters: int = 1,
        progress_bar: Union[bool, dict, ProgressBar] = True,
        prefer_kwargs: bool = True,
        **kwargs,
    ) -> None:
        ...
    
    def update(self, n: int = 1) -> Optional[bool]:
        ...
    
    def set_n(self, new_n: int) -> None:
        ...
    
    def set_total(self, new_total: int) -> None:
        ...
    
    def set_description(
        self,
        desc: Optional[str] = None,
        refresh: Optional[bool] = True
    ) -> Optional[str]:
        ...
    
    def set_unit(self, new_unit: str) -> None:
        ...
    
    def success(
        self,
        desc: Optional[str] = None,
        close: bool = True,
        append_desc: bool = True
    ) -> None:
        ...
    
    def stop(
        self,
        desc: Optional[str] = None,
        close: bool = True,
        append_desc: bool = True
    ) -> None:
        ...
    
    def failure(
        self,
        desc: Optional[str] = None,
        close: bool = True,
        append_desc: bool = True
    ) -> None:
        ...
    
    def refresh(self) -> None:
        ...
    
    def close(self) -> None:
        ...
```

## Progress Bar Styles

### auto

Automatically detects the environment:
- Uses notebook style in Jupyter/IPython
- Uses standard terminal style otherwise

### notebook

Optimized for Jupyter notebooks with rich HTML widgets.

### std

Standard terminal-based progress bar.

### ray

Integrates with Ray's distributed progress tracking (requires Ray).

## Color Codes

Default colors for different states:

- **Progress**: `#0288d1` (blue)
- **Success**: `#43a047` (green)
- **Failure**: `#e64a19` (red)
- **Stop**: `#b0bec5` (grey)

Custom colors can be specified as hex codes:

```python
pbar = ProgressBar(total=100, color="#9c27b0")  # Purple
```

## See Also

- [Progress User Guide](../user-guide/progress.md) - Learn how to use progress bars
- [Quick Recipes](../user-guide/getting-started.md#quick-recipes) - See common usage patterns
- [Futures API](futures.md) - Futures API reference

