"""Progress tracking utilities for concurry."""

import inspect
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    ItemsView,
    Iterator,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
)

from tqdm.auto import tqdm as AutoTqdmProgressBar
from tqdm.autonotebook import tqdm as NotebookTqdmProgressBar
from tqdm.std import tqdm as StdTqdmProgressBar

from .frameworks import _IS_IPYWIDGETS_INSTALLED, _IS_RAY_INSTALLED


def get_args_and_kwargs(fn: Callable) -> Set[str]:
    argspec: inspect.FullArgSpec = inspect.getfullargspec(fn)  ## Ref: https://stackoverflow.com/a/218709
    args: Tuple[str, ...] = tuple(argspec.args if argspec.args is not None else [])
    kwargs: Tuple[str, ...] = tuple(argspec.kwonlyargs if argspec.kwonlyargs is not None else [])
    return set.union(set(args), set(kwargs))


TqdmProgressBar = Union[AutoTqdmProgressBar, NotebookTqdmProgressBar, StdTqdmProgressBar]


class ProgressBar:
    """A progress bar implementation using tqdm with additional features.

    This class provides beautiful, informative progress bars with rich features including:
    - Automatic success/failure/stop state indicators with color coding
    - Multiple styles (auto, notebook, standard, Ray)
    - Iterable wrapping for easy integration
    - Fine-grained control over updates
    - Customizable appearance

    Example:
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
    """

    def __init__(
        self,
        *args,
        pbar: Optional[TqdmProgressBar] = None,
        style: Literal["auto", "notebook", "std", "ray"] = "auto",
        unit: str = "row",
        color: str = "#0288d1",  # Bluish
        ncols: int = 100,
        smoothing: float = 0.15,
        total: Optional[int] = None,
        disable: bool = False,
        miniters: int = 1,
        progress_bar: Union[bool, dict, "ProgressBar"] = True,
        prefer_kwargs: bool = True,
        **kwargs,
    ):
        """Initialize a progress bar.

        Args:
            pbar: Optional existing tqdm progress bar instance
            style: Progress bar style. Options:
                - "auto": Automatically detect environment (default)
                - "notebook": Optimized for Jupyter notebooks
                - "std": Standard terminal output
                - "ray": Ray distributed progress (requires Ray)
            unit: Unit name for items being processed (default: "row")
            color: Hex color code for the progress bar (default: "#0288d1" - blue)
            ncols: Width of the progress bar in characters (default: 100)
            smoothing: Smoothing factor for progress updates (default: 0.15)
            total: Total number of items to process
            disable: Whether to disable the progress bar (default: False)
            miniters: Minimum iterations between display updates (default: 1)
                Higher values improve performance for large iteration counts
            progress_bar: Progress bar configuration:
                - True: Use default configuration
                - False/None: Disable progress bar
                - dict: Configuration dictionary
                - ProgressBar: Existing ProgressBar instance to reuse
            prefer_kwargs: Whether to prefer kwargs over progress_bar dict (default: True)
            **kwargs: Additional arguments to pass to tqdm

        Example:
            ```python
            # Basic usage
            pbar = ProgressBar(total=100, desc="Processing")

            # Custom colors and styling
            pbar = ProgressBar(
                total=100,
                desc="Custom",
                unit="files",
                color="#9c27b0",  # Purple
                ncols=120,
                miniters=5
            )

            # Disable for non-interactive environments
            pbar = ProgressBar(total=100, disable=True)
            ```
        """
        # Initialize _extra_fields first using object.__setattr__
        object.__setattr__(self, "_extra_fields", {})

        # Handle progress_bar parameter
        if isinstance(progress_bar, ProgressBar):
            if prefer_kwargs:
                if "total" in kwargs:
                    progress_bar.set_total(kwargs["total"])
                if "initial" in kwargs:
                    progress_bar.set_n(kwargs["initial"])
                if "desc" in kwargs:
                    progress_bar.set_description(kwargs["desc"])
                if "unit" in kwargs:
                    progress_bar.set_unit(kwargs["unit"])
            self.__dict__.update(progress_bar.__dict__)
            return

        if progress_bar is not None and not isinstance(progress_bar, (bool, dict)):
            raise ValueError(
                f"You must pass `progress_bar` as either a bool, dict or None (None or False disables it). "
                f"Found: {type(progress_bar)}"
            )

        if progress_bar is True:
            progress_bar = dict()
        elif progress_bar is False:
            progress_bar = None

        if progress_bar is not None and not isinstance(progress_bar, dict):
            raise ValueError(
                "You must pass `progress_bar` as either a bool, dict or None. None or False disables it."
            )

        if progress_bar is None:
            progress_bar = dict(disable=True)
        elif isinstance(progress_bar, dict) and len(kwargs) > 0:
            if prefer_kwargs is True:
                progress_bar = {
                    **progress_bar,
                    **kwargs,
                }
            else:
                progress_bar = {
                    **kwargs,
                    **progress_bar,
                }

        # Set instance attributes using object.__setattr__ to avoid recursion
        object.__setattr__(self, "pbar", pbar)
        object.__setattr__(self, "style", style)
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "color", color)
        object.__setattr__(self, "ncols", ncols)
        object.__setattr__(self, "smoothing", smoothing)
        object.__setattr__(self, "total", total)
        object.__setattr__(self, "disable", disable)
        object.__setattr__(self, "miniters", miniters)
        object.__setattr__(self, "_pending_updates", 0)

        # Validate miniters
        if self.miniters < 1:
            raise ValueError("miniters must be greater than or equal to 1")

        # Store extra fields
        for field_name, field_value in progress_bar.items():
            if not hasattr(self, field_name):
                self._extra_fields[field_name] = field_value

        # Create progress bar with current settings
        pbar = self._create_pbar(
            **{
                k: v
                for k, v in {**self.__dict__, **self._extra_fields}.items()
                if k not in {"pbar", "color", "_pending_updates", "_extra_fields"}
            }
        )
        pbar.color = self.color
        pbar.refresh()
        self.pbar = pbar

    @classmethod
    def _create_pbar(
        cls,
        style: Literal["auto", "notebook", "std", "ray"],
        **kwargs,
    ) -> TqdmProgressBar:
        """Create a tqdm progress bar with the specified style."""
        if style == "auto":
            if _IS_IPYWIDGETS_INSTALLED:
                kwargs["ncols"]: Optional[int] = None
            return AutoTqdmProgressBar(**kwargs)
        elif style == "notebook":
            if _IS_IPYWIDGETS_INSTALLED:
                kwargs["ncols"]: Optional[int] = None
            return NotebookTqdmProgressBar(**kwargs)
        elif _IS_RAY_INSTALLED and style == "ray":
            from ray.experimental import tqdm_ray

            kwargs = {k: v for k, v in kwargs.items() if k in get_args_and_kwargs(tqdm_ray.tqdm)}
            return tqdm_ray.tqdm(**kwargs)
        else:
            return StdTqdmProgressBar(**kwargs)

    def __new__(cls, *args, **kwargs):
        """Handle both direct instantiation and iterable wrapping."""
        # If first argument is an iterable (but not a string), use iter
        if (
            len(args) > 0
            and hasattr(args[0], "__iter__")
            and not isinstance(args[0], (str, bytes, bytearray))
        ):
            return cls._iter(args[0], **kwargs)
        return super().__new__(cls)

    @classmethod
    def _iter(
        cls,
        iterable: Union[Generator, Iterator, List, Tuple, Set, Dict, ItemsView],
        **kwargs,
    ):
        """Create a progress bar that wraps an iterable."""
        if isinstance(iterable, (list, tuple, dict, ItemsView, set, frozenset)):
            kwargs["total"] = len(iterable)
        if isinstance(iterable, dict):
            iterable: ItemsView = iterable.items()

        pbar = cls(**kwargs)

        try:
            for item in iterable:
                yield item
                pbar.update(1)
            pbar.success()
        except Exception as e:
            pbar.failure()
            raise e

    def update(self, n: int = 1) -> Optional[bool]:
        """Update the progress bar by n steps.

        Updates are batched based on the `miniters` parameter for better performance.
        The display only updates when enough iterations have accumulated.

        Args:
            n: Number of steps to increment (default: 1)

        Returns:
            True if the display was updated, None if the update was buffered

        Example:
            ```python
            pbar = ProgressBar(total=100)
            for i in range(100):
                # Do work
                pbar.update(1)
            ```
        """
        self._pending_updates += n
        if abs(self._pending_updates) >= self.miniters:
            out = self.pbar.update(n=self._pending_updates)
            self.refresh()
            self._pending_updates = 0
            return out
        else:
            return None

    def set_n(self, new_n: int) -> None:
        """Set the current progress value directly.

        This method sets the progress to an absolute value rather than incrementing.
        Useful for jumping to a specific point in the progress.

        Args:
            new_n: The new progress value

        Example:
            ```python
            pbar = ProgressBar(total=100)
            pbar.set_n(50)  # Jump to 50% complete
            ```
        """
        self.pbar.update(n=new_n - self.pbar.n)
        self._pending_updates = 0  # Clear all updates after setting new value
        self.refresh()

    def set_total(self, new_total: int) -> None:
        """Set the total number of items to process.

        This method allows dynamically updating the total when the final count
        becomes known or changes during processing.

        Args:
            new_total: The new total value

        Example:
            ```python
            pbar = ProgressBar(total=100)
            # ... process 50 items ...
            # More work discovered!
            pbar.set_total(150)
            ```
        """
        self.pbar.total = new_total
        self._pending_updates = 0  # Clear all updates after setting new value
        self.refresh()

    def set_description(self, desc: Optional[str] = None, refresh: Optional[bool] = True) -> Optional[str]:
        """Set the description of the progress bar.

        This method allows dynamically updating the description text during processing,
        useful for showing different phases or stages.

        Args:
            desc: New description text
            refresh: Whether to refresh the display (default: True)

        Returns:
            The previous description

        Example:
            ```python
            pbar = ProgressBar(total=100, desc="Phase 1")
            for i in range(50):
                pbar.update(1)
            pbar.set_description("Phase 2")
            for i in range(50):
                pbar.update(1)
            ```
        """
        out = self.pbar.set_description(desc=desc, refresh=refresh)
        self.refresh()
        return out

    def set_unit(self, new_unit: str) -> None:
        """Set the unit displayed in the progress bar.

        This method allows changing the unit name during processing, useful when
        switching between different types of work (e.g., files to MB).

        Args:
            new_unit: The new unit name (e.g., "files", "MB", "items")

        Example:
            ```python
            pbar = ProgressBar(total=100, unit="files")
            # ... process files ...
            pbar.set_unit("MB")  # Switch to showing MB processed
            ```
        """
        self.pbar.unit = new_unit
        self.refresh()

    def success(self, desc: Optional[str] = None, close: bool = True, append_desc: bool = True) -> None:
        """Mark the progress bar as successful (green color).

        This method completes the progress bar with a success indicator, changing
        the color to green and optionally adding a success message.

        Args:
            desc: Success message to display
            close: Whether to close the progress bar (default: True)
            append_desc: Whether to append to existing description (default: True)

        Example:
            ```python
            pbar = ProgressBar(total=100, desc="Processing")
            for i in range(100):
                pbar.update(1)
            pbar.success("All done!")  # Shows green bar with message
            ```
        """
        self._complete_with_status(
            color="#43a047",  # Dark Green
            desc=desc,
            close=close,
            append_desc=append_desc,
        )

    def stop(self, desc: Optional[str] = None, close: bool = True, append_desc: bool = True) -> None:
        """Mark the progress bar as stopped (grey color).

        This method completes the progress bar with a stop indicator, changing
        the color to grey and optionally adding a stop message. Useful for
        indicating early termination or cancellation.

        Args:
            desc: Stop message to display
            close: Whether to close the progress bar (default: True)
            append_desc: Whether to append to existing description (default: True)

        Example:
            ```python
            pbar = ProgressBar(total=100, desc="Processing")
            for i in range(100):
                if should_stop():
                    pbar.stop("Cancelled by user")  # Shows grey bar
                    break
                pbar.update(1)
            ```
        """
        self._complete_with_status(
            color="#b0bec5",  # Dark Grey
            desc=desc,
            close=close,
            append_desc=append_desc,
        )

    def failure(self, desc: Optional[str] = None, close: bool = True, append_desc: bool = True) -> None:
        """Mark the progress bar as failed (red color).

        This method completes the progress bar with a failure indicator, changing
        the color to red and optionally adding a failure message. Use in exception
        handlers to indicate errors.

        Args:
            desc: Failure message to display
            close: Whether to close the progress bar (default: True)
            append_desc: Whether to append to existing description (default: True)

        Example:
            ```python
            pbar = ProgressBar(total=100, desc="Processing")
            try:
                for i in range(100):
                    if error_occurred():
                        raise ValueError("Error")
                    pbar.update(1)
                pbar.success()
            except Exception as e:
                pbar.failure(f"Failed: {e}")  # Shows red bar
                raise
            ```
        """
        self._complete_with_status(
            color="#e64a19",  # Dark Red
            desc=desc,
            close=close,
            append_desc=append_desc,
        )

    def _complete_with_status(
        self,
        color: str,
        desc: Optional[str],
        close: bool,
        append_desc: bool,
    ) -> None:
        """Complete the progress bar with a status."""
        if getattr(self.pbar, "disable", None) is False:
            self.pbar.update(n=self._pending_updates)
            self._pending_updates = 0
            self.color = color
            self.pbar.colour = color
            if desc is not None:
                if append_desc:
                    desc: str = f"[{desc}] {self.pbar.desc}"
                self.pbar.desc = desc
            self.pbar.refresh()
            if close:
                self.close()

    def refresh(self) -> None:
        """Refresh the progress bar display.

        This method manually updates the progress bar display. It's typically
        called automatically by other methods, but can be called manually if needed.

        Example:
            ```python
            pbar = ProgressBar(total=100)
            pbar.color = "#9c27b0"  # Change color
            pbar.refresh()  # Apply the change
            ```
        """
        self.pbar.colour = self.color
        self.pbar.refresh()

    def close(self) -> None:
        """Close and clean up the progress bar.

        This method closes the progress bar and releases any resources. It's
        automatically called by success(), failure(), and stop() when close=True.

        Example:
            ```python
            pbar = ProgressBar(total=100)
            try:
                for i in range(100):
                    pbar.update(1)
            finally:
                pbar.close()  # Ensure cleanup
            ```
        """
        self.pbar.refresh()
        self.pbar.close()
        self.pbar.refresh()

    def __del__(self) -> None:
        """Clean up the progress bar when the object is deleted."""
        if hasattr(self, "pbar") and self.pbar is not None:
            self.pbar.close()

    def __getattr__(self, name: str) -> Any:
        """Handle access to extra fields."""
        # Use object.__getattribute__ to avoid recursion
        extra_fields = object.__getattribute__(self, "_extra_fields")
        if name in extra_fields:
            return extra_fields[name]
        raise AttributeError(f"'{self.__class__.__name__}' has no attribute '{name}'")

    def __setattr__(self, name: str, value: Any) -> None:
        """Handle setting extra fields."""
        # Use object.__getattribute__ to avoid recursion
        if name == "_extra_fields":
            object.__setattr__(self, name, value)
            return

        if name in self.__dict__ or name in self.__class__.__dict__:
            object.__setattr__(self, name, value)
        else:
            extra_fields = object.__getattribute__(self, "_extra_fields")
            extra_fields[name] = value
