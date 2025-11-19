"""Task decorator for function-level worker creation."""

from typing import Any, Callable, Union

from morphic import get_fn_args, validate

from ...utils import _NO_ARG, _NO_ARG_TYPE
from ..constants import ExecutionMode
from .task_worker import TaskWorker


@validate
def task(
    *,
    mode: ExecutionMode,
    on_demand: Union[bool, _NO_ARG_TYPE] = _NO_ARG,
    **kwargs: Any,
) -> Callable:
    """Decorator to create a TaskWorker bound to a function.

    This decorator **transforms** the decorated function into a `TaskWorker` instance.
    The original function is bound to this worker.

    **Crucial Behavior**:
    1. The decorated symbol is **no longer a function**, but a **TaskWorker instance**.
    2. Calling the decorated symbol invokes `worker.submit()`, returning a `Future`.
    3. You **must** call `.stop()` on the decorated symbol to clean up resources.
    4. All worker configuration (e.g., `mode`) is **required** here to initialize the worker.

    Args:
        mode: Execution mode (sync, thread, process, asyncio, ray).
            Defaults to ExecutionMode.Sync
        on_demand: Create workers on-demand. If not specified, uses
            global_config.defaults.task_decorator_on_demand (defaults to True).
            Note: on_demand is automatically set to False for Sync and Asyncio modes.
        **kwargs: All other Worker.options() parameters are supported.

    Returns:
        Initialized TaskWorker instance bound to the decorated function

    Examples:
        Basic Usage:
            ```python
            from concurry import task

            # process_item becomes a TaskWorker instance
            @task(mode="thread", max_workers=4)
            def process_item(x):
                return x ** 2

            # Call like a function -> actually calls worker.submit()
            future = process_item(10)
            result = future.result()  # 100

            # Explicitly STOP the worker when done
            process_item.stop()
            ```

        With Limits:
            ```python
            from concurry import task, RateLimit

            limits = [RateLimit(key="api", capacity=100, window_seconds=60)]

            @task(mode="thread", limits=limits)
            def call_api(prompt, limits):  # limits param detected automatically
                with limits.acquire(requested={"api": 1}):
                    return external_api(prompt)

            result = call_api("Hello").result()
            ```

        With Progress Bar:
            ```python
            @task(mode="process", max_workers=4)
            def compute(x):
                return x ** 2

            results = list(compute.map(range(1000), progress=True))
            ```
    """
    # Import here to avoid circular imports
    from ...config import global_config

    local_config = global_config.clone()
    # Apply default for on_demand if not specified
    if on_demand is _NO_ARG:
        # on_demand is not supported for Sync and Asyncio modes
        if mode in (ExecutionMode.Sync, ExecutionMode.Asyncio):
            on_demand = False
        else:
            on_demand = local_config.defaults.task_decorator_on_demand
    on_demand: bool = bool(on_demand)

    def decorator(fn: Callable) -> TaskWorker:
        # Check if function accepts 'limits' parameter
        fn_args = get_fn_args(fn)
        has_limits_param = "limits" in fn_args

        # Extract limits from kwargs if present
        limits = kwargs.pop("limits", None)

        # If function has limits param and limits were provided, wrap function
        if has_limits_param and limits is not None:
            # Store original function
            original_fn = fn

            # Create wrapper that injects limits
            def wrapper_with_limits(*args, **fn_kwargs):
                # Only inject if not already provided
                if "limits" not in fn_kwargs:
                    # Get limits from worker instance (stored as closure variable)
                    fn_kwargs["limits"] = wrapper_with_limits._worker_limits
                return original_fn(*args, **fn_kwargs)

            # Mark wrapper to get limits later
            wrapper_with_limits._needs_limits = True
            wrapper_with_limits._original_fn = original_fn
            fn = wrapper_with_limits

        # Create worker options
        # Note: limits was already popped from kwargs earlier
        builder = TaskWorker.options(
            mode=mode,
            on_demand=on_demand,
            limits=limits,
            **kwargs,
        )

        # Initialize worker with the function
        worker = builder.init(fn=fn)

        # If we created a wrapper that needs limits, inject them now
        if hasattr(fn, "_needs_limits") and fn._needs_limits:
            # Access limits from the worker's underlying proxy
            # The worker is a WorkerProxy/WorkerProxyPool instance
            if hasattr(worker, "limits"):
                fn._worker_limits = worker.limits
            else:
                # For pools, we need to get limits from the first worker or the pool itself
                # Since limits are passed through, they should be accessible
                fn._worker_limits = None

        # Preserve function metadata
        if hasattr(fn, "_original_fn"):
            original = fn._original_fn
        else:
            original = fn

        worker.__name__ = original.__name__
        worker.__doc__ = original.__doc__
        worker.__module__ = original.__module__
        worker.__qualname__ = original.__qualname__

        # Add __del__ for cleanup
        def cleanup(self):
            try:
                if hasattr(self, "_stopped") and not self._stopped:
                    self.stop()
            except Exception:
                pass

        # Bind cleanup method to worker instance
        worker.__del__ = lambda: cleanup(worker)

        return worker

    return decorator
