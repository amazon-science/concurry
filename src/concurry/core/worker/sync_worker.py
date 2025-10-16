"""Synchronous worker implementation for concurry."""

import asyncio
import inspect
from typing import Any

from pydantic import PrivateAttr

from ..future import SyncFuture
from .base_worker import WorkerProxy, _create_worker_wrapper, _unwrap_futures_in_args


def _invoke_function(fn, *args, **kwargs):
    """Invoke a function, handling both sync and async functions.

    For async functions, this will run them using asyncio.run().
    """
    if inspect.iscoroutinefunction(fn):
        # Run async function using asyncio.run()
        return asyncio.run(fn(*args, **kwargs))
    else:
        # Run sync function directly
        return fn(*args, **kwargs)


class SyncWorkerProxy(WorkerProxy):
    """Worker proxy for synchronous execution.

    This proxy executes all methods synchronously in the current thread
    and wraps results in SyncFuture for API consistency.

    **Performance Optimizations for Tight Loops:**

    - Method wrapper caching
    - Inlined execution
    - Fast-path future unwrapping

    **Exception Handling:**

    - Setup errors (e.g., `AttributeError` for non-existent methods) fail immediately
    - Execution errors are stored in the `SyncFuture` and raised when `result()` is called
    - Original exception types and messages are preserved

    **Async Function Support:**

    Sync workers can execute async functions correctly using `asyncio.run()`.
    Execution is synchronous - no concurrency benefits. Useful for testing async code.

    **Example:**

        ```python
        import asyncio

        class MyWorker(Worker):
            async def async_method(self, x: int) -> int:
                await asyncio.sleep(0.01)
                return x * 2

        w = MyWorker.options(mode="sync").init()
        result = w.async_method(5).result()  # Works correctly, returns 10

        try:
            result = w.some_method().result()
        except ValueError as e:
            # Original ValueError is raised, not wrapped
            print(f"Got error: {e}")

        w.stop()
        ```
    """

    # Private attributes (use Any for non-serializable types)
    _worker: Any = PrivateAttr()

    def post_initialize(self) -> None:
        """Initialize private attributes after Typed validation."""
        super().post_initialize()

        # Create worker wrapper with limits and retry logic if needed
        # (limits and retry_config already processed by WorkerBuilder)
        worker_cls = _create_worker_wrapper(self.worker_cls, self.limits, self.retry_config)

        # Create the worker instance directly
        self._worker = worker_cls(*self.init_args, **self.init_kwargs)

    def __getattr__(self, name: str):
        """Intercept method calls with caching for maximum performance.

        This optimized implementation:
        1. Caches method wrappers after first access
        2. Inlines execution logic to reduce call stack depth
        3. Fast-path checks for futures before unwrapping
        """
        # Check cache first (saves ~0.7µs on subsequent calls)
        cache = self.__dict__.get("_method_cache")
        if cache is not None and name in cache:
            return cache[name]

        # Don't intercept private/dunder methods
        if name.startswith("_"):
            return super().__getattr__(name)

        # Create optimized method wrapper with inlined logic
        def method_wrapper(*args, **kwargs):
            # Check if stopped
            if self._stopped:
                raise RuntimeError("Worker is stopped")

            # Get method and validate (inline to save call stack)
            method = getattr(self._worker, name)
            if not callable(method):
                raise AttributeError(f"'{self.worker_cls.__name__}' has no callable method '{name}'")

            # Unwrap futures if needed (fast-path handled in _unwrap_futures_in_args)
            unwrapped_args, unwrapped_kwargs = _unwrap_futures_in_args(args, kwargs, self.unwrap_futures)

            # Execute directly (inline to save ~0.3µs)
            try:
                result = _invoke_function(method, *unwrapped_args, **unwrapped_kwargs)
                future = SyncFuture(result_value=result)
            except Exception as e:
                future = SyncFuture(exception_value=e)

            # Return result or future depending on blocking mode
            return future.result() if self.blocking else future

        # Cache the wrapper for next time
        if cache is not None:
            cache[name] = method_wrapper

        return method_wrapper

    def _execute_method(self, method_name: str, *args: Any, **kwargs: Any) -> SyncFuture:
        """Execute a method synchronously and wrap result in SyncFuture.

        Args:
            method_name: Name of the method to invoke
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            SyncFuture with the result or exception

        Raises:
            AttributeError: If the method doesn't exist or isn't callable (immediate failure)
        """
        # Validate method exists and is callable - these errors should propagate immediately
        method = getattr(self._worker, method_name)
        if not callable(method):
            raise AttributeError(f"'{self.worker_cls.__name__}' has no callable method '{method_name}'")

        # Delegate to _execute_task to avoid code duplication
        return self._execute_task(method, *args, **kwargs)

    def _execute_task(self, fn, *args: Any, **kwargs: Any) -> SyncFuture:
        """Execute an arbitrary function synchronously and wrap result in SyncFuture.

        This method applies retry logic for TaskWorker.submit() and TaskWorker.map().
        The retry logic is applied here (not in submit()) to avoid double-wrapping,
        since submit() would also be wrapped by __getattribute__ retry logic.

        Args:
            fn: Callable function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            SyncFuture with the result or exception

        Raises:
            TypeError: If fn is not callable (immediate failure)
        """
        # Validate that fn is callable - this error should propagate immediately
        if not callable(fn):
            raise TypeError(f"fn must be callable, got {type(fn).__name__}")

        # Unwrap any BaseFuture instances in args/kwargs
        args, kwargs = _unwrap_futures_in_args(args, kwargs, self.unwrap_futures)

        # Execute the function and wrap any execution errors in the future
        try:
            # Apply retry logic if configured (for TaskWorker functions)
            if self.retry_config is not None and self.retry_config.num_retries > 0:
                from ..retry import execute_with_retry_auto

                context = {
                    "method_name": fn.__name__ if hasattr(fn, "__name__") else "anonymous_function",
                    "worker_class_name": "TaskWorker",
                }
                # execute_with_retry_auto handles both sync and async functions automatically
                result = execute_with_retry_auto(fn, args, kwargs, self.retry_config, context)
            else:
                result = _invoke_function(fn, *args, **kwargs)
            return SyncFuture(result_value=result)
        except Exception as e:
            return SyncFuture(exception_value=e)

    def stop(self, timeout: float = 30) -> None:
        """Stop the worker.

        For sync workers, this just marks the worker as stopped.

        Args:
            timeout: Ignored for sync workers
        """
        super().stop(timeout)
