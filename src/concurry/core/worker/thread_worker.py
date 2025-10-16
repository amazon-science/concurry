"""Thread-based worker implementation for concurry."""

import asyncio
import inspect
import queue
import threading
from concurrent.futures import Future as PyFuture
from typing import Any, Dict

from pydantic import PrivateAttr

from ..future import ConcurrentFuture
from .base_worker import WorkerProxy, _create_worker_wrapper, _unwrap_futures_in_args


def _invoke_function(fn, *args, **kwargs):
    """Invoke a function, handling both sync and async functions.

    For async functions, this will run them using asyncio.run().
    Note: This provides basic support for async functions in thread workers,
    but won't provide the same performance benefits as AsyncioWorkerProxy.

    TODO: For true async performance in thread workers, we would need to run
    a persistent event loop in the worker thread, which would be a major implementation change.
    """
    if inspect.iscoroutinefunction(fn):
        # Run async function using asyncio.run()
        return asyncio.run(fn(*args, **kwargs))
    else:
        # Run sync function directly
        return fn(*args, **kwargs)


class ThreadWorkerProxy(WorkerProxy):
    """Worker proxy for thread-based execution.

    This proxy runs the worker in a dedicated thread and communicates
    via thread-safe queues.

    **Exception Handling:**

    - Setup errors (e.g., `AttributeError` for non-existent methods) are raised via futures
    - Execution errors are passed through the result queue and raised when `result()` is called
    - Original exception types and messages are preserved

    **Async Function Support:**

    Thread workers can execute async functions correctly using `asyncio.run()`.
    However, they won't provide concurrency benefits as each async call blocks the
    worker thread. Use `AsyncioWorkerProxy` for best async performance.

    **Example:**

        ```python
        import asyncio

        class MyWorker(Worker):
            async def async_method(self, x: int) -> int:
                await asyncio.sleep(0.01)
                return x * 2

        w = MyWorker.options(mode="thread").init()
        result = w.async_method(5).result()  # Works correctly, returns 10

        # Exceptions preserve their original type
        try:
            w.failing_method().result()
        except ValueError as e:
            # Original ValueError is raised
            print(f"Got error: {e}")

        w.stop()
        ```
    """

    # Private attributes (use Any for non-serializable types)
    _command_queue: Any = PrivateAttr()
    _futures: Dict[str, Any] = PrivateAttr()  # Maps future.uuid -> ConcurrentFuture
    _futures_lock: Any = PrivateAttr()
    _thread: Any = PrivateAttr()

    def post_initialize(self) -> None:
        """Initialize private attributes after Typed validation."""
        super().post_initialize()

        # Create queues for communication
        self._command_queue = queue.Queue()
        self._futures = {}  # future.uuid -> ConcurrentFuture
        self._futures_lock = threading.Lock()

        # Start worker thread
        self._thread = threading.Thread(target=self._worker_thread_main, daemon=True)
        self._thread.start()

        # Wait for initialization to complete
        self._wait_for_initialization()

    def _wait_for_initialization(self):
        """Wait for worker thread to initialize."""

        # Create future and wrap in ConcurrentFuture
        py_future = PyFuture()
        future = ConcurrentFuture(future=py_future)

        with self._futures_lock:
            self._futures[future.uuid] = future

        self._command_queue.put((future.uuid, "__initialize__", (), {}))

        try:
            future.result(timeout=30)
        except Exception as e:
            raise RuntimeError(f"Worker initialization failed: {e}")

    def _worker_thread_main(self):
        """Main function for the worker thread."""
        worker = None

        while not self._stopped:
            try:
                # Get command with timeout to allow checking stopped flag
                try:
                    command = self._command_queue.get(timeout=0.1)
                except queue.Empty:
                    continue

                if command is None:
                    break

                request_id, method_name, args, kwargs = command

                # Get future for this request
                with self._futures_lock:
                    future = self._futures.get(request_id)

                if future is None:
                    continue

                try:
                    if method_name == "__initialize__":
                        # Create worker wrapper with limits and retry logic if needed
                        # (limits and retry_config already processed by WorkerBuilder)

                        worker_cls = _create_worker_wrapper(self.worker_cls, self.limits, self.retry_config)

                        worker = worker_cls(*self.init_args, **self.init_kwargs)
                        future._future.set_result(None)
                        with self._futures_lock:
                            self._futures.pop(request_id, None)
                        continue

                    if method_name == "__task__":
                        # Execute arbitrary function with optional retry logic
                        # Retry logic is applied here (not in submit()) to avoid double-wrapping
                        fn, task_args, task_kwargs = args
                        if not callable(fn):
                            future._future.set_exception(
                                TypeError(f"fn must be callable, got {type(fn).__name__}")
                            )
                            with self._futures_lock:
                                self._futures.pop(request_id, None)
                            continue

                        # Apply retry logic if configured (for TaskWorker functions)
                        if self.retry_config is not None and self.retry_config.num_retries > 0:
                            from ..retry import execute_with_retry_auto

                            context = {
                                "method_name": fn.__name__
                                if hasattr(fn, "__name__")
                                else "anonymous_function",
                                "worker_class_name": "TaskWorker",
                            }
                            # execute_with_retry_auto handles both sync and async functions automatically
                            result = execute_with_retry_auto(
                                fn, task_args, task_kwargs, self.retry_config, context
                            )
                        else:
                            result = _invoke_function(fn, *task_args, **task_kwargs)

                        future._future.set_result(result)
                        with self._futures_lock:
                            self._futures.pop(request_id, None)
                        continue

                    if worker is None:
                        future._future.set_exception(RuntimeError("Worker not initialized"))
                        with self._futures_lock:
                            self._futures.pop(request_id, None)
                        continue

                    method = getattr(worker, method_name)
                    if not callable(method):
                        future._future.set_exception(
                            AttributeError(
                                f"'{self.worker_cls.__name__}' has no callable method '{method_name}'"
                            )
                        )
                        with self._futures_lock:
                            self._futures.pop(request_id, None)
                        continue

                    result = _invoke_function(method, *args, **kwargs)
                    future._future.set_result(result)
                    with self._futures_lock:
                        self._futures.pop(request_id, None)
                except Exception as e:
                    future._future.set_exception(e)
                    with self._futures_lock:
                        self._futures.pop(request_id, None)

            except Exception:
                # Catch any unexpected exceptions to keep thread alive
                break

    def _execute_method(self, method_name: str, *args: Any, **kwargs: Any):
        """Execute a method in the worker thread.

        Optimized with:
        - Fast-path future unwrapping check
        - Minimized locked section
        - Reduced queue operation overhead

        Args:
            method_name: Name of the method to invoke
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            ConcurrentFuture for the method execution
        """
        # Unwrap futures if needed (fast-path handled in _unwrap_futures_in_args)
        args, kwargs = _unwrap_futures_in_args(args, kwargs, self.unwrap_futures)

        # Create future and wrap in ConcurrentFuture immediately
        py_future = PyFuture()
        future = ConcurrentFuture(future=py_future)

        # Minimize locked section
        with self._futures_lock:
            self._futures[future.uuid] = future

        self._command_queue.put((future.uuid, method_name, args, kwargs))

        return future

    def _execute_task(self, fn, *args: Any, **kwargs: Any):
        """Execute an arbitrary function in the worker thread.

        Optimized with:
        - Fast-path future unwrapping check
        - Minimized locked section
        - Reduced queue operation overhead

        Args:
            fn: Callable function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            ConcurrentFuture for the task execution
        """
        # Unwrap futures if needed (fast-path handled in _unwrap_futures_in_args)
        args, kwargs = _unwrap_futures_in_args(args, kwargs, self.unwrap_futures)

        # Create future and wrap in ConcurrentFuture immediately
        py_future = PyFuture()
        future = ConcurrentFuture(future=py_future)

        # Minimize locked section
        with self._futures_lock:
            self._futures[future.uuid] = future

        # Send task command with special marker
        self._command_queue.put((future.uuid, "__task__", (fn, args, kwargs), {}))

        return future

    def stop(self, timeout: float = 30) -> None:
        """Stop the worker thread.

        Args:
            timeout: Maximum time to wait for thread to stop in seconds
        """
        super().stop(timeout)

        # Cancel all pending futures
        with self._futures_lock:
            for future in self._futures.values():
                future.cancel()
            self._futures.clear()

        self._command_queue.put(None)
        self._thread.join(timeout=timeout)
