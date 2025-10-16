"""Process-based worker implementation for concurry."""

import asyncio
import inspect
import multiprocessing as mp
import queue
import threading
import traceback
from concurrent.futures import Future as PyFuture
from typing import Any, Literal

import cloudpickle
from pydantic import PrivateAttr

from ..future import ConcurrentFuture
from .base_worker import WorkerProxy, _create_worker_wrapper, _unwrap_futures_in_args


def _invoke_function(fn, *args, **kwargs):
    """Invoke a function, handling both sync and async functions.

    For async functions, this will run them using asyncio.run().
    Note: This provides basic support for async functions in process workers,
    but won't provide the same performance benefits as AsyncioWorkerProxy.

    TODO: For true async performance in process workers, we would need to run
    a persistent event loop in each process, which would be a major implementation change.
    """
    if inspect.iscoroutinefunction(fn):
        # Run async function using asyncio.run()
        return asyncio.run(fn(*args, **kwargs))
    else:
        # Run sync function directly
        return fn(*args, **kwargs)


def _process_worker_main(
    worker_cls_bytes, init_args, init_kwargs, limits, retry_config, command_queue, result_queue
):
    """Main function for the worker process.

    Args:
        worker_cls_bytes: Cloudpickle-serialized worker class
        init_args: Positional arguments for worker initialization
        init_kwargs: Keyword arguments for worker initialization
        limits: LimitSet instance (or None)
        retry_config: RetryConfig instance (or None)
        command_queue: Queue for receiving commands
        result_queue: Queue for sending results
    """
    worker_cls = cloudpickle.loads(worker_cls_bytes)
    worker = None

    while True:
        try:
            command = command_queue.get()
            if command is None:
                break

            request_id, method_name, args, kwargs = command

            try:
                if method_name == "__initialize__":
                    # Create wrapper class with limits and retry logic if needed
                    actual_worker_cls = _create_worker_wrapper(worker_cls, limits, retry_config)

                    worker = actual_worker_cls(*init_args, **init_kwargs)
                    result_queue.put((request_id, "ok", None))
                    continue

                if method_name == "__task__":
                    # Execute arbitrary function with optional retry logic
                    # Retry logic is applied here (not in submit()) to avoid double-wrapping
                    fn_bytes, task_args, task_kwargs = args
                    fn = cloudpickle.loads(fn_bytes)
                    if not callable(fn):
                        raise TypeError(f"fn must be callable, got {type(fn).__name__}")

                    # Apply retry logic if configured (for TaskWorker functions)
                    if retry_config is not None and retry_config.num_retries > 0:
                        from ..retry import execute_with_retry_auto

                        context = {
                            "method_name": fn.__name__ if hasattr(fn, "__name__") else "anonymous_function",
                            "worker_class_name": "TaskWorker",
                        }
                        # execute_with_retry_auto handles both sync and async functions automatically
                        result = execute_with_retry_auto(fn, task_args, task_kwargs, retry_config, context)
                    else:
                        result = _invoke_function(fn, *task_args, **task_kwargs)

                    result_queue.put((request_id, "ok", result))
                    continue

                if worker is None:
                    raise RuntimeError("Worker not initialized")

                method = getattr(worker, method_name, None)
                if method is None or not callable(method):
                    raise AttributeError(f"Method '{method_name}' not found or not callable")

                result = _invoke_function(method, *args, **kwargs)
                result_queue.put((request_id, "ok", result))
            except Exception as e:
                tb_str = traceback.format_exc()
                result_queue.put((request_id, "error", (e, tb_str)))

        except Exception as e:
            # Catch any unexpected exceptions in the process loop
            try:
                result_queue.put((None, "error", (e, traceback.format_exc())))
            except Exception:
                pass
            break


class ProcessWorkerProxy(WorkerProxy):
    """Worker proxy for process-based execution.

    This proxy runs the worker in a dedicated process and communicates
    via multiprocessing queues with cloudpickle serialization.

    **Exception Handling:**

    - Setup errors (e.g., `AttributeError` for non-existent methods) are raised via futures
    - Execution errors are serialized across process boundaries and raised when `result()` is called
    - **Original exception types are preserved** (not wrapped in RuntimeError)
    - Exception tracebacks are preserved for debugging

    **Multiprocessing Context:**

    - `mp_context = "fork"`: Default on Unix-like systems (fastest, but not safe with threads)
    - `mp_context = "spawn"`: Recommended for cross-platform code
    - `mp_context = "forkserver"`: Hybrid approach

    **Async Function Support:**

    Process workers can execute async functions correctly using `asyncio.run()`.
    However, they won't provide concurrency benefits for async operations due to
    process isolation. Use `AsyncioWorkerProxy` for best async performance.

    **Example:**

        ```python
        import asyncio

        class MyWorker(Worker):
            async def async_method(self, x: int) -> int:
                await asyncio.sleep(0.01)
                return x * 2

        # Use default fork context
        w = MyWorker.options(mode="process").init()
        result = w.async_method(5).result()  # Works correctly, returns 10

        # Use spawn context (cross-platform)
        w = MyWorker.options(mode="process", mp_context="spawn").init()

        # Exceptions preserve their original type
        try:
            w.failing_method().result()
        except ValueError as e:
            # Original ValueError, not wrapped
            print(f"Got error: {e}")

        w.stop()
        ```
    """

    mp_context: Literal["fork", "spawn", "forkserver"] = "fork"

    # Private attributes (use Any for non-serializable types)
    _command_queue: Any = PrivateAttr()
    _result_queue: Any = PrivateAttr()
    _futures: dict = PrivateAttr()
    _futures_lock: Any = PrivateAttr()
    _process: Any = PrivateAttr()
    _result_thread: Any = PrivateAttr()

    def post_initialize(self) -> None:
        """Initialize private attributes after Typed validation."""
        super().post_initialize()

        # Create multiprocessing context using public field
        ctx = mp.get_context(self.mp_context)

        # Create queues for communication
        self._command_queue = ctx.Queue()
        self._result_queue = ctx.Queue()

        # Dictionary to track pending futures
        self._futures = {}
        self._futures_lock = threading.Lock()

        # Serialize the worker class
        worker_cls_bytes = cloudpickle.dumps(self.worker_cls)

        # Process limits for worker
        # Limits already processed by WorkerBuilder

        # Start worker process using public fields
        self._process = ctx.Process(
            target=_process_worker_main,
            args=(
                worker_cls_bytes,
                self.init_args,
                self.init_kwargs,
                self.limits,
                self.retry_config,
                self._command_queue,
                self._result_queue,
            ),
        )
        self._process.start()

        # Wait for initialization
        self._wait_for_initialization()

        # Start result handling thread
        self._result_thread = threading.Thread(target=self._handle_results, daemon=True)
        self._result_thread.start()

    def _wait_for_initialization(self):
        """Wait for worker process to initialize."""

        # Create future and wrap in ConcurrentFuture
        py_future = PyFuture()
        future = ConcurrentFuture(future=py_future)

        with self._futures_lock:
            self._futures[future.uuid] = py_future

        self._command_queue.put((future.uuid, "__initialize__", (), {}))

        try:
            request_id, status, payload = self._result_queue.get(timeout=30)
            if status == "error":
                e, tb_str = payload
                raise RuntimeError(f"Worker initialization failed:\n{tb_str}")
        except queue.Empty:
            raise RuntimeError("Worker initialization timed out")

    def _handle_results(self):
        """Thread that handles results from the worker process."""
        while not self._stopped:
            try:
                if not self._process.is_alive() and self._result_queue.empty():
                    break

                try:
                    item = self._result_queue.get(timeout=1)
                except queue.Empty:
                    continue
                except (ValueError, OSError):
                    # Queue was closed
                    break

                if item is None:
                    break

                request_id, status, payload = item

                with self._futures_lock:
                    py_future = self._futures.pop(request_id, None)

                if py_future is not None:
                    if status == "ok":
                        py_future.set_result(payload)
                    else:
                        # Set the original exception, not a wrapped version
                        e, tb_str = payload
                        py_future.set_exception(e)

            except Exception:
                # Any unexpected exception, exit the thread
                break

    def _execute_method(self, method_name: str, *args: Any, **kwargs: Any):
        """Execute a method in the worker process.

        Args:
            method_name: Name of the method to invoke
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            ConcurrentFuture for the method execution
        """
        # Unwrap any BaseFuture instances in args/kwargs
        args, kwargs = _unwrap_futures_in_args(args, kwargs, self.unwrap_futures)

        # Create future and wrap in ConcurrentFuture
        py_future = PyFuture()
        future = ConcurrentFuture(future=py_future)

        with self._futures_lock:
            self._futures[future.uuid] = py_future

        self._command_queue.put((future.uuid, method_name, args, kwargs))

        return future

    def _execute_task(self, fn, *args: Any, **kwargs: Any):
        """Execute an arbitrary function in the worker process.

        Args:
            fn: Callable function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            ConcurrentFuture for the task execution
        """
        # Unwrap any BaseFuture instances in args/kwargs
        args, kwargs = _unwrap_futures_in_args(args, kwargs, self.unwrap_futures)

        # Create future and wrap in ConcurrentFuture
        py_future = PyFuture()
        future = ConcurrentFuture(future=py_future)

        with self._futures_lock:
            self._futures[future.uuid] = py_future

        # Serialize the function with cloudpickle
        fn_bytes = cloudpickle.dumps(fn)
        self._command_queue.put((future.uuid, "__task__", (fn_bytes, args, kwargs), {}))

        return future

    def stop(self, timeout: float = 30) -> None:
        """Stop the worker process.

        Args:
            timeout: Maximum time to wait for process to stop in seconds
        """
        if self._stopped:
            return

        super().stop(timeout)

        # Signal the process to stop
        try:
            self._command_queue.put(None)
        except (ValueError, OSError):
            pass

        # Wait for the process to finish
        self._process.join(timeout=timeout)

        # Signal the result thread to stop
        try:
            self._result_queue.put(None)
            self._result_thread.join(timeout=timeout)
        except (ValueError, OSError):
            pass

        # Close the queues
        try:
            self._command_queue.close()
            self._result_queue.close()
        except (ValueError, OSError):
            pass

        # Cancel any remaining futures
        with self._futures_lock:
            for py_future in self._futures.values():
                # Try to cancel; if already running/done, this will return False
                if not py_future.done():
                    py_future.cancel()
            self._futures.clear()
