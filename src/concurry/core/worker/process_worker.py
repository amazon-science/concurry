"""Process-based worker implementation for concurry."""

import asyncio
import inspect
import multiprocessing as mp
import queue
import threading
import traceback
from concurrent.futures import Future as PyFuture
from typing import Any, ClassVar, Literal

import cloudpickle
from pydantic import PrivateAttr, confloat

from ..constants import ExecutionMode
from ..future import ConcurrentFuture
from ..retry import execute_with_retry_auto
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
    worker_cls_bytes,
    init_args_bytes,
    init_kwargs_bytes,
    limits,
    retry_configs_bytes,
    command_queue,
    result_queue,
):
    """Main function for the worker process.

    Args:
        worker_cls_bytes: Cloudpickle-serialized worker class
        init_args_bytes: Cloudpickle-serialized positional arguments for worker initialization
        init_kwargs_bytes: Cloudpickle-serialized keyword arguments for worker initialization
        limits: LimitSet instance (or None)
        retry_configs_bytes: Cloudpickle-serialized dict of RetryConfig instances (or None)
        command_queue: Queue for receiving commands
        result_queue: Queue for sending results
    """
    worker_cls = cloudpickle.loads(worker_cls_bytes)
    init_args = cloudpickle.loads(init_args_bytes)
    init_kwargs = cloudpickle.loads(init_kwargs_bytes)
    retry_configs = cloudpickle.loads(retry_configs_bytes)
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
                    actual_worker_cls = _create_worker_wrapper(worker_cls, limits, retry_configs)

                    # CRITICAL: Pass _from_proxy=True to bypass auto_init logic in Worker.__new__
                    # This prevents infinite recursion when the worker class has auto_init=True
                    worker_init_kwargs = dict(init_kwargs)
                    worker_init_kwargs["_from_proxy"] = True

                    worker = actual_worker_cls(*init_args, **worker_init_kwargs)
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
                    # Get retry config for "submit" method (fallback to "*")
                    submit_retry_config = None
                    if retry_configs is not None:
                        submit_retry_config = retry_configs.get("submit") or retry_configs.get("*")

                    # Apply retry logic if configured (num_retries > 0 or retry_until is set)
                    if submit_retry_config is not None and (
                        submit_retry_config.num_retries > 0 or submit_retry_config.retry_until is not None
                    ):
                        context = {
                            "method_name": fn.__name__ if hasattr(fn, "__name__") else "anonymous_function",
                            "worker_class_name": "TaskWorker",
                        }
                        # execute_with_retry_auto handles both sync and async functions automatically
                        result = execute_with_retry_auto(
                            fn, task_args, task_kwargs, submit_retry_config, context
                        )
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

    - `mp_context = "forkserver"`: **Default** - Safe with active threads (e.g., gRPC), fast startup (~200ms)
    - `mp_context = "spawn"`: Safest but slowest (~10-20s startup on Linux, 1-2s on macOS)
    - `mp_context = "fork"`: Fastest but **UNSAFE with active threads** (causes deadlocks/segfaults with Ray client)

    **⚠️ WARNING - Fork with Ray Client:**

    If you use Ray client mode (common workflow) alongside process workers with `mp_context="fork"`,
    you WILL experience segmentation faults and deadlocks. This is because `fork()` copies active
    gRPC threads from Ray client, leaving them in corrupted states.

    **Real error example:**
    ```
    [mutex.cc : 2443] RAW: Check w->waitp->cond == nullptr failed
    Segmentation fault (core dumped)
    ```

    **Solution:** Use `mp_context="forkserver"` (default) or `mp_context="spawn"`.

    **Why forkserver is the default:**

    Forkserver uses a clean server process that forks workers on demand. This provides:
    - **Safety**: No active gRPC threads (Ray client), no inherited locks/mutexes
    - **Speed**: ~200ms startup vs. 10-20s for spawn
    - **Compatibility**: Works alongside Ray client + process mode (common workflow)

    The server process starts clean before any threads are created, then forks workers
    as needed. This avoids the corruption issues of fork() while being much faster than spawn().

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

        # Use default forkserver context (safe + fast)
        w = MyWorker.options(mode="process").init()
        result = w.async_method(5).result()  # Works correctly, returns 10

        # Override to use spawn context (safest, slowest)
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

    # Class-level mode attribute (not passed as parameter)
    mode: ClassVar[ExecutionMode] = ExecutionMode.Processes

    # Configuration (NO defaults - values passed from WorkerBuilder via global config)
    mp_context: Literal["fork", "spawn", "forkserver"]
    result_queue_timeout: confloat(ge=0)
    result_queue_cleanup_timeout: confloat(ge=0)

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

        # Serialize the worker class, init args/kwargs, and retry_configs with cloudpickle
        # This allows local functions, lambdas, and other non-standard objects
        # (e.g., functions defined in Jupyter notebooks, retry filters) to be pickled correctly
        worker_cls_bytes = cloudpickle.dumps(self.worker_cls)
        init_args_bytes = cloudpickle.dumps(self.init_args)
        init_kwargs_bytes = cloudpickle.dumps(self.init_kwargs)
        retry_configs_bytes = cloudpickle.dumps(self.retry_configs)

        # Process limits for worker
        # Limits already processed by WorkerBuilder

        # Start worker process using public fields
        self._process = ctx.Process(
            target=_process_worker_main,
            args=(
                worker_cls_bytes,
                init_args_bytes,
                init_kwargs_bytes,
                self.limits,
                retry_configs_bytes,
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
            request_id, status, payload = self._result_queue.get(timeout=self.result_queue_timeout)
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
                    item = self._result_queue.get(timeout=self.result_queue_cleanup_timeout)
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
            timeout: Maximum time to wait for process to stop in seconds.
                Default value is determined by global_config.<mode>.stop_timeout
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
