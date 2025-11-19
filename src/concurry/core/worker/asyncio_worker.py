"""Asyncio-based worker implementation for concurry."""

import asyncio
import queue
import threading
from concurrent.futures import Future as PyFuture
from typing import Any, ClassVar, Dict

from pydantic import PrivateAttr, confloat

from ..constants import ExecutionMode
from ..future import ConcurrentFuture
from ..retry import execute_with_retry, execute_with_retry_async
from .base_worker import WorkerProxy, _create_worker_wrapper, _unwrap_futures_in_args


class AsyncioWorkerProxy(WorkerProxy):
    """Worker proxy for asyncio-based execution with smart routing.

    This proxy intelligently routes methods to the appropriate execution context:
    - **Async methods** → Event loop thread (concurrent execution)
    - **Sync methods** → Dedicated sync thread (avoids blocking event loop)

    **Architecture:**

    - Event loop thread: Runs asyncio event loop for async methods
    - Sync worker thread: Executes sync methods without blocking the event loop
    - Automatic routing: Detects method type using `asyncio.iscoroutinefunction()`

    **Return Type:**

    All methods return `ConcurrentFuture` (wrapping `concurrent.futures.Future`) for
    efficient blocking behavior. This provides:
    - Fast `.result()` calls (no polling overhead)
    - Thread-safe operations
    - Consistent API across sync and async methods

    **Performance:**

    - **Async methods**: 10-50x speedup for concurrent I/O operations
    - **Sync methods**: ~13% overhead vs ThreadWorker (minimal impact)
    - **Best for**: Network I/O (HTTP, WebSocket, database), concurrent operations

    **Use Cases:**

    ✅ **Excellent for:**
    - HTTP requests and API calls
    - Database queries with async drivers
    - WebSocket connections
    - Any I/O with significant wait time
    - Mixed sync/async worker methods

    ❌ **Not recommended for:**
    - Small local file I/O (use ThreadWorker or SyncWorker)
    - CPU-bound tasks (use ProcessWorker or Ray)
    - Pure sequential operations (use SyncWorker)

    **Exception Handling:**

    - Setup errors (e.g., `AttributeError` for non-existent methods) fail immediately
    - Execution errors propagate naturally through futures
    - Original exception types and messages are preserved
    - Both sync and async method exceptions are handled consistently

    **Example:**

        ```python
        import asyncio
        import aiohttp

        class APIWorker(Worker):
            async def fetch_url(self, url: str) -> str:
                \"\"\"Async method - executes in event loop.\"\"\"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        return await response.text()

            def process_data(self, data: str) -> dict:
                \"\"\"Sync method - executes in dedicated sync thread.\"\"\"
                return {"length": len(data), "data": data}

            async def fetch_multiple(self, urls: list) -> list:
                \"\"\"Concurrent async execution for major speedup.\"\"\"
                tasks = [self.fetch_url(url) for url in urls]
                return await asyncio.gather(*tasks)

        w = APIWorker.options(mode="asyncio").init()

        # Concurrent async requests (10x+ faster than sequential)
        urls = [f"https://api.example.com/data/{i}" for i in range(50)]
        future = w.fetch_multiple(urls)
        results = future.result()

        # Sync method works too (no event loop blocking)
        processed = w.process_data(results[0]).result()

        w.stop()
        ```

    **Performance Comparison:**

        ```python
        # 30 HTTP requests with 50ms latency each:
        # SyncWorker:    1.66s (sequential)
        # ThreadWorker:  1.66s (sequential)
        # ProcessWorker: 1.67s (sequential)
        # AsyncioWorker: 0.16s (concurrent) ✅ 10x faster!
        ```
    """

    # Class-level mode attribute (not passed as parameter)
    mode: ClassVar[ExecutionMode] = ExecutionMode.Asyncio

    # Configuration (NO defaults - values passed from WorkerBuilder via global config)
    loop_ready_timeout: confloat(ge=0)
    thread_ready_timeout: confloat(ge=0)
    sync_queue_timeout: confloat(ge=0)

    # Private attributes (use Any for non-serializable types)
    _loop: Any = PrivateAttr(default=None)
    _worker: Any = PrivateAttr(default=None)
    _loop_thread: Any = PrivateAttr()
    _loop_ready: Any = PrivateAttr()
    _sync_thread: Any = PrivateAttr()  # Dedicated thread for sync methods
    _sync_queue: Any = PrivateAttr()  # Queue for sync method calls
    _sync_thread_ready: Any = PrivateAttr()
    _futures: Dict[str, Any] = PrivateAttr()  # Maps future.uuid -> AsyncioFuture
    _futures_lock: Any = PrivateAttr()

    def post_initialize(self) -> None:
        """Initialize private attributes after Typed validation."""
        super().post_initialize()

        # Initialize futures tracking
        self._futures = {}  # future.uuid -> AsyncioFuture
        self._futures_lock = threading.Lock()

        # Create event loop in a dedicated thread
        self._loop_thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self._loop_ready = threading.Event()
        self._loop_thread.start()

        # Wait for event loop to be ready
        if not self._loop_ready.wait(timeout=self.loop_ready_timeout):
            raise RuntimeError("Failed to start asyncio event loop")

        # Create dedicated thread for sync methods
        self._sync_queue = queue.Queue()
        self._sync_thread_ready = threading.Event()
        self._sync_thread = threading.Thread(target=self._run_sync_thread, daemon=True)
        self._sync_thread.start()

        # Wait for sync thread to be ready
        if not self._sync_thread_ready.wait(timeout=self.thread_ready_timeout):
            raise RuntimeError("Failed to start sync worker thread")

        # Initialize the worker
        self._initialize_worker()

    def _run_event_loop(self):
        """Run the asyncio event loop in a dedicated thread."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop_ready.set()

        try:
            self._loop.run_forever()
        finally:
            self._loop.close()

    def _initialize_worker(self):
        """Initialize the worker instance in the event loop."""
        future = asyncio.run_coroutine_threadsafe(self._async_initialize(), self._loop)
        try:
            future.result(timeout=self.thread_ready_timeout)
        except Exception as e:
            raise RuntimeError(f"Worker initialization failed: {e}")

    async def _async_initialize(self):
        """Async initialization of the worker."""
        # Create worker wrapper with limits and retry logic if needed
        # (limits and retry_configs already processed by WorkerBuilder)
        worker_cls = _create_worker_wrapper(self.worker_cls, self.limits, self.retry_configs)

        # CRITICAL: Pass _from_proxy=True to bypass auto_init logic in Worker.__new__
        # This prevents infinite recursion when the worker class has auto_init=True
        init_kwargs = dict(self.init_kwargs)
        init_kwargs["_from_proxy"] = True

        self._worker = worker_cls(*self.init_args, **init_kwargs)

    def _run_sync_thread(self):
        """Run the dedicated thread for sync method execution.

        This thread processes sync methods without blocking the event loop,
        allowing for better concurrency when mixing sync and async operations.
        """
        # Signal that thread is ready
        self._sync_thread_ready.set()

        while not self._stopped:
            try:
                # Get command with timeout to allow checking stopped flag
                try:
                    command = self._sync_queue.get(timeout=self.sync_queue_timeout)
                except queue.Empty:
                    continue

                if command is None:
                    break

                future, method_name, args, kwargs = command

                try:
                    if method_name == "__sync_task__":
                        # Execute arbitrary sync function
                        execute_fn = args[0]
                        result = execute_fn()
                        future._future.set_result(result)
                    else:
                        # Execute the sync method
                        method = getattr(self._worker, method_name)
                        if not callable(method):
                            future._future.set_exception(
                                AttributeError(
                                    f"'{self.worker_cls.__name__}' has no callable method '{method_name}'"
                                )
                            )
                        else:
                            result = method(*args, **kwargs)
                            future._future.set_result(result)
                except Exception as e:
                    future._future.set_exception(e)
                finally:
                    # Remove from futures tracking
                    with self._futures_lock:
                        self._futures.pop(future.uuid, None)

            except Exception:
                # Catch any unexpected exceptions to keep thread alive
                break

    def _execute_method(self, method_name: str, *args: Any, **kwargs: Any):
        """Execute a method - routes to sync thread or async event loop.

        Sync methods are executed in a dedicated thread to avoid blocking the event loop.
        Async methods are executed in the event loop for true concurrent execution.

        Args:
            method_name: Name of the method to invoke
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            ConcurrentFuture for the method execution
        """
        # Unwrap futures if needed (fast-path handled in _unwrap_futures_in_args)
        unwrapped_args, unwrapped_kwargs = _unwrap_futures_in_args(args, kwargs, self.unwrap_futures)

        # Check if method is async or sync
        # We need to check this on the worker instance
        try:
            method = getattr(self._worker, method_name)
            is_async = asyncio.iscoroutinefunction(method)
        except AttributeError:
            # Method doesn't exist - will be caught later
            is_async = False

        result_future = PyFuture()
        future = ConcurrentFuture(future=result_future)

        # Store future for cancellation on stop()
        with self._futures_lock:
            self._futures[future.uuid] = future

        if is_async:
            # Route to event loop for async methods
            def create_and_schedule():
                """Runs in event loop thread - schedules work and manages result."""

                # Schedule actual work as a task
                async def _run_method():
                    try:
                        method = getattr(self._worker, method_name)
                        if not callable(method):
                            raise AttributeError(
                                f"'{self.worker_cls.__name__}' has no callable method '{method_name}'"
                            )

                        result = await method(*unwrapped_args, **unwrapped_kwargs)
                        result_future.set_result(result)
                    except Exception as e:
                        result_future.set_exception(e)
                    finally:
                        # Remove from futures tracking
                        with self._futures_lock:
                            self._futures.pop(future.uuid, None)

                # Schedule coroutine on event loop
                asyncio.ensure_future(_run_method(), loop=self._loop)

            # Schedule callback in event loop (fast, ~5-10µs)
            self._loop.call_soon_threadsafe(create_and_schedule)
        else:
            # Route to sync thread for sync methods
            self._sync_queue.put((future, method_name, unwrapped_args, unwrapped_kwargs))

        return future

    def _execute_task(self, fn, *args: Any, **kwargs: Any):
        """Execute an arbitrary function - routes to sync thread or async event loop.

        This method applies retry logic for TaskWorker.submit() and TaskWorker.map().
        The retry logic is applied here (not in submit()) to avoid double-wrapping.

        Sync functions are executed in a dedicated thread to avoid blocking the event loop.
        Async functions are executed in the event loop for true concurrent execution.

        Args:
            fn: Callable function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            ConcurrentFuture for the task execution
        """
        # Unwrap futures if needed (fast-path handled in _unwrap_futures_in_args)
        unwrapped_args, unwrapped_kwargs = _unwrap_futures_in_args(args, kwargs, self.unwrap_futures)

        # Check if function is async or sync
        is_async = asyncio.iscoroutinefunction(fn)

        result_future = PyFuture()
        future = ConcurrentFuture(future=result_future)

        # Store future for cancellation on stop()
        with self._futures_lock:
            self._futures[future.uuid] = future

        if is_async:
            # Route to event loop for async functions
            def create_and_schedule():
                """Runs in event loop thread - schedules work and manages result."""

                # Schedule actual work as a task
                async def _run_task():
                    try:
                        if not callable(fn):
                            raise TypeError(f"fn must be callable, got {type(fn).__name__}")

                        # Apply retry logic if configured (for TaskWorker async functions)
                        # Get retry config for "submit" method (fallback to "*")
                        submit_retry_config = None
                        if self.retry_configs is not None:
                            submit_retry_config = self.retry_configs.get("submit") or self.retry_configs.get(
                                "*"
                            )

                        # Apply retry logic if configured (num_retries > 0 or retry_until is set)
                        if submit_retry_config is not None and (
                            submit_retry_config.num_retries > 0 or submit_retry_config.retry_until is not None
                        ):
                            context = {
                                "method_name": fn.__name__
                                if hasattr(fn, "__name__")
                                else "anonymous_function",
                                "worker_class_name": "TaskWorker",
                            }
                            result = await execute_with_retry_async(
                                fn, unwrapped_args, unwrapped_kwargs, submit_retry_config, context
                            )
                        else:
                            result = await fn(*unwrapped_args, **unwrapped_kwargs)

                        result_future.set_result(result)
                    except Exception as e:
                        result_future.set_exception(e)
                    finally:
                        # Remove from futures tracking
                        with self._futures_lock:
                            self._futures.pop(future.uuid, None)

                # Schedule coroutine on event loop
                asyncio.ensure_future(_run_task(), loop=self._loop)

            # Schedule callback in event loop (fast, ~5-10µs)
            self._loop.call_soon_threadsafe(create_and_schedule)
        else:
            # Route to sync thread for sync functions
            # Create a wrapper that executes the function with retry logic
            def execute_sync_task():
                if not callable(fn):
                    raise TypeError(f"fn must be callable, got {type(fn).__name__}")

                # Apply retry logic if configured (for TaskWorker sync functions)
                # Get retry config for "submit" method (fallback to "*")
                submit_retry_config = None
                if self.retry_configs is not None:
                    submit_retry_config = self.retry_configs.get("submit") or self.retry_configs.get("*")

                # Apply retry logic if configured (num_retries > 0 or retry_until is set)
                if submit_retry_config is not None and (
                    submit_retry_config.num_retries > 0 or submit_retry_config.retry_until is not None
                ):
                    context = {
                        "method_name": fn.__name__ if hasattr(fn, "__name__") else "anonymous_function",
                        "worker_class_name": "TaskWorker",
                    }
                    return execute_with_retry(
                        fn, unwrapped_args, unwrapped_kwargs, submit_retry_config, context
                    )
                else:
                    return fn(*unwrapped_args, **unwrapped_kwargs)

            # Queue task to sync thread
            # We use a special marker "__sync_task__" to indicate this is a task, not a method
            self._sync_queue.put((future, "__sync_task__", (execute_sync_task,), {}))

        return future

    def stop(self, timeout: float = 30) -> None:
        """Stop the worker, sync thread, and event loop.

        Args:
            timeout: Maximum time to wait for cleanup in seconds.
                Default value is determined by global_config.<mode>.stop_timeout
        """
        if self._stopped:
            return

        super().stop(timeout)

        # Cancel all pending futures
        with self._futures_lock:
            for future in self._futures.values():
                future.cancel()
            self._futures.clear()

        # Stop sync thread
        if self._sync_queue is not None:
            self._sync_queue.put(None)
            self._sync_thread.join(timeout=timeout / 2)

        # Stop event loop
        if self._loop is not None and not self._loop.is_closed():
            try:
                self._loop.call_soon_threadsafe(self._loop.stop)
            except RuntimeError:
                # Loop might be closed between check and call
                pass
            self._loop_thread.join(timeout=timeout / 2)
