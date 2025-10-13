"""Asyncio-based worker implementation for concurry."""

import asyncio
import queue
import threading
from typing import Any, Dict

from pydantic import PrivateAttr

from ..future import ConcurrentFuture
from .base_worker import WorkerProxy, _unwrap_futures_in_args


class AsyncioWorkerProxy(WorkerProxy):
    """Worker proxy for asyncio-based execution.

    This proxy runs the worker with an asyncio event loop in a dedicated thread.
    Supports both synchronous and asynchronous worker methods.

    **Exception Handling:**

    - Setup errors (e.g., `AttributeError` for non-existent methods) fail immediately
    - Execution errors propagate naturally through asyncio futures
    - Original exception types and messages are preserved
    - Both sync and async method exceptions are handled consistently

    **Async Support:**

    - Automatically detects and awaits coroutine functions using `asyncio.iscoroutinefunction()`
    - Synchronous methods work without modification
    - Event loop runs in a dedicated background thread
    - **Provides significant performance benefits for I/O-bound async operations**
    - Multiple async tasks can execute concurrently within the same event loop

    **Example:**

        ```python
        import asyncio

        class MyAsyncWorker(Worker):
            async def async_method(self):
                await asyncio.sleep(1)
                return "done"

            def sync_method(self):
                return "also works"

            async def fetch_multiple(self, urls: list):
                # True concurrent execution in the event loop
                tasks = [self.fetch(url) for url in urls]
                return await asyncio.gather(*tasks)

        w = MyAsyncWorker.options(mode="asyncio").init()

        # Both async and sync methods work
        result1 = w.async_method().result()
        result2 = w.sync_method().result()

        # Concurrent async execution for major speedup
        result3 = w.fetch_multiple(['url1', 'url2', 'url3']).result()

        # Exceptions preserve their original type
        try:
            w.failing_method().result()
        except ValueError as e:
            print(f"Got error: {e}")

        w.stop()
        ```

    **Performance Benefits:**

        AsyncioWorkerProxy provides 5-15x speedup for I/O-bound async operations:

        ```python
        # Example: Reading 100 files
        # Thread worker (sync): 0.500s
        # AsyncIO worker (async): 0.045s
        # Speedup: 11x

        class FileReader(Worker):
            async def read_file(self, path: str) -> str:
                async with aiofiles.open(path, 'r') as f:
                    return await f.read()

        worker = FileReader.options(mode="asyncio").init()
        futures = [worker.read_file(f"file_{i}.txt") for i in range(100)]
        results = [f.result() for f in futures]
        worker.stop()
        ```
    """

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
        if not self._loop_ready.wait(timeout=30):
            raise RuntimeError("Failed to start asyncio event loop")

        # Create dedicated thread for sync methods
        self._sync_queue = queue.Queue()
        self._sync_thread_ready = threading.Event()
        self._sync_thread = threading.Thread(target=self._run_sync_thread, daemon=True)
        self._sync_thread.start()

        # Wait for sync thread to be ready
        if not self._sync_thread_ready.wait(timeout=30):
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
            future.result(timeout=30)
        except Exception as e:
            raise RuntimeError(f"Worker initialization failed: {e}")

    async def _async_initialize(self):
        """Async initialization of the worker."""
        self._worker = self.worker_cls(*self.init_args, **self.init_kwargs)

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
                    command = self._sync_queue.get(timeout=0.1)
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

        # Use concurrent.futures.Future for efficient blocking
        from concurrent.futures import Future as PyFuture

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

        # Use concurrent.futures.Future for efficient blocking
        from concurrent.futures import Future as PyFuture

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
            # Create a wrapper that executes the function
            def execute_sync_task():
                if not callable(fn):
                    raise TypeError(f"fn must be callable, got {type(fn).__name__}")
                return fn(*unwrapped_args, **unwrapped_kwargs)

            # Queue task to sync thread
            # We use a special marker "__sync_task__" to indicate this is a task, not a method
            self._sync_queue.put((future, "__sync_task__", (execute_sync_task,), {}))

        return future

    def stop(self, timeout: float = 30) -> None:
        """Stop the worker, sync thread, and event loop.

        Args:
            timeout: Maximum time to wait for cleanup in seconds
        """
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
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._loop_thread.join(timeout=timeout / 2)
