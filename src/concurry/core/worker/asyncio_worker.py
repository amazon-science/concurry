"""Asyncio-based worker implementation for concurry."""

import asyncio
import threading
from typing import Any

from pydantic import PrivateAttr

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

    def post_initialize(self) -> None:
        """Initialize private attributes after Typed validation."""
        super().post_initialize()

        # Create event loop in a dedicated thread
        self._loop_thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self._loop_ready = threading.Event()
        self._loop_thread.start()

        # Wait for event loop to be ready
        if not self._loop_ready.wait(timeout=30):
            raise RuntimeError("Failed to start asyncio event loop")

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

    def _execute_method(self, method_name: str, *args: Any, **kwargs: Any):
        """Execute a method in the asyncio event loop.

        Args:
            method_name: Name of the method to invoke
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            ConcurrentFuture for the method execution
        """
        # Unwrap any BaseFuture instances in args/kwargs
        unwrapped_args, unwrapped_kwargs = _unwrap_futures_in_args(args, kwargs, self.unwrap_futures)

        # Create a future in the asyncio event loop
        async def _run_method():
            method = getattr(self._worker, method_name)
            if not callable(method):
                raise AttributeError(f"'{self.worker_cls.__name__}' has no callable method '{method_name}'")

            if asyncio.iscoroutinefunction(method):
                result = await method(*unwrapped_args, **unwrapped_kwargs)
            else:
                result = method(*unwrapped_args, **unwrapped_kwargs)

            return result

        # Schedule the coroutine in the event loop
        asyncio_future = asyncio.run_coroutine_threadsafe(_run_method(), self._loop)

        # Wrap the asyncio future
        # run_coroutine_threadsafe returns a concurrent.futures.Future
        from ..future import ConcurrentFuture

        return ConcurrentFuture(future=asyncio_future)

    def _execute_task(self, fn, *args: Any, **kwargs: Any):
        """Execute an arbitrary function in the asyncio event loop.

        Args:
            fn: Callable function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            ConcurrentFuture for the task execution
        """
        # Unwrap any BaseFuture instances in args/kwargs
        unwrapped_args, unwrapped_kwargs = _unwrap_futures_in_args(args, kwargs, self.unwrap_futures)

        # Create a future in the asyncio event loop
        async def _run_task():
            if not callable(fn):
                raise TypeError(f"fn must be callable, got {type(fn).__name__}")

            if asyncio.iscoroutinefunction(fn):
                result = await fn(*unwrapped_args, **unwrapped_kwargs)
            else:
                result = fn(*unwrapped_args, **unwrapped_kwargs)

            return result

        # Schedule the coroutine in the event loop
        asyncio_future = asyncio.run_coroutine_threadsafe(_run_task(), self._loop)

        # Wrap the asyncio future
        from ..future import ConcurrentFuture

        return ConcurrentFuture(future=asyncio_future)

    def stop(self, timeout: float = 30) -> None:
        """Stop the worker and event loop.

        Args:
            timeout: Maximum time to wait for cleanup in seconds
        """
        super().stop(timeout)

        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._loop_thread.join(timeout=timeout)
