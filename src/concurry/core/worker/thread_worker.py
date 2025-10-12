"""Thread-based worker implementation for concurry."""

import asyncio
import inspect
import queue
import threading
import uuid
from typing import Any, Dict

from pydantic import PrivateAttr

from ..future import ConcurrentFuture
from .base_worker import WorkerProxy


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
    _result_queues: Dict[str, Any] = PrivateAttr()
    _result_queues_lock: Any = PrivateAttr()
    _thread: Any = PrivateAttr()

    def post_initialize(self) -> None:
        """Initialize private attributes after Typed validation."""
        super().post_initialize()

        # Create queues for communication
        self._command_queue = queue.Queue()
        self._result_queues = {}  # request_id -> result_queue
        self._result_queues_lock = threading.Lock()

        # Start worker thread
        self._thread = threading.Thread(target=self._worker_thread_main, daemon=True)
        self._thread.start()

        # Wait for initialization to complete
        self._wait_for_initialization()

    def _wait_for_initialization(self):
        """Wait for worker thread to initialize."""
        init_id = str(uuid.uuid4())
        result_queue = queue.Queue()

        with self._result_queues_lock:
            self._result_queues[init_id] = result_queue

        self._command_queue.put((init_id, "__initialize__", (), {}))

        try:
            status, payload = result_queue.get(timeout=30)
            if status == "error":
                raise RuntimeError(f"Worker initialization failed: {payload}")
        finally:
            with self._result_queues_lock:
                del self._result_queues[init_id]

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

                # Get result queue for this request
                with self._result_queues_lock:
                    result_queue = self._result_queues.get(request_id)

                if result_queue is None:
                    continue

                try:
                    if method_name == "__initialize__":
                        worker = self.worker_cls(*self.init_args, **self.init_kwargs)
                        result_queue.put(("ok", None))
                        continue

                    if method_name == "__task__":
                        # Execute arbitrary function
                        fn, task_args, task_kwargs = args
                        if not callable(fn):
                            result_queue.put(
                                ("error", TypeError(f"fn must be callable, got {type(fn).__name__}"))
                            )
                            continue
                        result = _invoke_function(fn, *task_args, **task_kwargs)
                        result_queue.put(("ok", result))
                        continue

                    if worker is None:
                        result_queue.put(("error", RuntimeError("Worker not initialized")))
                        continue

                    method = getattr(worker, method_name)
                    if not callable(method):
                        result_queue.put(
                            (
                                "error",
                                AttributeError(
                                    f"'{self.worker_cls.__name__}' has no callable method '{method_name}'"
                                ),
                            )
                        )
                        continue

                    result = _invoke_function(method, *args, **kwargs)
                    result_queue.put(("ok", result))
                except Exception as e:
                    result_queue.put(("error", e))

            except Exception:
                # Catch any unexpected exceptions to keep thread alive
                break

    def _execute_method(self, method_name: str, *args: Any, **kwargs: Any):
        """Execute a method in the worker thread.

        Args:
            method_name: Name of the method to invoke
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            ConcurrentFuture for the method execution
        """
        request_id = str(uuid.uuid4())
        result_queue = queue.Queue()

        with self._result_queues_lock:
            self._result_queues[request_id] = result_queue

        self._command_queue.put((request_id, method_name, args, kwargs))

        # Create a future that will wait for the result
        from concurrent.futures import Future as PyFuture

        py_future = PyFuture()

        def get_result():
            try:
                status, payload = result_queue.get()
                with self._result_queues_lock:
                    self._result_queues.pop(request_id, None)

                if status == "ok":
                    py_future.set_result(payload)
                else:
                    py_future.set_exception(payload)
            except Exception as e:
                py_future.set_exception(e)

        # Start a thread to wait for the result
        threading.Thread(target=get_result, daemon=True).start()

        return ConcurrentFuture(future=py_future)

    def _execute_task(self, fn, *args: Any, **kwargs: Any):
        """Execute an arbitrary function in the worker thread.

        Args:
            fn: Callable function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            ConcurrentFuture for the task execution
        """
        request_id = str(uuid.uuid4())
        result_queue = queue.Queue()

        with self._result_queues_lock:
            self._result_queues[request_id] = result_queue

        # Send task command with special marker
        self._command_queue.put((request_id, "__task__", (fn, args, kwargs), {}))

        # Create a future that will wait for the result
        from concurrent.futures import Future as PyFuture

        py_future = PyFuture()

        def get_result():
            try:
                status, payload = result_queue.get()
                with self._result_queues_lock:
                    self._result_queues.pop(request_id, None)

                if status == "ok":
                    py_future.set_result(payload)
                else:
                    py_future.set_exception(payload)
            except Exception as e:
                py_future.set_exception(e)

        # Start a thread to wait for the result
        threading.Thread(target=get_result, daemon=True).start()

        return ConcurrentFuture(future=py_future)

    def stop(self, timeout: float = 30) -> None:
        """Stop the worker thread.

        Args:
            timeout: Maximum time to wait for thread to stop in seconds
        """
        super().stop(timeout)
        self._command_queue.put(None)
        self._thread.join(timeout=timeout)
