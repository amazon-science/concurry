"""TaskWorker implementation for concurry."""

from typing import Any, Callable, Iterator, Optional, Union

from ...utils import _NO_ARG
from ...utils.progress import ProgressBar
from .base_worker import Worker


class TaskWorkerPoolMixin:
    """Mixin that adds submit(), map(), and __call__() to worker pools for TaskWorker.

    This mixin extends WorkerProxyPool classes with TaskWorker-specific methods.
    Worker pools dispatch method calls to individual workers via __getattr__, but
    for TaskWorker we need to add these methods directly to the pool.
    """

    def __call__(self, *args: Any, **kwargs: Any):
        """Call the bound function via submit().

        Returns:
            BaseFuture for the task execution (or result directly if blocking=True)
        """
        return self.submit(*args, **kwargs)

    def submit(self, fn: Any = _NO_ARG, *args: Any, **kwargs: Any):
        """Dispatch submit to a worker in the pool.

        This method handles bound functions and dispatches to individual workers.

        Behavior:
        - If bound function exists (from .init(fn=...)): First argument is treated as data
        - If no bound function: First argument must be a callable function
        """
        # Check if we have a bound function
        has_bound_fn = hasattr(self, "init_kwargs") and "fn" in self.init_kwargs

        if has_bound_fn:
            # Bound function exists - treat fn as first data argument
            bound_fn = self.init_kwargs["fn"]
            if fn is not _NO_ARG:
                # fn was explicitly passed (even if None) - add to args
                args = (fn,) + args
            fn = bound_fn
        else:
            # No bound function - fn must be callable
            if fn is _NO_ARG:
                raise TypeError(
                    "submit() requires a function argument when no bound function is set via .init(fn=...)"
                )
            if not callable(fn):
                raise TypeError(f"submit() requires a callable function, got {type(fn).__name__}")

        # Dispatch to a worker via __getattr__
        submit_method = object.__getattribute__(self, "__getattr__")("submit")
        return submit_method(fn, *args, **kwargs)

    def map(
        self,
        fn: Any = _NO_ARG,
        *iterables: Any,
        timeout: Optional[float] = None,
        chunksize: int = 1,
        buffersize: Optional[int] = None,
        progress: Union[bool, dict, "ProgressBar"] = False,
    ) -> Iterator:
        """Dispatch map to a worker in the pool.

        This method handles bound functions and dispatches to individual workers.

        Behavior:
        - If bound function exists (from .init(fn=...)): First argument is treated as data (iterable)
        - If no bound function: First argument must be a callable function
        """
        # Check if we have a bound function
        has_bound_fn = hasattr(self, "init_kwargs") and "fn" in self.init_kwargs

        if has_bound_fn:
            # Bound function exists
            bound_fn = self.init_kwargs["fn"]
            if fn is not _NO_ARG and callable(fn):
                # fn is a callable - use it as explicit function (allows overriding bound function)
                pass  # Use fn as-is
            elif fn is not _NO_ARG:
                # fn is not callable - treat as first iterable argument
                iterables = (fn,) + iterables
                fn = bound_fn
            else:
                # fn not provided - use bound function
                fn = bound_fn
        else:
            # No bound function - fn must be callable
            if fn is _NO_ARG:
                raise TypeError(
                    "map() requires a function argument when no bound function is set via .init(fn=...)"
                )
            if not callable(fn):
                raise TypeError(f"map() requires a callable function, got {type(fn).__name__}")

        # For pools, we need to handle map differently since it returns an iterator
        # We'll call the regular map method which will use submit internally
        map_method = object.__getattribute__(self, "__getattr__")("map")
        return map_method(
            fn, *iterables, timeout=timeout, chunksize=chunksize, buffersize=buffersize, progress=progress
        )


class TaskWorkerMixin:
    """Mixin that adds submit() and map() methods to worker proxies.

    This mixin extends any WorkerProxy subclass with submit() and map() methods
    that match the concurrent.futures.Executor interface. It's designed to be
    mixed with concrete worker proxy implementations (SyncWorkerProxy,
    ThreadWorkerProxy, etc.) to create TaskWorker proxies.

    The mixin assumes that the class it's mixed with provides:
    - self._stopped: Boolean flag indicating if worker is stopped
    - self.blocking: Boolean flag for blocking mode
    - self._execute_task(fn, *args, **kwargs): Method to execute arbitrary functions
    - self._worker: The actual TaskWorker instance (which may have _bound_fn)
    """

    def __call__(self, *args: Any, **kwargs: Any):
        """Call the bound function via submit().

        This allows decorated functions to be called directly:
            @task(mode="thread")
            def process(x):
                return x * 2

            result = process(5).result()  # Calls worker.submit() internally

        Returns:
            BaseFuture for the task execution (or result directly if blocking=True)

        Raises:
            TypeError: If no bound function was provided during initialization
        """
        return self.submit(*args, **kwargs)

    def submit(self, fn: Any = _NO_ARG, *args: Any, **kwargs: Any):
        """Schedule the callable to be executed as fn(*args, **kwargs).

        This method is identical to concurrent.futures.Executor.submit().
        It submits a function for execution in the worker's context and returns
        a Future representing the execution of the callable.

        Behavior:
        - If bound function exists (from .init(fn=...)): First argument is treated as data
        - If no bound function: First argument must be a callable function

        Args:
            fn: Callable function to execute, OR first data argument if bound function exists.
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            BaseFuture for the task execution (or result directly if blocking=True)

        Raises:
            TypeError: If no function is provided and no bound function exists

        Examples:
            Basic Function Submission:
                ```python
                def compute(x, y):
                    return x + y

                worker = TaskWorker.options(mode="process").init()
                future = worker.submit(compute, 10, 20)
                result = future.result()  # 30
                worker.stop()
                ```

            Using with Lambda Functions:
                ```python
                worker = TaskWorker.options(mode="thread").init()
                future = worker.submit(lambda x: x ** 2, 5)
                result = future.result()  # 25
                worker.stop()
                ```

            With Keyword Arguments:
                ```python
                def complex_calc(x, y, power=2, offset=0):
                    return (x ** power + y ** power) + offset

                worker = TaskWorker.options(mode="process").init()
                future = worker.submit(complex_calc, 3, 4, power=2, offset=10)
                result = future.result()  # 35
                worker.stop()
                ```

            In Blocking Mode:
                ```python
                worker = TaskWorker.options(mode="thread", blocking=True).init()
                result = worker.submit(lambda x: x * 2, 50)  # Returns 100 directly
                worker.stop()
                ```

            Async Function Submission:
                ```python
                import asyncio

                async def async_compute(x, y):
                    await asyncio.sleep(0.01)
                    return x + y

                worker = TaskWorker.options(mode="asyncio").init()
                future = worker.submit(async_compute, 10, 20)
                result = future.result()  # 30
                worker.stop()
                ```
        """
        # Check if we have a bound function
        # For Sync proxies: self._worker._bound_fn
        # For Thread/Process/Asyncio proxies: self.init_kwargs['fn']
        bound_fn = None
        if hasattr(self, "_worker") and hasattr(self._worker, "_bound_fn"):
            bound_fn = self._worker._bound_fn
        elif hasattr(self, "init_kwargs") and "fn" in self.init_kwargs:
            bound_fn = self.init_kwargs["fn"]

        if bound_fn is not None:
            # Bound function exists
            if fn is not _NO_ARG and callable(fn):
                # fn is a callable - use it as explicit function (allows overriding bound function)
                pass  # Use fn as-is
            elif fn is not _NO_ARG:
                # fn is not callable - treat as first data argument
                args = (fn,) + args
                fn = bound_fn
            else:
                # fn not provided - use bound function
                fn = bound_fn
        else:
            # No bound function - fn must be callable
            if fn is _NO_ARG:
                raise TypeError(
                    "submit() requires a function argument when no bound function is set via .init(fn=...)"
                )
            if not callable(fn):
                raise TypeError(f"submit() requires a callable function, got {type(fn).__name__}")

        # Check if worker is stopped
        if self._stopped:
            raise RuntimeError("Worker is stopped")

        # Execute the task using the underlying _execute_task implementation
        future = self._execute_task(fn, *args, **kwargs)

        if self.blocking:
            # Return result directly (blocking)
            return future.result()
        else:
            # Return future (non-blocking)
            return future

    def map(
        self,
        fn: Any = _NO_ARG,
        *iterables: Any,
        timeout: Optional[float] = None,
        chunksize: int = 1,
        buffersize: Optional[int] = None,
        progress: Union[bool, dict, ProgressBar] = False,
    ) -> Iterator:
        """Map a function over iterables, executing in parallel.

        This method is identical to concurrent.futures.Executor.map() with
        additional ProgressBar support.
        Similar to map(fn, *iterables) but executes asynchronously with support
        for timeouts, chunking, and buffering.

        Behavior:
        - If bound function exists (from .init(fn=...)): First argument is treated as data (iterable)
        - If no bound function: First argument must be a callable function

        Args:
            fn: Callable function to execute for each item, OR first iterable if bound function exists.
            *iterables: One or more iterables to map over
            timeout: Maximum time in seconds to wait for each result
            chunksize: Size of chunks for batch processing (currently ignored)
            buffersize: Maximum number of submitted tasks whose results haven't been yielded
                       (currently ignored)
            progress: Progress bar configuration:
                - False: No progress bar (default)
                - True: Use default progress bar
                - dict: Pass as kwargs to ProgressBar()
                - ProgressBar: Use existing ProgressBar instance

        Returns:
            Iterator that yields results in the same order as the input iterables

        Raises:
            TimeoutError: If any result isn't available within the timeout
            TypeError: If no function is provided and no bound function exists
            Exception: Any exception raised by fn is re-raised when its value is retrieved

        Examples:
            Basic Map:
                ```python
                worker = TaskWorker.options(mode="thread").init()

                def square(x):
                    return x ** 2

                results = list(worker.map(square, range(5)))
                print(results)  # [0, 1, 4, 9, 16]

                worker.stop()
                ```

            Multiple Iterables:
                ```python
                worker = TaskWorker.options(mode="process").init()

                def add(x, y):
                    return x + y

                results = list(worker.map(add, [1, 2, 3], [10, 20, 30]))
                print(results)  # [11, 22, 33]

                worker.stop()
                ```

            With Timeout:
                ```python
                import time

                def slow_fn(x):
                    time.sleep(x * 0.1)
                    return x

                worker = TaskWorker.options(mode="thread").init()

                try:
                    results = list(worker.map(slow_fn, range(10), timeout=0.5))
                except TimeoutError:
                    print("Some tasks timed out!")

                worker.stop()
                ```

            Exception Handling:
                ```python
                def may_fail(x):
                    if x < 0:
                        raise ValueError(f"Negative value: {x}")
                    return x * 2

                worker = TaskWorker.options(mode="thread").init()

                try:
                    results = list(worker.map(may_fail, [-1, 0, 1, 2]))
                except ValueError as e:
                    print(f"Task failed: {e}")

                worker.stop()
                ```
        """
        # Check if we have a bound function
        # For Sync proxies: self._worker._bound_fn
        # For Thread/Process/Asyncio proxies: self.init_kwargs['fn']
        bound_fn = None
        if hasattr(self, "_worker") and hasattr(self._worker, "_bound_fn"):
            bound_fn = self._worker._bound_fn
        elif hasattr(self, "init_kwargs") and "fn" in self.init_kwargs:
            bound_fn = self.init_kwargs["fn"]

        if bound_fn is not None:
            # Bound function exists
            if fn is not _NO_ARG and callable(fn):
                # fn is a callable - use it as explicit function (allows overriding bound function)
                pass  # Use fn as-is
            elif fn is not _NO_ARG:
                # fn is not callable - treat as first iterable argument
                iterables = (fn,) + iterables
                fn = bound_fn
            else:
                # fn not provided - use bound function
                fn = bound_fn
        else:
            # No bound function - fn must be callable
            if fn is _NO_ARG:
                raise TypeError(
                    "map() requires a function argument when no bound function is set via .init(fn=...)"
                )
            if not callable(fn):
                raise TypeError(f"map() requires a callable function, got {type(fn).__name__}")

        # Check if worker is stopped
        if self._stopped:
            raise RuntimeError("Worker is stopped")

        # Collect iterables immediately (matching concurrent.futures behavior)
        iterables_list = [list(it) for it in iterables]

        # Validate all iterables have values
        if len(iterables_list) == 0:
            return iter([])

        # Initialize ProgressBar if requested
        pbar = None
        if progress:
            from ...utils.progress import ProgressBar

            total = len(iterables_list[0]) if len(iterables_list) > 0 else None

            if isinstance(progress, ProgressBar):
                pbar = progress
            elif isinstance(progress, dict):
                pbar = ProgressBar(total=total, **progress)
            else:
                pbar = ProgressBar(total=total)

        # Use regular zip (stops at shortest iterable, matching Python behavior)
        # Note: We call _execute_task directly to avoid double-wrapping when bound function exists
        futures = []
        for args in zip(*iterables_list):
            future = self._execute_task(fn, *args)
            futures.append(future)

        # Return an iterator that yields results in order
        def result_iterator():
            for future in futures:
                result = future.result(timeout=timeout)
                if pbar is not None:
                    pbar.update(1)
                yield result

            if pbar is not None:
                pbar.success("Complete!")

        return result_iterator()


class TaskWorker(Worker):
    """A generic worker for submitting arbitrary tasks.

    TaskWorker is a concrete worker implementation that provides an Executor-like
    interface for executing arbitrary functions in different execution contexts
    (sync, thread, process, asyncio, ray). Unlike custom workers that define
    specific methods, TaskWorker is designed for general-purpose task execution.

    This class implements the same interface as concurrent.futures.Executor:
    - submit(fn, *args, **kwargs): Submit a single task
    - map(fn, *iterables, **kwargs): Submit multiple tasks with automatic iteration

    This class is intended to be used by higher-level abstractions like
    WorkerExecutor and WorkerPool, or directly when you don't need custom worker methods.

    Examples:
        Basic Task Execution:
            ```python
            from concurry import TaskWorker

            # Create a task worker
            worker = TaskWorker.options(mode="thread").init()

            # Submit arbitrary functions
            def compute(x, y):
                return x ** 2 + y ** 2

            future = worker.submit(compute, 3, 4)
            result = future.result()  # 25

            worker.stop()
            ```

        Using map() for Multiple Tasks:
            ```python
            worker = TaskWorker.options(mode="process").init()

            def square(x):
                return x ** 2

            # Process multiple items
            results = list(worker.map(square, range(10)))
            print(results)  # [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

            worker.stop()
            ```

        With Different Execution Modes:
            ```python
            # Thread-based execution
            thread_worker = TaskWorker.options(mode="thread").init()

            # Process-based execution for CPU-intensive tasks
            process_worker = TaskWorker.options(mode="process").init()

            # Asyncio-based execution
            async_worker = TaskWorker.options(mode="asyncio").init()

            # Submit tasks to any of them
            result1 = thread_worker.submit(lambda x: x * 2, 10).result()
            result2 = process_worker.submit(lambda x: x ** 3, 5).result()
            result3 = async_worker.submit(lambda x: x + 100, 7).result()

            thread_worker.stop()
            process_worker.stop()
            async_worker.stop()
            ```

        Blocking Mode:
            ```python
            # Get results directly without futures
            worker = TaskWorker.options(mode="thread", blocking=True).init()

            result = worker.submit(lambda x: x * 10, 5)  # Returns 50 directly

            worker.stop()
            ```

        Multiple Tasks with map():
            ```python
            worker = TaskWorker.options(mode="process").init()

            def factorial(n):
                if n <= 1:
                    return 1
                return n * factorial(n - 1)

            # Process multiple inputs concurrently
            results = list(worker.map(factorial, range(1, 11)))
            print(results)  # [1, 2, 6, 24, 120, 720, 5040, 40320, 362880, 3628800]

            worker.stop()
            ```

        With Timeout:
            ```python
            import time
            from concurry import TaskWorker

            def slow_task(x):
                time.sleep(1)
                return x * 2

            worker = TaskWorker.options(mode="thread").init()

            # This will raise TimeoutError
            try:
                results = list(worker.map(slow_task, range(5), timeout=0.5))
            except TimeoutError:
                print("Task timed out!")

            worker.stop()
            ```

        With Bound Function:
            ```python
            from concurry import TaskWorker

            def compute(x, y):
                return x ** 2 + y ** 2

            # Bind function during initialization
            worker = TaskWorker.options(mode="thread").init(fn=compute)

            # Submit without passing function
            future = worker.submit(3, 4)
            result = future.result()  # 25

            # Map without passing function
            results = list(worker.map([(1, 2), (3, 4), (5, 6)]))

            # Call directly
            result = worker(3, 4).result()  # 25

            worker.stop()
            ```

        With Progress Bar:
            ```python
            from concurry import TaskWorker

            def square(x):
                return x ** 2

            worker = TaskWorker.options(mode="process").init(fn=square)

            # Show progress bar during map
            results = list(worker.map(range(100), progress=True))

            # Custom progress bar configuration
            results = list(worker.map(
                range(100),
                progress={"desc": "Processing", "ncols": 80}
            ))

            worker.stop()
            ```
    """

    def __init__(self, fn: Optional[Callable] = None):
        """Initialize the TaskWorker.

        Args:
            fn: Optional callable to bind to this worker. When provided,
                submit() and map() can be called without passing a function.
        """
        super().__init__()
        self._bound_fn = fn
