"""Worker implementation for concurry."""

from abc import ABC
from typing import Any, Callable, Optional, Type, TypeVar

from morphic import Typed, validate
from morphic.structs import map_collection
from pydantic import ConfigDict, PrivateAttr

from ..config import ExecutionMode
from ..future import BaseFuture

T = TypeVar("T")


def _unwrap_future_value(obj: Any) -> Any:
    """Unwrap a single future or return object as-is.

    Args:
        obj: Object that might be a BaseFuture

    Returns:
        Materialized value if obj is a BaseFuture, otherwise obj unchanged
    """

    if isinstance(obj, BaseFuture):
        return obj.result()
    return obj


def _unwrap_futures_in_args(
    args: tuple,
    kwargs: dict,
    unwrap_futures: bool,
) -> tuple:
    """Unwrap all BaseFuture instances in args and kwargs.

    Recursively traverses nested collections (list, tuple, dict, set)
    and unwraps any BaseFuture instances found.

    Optimized with fast-path: for simple cases (no collections, no futures),
    returns immediately without calling map_collection. This saves ~0.5µs per call
    when no futures or collections are present (the common case in tight loops).

    Args:
        args: Positional arguments
        kwargs: Keyword arguments
        unwrap_futures: Whether to perform unwrapping

    Returns:
        Tuple of (unwrapped_args, unwrapped_kwargs)
    """
    if not unwrap_futures:
        return args, kwargs

    # Fast-path: Quick scan for BaseFuture instances or collections
    # If we find either, we need to do the expensive unwrapping
    has_future_or_collection = False

    for arg in args:
        if isinstance(arg, BaseFuture):
            has_future_or_collection = True
            break
        # Collections need recursive checking, so we can't skip them
        if isinstance(arg, (list, tuple, dict, set)):
            has_future_or_collection = True
            break

    if not has_future_or_collection:
        for value in kwargs.values():
            if isinstance(value, BaseFuture):
                has_future_or_collection = True
                break
            if isinstance(value, (list, tuple, dict, set)):
                has_future_or_collection = True
                break

    # Fast-path: if no futures or collections, return immediately
    if not has_future_or_collection:
        return args, kwargs

    # Do expensive recursive unwrapping for cases with futures or collections
    unwrapped_args = tuple(map_collection(arg, _unwrap_future_value, recurse=True) for arg in args)

    # Unwrap each kwarg value with recursive traversal
    unwrapped_kwargs = {
        key: map_collection(value, _unwrap_future_value, recurse=True) for key, value in kwargs.items()
    }

    return unwrapped_args, unwrapped_kwargs


class WorkerBuilder:
    """Builder for creating worker instances with deferred initialization.

    This class holds configuration from .options() or .pool() calls and provides
    a .init() method to instantiate the actual worker with initialization arguments.
    """

    def __init__(
        self,
        worker_cls: Type["Worker"],
        mode: str,
        blocking: bool = False,
        is_pool: bool = False,
        **options: Any,
    ):
        """Initialize the worker builder.

        Args:
            worker_cls: The worker class to instantiate
            mode: Execution mode (sync, thread, process, asyncio, ray)
            blocking: If True, method calls return results directly instead of futures
            is_pool: If True, create a worker pool instead of single worker
            **options: Additional options for the worker/pool

        Raises:
            ValueError: If deprecated init_args or init_kwargs are passed
        """
        # Reject old API explicitly
        if "init_args" in options:
            raise ValueError(
                "The 'init_args' parameter is no longer supported. "
                "Use .init(*args) instead. "
                "Example: Worker.options(mode='thread').init(arg1, arg2)"
            )
        if "init_kwargs" in options:
            raise ValueError(
                "The 'init_kwargs' parameter is no longer supported. "
                "Use .init(**kwargs) instead. "
                "Example: Worker.options(mode='thread').init(key1=val1, key2=val2)"
            )

        self._worker_cls = worker_cls
        self._mode = mode
        self._blocking = blocking
        self._is_pool = is_pool
        self._options = options

    def init(self, *args: Any, **kwargs: Any) -> "WorkerProxy":
        """Initialize the worker instance with initialization arguments.

        Args:
            *args: Positional arguments for worker __init__
            **kwargs: Keyword arguments for worker __init__

        Returns:
            WorkerProxy configured and initialized with the given arguments

        Example:
            ```python
            # Initialize single worker
            worker = MyWorker.options(mode="thread").init(multiplier=3)

            # Initialize with positional and keyword args
            worker = MyWorker.options(mode="process").init(10, name="processor")
            ```
        """
        if self._is_pool:
            raise NotImplementedError(
                "Worker pools will be implemented in a future update. "
                "For now, use .options() to create individual workers."
            )

        from .asyncio_worker import AsyncioWorkerProxy
        from .process_worker import ProcessWorkerProxy
        from .sync_worker import SyncWorkerProxy
        from .thread_worker import ThreadWorkerProxy

        # Convert mode string to ExecutionMode
        execution_mode = ExecutionMode(self._mode)

        # Select appropriate proxy class
        if execution_mode == ExecutionMode.Sync:
            proxy_cls = SyncWorkerProxy
        elif execution_mode == ExecutionMode.Threads:
            proxy_cls = ThreadWorkerProxy
        elif execution_mode == ExecutionMode.Processes:
            proxy_cls = ProcessWorkerProxy
        elif execution_mode == ExecutionMode.Asyncio:
            proxy_cls = AsyncioWorkerProxy
        elif execution_mode == ExecutionMode.Ray:
            from .ray_worker import RayWorkerProxy

            proxy_cls = RayWorkerProxy
        else:
            raise ValueError(f"Unsupported execution mode: {execution_mode}")

        # Create proxy with init args/kwargs
        # Typed expects all parameters as keyword arguments
        return proxy_cls(
            worker_cls=self._worker_cls,
            init_args=args,
            init_kwargs=kwargs,
            blocking=self._blocking,
            **self._options,
        )


class Worker:
    """Base class for workers in concurry.

    This class provides the foundation for user-defined workers. Users should inherit from this class
    and implement their worker logic. The worker will be automatically managed by the executor.

    The Worker class implements the actor pattern, allowing you to run methods in different execution
    contexts (sync, thread, process, asyncio, ray) while maintaining state isolation and providing
    a unified Future-based API.

    **Important Design Note:**

    The Worker class itself does NOT inherit from morphic.Typed.     This design choice allows you
    complete freedom in defining your `__init__` method - you can use any signature with any
    combination of positional arguments, keyword arguments, *args, and **kwargs. The Typed
    integration is applied at the WorkerProxy layer, which wraps your worker and provides
    validation for worker configuration (mode, blocking, etc.) but not for worker initialization.

    This means you can use:
    - Plain Python classes
    - Pydantic models (if you want)
    - Dataclasses (if you want)
    - Attrs classes (if you want)
    - Any other class structure

    The only requirement is that your worker class is instantiable via `__init__` with the
    arguments you pass to `.init()`.

    Basic Usage:
        ```python
        from concurry import Worker

        class DataProcessor(Worker):
            def __init__(self, multiplier: int):
                self.multiplier = multiplier
                self.count = 0

            def process(self, value: int) -> int:
                self.count += 1
                return value * self.multiplier

        # Initialize worker with thread execution
        worker = DataProcessor.options(mode="thread").init(3)
        future = worker.process(10)
        result = future.result()  # 30
        worker.stop()
        ```

    Different Execution Modes:
        ```python
        # Synchronous (for testing/debugging)
        worker = DataProcessor.options(mode="sync").init(2)

        # Thread-based (good for I/O-bound tasks)
        worker = DataProcessor.options(mode="thread").init(2)

        # Process-based (good for CPU-bound tasks)
        worker = DataProcessor.options(mode="process").init(2)

        # Asyncio-based (good for async I/O)
        worker = DataProcessor.options(mode="asyncio").init(2)

        # Ray-based (distributed computing)
        import ray
        ray.init()
        worker = DataProcessor.options(mode="ray", num_cpus=1).init(2)
        ```

    Async Function Support:
        All workers can execute both sync and async functions. Async functions are
        automatically detected and executed correctly across all modes.

        ```python
        import asyncio

        class AsyncWorker(Worker):
            def __init__(self):
                self.count = 0

            async def async_method(self, x: int) -> int:
                await asyncio.sleep(0.01)  # Simulate async I/O
                self.count += 1
                return x * 2

            def sync_method(self, x: int) -> int:
                return x + 10

        # Use asyncio mode for best async performance
        worker = AsyncWorker.options(mode="asyncio").init()
        result1 = worker.async_method(5).result()  # 10
        result2 = worker.sync_method(5).result()  # 15
        worker.stop()

        # Submit async functions via submit_task
        async def compute(x, y):
            await asyncio.sleep(0.01)
            return x ** 2 + y ** 2

        worker = AsyncWorker.options(mode="asyncio").init()
        result = worker.submit_task(compute, 3, 4).result()  # 25
        worker.stop()
        ```

        **Performance:** AsyncioWorkerProxy provides significant speedup (5-15x) for
        I/O-bound async operations by enabling true concurrent execution. Other modes
        execute async functions correctly but without concurrency benefits.

    Blocking Mode:
        ```python
        # Returns results directly instead of futures
        worker = DataProcessor.options(mode="thread", blocking=True).init(5)
        result = worker.process(10)  # Returns 50 directly, not a future
        worker.stop()
        ```

    Submitting Arbitrary Functions:
        ```python
        # Use submit_task() like Executor.submit()
        def compute(x, y):
            return x ** 2 + y ** 2

        worker = DataProcessor.options(mode="process").init(1)

        # Submit function that's not a worker method
        future = worker.submit_task(compute, 3, 4)
        result = future.result()  # 25

        # Mix method calls and task submission
        result1 = worker.process(10).result()  # Uses worker method
        result2 = worker.submit_task(lambda x: x * 100, 5).result()  # Arbitrary function

        worker.stop()
        ```

    State Management:
        ```python
        class Counter(Worker):
            def __init__(self):
                self.count = 0

            def increment(self):
                self.count += 1
                return self.count

        # Each worker maintains its own state
        worker1 = Counter.options(mode="thread").init()
        worker2 = Counter.options(mode="thread").init()

        print(worker1.increment().result())  # 1
        print(worker1.increment().result())  # 2
        print(worker2.increment().result())  # 1 (separate state)

        worker1.stop()
        worker2.stop()
        ```
    """

    @classmethod
    @validate
    def options(
        cls: Type[T],
        mode: str = "sync",
        blocking: bool = False,
        **kwargs: Any,
    ) -> WorkerBuilder:
        """Configure worker execution options.

        Returns a WorkerBuilder that can be used to create worker instances
        with .init(*args, **kwargs).

        **Type Validation:**

        This method uses the `@validate` decorator from morphic, providing:
        - Automatic type checking and conversion
        - String-to-bool coercion (e.g., "true" → True)
        - AutoEnum fuzzy matching for mode parameter
        - Enhanced error messages for invalid inputs

        Args:
            mode: Execution mode (sync, thread, process, asyncio, ray)
                Accepts string or ExecutionMode enum value
            blocking: If True, method calls return results directly instead of futures
                Accepts bool or string representation ("true", "false", "1", "0")
            unwrap_futures: If True (default), automatically unwrap BaseFuture arguments
                by calling .result() on them before passing to worker methods. This enables
                seamless composition of workers. Set to False to pass futures as-is.
            **kwargs: Additional options passed to the worker implementation
                - For ray: num_cpus, num_gpus, resources, etc.
                - For process: mp_context (fork, spawn, forkserver)

        Returns:
            A WorkerBuilder instance that can create workers via .init()

        Examples:
            Basic Usage:
                ```python
                # Configure and create worker
                worker = MyWorker.options(mode="thread").init(multiplier=3)
                ```

            Type Coercion:
                ```python
                # String booleans are automatically converted
                worker = MyWorker.options(mode="thread", blocking="true").init()
                assert worker.blocking is True
                ```

            Mode-Specific Options:
                ```python
                # Ray with resource requirements
                worker = MyWorker.options(
                    mode="ray",
                    num_cpus=2,
                    num_gpus=1
                ).init(multiplier=3)

                # Process with spawn context
                worker = MyWorker.options(
                    mode="process",
                    mp_context="spawn"
                ).init(multiplier=3)
                ```

            Future Unwrapping (Default Enabled):
                ```python
                # Automatic future unwrapping (default)
                producer = Worker1.options(mode="thread").init()
                consumer = Worker2.options(mode="thread").init()

                future = producer.compute(10)  # Returns BaseFuture
                result = consumer.process(future).result()  # future is auto-unwrapped

                # Disable unwrapping to pass futures as objects
                worker = MyWorker.options(mode="thread", unwrap_futures=False).init()
                result = worker.inspect_future(future).result()  # Receives BaseFuture object
                ```
        """
        return WorkerBuilder(worker_cls=cls, mode=mode, blocking=blocking, is_pool=False, **kwargs)

    @classmethod
    @validate
    def pool(
        cls: Type[T],
        max_workers: Optional[int] = None,
        mode: str = "thread",
        blocking: bool = False,
        **kwargs: Any,
    ) -> WorkerBuilder:
        """Configure a worker pool (not yet implemented).

        Returns a WorkerBuilder configured for pool mode. When implemented,
        this will create a pool of workers that share the same interface
        as a single worker but with automatic load balancing.

        Args:
            max_workers: Maximum number of workers in the pool
            mode: Execution mode for workers in the pool
            blocking: If True, method calls return results directly instead of futures
            **kwargs: Additional options for the worker pool

        Returns:
            A WorkerBuilder that will create a worker pool

        Raises:
            NotImplementedError: Pool support will be added in a future update

        Example (future API):
            ```python
            # Create pool of workers
            pool = MyWorker.pool(max_workers=5, mode="thread").init(multiplier=3)

            # Use exactly like a single worker
            future = pool.process(10)
            result = future.result()  # Dispatches to available worker
            ```
        """
        return WorkerBuilder(
            worker_cls=cls, mode=mode, blocking=blocking, is_pool=True, max_workers=max_workers, **kwargs
        )

    def __new__(cls, *args, **kwargs):
        """Override __new__ to support direct instantiation as sync mode."""
        # If instantiated directly (not via options), behave as sync mode
        if cls is Worker:
            raise TypeError("Worker cannot be instantiated directly. Subclass it or use @worker decorator.")

        # Check if this is being called from a proxy
        # This is a bit of a hack but allows: worker = MLModelWorker() to work
        instance = super().__new__(cls)
        return instance

    def __init__(self, *args, **kwargs):
        """Initialize the worker. Subclasses can override this freely."""
        pass


class WorkerProxy(Typed, ABC):
    """Base class for worker proxies.

    This class defines the interface for worker proxies. Each executor type will provide
    its own implementation of this class.

    **Typed Integration:**

    WorkerProxy inherits from morphic.Typed (a Pydantic BaseModel wrapper) to provide:

    - **Automatic Validation**: All configuration fields are validated at creation time
    - **Immutable Configuration**: Public fields (worker_cls, blocking, etc.) are frozen
      and cannot be modified after initialization
    - **Type-Checked Private Attributes**: Private attributes (prefixed with _) support
      automatic type checking on updates using Pydantic's validation system
    - **Enhanced Error Messages**: Clear validation errors with detailed context

    **Architecture:**

    - **Public Fields**: Defined as regular Pydantic fields, frozen after initialization
      - `worker_cls`: The worker class to instantiate
      - `blocking`: Whether method calls return results directly instead of futures
      - `unwrap_futures`: Whether to automatically unwrap BaseFuture arguments (default: True)
      - `init_args`: Positional arguments for worker initialization
      - `init_kwargs`: Keyword arguments for worker initialization
      - Subclass-specific fields (e.g., `num_cpus` for RayWorkerProxy)

    - **Private Attributes**: Defined using PrivateAttr(), initialized in post_initialize()
      - `_stopped`: Boolean flag indicating if worker is stopped
      - `_options`: Dictionary of additional options
      - Implementation-specific attributes (e.g., `_thread`, `_process`, `_loop`)

    **Future Unwrapping:**

    By default (`unwrap_futures=True`), BaseFuture arguments are automatically unwrapped
    by calling `.result()` before passing to worker methods. This enables seamless worker
    composition where one worker's output can be directly passed to another worker.
    Nested futures in collections (lists, dicts, tuples) are also unwrapped recursively.

    **Usage Notes:**

    - Subclasses should define public fields as regular Pydantic fields with type hints
    - Private attributes should use `PrivateAttr()` and be initialized in `post_initialize()`
    - Use `Any` type hint for non-serializable private attributes (Queue, Thread, etc.)
    - Private attributes can be updated during execution with automatic type checking
    - Call `super().post_initialize()` in subclass post_initialize methods
    - Access public fields directly (e.g., `self.num_cpus`) instead of copying to private attrs

    **Example Subclass:**

        ```python
        from pydantic import PrivateAttr
        from typing import Any

        class CustomWorkerProxy(WorkerProxy):
            # Public fields (immutable after creation)
            custom_option: str = "default"

            # Private attributes (mutable, type-checked)
            _custom_state: int = PrivateAttr()
            _custom_resource: Any = PrivateAttr()  # Use Any for non-serializable types

            def post_initialize(self) -> None:
                super().post_initialize()
                self._custom_state = 0
                self._custom_resource = SomeNonSerializableObject()
        ```
    """

    # Override Typed's config to allow extra fields
    model_config = ConfigDict(
        extra="allow",  # Allow extra fields beyond defined ones
        frozen=True,
        validate_default=True,
        arbitrary_types_allowed=True,
        validate_assignment=False,
        validate_private_assignment=True,
    )

    worker_cls: Type[Worker]
    blocking: bool = False
    unwrap_futures: bool = True
    init_args: tuple = ()
    init_kwargs: dict = {}

    # Private attributes (defined with PrivateAttr, initialized in post_initialize)
    _stopped: bool = PrivateAttr(default=False)
    _options: dict = PrivateAttr(default_factory=dict)
    _method_cache: dict = PrivateAttr(default_factory=dict)

    def post_initialize(self) -> None:
        """Initialize private attributes after Typed validation."""
        # Capture any extra fields that weren't explicitly defined
        # Pydantic stores extra fields in __pydantic_extra__
        if hasattr(self, "__pydantic_extra__") and self.__pydantic_extra__:
            self._options = dict(self.__pydantic_extra__)

        # Initialize method cache for performance
        self._method_cache = {}

    def __getattr__(self, name: str) -> Callable:
        """Intercept method calls and dispatch them appropriately.

        This implementation caches method wrappers for performance,
        saving ~0.5-1µs per call after the first invocation.

        Args:
            name: Method name

        Returns:
            A callable that will execute the method
        """
        # Check cache first (performance optimization)
        cache = self.__dict__.get("_method_cache")
        if cache is not None and name in cache:
            return cache[name]

        # Don't intercept private/dunder methods - let Pydantic's BaseModel handle them
        if name.startswith("_"):
            # Call parent's __getattr__ to properly handle Pydantic private attributes
            return super().__getattr__(name)

        def method_wrapper(*args, **kwargs):
            # Access private attributes using Pydantic's mechanism
            # Pydantic automatically handles __pydantic_private__ lookup
            if self._stopped:
                raise RuntimeError("Worker is stopped")

            future = self._execute_method(name, *args, **kwargs)

            if self.blocking:
                # Return result directly (blocking)
                return future.result()
            else:
                # Return future (non-blocking)
                return future

        # Cache the wrapper for next time
        if cache is not None:
            cache[name] = method_wrapper

        return method_wrapper

    def _execute_method(self, method_name: str, *args: Any, **kwargs: Any):
        """Execute a method on the worker.

        Args:
            method_name: Name of the method to invoke
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            BaseFuture for the method execution
        """
        raise NotImplementedError("Subclasses must implement _execute_method")

    def submit_task(self, fn: Callable, *args: Any, **kwargs: Any):
        """Submit an arbitrary function to be executed by the worker.

        This method allows submitting any callable function to be executed in the
        worker's execution context (thread, process, asyncio loop, or Ray actor).
        Similar to Executor.submit() but runs in the worker's context.

        Args:
            fn: Callable function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            BaseFuture for the task execution (or result directly if blocking=True)

        Examples:
            Basic Function Submission:
                ```python
                def compute(x, y):
                    return x + y

                worker = Worker.options(mode="process")
                future = worker.submit_task(compute, 10, 20)
                result = future.result()  # 30
                worker.stop()
                ```

            Using with Lambda Functions:
                ```python
                worker = Worker.options(mode="thread")
                future = worker.submit_task(lambda x: x ** 2, 5)
                result = future.result()  # 25
                worker.stop()
                ```

            Mixing Method Calls and Task Submission:
                ```python
                class DataProcessor(Worker):
                    def __init__(self, multiplier: int):
                        self.multiplier = multiplier

                    def process(self, value: int) -> int:
                        return value * self.multiplier

                worker = DataProcessor.options(mode="thread").init(2)

                # Call worker method
                result1 = worker.process(10).result()  # 20

                # Submit arbitrary function
                result2 = worker.submit_task(lambda x: x + 100, 5).result()  # 105

                worker.stop()
                ```

            With Keyword Arguments:
                ```python
                def complex_calc(x, y, power=2, offset=0):
                    return (x ** power + y ** power) + offset

                worker = Worker.options(mode="process")
                future = worker.submit_task(complex_calc, 3, 4, power=2, offset=10)
                result = future.result()  # 35
                worker.stop()
                ```

            In Blocking Mode:
                ```python
                worker = Worker.options(mode="thread", blocking=True)
                result = worker.submit_task(lambda x: x * 2, 50)  # Returns 100 directly
                worker.stop()
                ```
        """
        # Access attributes using Pydantic's mechanism
        if self._stopped:
            raise RuntimeError("Worker is stopped")

        future = self._execute_task(fn, *args, **kwargs)

        if self.blocking:
            # Return result directly (blocking)
            return future.result()
        else:
            # Return future (non-blocking)
            return future

    def _execute_task(self, fn: Callable, *args: Any, **kwargs: Any):
        """Execute an arbitrary function on the worker.

        Args:
            fn: Callable function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            BaseFuture for the task execution
        """
        raise NotImplementedError("Subclasses must implement _execute_task")

    def stop(self, timeout: float = 30) -> None:
        """Stop the worker and clean up resources.

        Args:
            timeout: Maximum time to wait for cleanup in seconds
        """
        # Pydantic allows setting private attributes even on frozen models
        self._stopped = True


def worker(cls: Type[T]) -> Type[T]:
    """Decorator to mark a class as a worker.

    This decorator converts a regular class into a Worker, allowing it to use
    the `.options()` method for execution mode selection. This is optional -
    classes can also directly inherit from Worker.

    Args:
        cls: The class to convert into a worker

    Returns:
        The worker class with Worker capabilities

    Examples:
        Basic Decorator Usage:
            ```python
            from concurry import worker

            @worker
            class DataProcessor:
                def __init__(self, multiplier: int):
                    self.multiplier = multiplier

                def process(self, value: int) -> int:
                    return value * self.multiplier

            # Use like any Worker
            processor = DataProcessor.options(mode="thread").init(3)
            result = processor.process(10).result()  # 30
            processor.stop()
            ```

        Equivalent to Inheriting from Worker:
            ```python
            # These two are equivalent:

            # Using decorator
            @worker
            class ProcessorA:
                def __init__(self, value: int):
                    self.value = value

            # Inheriting from Worker
            class ProcessorB(Worker):
                def __init__(self, value: int):
                    self.value = value
            ```

        With Different Execution Modes:
            ```python
            @worker
            class Calculator:
                def __init__(self):
                    self.operations = 0

                def calculate(self, x: int, y: int) -> int:
                    self.operations += 1
                    return x + y

            # Use with any execution mode
            calc_thread = Calculator.options(mode="thread")
            calc_process = Calculator.options(mode="process")
            calc_sync = Calculator.options(mode="sync")
            ```
    """
    if not isinstance(cls, type):
        raise TypeError(f"@worker decorator requires a class, got {type(cls).__name__}")

    # Make the class inherit from Worker if it doesn't already
    if not issubclass(cls, Worker):
        # Create a new class that inherits from both Worker and the original class
        cls = type(cls.__name__, (Worker, cls), dict(cls.__dict__))

    return cls


class TaskWorker(Worker):
    """A generic worker for submitting arbitrary tasks.

    TaskWorker is a concrete worker implementation that has no custom methods
    and is designed specifically for executing arbitrary functions via submit_task().
    This is useful when you don't need to define custom worker methods but just
    want to execute functions in different execution contexts.

    This class is intended to be used by higher-level abstractions like
    WorkerExecutor and WorkerPool.

    Examples:
        Basic Task Execution:
            ```python
            from concurry import TaskWorker

            # Create a task worker
            worker = TaskWorker.options(mode="thread").init()

            # Submit arbitrary functions
            def compute(x, y):
                return x ** 2 + y ** 2

            future = worker.submit_task(compute, 3, 4)
            result = future.result()  # 25

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
            result1 = thread_worker.submit_task(lambda x: x * 2, 10).result()
            result2 = process_worker.submit_task(lambda x: x ** 3, 5).result()
            result3 = async_worker.submit_task(lambda x: x + 100, 7).result()

            thread_worker.stop()
            process_worker.stop()
            async_worker.stop()
            ```

        Blocking Mode:
            ```python
            # Get results directly without futures
            worker = TaskWorker.options(mode="thread", blocking=True).init()

            result = worker.submit_task(lambda x: x * 10, 5)  # Returns 50 directly

            worker.stop()
            ```

        Multiple Tasks:
            ```python
            worker = TaskWorker.options(mode="process").init()

            # Submit multiple tasks
            futures = [
                worker.submit_task(lambda x: x ** 2, i)
                for i in range(10)
            ]

            # Collect results
            results = [f.result() for f in futures]
            print(results)  # [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

            worker.stop()
            ```
    """

    def __init__(self):
        """Initialize the TaskWorker.

        TaskWorker requires no initialization arguments.
        """
        super().__init__()
