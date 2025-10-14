"""Worker implementation for concurry."""

import warnings
from abc import ABC
from typing import Any, Callable, Optional, Type, TypeVar

from morphic import Typed, validate
from morphic.structs import map_collection
from pydantic import ConfigDict, PrivateAttr

from ..config import ExecutionMode
from ..future import BaseFuture

T = TypeVar("T")


def _transform_worker_limits(
    limits: Any,
    mode: ExecutionMode,
    is_pool: bool,
) -> Optional[Any]:
    """Process limits parameter and return appropriate LimitSet or list of Limits.

    This function handles the common logic for processing limits for both
    WorkerProxy and WorkerProxyPool:
    - Converting list of Limits to LimitSet (for pools or in-memory workers)
    - Keeping list of Limits as-is (for Ray/Process single workers - to be created remotely)
    - Validating shared LimitSets
    - Checking mode compatibility

    Args:
        limits: The limits parameter (None, List[Limit], or LimitSet)
        mode: Execution mode (ExecutionMode enum)
        is_pool: True if processing for WorkerProxyPool, False for WorkerProxy

    Returns:
        - For pools: Processed LimitSet instance
        - For Ray/Process single workers: List of Limit objects (to be created remotely)
        - For in-memory single workers: LimitSet instance
        - None if no limits

    Raises:
        ValueError: If limits configuration is invalid
    """
    if limits is None:
        return None

    # Import here to avoid circular imports
    from ..limit import Limit
    from ..limit.limit_set import (
        BaseLimitSet,
        InMemorySharedLimitSet,
        LimitSet,
        MultiprocessSharedLimitSet,
        RaySharedLimitSet,
    )

    # Case 1: List of Limits
    if isinstance(limits, list):
        # Validate all items are Limit instances
        if len(limits) == 0 or not all(isinstance(item, Limit) for item in limits):
            raise ValueError("limits parameter must be either a LimitSet or a list of Limit objects")

        if is_pool:
            # WorkerProxyPool: create shared LimitSet with pool's mode
            return LimitSet(limits=limits, shared=True, mode=mode)
        else:
            # Single worker
            if mode in (ExecutionMode.Ray, ExecutionMode.Processes):
                # Ray/Process: Keep as list - LimitSet will be created inside the actor/process
                # This avoids serialization issues with threading locks
                return limits
            else:
                # Sync/Asyncio/Thread: Create LimitSet now (in-memory, non-shared)
                return LimitSet(limits=limits, shared=False, mode=ExecutionMode.Sync)

    # Case 2: Already a BaseLimitSet
    elif isinstance(limits, BaseLimitSet):
        # Check if it's a shared LimitSet
        assert limits.shared in {True, False}
        if limits.shared is False:
            if is_pool:
                # WorkerProxyPool: must be shared
                raise ValueError(
                    "WorkerProxyPool requires a shared LimitSet. "
                    "Create with: LimitSet(limits=[...], shared=True, mode='...')"
                )

            # WorkerProxy: if not shared, extract limits and handle based on mode
            limits_list = getattr(limits, "limits", [])

            if mode in (ExecutionMode.Ray, ExecutionMode.Processes):
                # Ray/Process: Keep as list
                warnings.warn(
                    "Passing non-shared LimitSet to Ray/Process worker. "
                    "The limits will be extracted and recreated inside the actor/process.",
                    UserWarning,
                    stacklevel=4,
                )
                return limits_list
            else:
                # Sync/Asyncio/Thread: Create new LimitSet
                warnings.warn(
                    "Passing non-shared LimitSet to WorkerProxy. "
                    "The limits will be copied as a new private LimitSet with shared=False and mode='sync'.",
                    UserWarning,
                    stacklevel=4,
                )
                return LimitSet(limits=limits_list, shared=False, mode=ExecutionMode.Sync)

        assert limits.shared is True
        # Validate mode compatibility for shared LimitSets:
        if isinstance(limits, InMemorySharedLimitSet):
            # InMemory backend - compatible with sync, asyncio, thread
            if mode not in (ExecutionMode.Sync, ExecutionMode.Asyncio, ExecutionMode.Threads):
                raise ValueError(
                    f"InMemorySharedLimitSet is not compatible with worker mode '{mode}'. "
                    f"Use mode='sync', 'asyncio', or 'thread' workers."
                )
        elif isinstance(limits, MultiprocessSharedLimitSet):
            # Multiprocess backend - only compatible with process
            if mode != ExecutionMode.Processes:
                raise ValueError(
                    f"MultiprocessSharedLimitSet is not compatible with worker mode '{mode}'. "
                    f"Use mode='process' workers."
                )
        elif isinstance(limits, RaySharedLimitSet):
            # Ray backend - only compatible with ray
            if mode != ExecutionMode.Ray:
                raise ValueError(
                    f"RaySharedLimitSet is not compatible with worker mode '{mode}'. Use mode='ray' workers."
                )

        return limits

    else:
        raise ValueError(
            f"limits parameter must be either a LimitSet or a list of Limit objects, "
            f"got {type(limits).__name__}"
        )


def _validate_shared_limitset_mode_compatibility(limit_set: Any, worker_mode: ExecutionMode) -> None:
    """Validate that a LimitSet is compatible with the worker mode.

    Args:
        limit_set: The LimitSet to validate
        worker_mode: The worker's execution mode

    Raises:
        ValueError: If the LimitSet is not compatible with the worker mode
    """


def _create_worker_wrapper(worker_cls: Type, limits: Any) -> Type:
    """Create a wrapper class that injects limits after worker initialization.

    This wrapper dynamically inherits from the user's worker class and adds
    a limits attribute after calling the parent's __init__.

    If limits is a list of Limit objects (for Ray/Process workers), it creates
    a LimitSet inside the worker (in the remote actor/process context). This
    avoids serialization issues with threading locks in LimitSet.

    Args:
        worker_cls: The original worker class
        limits: LimitSet instance OR list of Limit objects

    Returns:
        Wrapper class that sets limits attribute

    Example:
        ```python
        # For in-memory workers (limits is LimitSet):
        wrapper_cls = _create_worker_wrapper(MyWorker, limit_set)
        worker = wrapper_cls(*args, **kwargs)
        # worker.limits is the LimitSet

        # For Ray/Process workers (limits is list):
        wrapper_cls = _create_worker_wrapper(MyWorker, [CallLimit(...)])
        worker = wrapper_cls(*args, **kwargs)
        # worker.limits is a new LimitSet created inside the actor/process
        ```
    """

    class WorkerWithLimits(worker_cls):
        def __init__(self, *args, **kwargs):
            # If limits is a list, create LimitSet here (inside the actor/process)
            if isinstance(limits, list):
                # Import here to avoid circular imports
                from ..limit.limit_set import LimitSet

                # Create private LimitSet with mode=sync (uses threading.Lock, works everywhere)
                self.limits = LimitSet(limits=limits, shared=False, mode=ExecutionMode.Sync)
            else:
                # Already a LimitSet, use it directly
                self.limits = limits

            super().__init__(*args, **kwargs)

    # Preserve original class name for debugging
    WorkerWithLimits.__name__ = f"{worker_cls.__name__}_WithLimits"
    WorkerWithLimits.__qualname__ = f"{worker_cls.__qualname__}_WithLimits"

    return WorkerWithLimits


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
        max_workers: Optional[int] = None,
        load_balancing: Optional[str] = None,
        on_demand: bool = False,
        **options: Any,
    ):
        """Initialize the worker builder.

        Args:
            worker_cls: The worker class to instantiate
            mode: Execution mode (sync, thread, process, asyncio, ray)
            blocking: If True, method calls return results directly instead of futures
            max_workers: Maximum number of workers in pool (None = single worker)
            load_balancing: Load balancing algorithm for pool
            on_demand: If True, create workers on-demand
            **options: Additional options for the worker/pool

        Raises:
            ValueError: If deprecated init_args/init_kwargs are passed or invalid configuration
        """
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
        self._max_workers = max_workers
        self._load_balancing = load_balancing
        self._on_demand = on_demand
        self._options = options

        # Validate configuration
        self._validate_pool_config()

    def _validate_pool_config(self) -> None:
        """Validate pool configuration parameters.

        Raises:
            ValueError: If configuration is invalid
        """
        from ..config import ExecutionMode

        execution_mode = ExecutionMode(self._mode)

        # Validate max_workers for different modes
        if self._max_workers is not None:
            if self._max_workers < 0:
                raise ValueError("max_workers must be non-negative")

            # Sync and Asyncio must have max_workers=1 or None
            if execution_mode in (ExecutionMode.Sync, ExecutionMode.Asyncio):
                if self._max_workers != 1:
                    raise ValueError(
                        f"max_workers must be 1 for {execution_mode.value} mode, got {self._max_workers}"
                    )

        # Validate on_demand for different modes
        if self._on_demand:
            # Sync and Asyncio don't support on_demand
            if execution_mode in (ExecutionMode.Sync, ExecutionMode.Asyncio):
                raise ValueError(f"on_demand mode is not supported for {execution_mode.value} execution")

            # With on_demand and max_workers=0, validate limits
            if self._max_workers == 0:
                # This is valid for Thread, Process, and Ray
                pass

    def _get_default_max_workers(self) -> int:
        """Get default max_workers for pool based on mode.

        Returns:
            Default number of workers for the mode
        """
        from ..config import ExecutionMode

        execution_mode = ExecutionMode(self._mode)

        if execution_mode == ExecutionMode.Sync:
            return 1
        elif execution_mode == ExecutionMode.Asyncio:
            return 1
        elif execution_mode == ExecutionMode.Threads:
            return 24
        elif execution_mode == ExecutionMode.Processes:
            return 4
        elif execution_mode == ExecutionMode.Ray:
            return 0  # Unlimited for on-demand
        else:
            return 1

    def _get_default_load_balancing(self) -> str:
        """Get default load balancing algorithm.

        Returns:
            Default load balancing algorithm name
        """
        if self._on_demand:
            return "random"  # Random is best for ephemeral workers
        else:
            return "round_robin"  # Round-robin is best for persistent pools

    def _should_create_pool(self) -> bool:
        """Determine if a pool should be created.

        Returns:
            True if pool should be created, False for single worker
        """
        # On-demand always creates pool
        if self._on_demand:
            return True

        # max_workers > 1 creates pool
        if self._max_workers is not None and self._max_workers > 1:
            return True

        return False

    def init(self, *args: Any, **kwargs: Any) -> Any:
        """Initialize the worker instance with initialization arguments.

        Args:
            *args: Positional arguments for worker __init__
            **kwargs: Keyword arguments for worker __init__

        Returns:
            WorkerProxy (single worker) or WorkerProxyPool (pool)

        Example:
            ```python
            # Initialize single worker
            worker = MyWorker.options(mode="thread").init(multiplier=3)

            # Initialize worker pool
            pool = MyWorker.options(mode="thread", max_workers=10).init(multiplier=3)

            # Initialize with positional and keyword args
            worker = MyWorker.options(mode="process").init(10, name="processor")
            ```
        """
        # Determine if we should create a pool
        if self._should_create_pool():
            return self._create_pool(args, kwargs)
        else:
            return self._create_single_worker(args, kwargs)

    def _create_single_worker(self, args: tuple, kwargs: dict) -> "WorkerProxy":
        """Create a single worker instance.

        Args:
            args: Positional arguments for worker __init__
            kwargs: Keyword arguments for worker __init__

        Returns:
            WorkerProxy instance
        """
        from .asyncio_worker import AsyncioWorkerProxy
        from .process_worker import ProcessWorkerProxy
        from .sync_worker import SyncWorkerProxy
        from .task_worker import TaskWorker, TaskWorkerMixin
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

        # If this is TaskWorker, create a combined proxy class with TaskWorkerMixin
        if self._worker_cls is TaskWorker or (
            isinstance(self._worker_cls, type) and issubclass(self._worker_cls, TaskWorker)
        ):
            # Create a dynamic class that combines the base proxy with TaskWorkerMixin
            # Use TaskWorkerMixin as the first base class so its methods take precedence
            proxy_cls = type(
                f"Task{proxy_cls.__name__}",
                (TaskWorkerMixin, proxy_cls),
                {},
            )

        # Process limits if present
        processed_options = dict(self._options)
        if "limits" in processed_options and processed_options["limits"] is not None:
            processed_options["limits"] = _transform_worker_limits(
                limits=processed_options["limits"],
                mode=execution_mode,
                is_pool=False,
            )

        # Create proxy with init args/kwargs
        # Typed expects all parameters as keyword arguments
        return proxy_cls(
            worker_cls=self._worker_cls,
            init_args=args,
            init_kwargs=kwargs,
            blocking=self._blocking,
            **processed_options,
        )

    def _create_pool(self, args: tuple, kwargs: dict) -> Any:
        """Create a worker pool.

        Args:
            args: Positional arguments for worker __init__
            kwargs: Keyword arguments for worker __init__

        Returns:
            WorkerProxyPool instance
        """
        from ..config import ExecutionMode, LoadBalancingAlgorithm
        from .worker_pool import (
            InMemoryWorkerProxyPool,
            MultiprocessWorkerProxyPool,
            RayWorkerProxyPool,
        )

        # Convert mode string to ExecutionMode
        execution_mode = ExecutionMode(self._mode)

        # Determine max_workers (use defaults if not specified)
        max_workers = self._max_workers
        if max_workers is None:
            max_workers = self._get_default_max_workers()

        # Determine load_balancing algorithm
        load_balancing_str = self._load_balancing
        if load_balancing_str is None:
            load_balancing_str = self._get_default_load_balancing()
        load_balancing = LoadBalancingAlgorithm(load_balancing_str)

        # Process limits for pool using common function
        limits = self._options.get("limits")
        if limits is not None:
            limits = _transform_worker_limits(
                limits=limits,
                mode=execution_mode,
                is_pool=True,
            )

        # Update options with processed limits
        pool_options = dict(self._options)
        pool_options["limits"] = limits

        # Select appropriate pool class
        if execution_mode in (ExecutionMode.Sync, ExecutionMode.Asyncio, ExecutionMode.Threads):
            pool_cls = InMemoryWorkerProxyPool
        elif execution_mode == ExecutionMode.Processes:
            pool_cls = MultiprocessWorkerProxyPool
        elif execution_mode == ExecutionMode.Ray:
            pool_cls = RayWorkerProxyPool
        else:
            raise ValueError(f"Unsupported execution mode for pool: {execution_mode}")

        # Create pool instance
        return pool_cls(
            worker_cls=self._worker_cls,
            mode=execution_mode,
            max_workers=max_workers,
            load_balancing=load_balancing,
            on_demand=self._on_demand,
            blocking=self._blocking,
            unwrap_futures=self._options.get("unwrap_futures", True),
            limits=limits,
            init_args=args,
            init_kwargs=kwargs,
            **{k: v for k, v in pool_options.items() if k not in ("limits", "unwrap_futures")},
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
        worker = DataProcessor.options(mode="ray", actor_options={"num_cpus": 1}).init(2)
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

        # Submit async functions via TaskWorker
        from concurry import TaskWorker
        import asyncio

        async def compute(x, y):
            await asyncio.sleep(0.01)
            return x ** 2 + y ** 2

        task_worker = TaskWorker.options(mode="asyncio").init()
        result = task_worker.submit(compute, 3, 4).result()  # 25
        task_worker.stop()
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

    Submitting Arbitrary Functions with TaskWorker:
        ```python
        # Use TaskWorker for Executor-like interface
        from concurry import TaskWorker

        def compute(x, y):
            return x ** 2 + y ** 2

        task_worker = TaskWorker.options(mode="process").init()

        # Submit arbitrary functions
        future = task_worker.submit(compute, 3, 4)
        result = future.result()  # 25

        # Use map() for multiple tasks
        results = list(task_worker.map(lambda x: x * 100, [1, 2, 3, 4, 5]))

        task_worker.stop()
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

    Resource Protection with Limits:
        Workers support resource protection and rate limiting via the `limits` parameter.
        Limits enable control over API rates, resource pools, and call frequency.

        ```python
        from concurry import Worker, LimitSet, RateLimit, CallLimit, ResourceLimit
        from concurry import RateLimiterAlgorithm

        # Define limits
        limits = LimitSet(limits=[
            CallLimit(window_seconds=60, capacity=100),  # 100 calls/min
            RateLimit(
                key="api_tokens",
                window_seconds=60,
                algorithm=RateLimiterAlgorithm.TokenBucket,
                capacity=1000
            ),
            ResourceLimit(key="connections", capacity=10)
        ])

        class APIWorker(Worker):
            def __init__(self, api_key: str):
                self.api_key = api_key

            def call_api(self, prompt: str):
                # Acquire limits before operation
                # CallLimit automatically acquired with default of 1
                with self.limits.acquire(requested={"api_tokens": 100}) as acq:
                    result = external_api_call(prompt)
                    # Update with actual usage
                    acq.update(usage={"api_tokens": result.tokens_used})
                    return result.response

        # Option 1: Share limits across workers
        worker1 = APIWorker.options(mode="thread", limits=limits).init("key1")
        worker2 = APIWorker.options(mode="thread", limits=limits).init("key2")
        # Both workers share the 1000 token/min pool

        # Option 2: Private limits per worker
        limit_defs = [
            RateLimit(key="tokens", window_seconds=60, capacity=1000)
        ]
        worker = APIWorker.options(mode="thread", limits=limit_defs).init("key")
        # This worker has its own private 1000 token/min pool
        ```

        **Limit Types:**
        - `CallLimit`: Count calls (usage always 1, no update needed)
        - `RateLimit`: Token/bandwidth limiting (requires update() call)
        - `ResourceLimit`: Semaphore-based resources (no update needed)

        **Key Behaviors:**
        - Passing `LimitSet`: Workers share the same limit pool
        - Passing `List[Limit]`: Each worker gets private limits
        - CallLimit/ResourceLimit auto-acquired with default of 1
        - RateLimits must be explicitly specified in `requested` dict
        - RateLimits require `update()` call (raises RuntimeError if missing)

        See user guide for more: `/docs/user-guide/limits.md`
    """

    @classmethod
    @validate
    def options(
        cls: Type[T],
        mode: str = "sync",
        blocking: bool = False,
        max_workers: Optional[int] = None,
        load_balancing: Optional[str] = None,
        on_demand: bool = False,
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
            max_workers: Maximum number of workers in pool (optional)
                - If None or 1: Creates single worker
                - If > 1: Creates worker pool with specified size
                - Sync/Asyncio: Must be 1 or None (raises error otherwise)
                - Thread: Default 24 when pool requested
                - Process: Default 4 when pool requested
                - Ray: Default 0 (unlimited for on-demand)
            load_balancing: Load balancing algorithm (optional)
                - "round_robin": Distribute requests evenly (default for pools)
                - "least_active": Select worker with fewest active calls
                - "least_total": Select worker with fewest total calls
                - "random": Random selection (default for on-demand)
            on_demand: If True, create workers on-demand per request (default: False)
                - Workers are created for each request and destroyed after completion
                - Useful for bursty workloads or resource-constrained environments
                - Cannot be used with Sync/Asyncio modes
                - With max_workers=0: Unlimited concurrent workers (Ray) or
                  limited to cpu_count()-1 (Thread/Process)
            unwrap_futures: If True (default), automatically unwrap BaseFuture arguments
                by calling .result() on them before passing to worker methods. This enables
                seamless composition of workers. Set to False to pass futures as-is.
            limits: Resource protection and rate limiting (optional)
                - Pass LimitSet: Workers share the same limit pool
                - Pass List[Limit]: Each worker gets private limits (creates shared LimitSet for pools)
                See Worker docstring "Resource Protection with Limits" section for details.
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

            Worker Pools:
                ```python
                # Create a thread pool with 10 workers
                pool = MyWorker.options(mode="thread", max_workers=10).init(multiplier=3)
                future = pool.process(10)  # Dispatched to one of 10 workers

                # Process pool with load balancing
                pool = MyWorker.options(
                    mode="process",
                    max_workers=4,
                    load_balancing="least_active"
                ).init(multiplier=3)

                # On-demand workers for bursty workloads
                pool = MyWorker.options(
                    mode="ray",
                    on_demand=True,
                    max_workers=0  # Unlimited
                ).init(multiplier=3)
                ```
        """
        return WorkerBuilder(
            worker_cls=cls,
            mode=mode,
            blocking=blocking,
            max_workers=max_workers,
            load_balancing=load_balancing,
            on_demand=on_demand,
            **kwargs,
        )

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
    limits: Optional[Any] = None  # LimitSet instance (processed by WorkerBuilder)

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
