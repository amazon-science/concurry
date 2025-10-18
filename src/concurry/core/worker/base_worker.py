"""Worker implementation for concurry."""

import warnings
from abc import ABC
from typing import Any, Callable, Optional, Type, TypeVar

from morphic import Typed, validate
from morphic.structs import map_collection
from pydantic import ConfigDict, PrivateAttr

from ..constants import ExecutionMode
from ..future import BaseFuture
from ..limit.limit_set import LimitSet
from ..retry import RetryAlgorithm, RetryConfig, create_retry_wrapper

T = TypeVar("T")


def _transform_worker_limits(
    limits: Any,
    mode: ExecutionMode,
    is_pool: bool,
    worker_index: int = 0,
) -> Any:
    """Process limits parameter and return LimitPool.

    This function always returns a LimitPool wrapping one or more LimitSets.
    This provides a unified interface and enables multi-region/multi-account scenarios.

    Args:
        limits: The limits parameter (None, List[Limit], LimitSet, List[LimitSet], or LimitPool)
        mode: Execution mode (ExecutionMode enum)
        is_pool: True if processing for WorkerProxyPool, False for WorkerProxy
        worker_index: Starting offset for round-robin selection in LimitPool (default 0)

    Returns:
        LimitPool instance wrapping one or more LimitSets

    Raises:
        ValueError: If limits configuration is invalid
    """
    # Import here to avoid circular imports
    from ..limit import Limit
    from ..limit.limit_pool import LimitPool
    from ..limit.limit_set import (
        BaseLimitSet,
        InMemorySharedLimitSet,
        LimitSet,
        MultiprocessSharedLimitSet,
        RaySharedLimitSet,
    )

    # Case 1: None -> Create empty LimitPool with empty LimitSet
    if limits is None:
        # Create empty LimitSet
        if is_pool:
            empty_limitset = LimitSet(limits=[], shared=True, mode=mode)
        else:
            if mode in (ExecutionMode.Ray, ExecutionMode.Processes):
                # For Ray/Process, create list to be wrapped remotely
                empty_limitset = []
            else:
                empty_limitset = LimitSet(limits=[], shared=False, mode=ExecutionMode.Sync)

        # Wrap in LimitPool (unless it's a list for remote creation)
        if isinstance(empty_limitset, list):
            return empty_limitset  # Will be wrapped in LimitPool by _create_worker_wrapper
        return LimitPool(limit_sets=[empty_limitset], load_balancing="round_robin", worker_index=worker_index)

    # Case 2: Already a LimitPool -> pass through or validate
    if isinstance(limits, LimitPool):
        return limits

    # Case 3: List - could be List[Limit] or List[LimitSet]
    if isinstance(limits, list):
        if len(limits) == 0:
            # Empty list -> treat as no limits
            if is_pool:
                empty_limitset = LimitSet(limits=[], shared=True, mode=mode)
            else:
                if mode in (ExecutionMode.Ray, ExecutionMode.Processes):
                    return []  # Will be wrapped remotely
                empty_limitset = LimitSet(limits=[], shared=False, mode=ExecutionMode.Sync)
            return LimitPool(
                limit_sets=[empty_limitset], load_balancing="round_robin", worker_index=worker_index
            )

        # Check if List[Limit]
        if all(isinstance(item, Limit) for item in limits):
            # Create LimitSet from Limits
            if is_pool:
                limitset = LimitSet(limits=limits, shared=True, mode=mode)
            else:
                if mode in (ExecutionMode.Ray, ExecutionMode.Processes):
                    return limits  # Keep as list, will be wrapped remotely
                limitset = LimitSet(limits=limits, shared=False, mode=ExecutionMode.Sync)
            return LimitPool(limit_sets=[limitset], load_balancing="round_robin", worker_index=worker_index)

        # Check if List[LimitSet]
        if all(isinstance(item, BaseLimitSet) for item in limits):
            # Validate all are shared and compatible with mode
            for ls in limits:
                if not ls.shared:
                    raise ValueError(
                        "All LimitSets in a list must be shared. "
                        "Create with: LimitSet(limits=[...], shared=True, mode='...')"
                    )
                # Validate mode compatibility
                if isinstance(ls, InMemorySharedLimitSet):
                    if mode not in (ExecutionMode.Sync, ExecutionMode.Asyncio, ExecutionMode.Threads):
                        raise ValueError(
                            f"InMemorySharedLimitSet is not compatible with worker mode '{mode}'. "
                            f"Use mode='sync', 'asyncio', or 'thread' workers."
                        )
                elif isinstance(ls, MultiprocessSharedLimitSet):
                    if mode != ExecutionMode.Processes:
                        raise ValueError(
                            f"MultiprocessSharedLimitSet is not compatible with worker mode '{mode}'. "
                            f"Use mode='process' workers."
                        )
                elif isinstance(ls, RaySharedLimitSet):
                    if mode != ExecutionMode.Ray:
                        raise ValueError(
                            f"RaySharedLimitSet is not compatible with worker mode '{mode}'. "
                            f"Use mode='ray' workers."
                        )
            return LimitPool(limit_sets=limits, load_balancing="round_robin", worker_index=worker_index)

        raise ValueError("List must contain either all Limit objects or all LimitSet objects")

    # Case 4: Single LimitSet
    if isinstance(limits, BaseLimitSet):
        # Check if it's shared
        if not limits.shared:
            if is_pool:
                raise ValueError(
                    "WorkerProxyPool requires a shared LimitSet. "
                    "Create with: LimitSet(limits=[...], shared=True, mode='...')"
                )

            # Single worker with non-shared LimitSet: extract limits and recreate
            limits_list = getattr(limits, "limits", [])

            if mode in (ExecutionMode.Ray, ExecutionMode.Processes):
                warnings.warn(
                    "Passing non-shared LimitSet to Ray/Process worker. "
                    "The limits will be extracted and recreated inside the actor/process.",
                    UserWarning,
                    stacklevel=4,
                )
                return limits_list  # Will be wrapped remotely
            else:
                warnings.warn(
                    "Passing non-shared LimitSet to WorkerProxy. "
                    "The limits will be copied as a new private LimitSet with shared=False and mode='sync'.",
                    UserWarning,
                    stacklevel=4,
                )
                new_limitset = LimitSet(limits=limits_list, shared=False, mode=ExecutionMode.Sync)
                return LimitPool(
                    limit_sets=[new_limitset], load_balancing="round_robin", worker_index=worker_index
                )

        # Shared LimitSet - validate mode compatibility
        if isinstance(limits, InMemorySharedLimitSet):
            if mode not in (ExecutionMode.Sync, ExecutionMode.Asyncio, ExecutionMode.Threads):
                raise ValueError(
                    f"InMemorySharedLimitSet is not compatible with worker mode '{mode}'. "
                    f"Use mode='sync', 'asyncio', or 'thread' workers."
                )
        elif isinstance(limits, MultiprocessSharedLimitSet):
            if mode != ExecutionMode.Processes:
                raise ValueError(
                    f"MultiprocessSharedLimitSet is not compatible with worker mode '{mode}'. "
                    f"Use mode='process' workers."
                )
        elif isinstance(limits, RaySharedLimitSet):
            if mode != ExecutionMode.Ray:
                raise ValueError(
                    f"RaySharedLimitSet is not compatible with worker mode '{mode}'. Use mode='ray' workers."
                )

        return LimitPool(limit_sets=[limits], load_balancing="round_robin", worker_index=worker_index)

    raise ValueError(
        f"limits parameter must be None, LimitSet, LimitPool, List[Limit], or List[LimitSet], "
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


def _create_worker_wrapper(
    worker_cls: Type, limits: Any, retry_config: Optional[Any] = None, for_ray: bool = False
) -> Type:
    """Create a wrapper class that injects limits and retry logic.

    This wrapper dynamically inherits from the user's worker class and:
    1. Sets self.limits in __init__ (if limits provided)
    2. Wraps all public methods with retry logic (if retry_config provided and num_retries > 0)
    3. Handles both sync and async methods automatically

    The wrapper uses `object.__setattr__` to set attributes to support
    Pydantic BaseModel/Typed workers which have frozen instances by default.

    Retry logic runs inside the actor/process for all execution modes,
    ensuring efficient retries without client-side round-trips.

    If limits is a list of Limit objects (for Ray/Process workers), it creates
    a LimitSet inside the worker (in the remote actor/process context). This
    avoids serialization issues with threading locks in LimitSet.

    Args:
        worker_cls: The original worker class
        limits: LimitSet instance OR list of Limit objects (optional)
        retry_config: RetryConfig instance (optional, defaults to None)
        for_ray: If True, pre-wrap methods on the class (Ray actors need this)

    Returns:
        Wrapper class that sets limits attribute and applies retry logic

    Example:
        ```python
        # With limits only:
        wrapper_cls = _create_worker_wrapper(MyWorker, limit_set)
        worker = wrapper_cls(*args, **kwargs)
        # worker.limits is accessible

        # With limits and retries:
        from concurry import RetryConfig
        config = RetryConfig(num_retries=3, retry_algorithm="exponential")
        wrapper_cls = _create_worker_wrapper(MyWorker, limit_set, config)
        worker = wrapper_cls(*args, **kwargs)
        # worker.limits is accessible
        # worker methods automatically retry on failure

        # With retries only (no limits):
        wrapper_cls = _create_worker_wrapper(MyWorker, None, config)
        worker = wrapper_cls(*args, **kwargs)
        # worker methods automatically retry on failure
        ```
    """
    # Import here to avoid circular imports

    # Determine if we need to apply any wrapping
    # Note: limits is now always provided (may be empty list or empty LimitSet)
    has_limits = limits is not None
    has_retry = retry_config is not None and retry_config.num_retries > 0

    # If no retry, we still need to wrap to set limits attribute
    # (limits is always provided now, even if empty)
    if not has_retry:
        # Only need to set limits, no retry logic
        class WorkerWithLimits(worker_cls):
            def __init__(self, *args, **kwargs):
                # Call parent __init__ first to properly initialize Pydantic models
                super().__init__(*args, **kwargs)

                # Always set limits (may be empty)
                # If limits is a list, create LimitSet and wrap in LimitPool (inside actor/process)
                if isinstance(limits, list):
                    from ..limit.limit_pool import LimitPool

                    # Create private LimitSet with mode=sync (uses threading.Lock, works everywhere)
                    limit_set = LimitSet(limits=limits, shared=False, mode=ExecutionMode.Sync)
                    # Wrap in LimitPool with worker_index=0 (single worker)
                    limit_pool = LimitPool(
                        limit_sets=[limit_set], load_balancing="round_robin", worker_index=0
                    )
                else:
                    # Already a LimitPool, use it directly
                    limit_pool = limits

                # Use object.__setattr__ to bypass frozen models (Typed/BaseModel)
                # This allows limits to work with frozen Pydantic models
                object.__setattr__(self, "limits", limit_pool)

        WorkerWithLimits.__name__ = f"{worker_cls.__name__}_WithLimits"
        WorkerWithLimits.__qualname__ = f"{worker_cls.__qualname__}_WithLimits"
        return WorkerWithLimits

    class WorkerWithLimitsAndRetry(worker_cls):
        def __init__(self, *args, **kwargs):
            # Call parent __init__ first to properly initialize Pydantic models
            super().__init__(*args, **kwargs)

            # Always set limits (may be empty)
            # If limits is a list, create LimitSet and wrap in LimitPool (inside actor/process)
            if isinstance(limits, list):
                from ..limit.limit_pool import LimitPool

                # Create private LimitSet with mode=sync (uses threading.Lock, works everywhere)
                limit_set = LimitSet(limits=limits, shared=False, mode=ExecutionMode.Sync)
                # Wrap in LimitPool with worker_index=0 (single worker)
                limit_pool = LimitPool(limit_sets=[limit_set], load_balancing="round_robin", worker_index=0)
            else:
                # Already a LimitPool, use it directly
                limit_pool = limits

            # Use object.__setattr__ to bypass frozen models (Typed/BaseModel)
            # This allows limits to work with frozen Pydantic models
            object.__setattr__(self, "limits", limit_pool)

        def __getattribute__(self, name: str):
            """Intercept method calls and wrap with retry logic if configured."""
            # Get the attribute using parent's __getattribute__
            attr = super().__getattribute__(name)

            # Only wrap public methods if retry is configured AND not for Ray
            # (Ray mode uses pre-wrapped methods at class level)
            if (
                has_retry
                and not for_ray
                and not name.startswith("_")
                and callable(attr)
                and not isinstance(attr, type)
            ):
                # Check if this method has already been wrapped
                # (to avoid double-wrapping on repeated access)
                if hasattr(attr, "__wrapped_with_retry__"):
                    return attr

                # Wrap the method with retry logic
                wrapped = create_retry_wrapper(
                    attr,
                    retry_config,
                    method_name=name,
                    worker_class_name=worker_cls.__name__,
                )

                # Mark as wrapped to avoid double-wrapping
                wrapped.__wrapped_with_retry__ = True

                return wrapped

            return attr

    # Preserve original class name for debugging (always has limits and retry here)
    WorkerWithLimitsAndRetry.__name__ = f"{worker_cls.__name__}_WithLimitsAndRetry"
    WorkerWithLimitsAndRetry.__qualname__ = f"{worker_cls.__qualname__}_WithLimitsAndRetry"

    # For Ray actors, __getattribute__ doesn't work the same way
    # Instead, wrap each public method individually at the class level
    # ONLY wrap methods that are defined directly on the worker class, not inherited ones
    if for_ray and has_retry:
        import inspect

        # Get methods defined directly on the worker class (not inherited)
        for attr_name in dir(worker_cls):
            # Skip private/dunder methods
            if attr_name.startswith("_"):
                continue

            # Only process if it's defined directly on worker_cls, not inherited
            if attr_name not in worker_cls.__dict__:
                continue

            try:
                attr = getattr(worker_cls, attr_name)
                # Only wrap actual callable methods (not properties, classmethods, staticmethods)
                if not callable(attr):
                    continue

                # Skip if it's a class or type
                if isinstance(attr, type):
                    continue

                # Check if it's a function/method we should wrap
                if not (inspect.isfunction(attr) or inspect.ismethod(attr)):
                    continue

                # Create a wrapper method that applies retry logic
                def make_wrapped_method(original_method, method_name):
                    # Check if it's async
                    is_async = inspect.iscoroutinefunction(original_method)

                    if is_async:

                        async def async_method_wrapper(self, *args, **kwargs):
                            from ..retry import execute_with_retry_async

                            context = {
                                "method_name": method_name,
                                "worker_class_name": worker_cls.__name__,
                            }
                            # Bind self to the original method
                            bound_method = original_method.__get__(self, type(self))
                            return await execute_with_retry_async(
                                bound_method, args, kwargs, retry_config, context
                            )

                        async_method_wrapper.__wrapped_with_retry__ = True
                        return async_method_wrapper
                    else:

                        def sync_method_wrapper(self, *args, **kwargs):
                            from ..retry import execute_with_retry

                            context = {
                                "method_name": method_name,
                                "worker_class_name": worker_cls.__name__,
                            }
                            # Bind self to the original method
                            bound_method = original_method.__get__(self, type(self))
                            return execute_with_retry(bound_method, args, kwargs, retry_config, context)

                        sync_method_wrapper.__wrapped_with_retry__ = True
                        return sync_method_wrapper

                wrapped = make_wrapped_method(attr, attr_name)
                setattr(WorkerWithLimitsAndRetry, attr_name, wrapped)
            except (AttributeError, TypeError):
                # Skip attributes that can't be wrapped
                pass

    return WorkerWithLimitsAndRetry


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
        # Retry parameters
        num_retries: int = 0,
        retry_on: Optional[Any] = None,
        retry_algorithm: str = "exponential",
        retry_wait: float = 1.0,
        retry_jitter: float = 0.3,
        retry_until: Optional[Any] = None,
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
            num_retries: Maximum number of retry attempts
            retry_on: Exception types or callables that trigger retries
            retry_algorithm: Backoff strategy (linear, exponential, fibonacci)
            retry_wait: Minimum wait time between retries
            retry_jitter: Jitter factor (0-1)
            retry_until: Validation functions for output
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
        self._num_retries = num_retries
        self._retry_on = retry_on
        self._retry_algorithm = retry_algorithm
        self._retry_wait = retry_wait
        self._retry_jitter = retry_jitter
        self._retry_until = retry_until
        self._options = options

        # Validate configuration
        self._validate_pool_config()

    def _create_retry_config(self) -> Optional[Any]:
        """Create RetryConfig from retry parameters.

        Returns:
            RetryConfig instance if num_retries > 0, else None
        """

        # Fast path: if num_retries is 0, don't create config
        if self._num_retries == 0:
            return None

        # Create RetryConfig
        return RetryConfig(
            num_retries=self._num_retries,
            retry_on=self._retry_on if self._retry_on is not None else [Exception],
            retry_algorithm=RetryAlgorithm(self._retry_algorithm),
            retry_wait=self._retry_wait,
            retry_jitter=self._retry_jitter,
            retry_until=self._retry_until,
        )

    def _validate_pool_config(self) -> None:
        """Validate pool configuration parameters.

        Raises:
            ValueError: If configuration is invalid
        """
        from ..constants import ExecutionMode

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
        from ..constants import ExecutionMode

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

    def _check_ray_pydantic_compatibility(self, execution_mode: ExecutionMode) -> None:
        """Check for Ray + Pydantic incompatibility and raise/warn appropriately.

        Args:
            execution_mode: The execution mode being used

        Raises:
            ValueError: If trying to create Ray worker with Pydantic-based class
        """
        try:
            from pydantic import BaseModel
        except ImportError:
            # Pydantic not installed, no issue
            return

        # Check if worker class is a Pydantic BaseModel subclass
        is_pydantic_based = isinstance(self._worker_cls, type) and issubclass(self._worker_cls, BaseModel)

        if not is_pydantic_based:
            return

        # Issue warning if Ray is installed (even if not using Ray mode)
        try:
            import ray

            if execution_mode != ExecutionMode.Ray:
                # Warn that Ray mode won't work with this worker
                warnings.warn(
                    f"Worker class '{self._worker_cls.__name__}' inherits from Pydantic BaseModel. "
                    f"This worker will NOT be compatible with Ray mode due to Ray's actor wrapping "
                    f"conflicting with Pydantic's __setattr__. Consider using composition instead of "
                    f"inheritance if you need Ray support.",
                    UserWarning,
                    stacklevel=5,
                )
        except ImportError:
            # Ray not installed, no warning needed
            pass

        # Raise error if actually trying to use Ray mode
        if execution_mode == ExecutionMode.Ray:
            raise ValueError(
                f"Cannot create Ray worker with Pydantic-based class '{self._worker_cls.__name__}'. "
                f"Ray's actor wrapping mechanism conflicts with Pydantic's __setattr__ implementation. "
                f"\n\nWorkaround: Use composition instead of inheritance:\n"
                f"  class {self._worker_cls.__name__}(Worker):\n"
                f"      def __init__(self, ...):\n"
                f"          self.config = YourPydanticModel(...)\n"
                f"\nThis applies to both morphic.Typed and pydantic.BaseModel."
            )

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

        Raises:
            ValueError: If trying to create Ray worker with Pydantic-based class
        """
        from .asyncio_worker import AsyncioWorkerProxy
        from .process_worker import ProcessWorkerProxy
        from .sync_worker import SyncWorkerProxy
        from .task_worker import TaskWorker, TaskWorkerMixin
        from .thread_worker import ThreadWorkerProxy

        # Convert mode string to ExecutionMode
        execution_mode = ExecutionMode(self._mode)

        # Check for Ray + Pydantic incompatibility
        self._check_ray_pydantic_compatibility(execution_mode)

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

        # Process limits (always, even if None - creates empty LimitPool)
        processed_options = dict(self._options)
        processed_options["limits"] = _transform_worker_limits(
            limits=processed_options.get("limits"),
            mode=execution_mode,
            is_pool=False,
            worker_index=0,  # Single workers use index 0
        )

        # Create retry config if needed
        retry_config = self._create_retry_config()
        if retry_config is not None:
            processed_options["retry_config"] = retry_config

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

        Raises:
            ValueError: If trying to create Ray pool with Pydantic-based class
        """
        from ..constants import ExecutionMode, LoadBalancingAlgorithm
        from .worker_pool import (
            InMemoryWorkerProxyPool,
            MultiprocessWorkerProxyPool,
            RayWorkerProxyPool,
        )

        # Convert mode string to ExecutionMode
        execution_mode = ExecutionMode(self._mode)

        # Check for Ray + Pydantic incompatibility
        self._check_ray_pydantic_compatibility(execution_mode)

        # Determine max_workers (use defaults if not specified)
        max_workers = self._max_workers
        if max_workers is None:
            max_workers = self._get_default_max_workers()

        # Determine load_balancing algorithm
        load_balancing_str = self._load_balancing
        if load_balancing_str is None:
            load_balancing_str = self._get_default_load_balancing()
        load_balancing = LoadBalancingAlgorithm(load_balancing_str)

        # Process limits for pool (always, even if None - creates empty LimitPool)
        # Note: worker_index will be assigned per-worker in pool initialization
        limits = _transform_worker_limits(
            limits=self._options.get("limits"),
            mode=execution_mode,
            is_pool=True,
            worker_index=0,  # Placeholder, actual indices assigned per worker
        )

        # Update options with processed limits
        pool_options = dict(self._options)
        pool_options["limits"] = limits

        # Create retry config if needed
        retry_config = self._create_retry_config()
        if retry_config is not None:
            pool_options["retry_config"] = retry_config

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

    The Worker class itself does NOT inherit from morphic.Typed. This design choice allows you
    complete freedom in defining your `__init__` method - you can use any signature with any
    combination of positional arguments, keyword arguments, *args, and **kwargs. The Typed
    integration is applied at the WorkerProxy layer, which wraps your worker and provides
    validation for worker configuration (mode, blocking, etc.) but not for worker initialization.

    **Model Inheritance Support:**

    Worker supports cooperative multiple inheritance, allowing you to combine Worker with
    model classes for automatic field validation and serialization:

    - ✅ **morphic.Typed**: Full support (sync, thread, process, asyncio)
    - ✅ **pydantic.BaseModel**: Full support (sync, thread, process, asyncio)
    - ❌ **Ray mode limitation**: Ray mode is NOT compatible with Typed/BaseModel workers

    **Validation Decorators (Works with ALL modes including Ray):**

    - ✅ **@morphic.validate**: Works on methods and __init__ (all modes including Ray)
    - ✅ **@pydantic.validate_call**: Works on methods and __init__ (all modes including Ray)

    These decorators provide runtime validation without class inheritance, making them
    compatible with Ray mode.

    This means you can use:
    - Plain Python classes (all modes including Ray)
    - Worker + morphic.Typed for validation and hooks (all modes EXCEPT Ray)
    - Worker + pydantic.BaseModel for Pydantic validation (all modes EXCEPT Ray)
    - @validate or @validate_call decorators on methods (all modes including Ray)
    - Dataclasses, Attrs, or any other class structure (all modes)

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

    Context Manager (Automatic Cleanup):
        Workers and pools support context manager protocol for automatic cleanup:

        ```python
        from concurry import Worker

        class DataProcessor(Worker):
            def __init__(self, multiplier: int):
                self.multiplier = multiplier

            def process(self, value: int) -> int:
                return value * self.multiplier

        # Context manager automatically calls .stop() on exit
        with DataProcessor.options(mode="thread").init(3) as worker:
            future = worker.process(10)
            result = future.result()  # 30
        # Worker is automatically stopped here

        # Works with pools too
        with DataProcessor.options(mode="thread", max_workers=5).init(3) as pool:
            results = [pool.process(i).result() for i in range(10)]
        # All workers in pool are automatically stopped here

        # Cleanup happens even on exceptions
        with DataProcessor.options(mode="thread").init(3) as worker:
            if some_error:
                raise ValueError("Error occurred")
        # Worker is still stopped despite exception
        ```

    Model Inheritance Usage:
        ```python
        from concurry import Worker
        from morphic import Typed
        from pydantic import BaseModel, Field
        from typing import List, Optional

        # Worker + Typed for validation and lifecycle hooks
        class TypedWorker(Worker, Typed):
            name: str
            value: int = Field(default=0, ge=0)
            tags: List[str] = []

            @classmethod
            def pre_initialize(cls, data: dict) -> None:
                # Normalize data before validation
                if 'name' in data:
                    data['name'] = data['name'].strip().title()

            def compute(self, x: int) -> int:
                return self.value * x

        # Initialize with validated fields
        worker = TypedWorker.options(mode="thread").init(
            name="processor",
            value=10,
            tags=["ml", "preprocessing"]
        )
        result = worker.compute(5).result()  # 50
        worker.stop()

        # Worker + Pydantic BaseModel for validation
        class PydanticWorker(Worker, BaseModel):
            name: str = Field(..., min_length=1, max_length=50)
            age: int = Field(..., ge=0, le=150)
            email: Optional[str] = None

            def get_info(self) -> dict:
                return {"name": self.name, "age": self.age, "email": self.email}

        worker = PydanticWorker.options(mode="process").init(
            name="Alice",
            age=30,
            email="alice@example.com"
        )
        info = worker.get_info().result()
        worker.stop()
        ```

    Validation Decorators (Ray-Compatible):
        ```python
        from concurry import Worker
        from morphic import validate
        from pydantic import validate_call

        # @validate decorator works with ALL modes including Ray
        class ValidatedWorker(Worker):
            def __init__(self, multiplier: int):
                self.multiplier = multiplier

            @validate
            def process(self, value: int, scale: float = 1.0) -> float:
                '''Process with automatic type validation and coercion.'''
                return (value * self.multiplier) * scale

        # Works with Ray mode!
        worker = ValidatedWorker.options(mode="ray").init(multiplier=5)
        result = worker.process("10", scale="2.0").result()  # "10" -> 10, "2.0" -> 2.0
        # result = 100.0
        worker.stop()

        # @validate_call also works with ALL modes including Ray
        class PydanticValidatedWorker(Worker):
            def __init__(self, base: int):
                self.base = base

            @validate_call
            def compute(self, x: int, y: int = 0) -> int:
                '''Compute with Pydantic validation.'''
                return (x + y) * self.base

        # Also works with Ray mode!
        worker = PydanticValidatedWorker.options(mode="ray").init(base=3)
        result = worker.compute("5", y="2").result()  # Strings coerced to ints
        # result = 21
        worker.stop()
        ```

    Ray Mode Limitations and Workarounds:
        ```python
        # ❌ BAD: Typed/BaseModel workers don't work with Ray
        class TypedWorker(Worker, Typed):
            name: str
            value: int = 0

        # This will raise ValueError with Ray mode
        try:
            worker = TypedWorker.options(mode="ray").init(name="test", value=10)
        except ValueError as e:
            print(e)  # "Cannot create Ray worker with Pydantic-based class..."

        # ✅ GOOD: Use composition instead of inheritance for Ray
        class RayCompatibleWorker(Worker):
            def __init__(self, name: str, value: int = 0):
                self.name = name
                self.value = value

            def compute(self, x: int) -> int:
                return self.value * x

        # This works with Ray!
        worker = RayCompatibleWorker.options(mode="ray").init(name="test", value=10)
        result = worker.compute(5).result()  # 50
        worker.stop()

        # ✅ EVEN BETTER: Use validation decorators for type checking
        class ValidatedRayWorker(Worker):
            @validate
            def __init__(self, name: str, value: int = 0):
                self.name = name
                self.value = value

            @validate
            def compute(self, x: int) -> int:
                return self.value * x

        # Validation + Ray compatibility!
        worker = ValidatedRayWorker.options(mode="ray").init(name="test", value="10")
        result = worker.compute("5").result()  # Types coerced, result = 50
        worker.stop()
        ```

        **Why Ray + Typed/BaseModel doesn't work:**

        Ray's `ray.remote()` wraps classes as actors and modifies their `__setattr__`
        behavior, which conflicts with Pydantic's frozen model implementation. When you
        try to create a Ray actor from a Pydantic-based class, Ray attempts to set
        internal attributes that trigger Pydantic's validation, causing AttributeError.

        **Automatic Error Detection:**

        Concurry automatically detects this incompatibility and raises a clear error:
        - **ValueError**: When attempting to create a Ray worker/pool with Typed/BaseModel
        - **UserWarning**: When creating non-Ray workers (if Ray is installed)

        The warning helps you know that your worker won't be compatible with Ray mode
        if you later decide to switch execution modes.

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

        # With context manager (recommended)
        with DataProcessor.options(mode="thread", blocking=True).init(5) as worker:
            result = worker.process(10)  # Returns 50 directly
        # Worker automatically stopped
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
        with Counter.options(mode="thread").init() as worker1:
            with Counter.options(mode="thread").init() as worker2:
                print(worker1.increment().result())  # 1
                print(worker1.increment().result())  # 2
                print(worker2.increment().result())  # 1 (separate state)
        # Both workers automatically stopped
        ```

    Resource Protection with Limits:
        Workers support resource protection and rate limiting via the `limits` parameter.
        Limits enable control over API rates, resource pools, and call frequency.

        **Important: Workers always have `self.limits` available, even when no limits
        are configured.** If no limits parameter is provided, workers get an empty
        LimitSet that always allows acquisition without blocking. This means your
        code can safely call `self.limits.acquire()` without checking if limits exist.

        ```python
        from concurry import Worker, LimitSet, RateLimit, CallLimit, ResourceLimit
        from concurry import RateLimitAlgorithm

        # Define limits
        limits = LimitSet(limits=[
            CallLimit(window_seconds=60, capacity=100),  # 100 calls/min
            RateLimit(
                key="api_tokens",
                window_seconds=60,
                algorithm=RateLimitAlgorithm.TokenBucket,
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

        # Option 3: No limits (always succeeds)
        worker = APIWorker.options(mode="thread").init("key")
        # self.limits.acquire() always succeeds immediately, no blocking
        ```

        **Limit Types:**
        - `CallLimit`: Count calls (usage always 1, no update needed)
        - `RateLimit`: Token/bandwidth limiting (requires update() call)
        - `ResourceLimit`: Semaphore-based resources (no update needed)

        **Key Behaviors:**
        - Passing `LimitSet`: Workers share the same limit pool
        - Passing `List[Limit]`: Each worker gets private limits
        - No limits parameter: Workers get empty LimitSet (always succeeds)
        - CallLimit/ResourceLimit auto-acquired with default of 1
        - RateLimits must be explicitly specified in `requested` dict
        - RateLimits require `update()` call (raises RuntimeError if missing)
        - Empty LimitSet has zero overhead (no synchronization, no waiting)

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
        # Retry parameters
        num_retries: int = 0,
        retry_on: Optional[Any] = None,
        retry_algorithm: str = "exponential",
        retry_wait: float = 1.0,
        retry_jitter: float = 0.3,
        retry_until: Optional[Any] = None,
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
            limits: Resource protection and rate limiting (optional, defaults to empty LimitSet)
                - Pass LimitSet: Workers share the same limit pool
                - Pass List[Limit]: Each worker gets private limits (creates shared LimitSet for pools)
                - Omit parameter: Workers get empty LimitSet (self.limits.acquire() always succeeds)
                Workers always have self.limits available, even when no limits configured.
                See Worker docstring "Resource Protection with Limits" section for details.
            num_retries: Maximum number of retry attempts after initial failure (default: 0)
                Total attempts = num_retries + 1 (initial attempt).
                Set to 0 to disable retries (zero overhead).
            retry_on: Exception types or callables that trigger retries (optional)
                - Single exception class: retry_on=ValueError
                - List of exceptions: retry_on=[ValueError, ConnectionError]
                - Callable filter: retry_on=lambda exception, **ctx: "retry" in str(exception)
                - Mixed list: retry_on=[ValueError, custom_filter]
                Default: [Exception] (retry on all exceptions when num_retries > 0)
            retry_algorithm: Backoff strategy for wait times (default: "exponential")
            retry_wait: Minimum wait time between retries in seconds (default: 1.0)
                Base wait time before applying strategy and jitter.
            retry_jitter: Jitter factor between 0 and 1 (default: 0.3)
                Uses Full Jitter algorithm from AWS: sleep = random(0, calculated_wait).
                Set to 0 to disable jitter. Prevents thundering herd when many workers retry.
            retry_until: Validation functions for output (optional)
                - Single validator: retry_until=lambda result, **ctx: result.get("status") == "success"
                - List of validators: retry_until=[validator1, validator2] (all must pass)
                Validators receive result and context as kwargs. Return True for valid output.
                If validation fails, triggers retry even without exception.
                Useful for LLM output validation (JSON schema, XML format, etc.)
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

            Retries:
                ```python
                # Basic retry with exponential backoff
                worker = APIWorker.options(
                    mode="thread",
                    num_retries=3,
                    retry_algorithm="exponential",
                    retry_wait=1.0,
                    retry_jitter=0.3
                ).init()

                # Retry only on specific exceptions
                worker = APIWorker.options(
                    mode="thread",
                    num_retries=5,
                    retry_on=[ConnectionError, TimeoutError]
                ).init()

                # Custom exception filter
                worker = APIWorker.options(
                    mode="thread",
                    num_retries=3,
                    retry_on=lambda exception, **ctx: (
                        isinstance(exception, ValueError) and "retry" in str(exception)
                    )
                ).init()

                # Output validation for LLM responses
                worker = LLMWorker.options(
                    mode="thread",
                    num_retries=5,
                    retry_until=lambda result, **ctx: (
                        isinstance(result, dict) and "data" in result
                    )
                ).init()

                # Multiple validators (all must pass)
                worker = LLMWorker.options(
                    mode="thread",
                    num_retries=5,
                    retry_until=[
                        lambda result, **ctx: isinstance(result, str),
                        lambda result, **ctx: result.startswith("{"),
                        lambda result, **ctx: validate_json(result)
                    ]
                ).init()
                ```
        """
        return WorkerBuilder(
            worker_cls=cls,
            mode=mode,
            blocking=blocking,
            max_workers=max_workers,
            load_balancing=load_balancing,
            on_demand=on_demand,
            num_retries=num_retries,
            retry_on=retry_on,
            retry_algorithm=retry_algorithm,
            retry_wait=retry_wait,
            retry_jitter=retry_jitter,
            retry_until=retry_until,
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
        """Initialize the worker. Subclasses can override this freely.

        This method supports cooperative multiple inheritance, allowing Worker
        to be combined with model classes like morphic.Typed or pydantic.BaseModel.

        Examples:
            ```python
            # Regular Worker subclass
            class MyWorker(Worker):
                def __init__(self, value: int):
                    self.value = value

            # Worker + Typed
            class TypedWorker(Worker, Typed):
                name: str
                value: int = 0

            # Worker + BaseModel
            class PydanticWorker(Worker, BaseModel):
                name: str
                value: int = 0
            ```
        """
        # Support cooperative multiple inheritance with Typed/BaseModel
        # Try to call super().__init__() to propagate to other base classes
        try:
            super().__init__(*args, **kwargs)
        except TypeError as e:
            # object.__init__() doesn't accept arguments
            # This happens when Worker is the only meaningful base class
            if "object.__init__()" in str(e) or "no arguments" in str(e).lower():
                pass
            else:
                raise


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
    retry_config: Optional[Any] = None  # RetryConfig instance (processed by WorkerBuilder)

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

    def __enter__(self) -> "WorkerProxy":
        """Enter context manager.

        Returns:
            Self for use in with statement
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager and stop worker.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        self.stop()


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
