"""Worker implementation for concurry."""

import threading
import warnings
from abc import ABC
from typing import Any, Callable, ClassVar, Optional, Type, TypeVar, Union

from morphic import Typed, validate
from morphic.structs import map_collection
from pydantic import PrivateAttr, confloat, conint

from ...utils import _NO_ARG, _NO_ARG_TYPE
from ..constants import ExecutionMode, LoadBalancingAlgorithm
from ..future import BaseFuture
from ..limit.limit_pool import LimitPool
from ..limit.limit_set import LimitSet
from ..retry import (
    RetryAlgorithm,
    RetryConfig,
    create_retry_wrapper,
    execute_with_retry,
    execute_with_retry_async,
)

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
        return LimitPool(
            limit_sets=[empty_limitset],
            worker_index=worker_index,
        )

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
                limit_sets=[empty_limitset],
                worker_index=worker_index,
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
            return LimitPool(
                limit_sets=[limitset],
                worker_index=worker_index,
            )

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
            return LimitPool(limit_sets=limits, worker_index=worker_index)

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
                    limit_sets=[new_limitset],
                    worker_index=worker_index,
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

        return LimitPool(limit_sets=[limits], worker_index=worker_index)

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


def _should_use_composition_wrapper(worker_cls: Type) -> bool:
    """Determine if a worker class should use composition wrapper.

    Workers that inherit from morphic.Typed or pydantic.BaseModel should use
    composition wrappers to avoid conflicts with infrastructure methods and frozen models.

    This is applied for ALL execution modes to ensure consistent behavior and avoid
    issues with:
    - Infrastructure methods being wrapped with retry logic
    - Frozen model constraints
    - Serialization issues (Ray's __setattr__ conflicts)

    Note: Check Typed FIRST as it's a subclass of BaseModel.

    Args:
        worker_cls: The worker class to check

    Returns:
        True if composition wrapper should be used, False otherwise

    Example:
        ```python
        class MyWorker(Worker, Typed):
            name: str

        assert _should_use_composition_wrapper(MyWorker) is True

        class PlainWorker(Worker):
            def __init__(self):
                pass

        assert _should_use_composition_wrapper(PlainWorker) is False
        ```
    """
    # Check for Typed first (it extends BaseModel)
    try:
        from morphic import Typed

        if isinstance(worker_cls, type) and issubclass(worker_cls, Typed):
            return True
    except ImportError:
        pass

    # Check for BaseModel
    try:
        from pydantic import BaseModel

        if isinstance(worker_cls, type) and issubclass(worker_cls, BaseModel):
            return True
    except ImportError:
        pass

    return False


def _is_infrastructure_method(
    method_name: str,
    _cache: dict = {},  # Mutable default for caching
) -> bool:
    """Check if a method is defined on infrastructure base classes (Typed/BaseModel).

    This is used to avoid wrapping infrastructure methods with retry logic.
    Only user-defined methods should be wrapped.

    Uses caching for performance - the method sets from Typed/BaseModel are computed
    once and reused for all subsequent calls. This is a fast O(1) set lookup.

    Args:
        method_name: Name of the method to check
        _cache: Internal cache dict (do not pass explicitly)

    Returns:
        True if method is defined on Typed or BaseModel, False otherwise
    """
    # Initialize cache on first call
    if len(_cache) == 0:
        _cache["typed_methods"] = set()
        _cache["basemodel_methods"] = set()
        _cache["initialized"] = False

    # Populate cache on first call
    if not _cache["initialized"]:
        # Import and cache Typed methods
        try:
            from morphic import Typed as TypedBase

            _cache["typed_methods"] = set(TypedBase.__dict__.keys())
        except ImportError:
            pass

        # Import and cache BaseModel methods
        try:
            from pydantic import BaseModel

            _cache["basemodel_methods"] = set(BaseModel.__dict__.keys())
        except ImportError:
            pass

        _cache["initialized"] = True

    # Fast path: O(1) set lookup
    if method_name in _cache["typed_methods"] or method_name in _cache["basemodel_methods"]:
        return True

    # Method not an infrastructure method
    return False


def _create_composition_wrapper(worker_cls: Type) -> Type:
    """Create a composition wrapper for BaseModel/Typed workers.

    This function automatically creates a composition-based wrapper that allows
    BaseModel/Typed workers to work seamlessly across ALL execution modes. The wrapper:

    1. Does NOT inherit from BaseModel/Typed (avoiding infrastructure method conflicts)
    2. Uses composition pattern - holds BaseModel/Typed instance internally
    3. Only exposes user-defined methods (infrastructure methods excluded)
    4. Delegates method calls to the wrapped instance

    This enables transparent support for workers that inherit from morphic.Typed
    or pydantic.BaseModel across sync, thread, process, asyncio, and ray modes.

    **Why Composition Instead of Inheritance?**

    - **Avoids infrastructure method wrapping**: Retry logic won't wrap Pydantic methods
    - **Cleaner separation**: User code separate from framework code
    - **Ray compatibility**: No conflicts with Ray's actor wrapping
    - **Consistent behavior**: Same code path for all execution modes

    Args:
        worker_cls: Original worker class (BaseModel/Typed subclass)

    Returns:
        Plain Python wrapper class using composition pattern

    Example:
        ```python
        # Works seamlessly in ALL modes!
        class MyWorker(Worker, Typed):
            name: str
            def process(self, x: int) -> int:
                return x * 2

        # Sync mode
        w = MyWorker.options(mode="sync").init(name="test")
        result = w.process(5).result()  # Works!

        # Ray mode
        w = MyWorker.options(mode="ray").init(name="test")
        result = w.process(5).result()  # Works!
        ```
    """
    # Import Worker class for inheritance
    # We need to import it locally to avoid circular imports
    from . import Worker as WorkerBase

    class CompositionWrapper(WorkerBase):
        """Auto-generated composition wrapper for BaseModel/Typed workers.

        This wrapper holds a BaseModel/Typed instance internally and delegates
        user-defined method calls to it. Infrastructure methods are not exposed.

        Inherits from Worker to satisfy worker_cls validation and enable
        seamless integration across all execution modes.
        """

        def __init__(self, *args, **kwargs):
            """Initialize by creating the wrapped BaseModel/Typed instance."""
            # Don't call super().__init__() since Worker base class doesn't define __init__
            # Create the actual BaseModel/Typed instance internally
            # This happens inside the Ray actor, so serialization is fine
            self._wrapped_instance = worker_cls(*args, **kwargs)

        def __getattr__(self, name: str):
            """Delegate attribute access to wrapped instance.

            Only allows access to user-defined methods, not infrastructure methods.
            This prevents Ray from trying to serialize infrastructure methods.
            """
            # Block access to infrastructure methods
            if _is_infrastructure_method(name):
                raise AttributeError(
                    f"Infrastructure method '{name}' not available in Ray wrapper. "
                    f"Only user-defined methods are exposed for Ray compatibility."
                )

            # Delegate to wrapped instance
            return getattr(self._wrapped_instance, name)

    # Copy all user-defined methods to the wrapper class
    # This makes them "real" methods on the wrapper, not just __getattr__ lookups
    for attr_name in dir(worker_cls):
        # Skip private/dunder methods
        if attr_name.startswith("_"):
            continue

        # Skip infrastructure methods
        if _is_infrastructure_method(attr_name):
            continue

        # Only process methods defined directly on worker class (not inherited)
        if attr_name not in worker_cls.__dict__:
            continue

        attr = getattr(worker_cls, attr_name)

        # Only process callable methods
        if not callable(attr):
            continue

        # Skip if it's a class or type
        if isinstance(attr, type):
            continue

        # Create a delegating method (async if original is async)
        # OPTIMIZATION: Capture the unbound method from the original class to avoid
        # repeated getattr() calls. This is critical for performance in tight loops.
        def make_method(method_name, is_async, unbound_method):
            """Create a method that delegates to the wrapped instance.

            Uses the captured unbound method and binds it directly to _wrapped_instance
            to avoid slow getattr() lookup on every call.
            """

            if is_async:

                async def async_delegating_method(self, *args, **kwargs):
                    # Fast path: Call unbound method with wrapped instance directly
                    # This avoids getattr() overhead (~200ns per call saved)
                    return await unbound_method(self._wrapped_instance, *args, **kwargs)

                async_delegating_method.__name__ = method_name
                async_delegating_method.__qualname__ = f"CompositionWrapper.{method_name}"
                return async_delegating_method
            else:

                def delegating_method(self, *args, **kwargs):
                    # Fast path: Call unbound method with wrapped instance directly
                    # This avoids getattr() overhead (~200ns per call saved)
                    return unbound_method(self._wrapped_instance, *args, **kwargs)

                delegating_method.__name__ = method_name
                delegating_method.__qualname__ = f"CompositionWrapper.{method_name}"
                return delegating_method

        # Check if method is async
        import inspect

        is_async_method = inspect.iscoroutinefunction(attr)

        # Add the delegating method to the wrapper class
        # Pass the unbound method to avoid getattr() on every call
        setattr(CompositionWrapper, attr_name, make_method(attr_name, is_async_method, attr))

    # Set wrapper class name for debugging
    CompositionWrapper.__name__ = f"{worker_cls.__name__}_CompositionWrapper"
    CompositionWrapper.__qualname__ = f"{worker_cls.__qualname__}_CompositionWrapper"
    CompositionWrapper.__module__ = worker_cls.__module__

    return CompositionWrapper


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
                    # Create private LimitSet with mode=sync (uses threading.Lock, works everywhere)
                    limit_set = LimitSet(limits=limits, shared=False, mode=ExecutionMode.Sync)
                    # Wrap in LimitPool with explicit defaults (don't rely on worker's global_config)
                    # Note: load_balancing doesn't matter with single LimitSet, but use Random for consistency
                    limit_pool = LimitPool(
                        limit_sets=[limit_set],
                        load_balancing=LoadBalancingAlgorithm.Random,
                        worker_index=0,
                    )
                else:
                    # Already a LimitPool, use it directly
                    limit_pool = limits

                # Use object.__setattr__ to bypass frozen models (Typed/BaseModel)
                # This allows limits to work with frozen Pydantic models
                # IMPORTANT: If this is a composition wrapper (Ray compatibility),
                # set limits on the wrapped instance where user methods execute
                if hasattr(self, "_wrapped_instance"):
                    object.__setattr__(self._wrapped_instance, "limits", limit_pool)
                else:
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
                # Create private LimitSet with mode=sync (uses threading.Lock, works everywhere)
                limit_set = LimitSet(limits=limits, shared=False, mode=ExecutionMode.Sync)
                # Wrap in LimitPool with explicit defaults (don't rely on worker's global_config)
                # Note: load_balancing doesn't matter with single LimitSet, but use Random for consistency
                limit_pool = LimitPool(
                    limit_sets=[limit_set],
                    load_balancing=LoadBalancingAlgorithm.Random,
                    worker_index=0,
                )
            else:
                # Already a LimitPool, use it directly
                limit_pool = limits

            # Use object.__setattr__ to bypass frozen models (Typed/BaseModel)
            # This allows limits to work with frozen Pydantic models
            # IMPORTANT: If this is a composition wrapper (Ray compatibility),
            # set limits on the wrapped instance where user methods execute
            if hasattr(self, "_wrapped_instance"):
                object.__setattr__(self._wrapped_instance, "limits", limit_pool)
            else:
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
                # For composition wrappers (Typed/BaseModel), infrastructure methods
                # are already filtered out - only user-defined methods are exposed

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
        # For composition wrappers (Typed/BaseModel), only user-defined methods
        # are exposed, so infrastructure methods are already filtered out
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


class WorkerBuilder(Typed):
    """Builder for creating worker instances with deferred initialization.

    This class holds configuration from .options() calls and provides
    a .init() method to instantiate the actual worker with initialization arguments.

    This is a Typed class that validates all configuration at creation time and
    provides immutable configuration with validation.
    """

    # ========================================================================
    # PUBLIC CONFIGURATION FIELDS - NO DEFAULTS ALLOWED
    # All values must be explicitly passed from Worker.options()
    # ========================================================================
    # CRITICAL: Public attributes MUST NOT have default values.
    # All defaults come from global_config and are applied in Worker.options()
    # ========================================================================

    # Core worker configuration
    worker_cls: Type["Worker"]
    mode: ExecutionMode
    blocking: bool
    max_workers: Optional[conint(ge=0)]
    load_balancing: LoadBalancingAlgorithm
    on_demand: bool
    max_queued_tasks: Optional[conint(ge=0)]

    # Retry parameters
    num_retries: conint(ge=0)
    retry_on: Any  # List of exception types or callables, default [Exception]
    retry_algorithm: RetryAlgorithm
    retry_wait: confloat(ge=0)
    retry_jitter: confloat(ge=0, le=1)
    retry_until: Optional[Any]  # Truly optional, default None

    # Worker-level configuration
    unwrap_futures: bool
    limits: Optional[Any]  # LimitSet, List[Limit], or None

    # Mode-specific options (passed through to worker implementation)
    # For Ray: num_cpus, num_gpus, resources, actor_options, etc.
    # For Process: mp_context (fork, spawn, forkserver)
    # These are passed as-is without validation
    mode_options: dict[str, Any]

    @classmethod
    def pre_initialize(cls, data: dict) -> None:
        """Validate configuration before initialization.

        This method is called by Typed before field validation.
        """
        # Check for deprecated parameters
        if "init_args" in data:
            raise ValueError(
                "The 'init_args' parameter is no longer supported. "
                "Use .init(*args) instead. "
                "Example: Worker.options(mode='thread').init(arg1, arg2)"
            )
        if "init_kwargs" in data:
            raise ValueError(
                "The 'init_kwargs' parameter is no longer supported. "
                "Use .init(**kwargs) instead. "
                "Example: Worker.options(mode='thread').init(key1=val1, key2=val2)"
            )

    def post_initialize(self) -> None:
        """Validate pool configuration after initialization.

        This method is called by Typed after all fields are set.
        """
        # Validate max_workers for different modes
        if self.max_workers is not None:
            # Sync and Asyncio must have max_workers=1 or None
            if self.mode in (ExecutionMode.Sync, ExecutionMode.Asyncio):
                if self.max_workers != 1:
                    raise ValueError(
                        f"max_workers must be 1 for {self.mode.value} mode, got {self.max_workers}"
                    )

        # Validate on_demand for different modes
        if self.on_demand:
            # Sync and Asyncio don't support on_demand
            if self.mode in (ExecutionMode.Sync, ExecutionMode.Asyncio):
                raise ValueError(f"on_demand mode is not supported for {self.mode.value} execution")

            # With on_demand and max_workers=0, validate limits
            if self.max_workers == 0:
                # This is valid for Thread, Process, and Ray
                pass

    def _create_retry_config(self) -> Optional[Any]:
        """Create RetryConfig from retry parameters.

        Returns:
            RetryConfig instance if num_retries > 0, else None
        """

        # Fast path: if num_retries is 0, don't create config
        if self.num_retries == 0:
            return None

        # Create RetryConfig
        return RetryConfig(
            num_retries=self.num_retries,
            retry_on=self.retry_on if self.retry_on is not None else [Exception],
            retry_algorithm=RetryAlgorithm(self.retry_algorithm),
            retry_wait=self.retry_wait,
            retry_jitter=self.retry_jitter,
            retry_until=self.retry_until,
        )

    def _should_create_pool(self) -> bool:
        """Determine if a pool should be created.

        Returns:
            True if pool should be created, False for single worker
        """
        # On-demand always creates pool
        if self.on_demand:
            return True

        # max_workers > 1 creates pool
        if self.max_workers is not None and self.max_workers > 1:
            return True

        return False

    def _apply_composition_wrapper_if_needed(self) -> None:
        """Apply composition wrapper for Typed/BaseModel workers across ALL modes.

        Workers that inherit from morphic.Typed or pydantic.BaseModel use composition
        wrappers to avoid conflicts with infrastructure methods, frozen model constraints,
        and serialization issues.

        This is applied for ALL execution modes (sync, thread, process, asyncio, ray)
        to ensure consistent behavior and avoid:
        - Infrastructure methods being wrapped with retry logic
        - Frozen model constraints
        - Ray's __setattr__ conflicts with Pydantic

        The composition wrapper transparently delegates to the wrapped instance, making
        this transformation invisible to user code.
        """
        # Check if this worker class should use composition wrapper
        if not _should_use_composition_wrapper(self.worker_cls):
            return

        # Create composition wrapper for ALL modes
        original_cls = self.worker_cls
        object.__setattr__(self, "worker_cls", _create_composition_wrapper(self.worker_cls))

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
        # Import here to avoid circular imports
        from ...config import global_config
        from .asyncio_worker import AsyncioWorkerProxy
        from .process_worker import ProcessWorkerProxy
        from .sync_worker import SyncWorkerProxy
        from .task_worker import TaskWorker, TaskWorkerMixin
        from .thread_worker import ThreadWorkerProxy

        local_config = global_config.clone()

        # Convert mode string to ExecutionMode
        execution_mode = self.mode

        # Apply composition wrapper for Typed/BaseModel workers (all modes)
        self._apply_composition_wrapper_if_needed()

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
        if self.worker_cls is TaskWorker or (
            isinstance(self.worker_cls, type) and issubclass(self.worker_cls, TaskWorker)
        ):
            # Create a dynamic class that combines the base proxy with TaskWorkerMixin
            # Use TaskWorkerMixin as the first base class so its methods take precedence
            proxy_cls = type(
                f"Task{proxy_cls.__name__}",
                (TaskWorkerMixin, proxy_cls),
                {},
            )

        # Process limits (always, even if None - creates empty LimitPool)
        limits = _transform_worker_limits(
            limits=self.limits,
            mode=execution_mode,
            is_pool=False,
            worker_index=0,  # Single workers use index 0
        )

        # Create retry config (always pass it, even if None)
        retry_config = self._create_retry_config()

        # Get mode defaults for worker timeouts (from global config)
        mode_defaults = local_config.get_defaults(execution_mode)

        # Build kwargs with only known proxy fields
        proxy_kwargs = {
            "worker_cls": self.worker_cls,
            "init_args": args,
            "init_kwargs": kwargs,
            "blocking": self.blocking,
            "max_queued_tasks": self.max_queued_tasks,
            "unwrap_futures": self.unwrap_futures,
            "limits": limits,
            "retry_config": retry_config,
        }

        # Add mode-specific timeout fields
        if execution_mode == ExecutionMode.Threads:
            proxy_kwargs["command_queue_timeout"] = mode_defaults.worker_command_queue_timeout
        elif execution_mode == ExecutionMode.Asyncio:
            proxy_kwargs["loop_ready_timeout"] = mode_defaults.worker_loop_ready_timeout
            proxy_kwargs["thread_ready_timeout"] = mode_defaults.worker_thread_ready_timeout
            proxy_kwargs["sync_queue_timeout"] = mode_defaults.worker_sync_queue_timeout
        elif execution_mode == ExecutionMode.Processes:
            proxy_kwargs["result_queue_timeout"] = mode_defaults.worker_result_queue_timeout
            proxy_kwargs["result_queue_cleanup_timeout"] = mode_defaults.worker_result_queue_cleanup_timeout
        # Sync and Ray modes have no worker-specific timeouts

        # Merge mode_options (pass through as-is to proxy)
        # For Ray: actor_options, num_cpus, num_gpus, resources, etc.
        # For Process: mp_context (fork, spawn, forkserver)
        proxy_kwargs.update(self.mode_options)

        return proxy_cls(**proxy_kwargs)

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
        # Import here to avoid circular imports
        from ...config import global_config
        from .worker_pool import (
            InMemoryWorkerProxyPool,
            MultiprocessWorkerProxyPool,
            RayWorkerProxyPool,
        )

        local_config = global_config.clone()

        # Convert mode string to ExecutionMode
        execution_mode = ExecutionMode(self.mode)

        # Apply composition wrapper for Typed/BaseModel workers (all modes)
        self._apply_composition_wrapper_if_needed()

        # Process limits for pool (always, even if None - creates empty LimitPool)
        # Note: worker_index will be assigned per-worker in pool initialization
        limits = _transform_worker_limits(
            limits=self.limits,
            mode=execution_mode,
            is_pool=True,
            worker_index=0,  # Placeholder, actual indices assigned per worker
        )

        # Create retry config (always pass it, even if None)
        retry_config = self._create_retry_config()

        # Select appropriate pool class
        if execution_mode in (ExecutionMode.Sync, ExecutionMode.Asyncio, ExecutionMode.Threads):
            pool_cls = InMemoryWorkerProxyPool
        elif execution_mode == ExecutionMode.Processes:
            pool_cls = MultiprocessWorkerProxyPool
        elif execution_mode == ExecutionMode.Ray:
            pool_cls = RayWorkerProxyPool
        else:
            raise ValueError(f"Unsupported execution mode for pool: {execution_mode}")

        # If this is TaskWorker, create a combined pool class with TaskWorkerPoolMixin
        from .task_worker import TaskWorker, TaskWorkerPoolMixin

        if self.worker_cls is TaskWorker or (
            isinstance(self.worker_cls, type) and issubclass(self.worker_cls, TaskWorker)
        ):
            # Create a dynamic class that combines the base pool with TaskWorkerPoolMixin
            # Use TaskWorkerPoolMixin as the first base class so its methods take precedence
            pool_cls = type(
                f"Task{pool_cls.__name__}",
                (TaskWorkerPoolMixin, pool_cls),
                {},
            )
        # Get mode defaults
        mode_defaults = local_config.get_defaults(execution_mode)

        # Apply default max_workers for pool if not specified
        max_workers = self.max_workers
        if max_workers is None:
            max_workers = mode_defaults.max_workers

        # Create pool instance with known pool fields + mode_options
        pool_kwargs = {
            "worker_cls": self.worker_cls,
            "mode": execution_mode,
            "max_workers": max_workers,
            "load_balancing": self.load_balancing,
            "on_demand": self.on_demand,
            "blocking": self.blocking,
            "max_queued_tasks": self.max_queued_tasks,
            "unwrap_futures": self.unwrap_futures,
            "limits": limits,
            "retry_config": retry_config,
            "init_args": args,
            "init_kwargs": kwargs,
            "on_demand_cleanup_timeout": mode_defaults.pool_on_demand_cleanup_timeout,
            "on_demand_slot_max_wait": mode_defaults.pool_on_demand_slot_max_wait,
        }

        # Merge mode_options (pass through as-is to pool)
        # For Ray: actor_options, num_cpus, num_gpus, resources, etc.
        # For Process: mp_context (fork, spawn, forkserver)
        pool_kwargs.update(self.mode_options)

        return pool_cls(**pool_kwargs)


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

    Submission Queue (Client-Side Task Queuing):
        Workers support client-side submission queuing via the `max_queued_tasks` parameter.
        This prevents overloading worker backends when submitting large batches of tasks.

        **Key Benefits:**
        - Prevents memory exhaustion from thousands of pending futures
        - Avoids backend overload (especially Ray actors)
        - Reduces network saturation for distributed workers
        - Works transparently with your submission loops

        **How it works:**
        The submission queue limits how many tasks can be "in-flight" (submitted but not completed)
        per worker. When the queue is full, further submissions block until a task completes.

        ```python
        # Create worker with submission queue
        worker = MyWorker.options(
            mode="thread",
            max_queued_tasks=10  # Max 10 tasks in-flight
        ).init()

        # Submit 1000 tasks - automatically blocks when queue is full
        futures = [worker.process(item) for item in range(1000)]
        results = gather(futures)  # Submission queue prevents overload
        worker.stop()
        ```

        **Default values by mode:**
        - sync/asyncio: None (bypassed) - immediate execution or event loop handles concurrency
        - thread: 100 - high concurrency, large queue
        - process: 5 - limited by CPU cores
        - ray: 2 - minimize data transfer overhead

        **Integration with other features:**
        - **Limits**: Submission queue (client-side) + resource limits (worker-side) work together
        - **Retries**: Only original submissions count, not retry attempts
        - **Load Balancing**: Each worker in a pool has its own independent queue
        - **On-Demand Workers**: Automatically bypass submission queue

        For comprehensive documentation and examples, see the user guide:
        `/docs/user-guide/limits.md#submission-queue`

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
        *,
        mode: ExecutionMode,
        blocking: Union[bool, _NO_ARG_TYPE] = _NO_ARG,
        max_workers: Union[conint(ge=0), None, _NO_ARG_TYPE] = _NO_ARG,
        load_balancing: Union[LoadBalancingAlgorithm, _NO_ARG_TYPE] = _NO_ARG,
        on_demand: Union[bool, _NO_ARG_TYPE] = _NO_ARG,
        max_queued_tasks: Union[conint(ge=0), None, _NO_ARG_TYPE] = _NO_ARG,
        # Retry parameters
        num_retries: Union[conint(ge=0), _NO_ARG_TYPE] = _NO_ARG,
        retry_on: Optional[Any] = None,
        retry_algorithm: Union[RetryAlgorithm, _NO_ARG_TYPE] = _NO_ARG,
        retry_wait: Union[confloat(ge=0), _NO_ARG_TYPE] = _NO_ARG,
        retry_jitter: Union[confloat(ge=0, le=1), _NO_ARG_TYPE] = _NO_ARG,
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
                Default value determined by global_config.<mode>.blocking
            max_workers: Maximum number of workers in pool (optional)
                - If None or 1: Creates single worker. If >1: Creates worker pool with specified size.
                - Sync/Asyncio: Must be 1 or None (raises error otherwise)
                - Default value determined by global_config.<mode>.max_workers
            load_balancing: Load balancing algorithm (optional)
                - "round_robin": Distribute requests evenly
                - "least_active": Select worker with fewest active calls
                - "least_total": Select worker with fewest total calls
                - "random": Random selection
                - Default value determined by global_config.<mode>.load_balancing (for pools)
                  or global_config.<mode>.load_balancing_on_demand (for on-demand pools)
            on_demand: If True, create workers on-demand per request (default: False)
                - Workers are created for each request and destroyed after completion
                - Useful for bursty workloads or resource-constrained environments
                - Cannot be used with Sync/Asyncio modes
                - With max_workers=0: Unlimited concurrent workers (Ray) or
                  limited to cpu_count()-1 (Thread/Process)
            max_queued_tasks: Maximum number of in-flight tasks per worker (default varies by mode)
                - Controls how many tasks can be submitted to a worker's backend before blocking
                - Per-worker limit: each worker in a pool has its own independent queue
                - Value of N means max N tasks submitted but not yet completed per worker
                - Automatically bypassed in blocking mode (unlimited submissions allowed)
                - Automatically bypassed in sync and asyncio modes
                - Prevents overload when submitting large batches (e.g., 5000+ tasks to Ray)
                - Default value determined by global_config.<mode>.max_queued_tasks
                - See user guide for detailed usage: /docs/user-guide/limits.md#submission-queue
            unwrap_futures: If True, automatically unwrap BaseFuture arguments
                by calling .result() on them before passing to worker methods. This enables
                seamless composition of workers. Set to False to pass futures as-is.
                Default value determined by global_config.<mode>.unwrap_futures
            limits: Resource protection and rate limiting (optional, defaults to empty LimitSet)
                - Pass LimitSet: Workers share the same limit pool
                - Pass List[Limit]: Each worker gets private limits (creates shared LimitSet for pools)
                - Omit parameter: Workers get empty LimitSet (self.limits.acquire() always succeeds)
                Workers always have self.limits available, even when no limits configured.
                See Worker docstring "Resource Protection with Limits" section for details.
            num_retries: Maximum number of retry attempts after initial failure
                Total attempts = num_retries + 1 (initial attempt).
                Set to 0 to disable retries (zero overhead).
                Default value determined by global_config.<mode>.num_retries
            retry_on: Exception types or callables that trigger retries (optional)
                - Single exception class: retry_on=ValueError
                - List of exceptions: retry_on=[ValueError, ConnectionError]
                - Callable filter: retry_on=lambda exception, **ctx: "retry" in str(exception)
                - Mixed list: retry_on=[ValueError, custom_filter]
                Default: [Exception] (retry on all exceptions when num_retries > 0)
            retry_algorithm: Backoff strategy for wait times
                Default value determined by global_config.<mode>.retry_algorithm
            retry_wait: Minimum wait time between retries in seconds
                Base wait time before applying strategy and jitter.
                Default value determined by global_config.<mode>.retry_wait
            retry_jitter: Jitter factor between 0 and 1
                Uses Full Jitter algorithm from AWS: sleep = random(0, calculated_wait).
                Set to 0 to disable jitter. Prevents thundering herd when many workers retry.
                Default value determined by global_config.<mode>.retry_jitter
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
        # Import here to avoid circular imports
        from ...config import global_config

        # Get defaults for this mode from global config
        mode_defaults = global_config.get_defaults(mode)

        # Apply defaults for all parameters if not specified
        if blocking is _NO_ARG:
            blocking = mode_defaults.blocking

        if max_workers is _NO_ARG:
            max_workers = mode_defaults.max_workers

        if on_demand is _NO_ARG:
            on_demand = mode_defaults.on_demand

        if max_queued_tasks is _NO_ARG:
            max_queued_tasks = mode_defaults.max_queued_tasks

        if load_balancing is _NO_ARG:
            if on_demand:
                load_balancing = mode_defaults.load_balancing_on_demand
            else:
                load_balancing = mode_defaults.load_balancing

        if num_retries is _NO_ARG:
            num_retries = mode_defaults.num_retries

        if retry_algorithm is _NO_ARG:
            retry_algorithm = mode_defaults.retry_algorithm

        if retry_wait is _NO_ARG:
            retry_wait = mode_defaults.retry_wait

        if retry_jitter is _NO_ARG:
            retry_jitter = mode_defaults.retry_jitter

        # Extract unwrap_futures from kwargs (with default)
        unwrap_futures = kwargs.pop("unwrap_futures", mode_defaults.unwrap_futures)

        # Extract limits from kwargs
        limits = kwargs.pop("limits", None)

        # Everything else in kwargs is mode-specific options (passed through as-is)
        # For Ray: actor_options dict containing num_cpus, num_gpus, resources, etc.
        # For Process: mp_context (fork, spawn, forkserver)
        mode_options = kwargs  # Pass through all remaining kwargs

        # Apply default for retry_on if None (default is [Exception])
        if retry_on is None:
            retry_on = [Exception]

        return WorkerBuilder(
            worker_cls=cls,
            mode=mode,
            blocking=blocking,
            max_workers=max_workers,
            load_balancing=load_balancing,
            on_demand=on_demand,
            max_queued_tasks=max_queued_tasks,
            num_retries=num_retries,
            retry_on=retry_on,
            retry_algorithm=retry_algorithm,
            retry_wait=retry_wait,
            retry_jitter=retry_jitter,
            retry_until=retry_until,
            unwrap_futures=unwrap_futures,
            limits=limits,
            mode_options=mode_options,
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
            # NOTE: DO NOT add defaults to public fields!
            # All values must be passed from WorkerBuilder via global_config
            custom_option: str

            # Private attributes (mutable, type-checked)
            _custom_state: int = PrivateAttr()
            _custom_resource: Any = PrivateAttr()  # Use Any for non-serializable types

            def post_initialize(self) -> None:
                super().post_initialize()
                self._custom_state = 0
                self._custom_resource = SomeNonSerializableObject()
        ```

    **CRITICAL: No Default Values on Public Attributes**

    Public attributes (those without _ prefix) MUST NOT have default values.
    All values must be explicitly passed from WorkerBuilder, which resolves defaults
    from global_config. This ensures:
    1. All defaults are centralized in global_config
    2. Users can override defaults globally via temp_config()
    3. Mode-specific defaults are correctly applied

    Private attributes (prefixed with _) can and should have defaults via PrivateAttr().
    """

    # NOTE: model_config is NOT to be overridden - we use Typed's default config
    mode: ClassVar[ExecutionMode]  # ExecutionMode (set by subclasses as class variable)

    # ========================================================================
    # PUBLIC ATTRIBUTES - NO DEFAULTS ALLOWED
    # All values must be passed from WorkerBuilder (with defaults picked from
    # global_config)
    # ========================================================================
    worker_cls: Type[Worker]
    blocking: bool
    unwrap_futures: bool
    init_args: tuple
    init_kwargs: dict
    limits: Optional[Any]  # Possibly non-shared LimitSet instance (processed by WorkerBuilder)
    retry_config: Optional[RetryConfig]
    max_queued_tasks: Optional[conint(ge=0)]

    # ========================================================================
    # PRIVATE ATTRIBUTES - Defaults via PrivateAttr() are okay
    # ========================================================================
    _stopped: bool = PrivateAttr(default=False)
    _method_cache: dict[str, Any] = PrivateAttr(default_factory=dict)
    _submission_semaphore: Optional[Any] = PrivateAttr(default=None)

    def post_initialize(self) -> None:
        """Initialize private attributes after Typed validation."""
        # Initialize submission queue semaphore
        # Skip if blocking mode, sync mode, asyncio mode, or max_queued_tasks is None (bypass queuing)
        # AsyncIO workers benefit from unlimited concurrent submissions since they handle
        # concurrency via the event loop, not by blocking threads

        if (
            self.mode in (ExecutionMode.Sync, ExecutionMode.Asyncio)
            or self.blocking
            or self.max_queued_tasks is None
        ):
            self._submission_semaphore = None
        else:
            self._submission_semaphore = threading.BoundedSemaphore(self.max_queued_tasks)

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

            # Acquire submission semaphore first (blocks if queue full)
            # This must happen BEFORE the stopped check to avoid race condition:
            # Without this order, a thread could check _stopped (False), then block
            # on semaphore acquisition, then stop() is called, then thread wakes up
            # and executes the method even though worker is stopped.
            if self._submission_semaphore is not None:
                self._submission_semaphore.acquire()

            # Now check if stopped - this is atomic with execution because we
            # already hold the semaphore. If stopped, release semaphore and raise.
            if self._stopped:
                if self._submission_semaphore is not None:
                    self._submission_semaphore.release()
                raise RuntimeError("Worker is stopped")

            future = self._execute_method(name, *args, **kwargs)

            # Wrap future to release semaphore on completion
            if self._submission_semaphore is not None:
                future = self._wrap_future_with_semaphore_release(future)

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

    def _wrap_future_with_semaphore_release(self, future: BaseFuture) -> BaseFuture:
        """Wrap a future to release submission semaphore when complete.

        This ensures the semaphore is released regardless of whether the task
        succeeded, failed, or was cancelled.

        Args:
            future: The future to wrap

        Returns:
            The same future (modified in-place with callback)
        """
        if self._submission_semaphore is None:
            return future  # No semaphore to release

        semaphore = self._submission_semaphore

        # Use add_done_callback to release semaphore when complete
        def release_semaphore(f):
            try:
                semaphore.release()
            except Exception:
                pass  # Ignore release errors (shouldn't happen)

        future.add_done_callback(release_semaphore)
        return future

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

    def stop(self, timeout: confloat(ge=0) = 30) -> None:
        """Stop the worker and clean up resources.

        Args:
            timeout: Maximum time to wait for cleanup in seconds.
                Default value is determined by global_config.<mode>.stop_timeout
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
