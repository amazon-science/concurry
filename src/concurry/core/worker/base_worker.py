"""Worker implementation for concurry."""

import queue
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
    from ...config import global_config
    from ..limit import Limit
    from ..limit.limit_pool import LimitPool
    from ..limit.limit_set import (
        BaseLimitSet,
        InMemorySharedLimitSet,
        LimitSet,
        MultiprocessSharedLimitSet,
        RaySharedLimitSet,
    )

    # Get mp_context from global config for process mode
    mp_context = None
    if mode == ExecutionMode.Processes:
        local_config = global_config.clone()
        mp_context = local_config.defaults.mp_context

    # Case 1: None -> Create empty LimitPool with empty LimitSet
    if limits is None:
        # Create empty LimitSet
        if is_pool:
            empty_limitset = LimitSet(limits=[], shared=True, mode=mode, mp_context=mp_context)
        else:
            if mode in (ExecutionMode.Ray, ExecutionMode.Processes):
                # For Ray/Process, create list to be wrapped remotely
                empty_limitset = []
            else:
                empty_limitset = LimitSet(limits=[], shared=False, mode=ExecutionMode.Sync, mp_context=None)

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
                empty_limitset = LimitSet(limits=[], shared=True, mode=mode, mp_context=mp_context)
            else:
                if mode in (ExecutionMode.Ray, ExecutionMode.Processes):
                    return []  # Will be wrapped remotely
                empty_limitset = LimitSet(limits=[], shared=False, mode=ExecutionMode.Sync, mp_context=None)
            return LimitPool(
                limit_sets=[empty_limitset],
                worker_index=worker_index,
            )

        # Check if List[Limit]
        if all(isinstance(item, Limit) for item in limits):
            # Create LimitSet from Limits
            if is_pool:
                limitset = LimitSet(limits=limits, shared=True, mode=mode, mp_context=mp_context)
            else:
                if mode in (ExecutionMode.Ray, ExecutionMode.Processes):
                    return limits  # Keep as list, will be wrapped remotely
                limitset = LimitSet(limits=limits, shared=False, mode=ExecutionMode.Sync, mp_context=None)
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
                new_limitset = LimitSet(
                    limits=limits_list, shared=False, mode=ExecutionMode.Sync, mp_context=None
                )
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

    This validation prevents runtime errors from mode mismatches:
    - MultiprocessSharedLimitSet uses multiprocessing.Manager() (only works across processes)
    - RaySharedLimitSet uses Ray actor (only works across Ray actors)
    - InMemorySharedLimitSet uses threading.Lock (works in same process only)

    Using the wrong LimitSet backend for a worker mode will either fail to share limits
    (each worker gets own copy) or cause serialization/communication errors.

    Args:
        limit_set: The LimitSet to validate
        worker_mode: The worker's execution mode

    Raises:
        ValueError: If the LimitSet is not compatible with the worker mode
    """
    from ..limit.limit_set import InMemorySharedLimitSet, MultiprocessSharedLimitSet, RaySharedLimitSet

    # Check for mode mismatches that would cause issues
    if isinstance(limit_set, MultiprocessSharedLimitSet) and worker_mode != ExecutionMode.Processes:
        raise ValueError(
            f"MultiprocessSharedLimitSet can only be used with process mode workers, "
            f"but worker_mode is {worker_mode}. "
            f"Create LimitSet with mode='{worker_mode.value}' to match worker mode."
        )
    elif isinstance(limit_set, RaySharedLimitSet) and worker_mode != ExecutionMode.Ray:
        raise ValueError(
            f"RaySharedLimitSet can only be used with ray mode workers, "
            f"but worker_mode is {worker_mode}. "
            f"Create LimitSet with mode='{worker_mode.value}' to match worker mode."
        )
    elif isinstance(limit_set, InMemorySharedLimitSet) and worker_mode in (
        ExecutionMode.Processes,
        ExecutionMode.Ray,
    ):
        # This is okay - InMemorySharedLimitSet can be used with process/ray workers
        # It just won't share across workers (each worker gets its own copy)
        pass


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


def _get_user_defined_methods(worker_cls: Type) -> list[str]:
    """Get list of user-defined method names on a worker class.

    Filters out:
    - Private/dunder methods (startswith("_"))
    - Infrastructure methods from Typed/BaseModel
    - Inherited methods (not in worker_cls.__dict__)
    - Non-callable attributes
    - Type objects

    Args:
        worker_cls: The worker class to inspect

    Returns:
        List of user-defined method names
    """
    method_names = []

    for attr_name in dir(worker_cls):
        # Skip private/dunder methods
        if attr_name.startswith("_"):
            continue

        # Skip infrastructure methods
        if _is_infrastructure_method(attr_name):
            continue

        # Only methods defined directly on worker class
        if attr_name not in worker_cls.__dict__:
            continue

        try:
            attr = getattr(worker_cls, attr_name)

            # Only callable methods
            if not callable(attr):
                continue

            # Skip type objects
            if isinstance(attr, type):
                continue

            method_names.append(attr_name)
        except (AttributeError, TypeError):
            continue

    return method_names


def _normalize_retry_param(
    param_value: Union[T, dict[str, T]],
    param_name: str,
    method_names: list[str],
) -> dict[str, T]:
    """Normalize retry parameter to dict format.

    Converts:
    - Single value → {"*": value, "method1": value, "method2": value, ...}
    - Dict with "*" → Expand "*" to all methods not explicitly listed

    Args:
        param_value: Single value or dict of method_name → value
        param_name: Name of parameter (for error messages)
        method_names: List of all user-defined method names

    Returns:
        Dict mapping each method name to its value (always includes "*")

    Raises:
        ValueError: If dict missing "*" key or contains invalid method names
    """
    # Single value: expand to all methods
    if not isinstance(param_value, dict):
        result = {"*": param_value}
        for method_name in method_names:
            result[method_name] = param_value
        return result

    # Dict: validate and expand
    if "*" not in param_value:
        raise ValueError(
            f"{param_name} dict must include '*' key for default value. Got keys: {list(param_value.keys())}"
        )

    # Validate all method names exist
    default_value = param_value["*"]
    invalid_methods = []
    for method_name in param_value.keys():
        if method_name != "*" and method_name not in method_names:
            invalid_methods.append(method_name)

    if len(invalid_methods) > 0:
        raise ValueError(
            f"{param_name} dict contains unknown method names: {invalid_methods}. "
            f"Valid methods: {method_names}"
        )

    # Build result: all methods get default, then override from dict
    result = {"*": default_value}
    for method_name in method_names:
        result[method_name] = param_value.get(method_name, default_value)

    return result


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
            # Remove _from_proxy flag from our kwargs (if present)
            kwargs.pop("_from_proxy", None)

            # Don't call super().__init__() since Worker base class doesn't define __init__
            # Create the actual BaseModel/Typed instance internally
            # This happens inside the Ray actor, so serialization is fine

            # CRITICAL: Only pass _from_proxy=True if Worker comes before Typed/BaseModel in MRO
            # This prevents auto_init recursion while avoiding Pydantic validation errors
            # If Typed/BaseModel comes first, they'll reject _from_proxy before Worker can pop it
            mro = worker_cls.__mro__
            worker_idx = mro.index(Worker) if Worker in mro else -1

            # Check if Typed or BaseModel comes before Worker in MRO
            typed_before_worker = False
            for i, cls in enumerate(mro):
                cls_name = cls.__name__
                if cls_name in ("Typed", "BaseModel") and (worker_idx == -1 or i < worker_idx):
                    typed_before_worker = True
                    break

            # Only pass _from_proxy if Worker comes first (can pop it before Typed validation)
            if typed_before_worker:
                # Typed/BaseModel comes first - don't pass _from_proxy
                # The wrapped class won't have auto_init anyway (we removed it from config)
                self._wrapped_instance = worker_cls(*args, **kwargs)
            else:
                # Worker comes first - pass _from_proxy to prevent auto_init recursion
                self._wrapped_instance = worker_cls(*args, _from_proxy=True, **kwargs)

        def __getattr__(self, name: str):
            """Delegate attribute access to wrapped instance.

            Only allows access to user-defined methods, not infrastructure methods.
            This prevents Ray from trying to serialize infrastructure methods.

            CRITICAL: This method must handle Ray-internal attributes carefully to avoid
            infinite recursion when Ray's tracing system or client mode inspects the wrapper.
            """
            # CRITICAL: Don't delegate Ray-internal attributes
            # Ray's tracing system and client mode check for these attributes,
            # and delegating them causes infinite recursion through Ray's tracing wrapper
            # Common Ray attributes: _ray_trace_ctx, __ray_*, RAY_CLIENT_MODE_ATTR, etc.
            if (
                name.startswith("_ray")
                or name.startswith("__ray")
                or name == "RAY_CLIENT_MODE_ATTR"
                or name.startswith("__pydantic")  # Also prevent Pydantic internals from being delegated
            ):
                raise AttributeError(f"Internal attribute '{name}' not available on composition wrapper")

            # Check if _wrapped_instance exists (handles access during initialization)
            # This prevents AttributeError during __init__ before _wrapped_instance is set
            if "_wrapped_instance" not in self.__dict__:
                raise AttributeError(f"'{name}' cannot be accessed before wrapper initialization is complete")

            # Block access to infrastructure methods
            if _is_infrastructure_method(name):
                raise AttributeError(
                    f"Infrastructure method '{name}' not available in Ray wrapper. "
                    f"Only user-defined methods are exposed for Ray compatibility."
                )

            # Delegate to wrapped instance
            # This will raise AttributeError if the attribute doesn't exist on wrapped instance
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
        # CRITICAL: Use getattr() to go through __getattribute__ on wrapped instance
        # This ensures retry logic and other wrapping is applied to internal method calls
        def make_method(method_name, is_async):
            """Create a method that delegates to the wrapped instance.

            Uses getattr() to access methods on _wrapped_instance, which ensures
            that __getattribute__ is called and retry/limits logic is applied.
            """

            if is_async:

                async def async_delegating_method(self, *args, **kwargs):
                    # Use getattr to go through __getattribute__ on wrapped instance
                    # This ensures retry wrapping is applied to internal method calls
                    method = getattr(self._wrapped_instance, method_name)
                    return await method(*args, **kwargs)

                async_delegating_method.__name__ = method_name
                async_delegating_method.__qualname__ = f"CompositionWrapper.{method_name}"
                return async_delegating_method
            else:

                def delegating_method(self, *args, **kwargs):
                    # Use getattr to go through __getattribute__ on wrapped instance
                    # This ensures retry wrapping is applied to internal method calls
                    method = getattr(self._wrapped_instance, method_name)
                    return method(*args, **kwargs)

                delegating_method.__name__ = method_name
                delegating_method.__qualname__ = f"CompositionWrapper.{method_name}"
                return delegating_method

        # Check if method is async
        import inspect

        is_async_method = inspect.iscoroutinefunction(attr)

        # Add the delegating method to the wrapper class
        setattr(CompositionWrapper, attr_name, make_method(attr_name, is_async_method))

    # Set wrapper class name for debugging
    CompositionWrapper.__name__ = f"{worker_cls.__name__}_CompositionWrapper"
    CompositionWrapper.__qualname__ = f"{worker_cls.__qualname__}_CompositionWrapper"
    CompositionWrapper.__module__ = worker_cls.__module__

    # CRITICAL: Copy config attributes to wrapper, but EXCLUDE auto_init
    # The composition wrapper should never auto-initialize itself (it's created by the proxy)
    # Copying auto_init would cause infinite recursion when proxy tries to instantiate the wrapper
    if hasattr(worker_cls, "_worker_inheritance_config"):
        inheritance_config = worker_cls._worker_inheritance_config.copy()
        inheritance_config.pop("auto_init", None)  # Remove auto_init
        if len(inheritance_config) > 0:  # Only set if there are other configs
            CompositionWrapper._worker_inheritance_config = inheritance_config

    if hasattr(worker_cls, "_worker_decorator_config"):
        decorator_config = worker_cls._worker_decorator_config.copy()
        decorator_config.pop("auto_init", None)  # Remove auto_init
        if len(decorator_config) > 0:  # Only set if there are other configs
            CompositionWrapper._worker_decorator_config = decorator_config

    return CompositionWrapper


def _create_worker_wrapper(
    worker_cls: Type,
    limits: Any,
    retry_configs: Optional[dict[str, Optional[RetryConfig]]] = None,
    for_ray: bool = False,
) -> Type:
    """Create a wrapper class that injects limits and retry logic.

    This wrapper dynamically inherits from the user's worker class and:
    1. Sets self.limits in __init__ (if limits provided)
    2. Wraps all public methods with retry logic (if retry_configs provided and num_retries > 0)
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
        retry_configs: Dict[str, RetryConfig] for per-method retry configs (optional, defaults to None)
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
        configs = {"*": RetryConfig(num_retries=3, retry_algorithm="exponential")}
        wrapper_cls = _create_worker_wrapper(MyWorker, limit_set, configs)
        worker = wrapper_cls(*args, **kwargs)
        # worker.limits is accessible
        # worker methods automatically retry on failure

        # With retries only (no limits):
        wrapper_cls = _create_worker_wrapper(MyWorker, None, configs)
        worker = wrapper_cls(*args, **kwargs)
        # worker methods automatically retry on failure
        ```
    """
    # Import here to avoid circular imports

    # Determine if we need to apply any wrapping
    # Note: limits is now always provided (may be empty list or empty LimitSet)
    has_limits = limits is not None
    # Check if retry logic is needed: either num_retries > 0 OR retry_until is set
    has_retry = retry_configs is not None and any(
        cfg is not None and (cfg.num_retries > 0 or cfg.retry_until is not None)
        for cfg in retry_configs.values()
    )

    # If no retry, we still need to wrap to set limits attribute
    # (limits is always provided now, even if empty)
    if not has_retry:
        # Only need to set limits, no retry logic
        class WorkerWithLimits(worker_cls):
            def __init__(self, *args, **kwargs):
                # Remove _from_proxy flag if present (internal use only)
                kwargs.pop("_from_proxy", None)

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

        # CRITICAL: Remove auto_init from inherited config to prevent infinite recursion
        # The worker wrapper should never auto-initialize itself (it's created by the proxy)
        if hasattr(WorkerWithLimits, "_worker_inheritance_config"):
            config = WorkerWithLimits._worker_inheritance_config.copy()
            config.pop("auto_init", None)
            if len(config) > 0:
                WorkerWithLimits._worker_inheritance_config = config
            else:
                delattr(WorkerWithLimits, "_worker_inheritance_config")

        if hasattr(WorkerWithLimits, "_worker_decorator_config"):
            config = WorkerWithLimits._worker_decorator_config.copy()
            config.pop("auto_init", None)
            if len(config) > 0:
                WorkerWithLimits._worker_decorator_config = config
            else:
                delattr(WorkerWithLimits, "_worker_decorator_config")

        return WorkerWithLimits

    class WorkerWithLimitsAndRetry(worker_cls):
        def __init__(self, *args, **kwargs):
            # Remove _from_proxy flag if present (internal use only)
            kwargs.pop("_from_proxy", None)

            # Call parent __init__ first to properly initialize Pydantic models
            super().__init__(*args, **kwargs)

            # Cache whether this is a composition wrapper (CRITICAL for Ray performance)
            # This check determines if retry wrapping should be skipped at this level
            # (because _wrapped_instance already has retry wrapping applied)
            # Caching avoids expensive type() introspection on every method access
            _is_composition = hasattr(self, "_wrapped_instance")
            object.__setattr__(self, "_is_composition_wrapper", _is_composition)

            # CRITICAL FIX: If this is a composition wrapper, replace _wrapped_instance
            # with a wrapped version that has retry logic applied
            if _is_composition:
                # The composition wrapper created _wrapped_instance from the original class
                # We need to replace it with an instance that has __getattribute__ retry wrapping

                # Get the original instance
                original_instance = self._wrapped_instance

                # Create a simple wrapper class with retry logic for this instance
                original_class = type(original_instance)

                class WrappedInstanceWithRetry(original_class):
                    def __getattribute__(self, name: str):
                        """Apply retry wrapping to method calls."""
                        attr = super().__getattribute__(name)

                        # Same retry wrapping logic as WorkerWithLimitsAndRetry
                        if (
                            has_retry
                            and not name.startswith("_")
                            and callable(attr)
                            and not isinstance(attr, type)
                        ):
                            if name in retry_configs:
                                method_config = retry_configs[name]
                            else:
                                method_config = retry_configs.get("*")

                            if method_config is None:
                                return attr
                            if method_config.num_retries == 0 and method_config.retry_until is None:
                                return attr

                            wrapped = create_retry_wrapper(
                                attr,
                                method_config,
                                method_name=name,
                                worker_class_name=original_class.__name__,
                            )
                            return wrapped

                        return attr

                # Replace the instance's class (Python allows this!)
                original_instance.__class__ = WrappedInstanceWithRetry
                # _wrapped_instance is now an instance with retry-wrapped __getattribute__

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

            # CRITICAL: If this is a composition wrapper, skip retry wrapping at this level
            # because _wrapped_instance already has retry wrapping applied
            # This prevents double-wrapping which would cause validators to be called multiple times
            # Use cached flag for performance (critical in Ray where type() is expensive)
            is_composition = False
            if name != "_is_composition_wrapper":  # Avoid recursion on the flag itself
                try:
                    is_composition = super().__getattribute__("_is_composition_wrapper")
                except AttributeError:
                    # Flag not set yet (during __init__), check if _wrapped_instance exists
                    # Use super().__getattribute__ directly to avoid recursion through hasattr()
                    try:
                        super().__getattribute__("_wrapped_instance")
                        is_composition = True
                    except AttributeError:
                        is_composition = False

            # Only wrap public methods if retry is configured AND not for Ray AND not a composition wrapper
            # (Ray mode uses pre-wrapped methods at class level)
            if (
                has_retry
                and not for_ray
                and not is_composition  # Skip if composition wrapper
                and not name.startswith("_")
                and callable(attr)
                and not isinstance(attr, type)
            ):
                # For composition wrappers (Typed/BaseModel), infrastructure methods
                # are already filtered out - only user-defined methods are exposed

                # Get retry config for this method
                # If method explicitly configured (even if None for 0 retries), use that
                # Otherwise fall back to "*" default
                if name in retry_configs:
                    method_config = retry_configs[name]
                else:
                    method_config = retry_configs.get("*")

                # Skip wrapping if no config or (num_retries=0 and no retry_until)
                # We need to wrap even with num_retries=0 if retry_until is set for validation
                if method_config is None:
                    return attr
                if method_config.num_retries == 0 and method_config.retry_until is None:
                    return attr

                # Wrap the method with retry logic
                # Note: This creates a new wrapper on every access, but that's okay because:
                # 1. The wrapper is lightweight (just adds retry logic)
                # 2. It's only created when the method is actually called
                # 3. Python's bound method mechanism already creates new objects on each access
                wrapped = create_retry_wrapper(
                    attr,
                    method_config,
                    method_name=name,
                    worker_class_name=worker_cls.__name__,
                )

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

                # Get retry config for this method
                # If method explicitly configured (even if None for 0 retries), use that
                # Otherwise fall back to "*" default
                if attr_name in retry_configs:
                    method_config = retry_configs[attr_name]
                else:
                    method_config = retry_configs.get("*")

                # Skip wrapping if no config or (num_retries=0 and no retry_until)
                # We need to wrap even with num_retries=0 if retry_until is set for validation
                if method_config is None:
                    continue
                if method_config.num_retries == 0 and method_config.retry_until is None:
                    continue

                # Create a wrapper method that applies retry logic
                def make_wrapped_method(original_method, method_name, method_retry_config):
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
                                bound_method, args, kwargs, method_retry_config, context
                            )

                        return async_method_wrapper
                    else:

                        def sync_method_wrapper(self, *args, **kwargs):
                            context = {
                                "method_name": method_name,
                                "worker_class_name": worker_cls.__name__,
                            }
                            # Bind self to the original method
                            bound_method = original_method.__get__(self, type(self))
                            return execute_with_retry(
                                bound_method, args, kwargs, method_retry_config, context
                            )

                        return sync_method_wrapper

                wrapped = make_wrapped_method(attr, attr_name, method_config)
                setattr(WorkerWithLimitsAndRetry, attr_name, wrapped)
            except (AttributeError, TypeError):
                # Skip attributes that can't be wrapped
                pass

    # CRITICAL: Remove auto_init from inherited config to prevent infinite recursion
    # The worker wrapper should never auto-initialize itself (it's created by the proxy)
    if hasattr(WorkerWithLimitsAndRetry, "_worker_inheritance_config"):
        config = WorkerWithLimitsAndRetry._worker_inheritance_config.copy()
        config.pop("auto_init", None)
        if len(config) > 0:
            WorkerWithLimitsAndRetry._worker_inheritance_config = config
        else:
            delattr(WorkerWithLimitsAndRetry, "_worker_inheritance_config")

    if hasattr(WorkerWithLimitsAndRetry, "_worker_decorator_config"):
        config = WorkerWithLimitsAndRetry._worker_decorator_config.copy()
        config.pop("auto_init", None)
        if len(config) > 0:
            WorkerWithLimitsAndRetry._worker_decorator_config = config
        else:
            delattr(WorkerWithLimitsAndRetry, "_worker_decorator_config")

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

    # Retry parameters (can be single values or dicts mapping method names to values)
    num_retries: Union[conint(ge=0), dict[str, Union[conint(ge=0), None]]]
    retry_on: Union[Any, dict[str, Any]]  # List of exception types or callables, default [Exception]
    retry_algorithm: Union[RetryAlgorithm, dict[str, Union[RetryAlgorithm, None]]]
    retry_wait: Union[confloat(ge=0), dict[str, Union[confloat(ge=0), None]]]
    retry_jitter: Union[confloat(ge=0, le=1), dict[str, Union[confloat(ge=0, le=1), None]]]
    retry_until: Optional[Union[Any, dict[str, Any]]]  # Truly optional, default None

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

    def _create_retry_configs(self) -> Optional[dict[str, Optional[RetryConfig]]]:
        """Create per-method RetryConfig objects from retry parameters.

        Returns:
            Dict mapping method names to RetryConfig instances (or None for num_retries=0 and no retry_until),
            or None if all num_retries=0 and no retry_until validators.
            Always includes "*" key for default config.
        """
        # Fast path: if num_retries is 0 (single value) and no retry_until, no retry
        if not isinstance(self.num_retries, dict) and self.num_retries == 0:
            # Check if retry_until is set - if so, we still need retry logic for validation
            if self.retry_until is None:
                return None

        # Check if dict with all zeros and no retry_until
        if isinstance(self.num_retries, dict):
            if all(v == 0 for v in self.num_retries.values()):
                # Check if any retry_until is set
                if self.retry_until is None or (
                    isinstance(self.retry_until, dict) and all(v is None for v in self.retry_until.values())
                ):
                    return None

        # Build per-method configs
        result = {}

        # Determine method names
        if isinstance(self.num_retries, dict):
            method_names = list(self.num_retries.keys())
        else:
            # Single value - just create default config
            method_names = ["*"]

        for method_name in method_names:
            # Extract values for this method
            if isinstance(self.num_retries, dict):
                num_retries = self.num_retries[method_name]
                retry_on = self.retry_on[method_name] if isinstance(self.retry_on, dict) else self.retry_on
                retry_algorithm = (
                    self.retry_algorithm[method_name]
                    if isinstance(self.retry_algorithm, dict)
                    else self.retry_algorithm
                )
                retry_wait = (
                    self.retry_wait[method_name] if isinstance(self.retry_wait, dict) else self.retry_wait
                )
                retry_jitter = (
                    self.retry_jitter[method_name]
                    if isinstance(self.retry_jitter, dict)
                    else self.retry_jitter
                )
                retry_until = (
                    self.retry_until[method_name]
                    if isinstance(self.retry_until, dict) and self.retry_until is not None
                    else self.retry_until
                )
            else:
                # All single values
                num_retries = self.num_retries
                retry_on = self.retry_on
                retry_algorithm = self.retry_algorithm
                retry_wait = self.retry_wait
                retry_jitter = self.retry_jitter
                retry_until = self.retry_until

            # Skip if num_retries is 0 AND no retry_until validator for this method
            if num_retries == 0 and retry_until is None:
                result[method_name] = None
                continue

            # Create RetryConfig for this method
            # Even with num_retries=0, we create config if retry_until is set for validation
            result[method_name] = RetryConfig(
                num_retries=num_retries,
                retry_on=retry_on if retry_on is not None else [Exception],
                retry_algorithm=RetryAlgorithm(retry_algorithm),
                retry_wait=retry_wait,
                retry_jitter=retry_jitter,
                retry_until=retry_until,
            )

        # If all configs are None, return None
        if all(v is None for v in result.values()):
            return None

        return result

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

        Typed/BaseModel workers are automatically wrapped with composition pattern
        for seamless Ray compatibility.

        Args:
            args: Positional arguments for worker __init__
            kwargs: Keyword arguments for worker __init__

        Returns:
            WorkerProxy instance
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

        # Create retry configs (always pass it, even if None)
        retry_configs = self._create_retry_configs()

        # Get mode defaults for worker timeouts (from global config)
        mode_defaults = local_config.get_defaults(execution_mode)

        # Add _from_proxy flag to init_kwargs to prevent auto_init recursion
        worker_init_kwargs = {**kwargs, "_from_proxy": True}

        # Build kwargs with only known proxy fields
        proxy_kwargs = {
            "worker_cls": self.worker_cls,
            "init_args": args,
            "init_kwargs": worker_init_kwargs,
            "blocking": self.blocking,
            "max_queued_tasks": self.max_queued_tasks,
            "unwrap_futures": self.unwrap_futures,
            "limits": limits,
            "retry_configs": retry_configs,
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
            # Add mp_context from config if not in mode_options
            if "mp_context" not in self.mode_options:
                proxy_kwargs["mp_context"] = mode_defaults.mp_context
        # Sync and Ray modes have no worker-specific timeouts

        # Merge mode_options (pass through as-is to proxy)
        # For Ray: actor_options, num_cpus, num_gpus, resources, etc.
        # For Process: mp_context (fork, spawn, forkserver)
        proxy_kwargs.update(self.mode_options)

        return proxy_cls(**proxy_kwargs)

    def _create_pool(self, args: tuple, kwargs: dict) -> Any:
        """Create a worker pool.

        Typed/BaseModel workers are automatically wrapped with composition pattern
        for seamless Ray compatibility across all pool types.

        Args:
            args: Positional arguments for worker __init__
            kwargs: Keyword arguments for worker __init__

        Returns:
            WorkerProxyPool instance
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

        # Create retry configs (always pass it, even if None)
        retry_configs = self._create_retry_configs()

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

        # Add _from_proxy flag to init_kwargs to prevent auto_init recursion
        pool_init_kwargs = {**kwargs, "_from_proxy": True}

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
            "retry_configs": retry_configs,
            "init_args": args,
            "init_kwargs": pool_init_kwargs,
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

    - ✅ **morphic.Typed**: Full support (ALL modes including Ray via automatic composition wrapper)
    - ✅ **pydantic.BaseModel**: Full support (ALL modes including Ray via automatic composition wrapper)
    - ✅ **Ray mode**: Fully compatible with Typed/BaseModel workers (automatic composition wrapper)

    **Validation Decorators (Works with ALL modes including Ray):**

    - ✅ **@morphic.validate**: Works on methods and __init__ (all modes including Ray)
    - ✅ **@pydantic.validate_call**: Works on methods and __init__ (all modes including Ray)

    These decorators provide runtime validation without class inheritance.

    **Automatic Composition Wrapper:**

    When you use Worker + Typed or Worker + BaseModel, concurry automatically applies a
    composition wrapper that makes them work seamlessly with Ray mode. This happens
    transparently - no code changes needed! The wrapper:
    - Isolates infrastructure methods from user methods
    - Avoids Ray's serialization conflicts with Pydantic's __setattr__
    - Maintains full validation and type checking
    - Has zero performance overhead (optimized delegation)

    This means you can use:
    - Plain Python classes (all modes including Ray)
    - Worker + morphic.Typed for validation and hooks (all modes including Ray ✅)
    - Worker + pydantic.BaseModel for Pydantic validation (all modes including Ray ✅)
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

    Ray Mode Support with Typed/BaseModel (Automatic Composition Wrapper):
        ```python
        # ✅ WORKS: Typed/BaseModel workers fully supported in Ray mode!
        from morphic import Typed
        from pydantic import BaseModel, Field

        class TypedWorker(Worker, Typed):
            name: str
            value: int = 0

        # Works with Ray mode via automatic composition wrapper!
        worker = TypedWorker.options(mode="ray").init(name="test", value=10)
        result = worker.compute(5).result()  # 50
        worker.stop()

        # ✅ Pydantic BaseModel also works with Ray
        class PydanticWorker(Worker, BaseModel):
            name: str = Field(..., min_length=1)
            value: int = Field(default=0, ge=0)

            def compute(self, x: int) -> int:
                return self.value * x

        # Fully supported in Ray mode!
        worker = PydanticWorker.options(mode="ray").init(name="test", value=10)
        result = worker.compute(5).result()  # 50
        worker.stop()

        # ✅ Validation decorators also work with Ray
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

        **How Composition Wrapper Enables Ray Compatibility:**

        When you use Worker + Typed or Worker + BaseModel, concurry automatically applies
        a composition wrapper that solves the historical Ray serialization conflict.

        The wrapper:
        - Creates a plain Python class that holds the Typed/BaseModel instance internally
        - Only exposes user-defined methods (infrastructure methods excluded)
        - Delegates method calls to the wrapped instance
        - Maintains full validation, type checking, and field constraints
        - Has zero performance overhead (optimized delegation)

        This happens transparently - no code changes needed! Your Typed/BaseModel workers
        just work with Ray mode out of the box.

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

    def __init_subclass__(
        cls,
        *,
        # Core worker configuration (all optional)
        mode: Union[ExecutionMode, _NO_ARG_TYPE] = _NO_ARG,
        blocking: Union[bool, _NO_ARG_TYPE] = _NO_ARG,
        max_workers: Optional[Union[conint(ge=0), _NO_ARG_TYPE]] = _NO_ARG,
        load_balancing: Union[LoadBalancingAlgorithm, _NO_ARG_TYPE] = _NO_ARG,
        on_demand: Union[bool, _NO_ARG_TYPE] = _NO_ARG,
        max_queued_tasks: Optional[Union[conint(ge=0), _NO_ARG_TYPE]] = _NO_ARG,
        # Retry parameters
        num_retries: Union[conint(ge=0), dict[str, conint(ge=0)], _NO_ARG_TYPE] = _NO_ARG,
        retry_on: Union[Any, dict[str, Any], _NO_ARG_TYPE] = _NO_ARG,
        retry_algorithm: Union[RetryAlgorithm, dict[str, RetryAlgorithm], _NO_ARG_TYPE] = _NO_ARG,
        retry_wait: Union[confloat(ge=0), dict[str, confloat(ge=0)], _NO_ARG_TYPE] = _NO_ARG,
        retry_jitter: Union[confloat(ge=0, le=1), dict[str, confloat(ge=0, le=1)], _NO_ARG_TYPE] = _NO_ARG,
        retry_until: Union[Any, dict[str, Any], _NO_ARG_TYPE] = _NO_ARG,
        # Worker-level configuration
        unwrap_futures: Union[bool, _NO_ARG_TYPE] = _NO_ARG,
        limits: Optional[Any] = None,
        # NEW: Control instantiation behavior
        auto_init: Union[bool, _NO_ARG_TYPE] = _NO_ARG,
        # Mode-specific options
        **kwargs: Any,
    ) -> None:
        """Called when Worker is subclassed, allowing parameter configuration.

        This enables syntax like:
            class LLM(Worker, mode='thread', max_workers=4, auto_init=True):
                ...

        All parameters are optional and match Worker.options() signature.
        Configuration is stored in cls._worker_inheritance_config.

        Args:
            mode: Execution mode (sync, thread, process, asyncio, ray)
            blocking: Whether to return results directly
            max_workers: Number of workers for pool
            auto_init: Whether direct instantiation creates workers (default: True if any param set)
            num_retries: Maximum number of retry attempts after initial failure
            retry_on: Exception types or callables that trigger retries
            retry_algorithm: Backoff strategy for wait times
            retry_wait: Minimum wait time between retries in seconds
            retry_jitter: Jitter factor between 0 and 1
            retry_until: Validation functions for output
            unwrap_futures: If True, automatically unwrap BaseFuture arguments
            limits: Resource protection and rate limiting
            load_balancing: Load balancing algorithm
            on_demand: If True, create workers on-demand per request
            max_queued_tasks: Maximum number of in-flight tasks per worker
            **kwargs: Mode-specific options (num_cpus, mp_context, etc.)

        Examples:
            Inheritance Configuration:
                ```python
                class LLM(Worker, mode='thread', max_workers=4):
                    def __init__(self, model_name: str):
                        self.model_name = model_name

                # Direct instantiation creates worker pool
                llm = LLM(model_name='gpt-4')
                future = llm.call_llm("prompt")
                ```

            With Decorator (decorator takes precedence):
                ```python
                @worker(mode='process')  # Overrides thread mode
                class LLM(Worker, mode='thread', max_workers=4):
                    ...
                ```
        """
        super().__init_subclass__(**kwargs)

        # Collect all provided parameters (skip _NO_ARG)
        inheritance_config = {}

        # Helper to add param if not _NO_ARG
        def add_if_set(key, value):
            if value is not _NO_ARG:
                inheritance_config[key] = value

        add_if_set("mode", mode)
        add_if_set("blocking", blocking)
        add_if_set("max_workers", max_workers)
        add_if_set("load_balancing", load_balancing)
        add_if_set("on_demand", on_demand)
        add_if_set("max_queued_tasks", max_queued_tasks)
        add_if_set("num_retries", num_retries)
        add_if_set("retry_on", retry_on)
        add_if_set("retry_algorithm", retry_algorithm)
        add_if_set("retry_wait", retry_wait)
        add_if_set("retry_jitter", retry_jitter)
        add_if_set("retry_until", retry_until)
        add_if_set("unwrap_futures", unwrap_futures)
        if limits is not None:
            inheritance_config["limits"] = limits
        add_if_set("auto_init", auto_init)

        # Add mode-specific options
        if len(kwargs) > 0:
            inheritance_config["mode_options"] = kwargs

        # Store configuration on class ONLY if non-empty
        # Note: Parent classes may also have this, creating inheritance chain
        # Don't set empty dict - let it remain unset so getattr returns None
        if len(inheritance_config) > 0:
            cls._worker_inheritance_config = inheritance_config

            # If any config provided but auto_init not specified, default to True
            if "auto_init" not in inheritance_config:
                inheritance_config["auto_init"] = True

    @classmethod
    @validate
    def options(
        cls: Type[T],
        *,
        mode: Union[ExecutionMode, _NO_ARG_TYPE] = _NO_ARG,
        blocking: Union[bool, _NO_ARG_TYPE] = _NO_ARG,
        max_workers: Optional[Union[conint(ge=0), _NO_ARG_TYPE]] = _NO_ARG,
        load_balancing: Union[LoadBalancingAlgorithm, _NO_ARG_TYPE] = _NO_ARG,
        on_demand: Union[bool, _NO_ARG_TYPE] = _NO_ARG,
        max_queued_tasks: Optional[Union[conint(ge=0), _NO_ARG_TYPE]] = _NO_ARG,
        # Retry parameters
        num_retries: Union[conint(ge=0), dict[str, conint(ge=0)], _NO_ARG_TYPE] = _NO_ARG,
        retry_on: Union[Any, dict[str, Any], _NO_ARG_TYPE] = _NO_ARG,
        retry_algorithm: Union[RetryAlgorithm, dict[str, RetryAlgorithm], _NO_ARG_TYPE] = _NO_ARG,
        retry_wait: Union[confloat(ge=0), dict[str, confloat(ge=0)], _NO_ARG_TYPE] = _NO_ARG,
        retry_jitter: Union[confloat(ge=0, le=1), dict[str, confloat(ge=0, le=1)], _NO_ARG_TYPE] = _NO_ARG,
        retry_until: Union[Any, dict[str, Any], _NO_ARG_TYPE] = _NO_ARG,
        **kwargs: Any,
    ) -> WorkerBuilder:
        """Configure worker execution options.

        Returns a WorkerBuilder that can be used to create worker instances
        with .init(*args, **kwargs).

        This method merges configuration from multiple sources in priority order:
        1. Parameters passed to this method (highest priority)
        2. @worker decorator parameters
        3. class LLM(Worker, ...) inheritance parameters
        4. global_config defaults (lowest priority)

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
                Default value determined by global_config.<mode>.retry_on
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
                Default value determined by global_config.<mode>.retry_until
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

            Per-Method Retry Configuration:
                All retry parameters support per-method configuration using dictionaries.
                This allows different retry settings for different worker methods.

                ```python
                # Different retry settings per method
                worker = APIWorker.options(
                    mode="thread",
                    num_retries={
                        "*": 0,              # Default: no retries
                        "fetch_data": 3,     # Moderate retries for fetch
                        "critical_op": 10    # Aggressive retries for critical
                    },
                    retry_wait={
                        "*": 1.0,
                        "critical_op": 3.0   # Longer wait for critical
                    },
                    retry_algorithm={
                        "*": RetryAlgorithm.Linear,
                        "critical_op": RetryAlgorithm.Exponential
                    }
                ).init()

                # Dictionary format requires "*" key for default
                # Keys are method names, values are the parameter values
                # Methods not explicitly listed use the "*" default value

                # Mix single values and dicts
                worker = APIWorker.options(
                    mode="thread",
                    num_retries={"*": 0, "critical": 10},  # Per-method
                    retry_wait=2.0,                         # Single: all methods
                    retry_algorithm="exponential"           # Single: all methods
                ).init()

                # LLM worker with per-method validation
                worker = LLMWorker.options(
                    mode="thread",
                    num_retries={"*": 0, "generate_json": 10, "generate_code": 15},
                    retry_until={
                        "*": None,
                        "generate_json": lambda result, **ctx: isinstance(result, dict),
                        "generate_code": lambda result, **ctx: is_valid_syntax(result)
                    }
                ).init()

                # TaskWorker: use "submit" as method name
                worker = TaskWorker.options(
                    mode="process",
                    num_retries={"*": 5, "submit": 3},
                    retry_on={"*": [Exception], "submit": [ConnectionError]}
                ).init()
                ```
        """
        # Import here to avoid circular imports
        from ...config import global_config

        # 1. Start with inheritance config (lowest priority)
        merged_params = {}
        inheritance_config = getattr(cls, "_worker_inheritance_config", None)
        if inheritance_config is not None:
            merged_params.update(inheritance_config)

        # 2. Override with decorator config (medium priority)
        decorator_config = getattr(cls, "_worker_decorator_config", None)
        if decorator_config is not None:
            merged_params.update(decorator_config)

        # 3. Override with provided parameters (highest priority)
        # Only override if parameter was explicitly provided (not _NO_ARG)
        if mode is not _NO_ARG:
            merged_params["mode"] = mode
        if blocking is not _NO_ARG:
            merged_params["blocking"] = blocking
        if max_workers is not _NO_ARG:
            merged_params["max_workers"] = max_workers
        if load_balancing is not _NO_ARG:
            merged_params["load_balancing"] = load_balancing
        if on_demand is not _NO_ARG:
            merged_params["on_demand"] = on_demand
        if max_queued_tasks is not _NO_ARG:
            merged_params["max_queued_tasks"] = max_queued_tasks
        if num_retries is not _NO_ARG:
            merged_params["num_retries"] = num_retries
        if retry_on is not _NO_ARG:
            merged_params["retry_on"] = retry_on
        if retry_algorithm is not _NO_ARG:
            merged_params["retry_algorithm"] = retry_algorithm
        if retry_wait is not _NO_ARG:
            merged_params["retry_wait"] = retry_wait
        if retry_jitter is not _NO_ARG:
            merged_params["retry_jitter"] = retry_jitter
        if retry_until is not _NO_ARG:
            merged_params["retry_until"] = retry_until

        # Handle unwrap_futures and limits from kwargs
        if "unwrap_futures" in kwargs:
            merged_params["unwrap_futures"] = kwargs.pop("unwrap_futures")
        if "limits" in kwargs:
            merged_params["limits"] = kwargs.pop("limits")

        # Merge mode_options from configs and kwargs
        final_mode_options = {}
        if "mode_options" in merged_params:
            final_mode_options.update(merged_params["mode_options"])
        final_mode_options.update(kwargs)  # kwargs override config mode_options

        # 4. Extract mode and validate it's present
        if "mode" not in merged_params:
            raise ValueError(
                f"mode parameter is required. Provide it via:\n"
                f"  - .options(mode='thread')\n"
                f"  - @worker(mode='thread')\n"
                f"  - class {cls.__name__}(Worker, mode='thread')"
            )

        execution_mode = merged_params["mode"]

        # Get defaults for this mode from global config
        mode_defaults = global_config.get_defaults(execution_mode)

        # Apply defaults for all parameters if not specified in merged_params
        if "blocking" not in merged_params:
            blocking = mode_defaults.blocking
        else:
            blocking = merged_params["blocking"]

        if "max_workers" not in merged_params:
            max_workers = mode_defaults.max_workers
        else:
            max_workers = merged_params["max_workers"]

        if "on_demand" not in merged_params:
            on_demand = mode_defaults.on_demand
        else:
            on_demand = merged_params["on_demand"]

        if "max_queued_tasks" not in merged_params:
            max_queued_tasks = mode_defaults.max_queued_tasks
        else:
            max_queued_tasks = merged_params["max_queued_tasks"]

        if "load_balancing" not in merged_params:
            if on_demand:
                load_balancing = mode_defaults.load_balancing_on_demand
            else:
                load_balancing = mode_defaults.load_balancing
        else:
            load_balancing = merged_params["load_balancing"]

        if "num_retries" not in merged_params:
            num_retries = mode_defaults.num_retries
        else:
            num_retries = merged_params["num_retries"]

        if "retry_algorithm" not in merged_params:
            retry_algorithm = mode_defaults.retry_algorithm
        else:
            retry_algorithm = merged_params["retry_algorithm"]

        if "retry_wait" not in merged_params:
            retry_wait = mode_defaults.retry_wait
        else:
            retry_wait = merged_params["retry_wait"]

        if "retry_jitter" not in merged_params:
            retry_jitter = mode_defaults.retry_jitter
        else:
            retry_jitter = merged_params["retry_jitter"]

        if "retry_on" not in merged_params:
            retry_on = mode_defaults.retry_on
        else:
            retry_on = merged_params["retry_on"]

        if "retry_until" not in merged_params:
            retry_until = mode_defaults.retry_until
        else:
            retry_until = merged_params["retry_until"]

        # Extract unwrap_futures from merged_params (with default)
        unwrap_futures = merged_params.get("unwrap_futures", mode_defaults.unwrap_futures)

        # Extract limits from merged_params
        limits = merged_params.get("limits", None)

        # Everything else in kwargs is mode-specific options (passed through as-is)
        # For Ray: actor_options dict containing num_cpus, num_gpus, resources, etc.
        # For Process: mp_context (fork, spawn, forkserver)
        mode_options = final_mode_options  # Use merged mode_options

        # Get user-defined methods for validation (if needed)
        # Only compute if any retry param is a dict
        needs_normalization = (
            isinstance(num_retries, dict)
            or isinstance(retry_on, dict)
            or isinstance(retry_algorithm, dict)
            or isinstance(retry_wait, dict)
            or isinstance(retry_jitter, dict)
            or isinstance(retry_until, dict)
        )

        if needs_normalization:
            # Get method names for normalization
            method_names = _get_user_defined_methods(cls)

            # Add "submit" for TaskWorker
            from .task_worker import TaskWorker

            if cls is TaskWorker or (isinstance(cls, type) and issubclass(cls, TaskWorker)):
                if "submit" not in method_names:
                    method_names.append("submit")

            # Normalize each parameter
            num_retries = _normalize_retry_param(num_retries, "num_retries", method_names)
            retry_on = _normalize_retry_param(retry_on, "retry_on", method_names)
            retry_algorithm = _normalize_retry_param(retry_algorithm, "retry_algorithm", method_names)
            retry_wait = _normalize_retry_param(retry_wait, "retry_wait", method_names)
            retry_jitter = _normalize_retry_param(retry_jitter, "retry_jitter", method_names)
            retry_until = _normalize_retry_param(retry_until, "retry_until", method_names)

        return WorkerBuilder(
            worker_cls=cls,
            mode=execution_mode,
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
        """Override __new__ to support automatic worker initialization.

        Checks for configuration from decorator or inheritance and automatically
        creates worker instances when auto_init=True.

        Returns:
            WorkerProxy/WorkerProxyPool if auto_init enabled, else plain instance
        """

        # CRITICAL PERFORMANCE OPTIMIZATION: Check _from_proxy FIRST before any other logic
        # This flag indicates we're being called from WorkerProxy/WorkerBuilder
        # Fast-path this to avoid overhead on every worker instantiation
        if "_from_proxy" in kwargs:
            kwargs.pop("_from_proxy")
            # Normal instantiation for proxy creation - bypass all auto_init logic
            instance = super().__new__(cls)
            return instance

        # 1. Check if Worker base class is being instantiated directly
        if cls is Worker:
            raise TypeError("Worker cannot be instantiated directly. Subclass it or use @worker decorator.")

        # 2. Merge configurations to determine auto_init
        # Priority: decorator > inheritance
        merged_config = {}

        # Start with inheritance config (lowest priority)
        inheritance_config = getattr(cls, "_worker_inheritance_config", None)
        if inheritance_config is not None:
            merged_config.update(inheritance_config)

        # Override with decorator config (higher priority)
        decorator_config = getattr(cls, "_worker_decorator_config", None)
        if decorator_config is not None:
            merged_config.update(decorator_config)

        # 3. Check auto_init flag
        should_auto_init = merged_config.get("auto_init", False)

        # 4. If auto_init enabled, create worker via .options().init()
        if should_auto_init:
            # Build options from merged config (excluding auto_init)
            options_params = {k: v for k, v in merged_config.items() if k != "auto_init"}

            # Create worker via .options().init()
            # Note: .options() will further merge with global_config
            builder = cls.options(**options_params)
            return builder.init(*args, **kwargs)

        # 5. Normal instantiation (auto_init=False or no config)
        instance = super().__new__(cls)
        return instance

    def __init__(self, *args, **kwargs):
        """Initialize the worker. Subclasses can override this freely.

        This method supports cooperative multiple inheritance, allowing Worker
        to be combined with model classes like morphic.Typed or pydantic.BaseModel.

        Removes internal _from_proxy flag before calling parent __init__.

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
        # Remove _from_proxy flag if present (internal use only)
        kwargs.pop("_from_proxy", None)

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
    retry_configs: Optional[
        dict[str, Optional[RetryConfig]]
    ]  # Per-method retry configs (None = no retry for that method)
    max_queued_tasks: Optional[conint(ge=0)]

    # ========================================================================
    # PRIVATE ATTRIBUTES - Defaults via PrivateAttr() are okay
    # ========================================================================
    _stopped: bool = PrivateAttr(default=False)
    _method_cache: dict[str, Any] = PrivateAttr(default_factory=dict)
    _submission_semaphore: Optional[Any] = PrivateAttr(default=None)
    _pending_submissions: Any = PrivateAttr(default=None)

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

        # Initialize pending submissions queue (holds futures waiting for semaphore)
        self._pending_submissions = queue.Queue()

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

            # Check if stopped first
            if self._stopped:
                raise RuntimeError("Worker is stopped")

            # Try to forward immediately (non-blocking semaphore check)
            if self._submission_semaphore is None:
                # No rate limiting - forward immediately
                future = self._execute_method(name, *args, **kwargs)
            elif self._submission_semaphore.acquire(blocking=False):
                # Got semaphore - forward to backend now
                future = self._execute_method(name, *args, **kwargs)
                # Add callback to release semaphore and forward next pending
                future = self._wrap_future_with_submission_tracking(future)
            else:
                # Semaphore unavailable - create shell future and queue for later
                from concurrent.futures import Future as PyFuture

                from ..future import ConcurrentFuture

                py_future = PyFuture()
                future = ConcurrentFuture(future=py_future)
                self._pending_submissions.put((future, py_future, name, args, kwargs))

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

    def _wrap_future_with_submission_tracking(self, future: BaseFuture) -> BaseFuture:
        """Wrap future to release semaphore and forward next pending submission on completion.

        This callback-driven approach forwards queued submissions without needing a thread.
        When a task completes, it releases the semaphore and immediately forwards the next
        pending submission (if any) to the backend.

        Args:
            future: The backend future to track

        Returns:
            The same future (modified with callback)
        """
        if self._submission_semaphore is None:
            return future

        def on_submission_complete(f):
            try:
                # Release semaphore first
                self._submission_semaphore.release()
            except Exception:
                pass

            # Don't forward new tasks if worker is stopped
            if self._stopped:
                return

            # Try to forward next pending submission (if any)
            try:
                shell_future, py_future, method_name, args, kwargs = self._pending_submissions.get_nowait()

                # Try to acquire semaphore (should succeed since we just released)
                if self._submission_semaphore.acquire(blocking=False):
                    # Forward to backend
                    backend_future = self._execute_method(method_name, *args, **kwargs)

                    # Chain backend future to shell future (propagate result/exception)
                    def chain_result(bf):
                        try:
                            result = bf.result()
                            py_future.set_result(result)
                        except Exception as e:
                            py_future.set_exception(e)

                    backend_future.add_done_callback(chain_result)
                    # Recursively track this forwarded submission
                    backend_future.add_done_callback(on_submission_complete)
                else:
                    # Race condition: another thread got semaphore
                    # Put back in queue for next opportunity
                    self._pending_submissions.put((shell_future, py_future, method_name, args, kwargs))
            except queue.Empty:
                # No pending submissions - done
                pass

        future.add_done_callback(on_submission_complete)
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

        # Cancel all pending futures in the submission queue
        # These futures were created but never forwarded to the backend
        if self._pending_submissions is not None:
            while True:
                try:
                    shell_future, py_future, method_name, args, kwargs = (
                        self._pending_submissions.get_nowait()
                    )
                    # Cancel the future - it will never be executed
                    try:
                        py_future.cancel()
                    except Exception:
                        pass  # Ignore errors during cleanup
                except queue.Empty:
                    break  # No more pending submissions

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


@validate
def worker(
    cls: Optional[Type[T]] = None,
    *,
    # Core worker configuration (all match __init_subclass__)
    mode: Union[ExecutionMode, _NO_ARG_TYPE] = _NO_ARG,
    blocking: Union[bool, _NO_ARG_TYPE] = _NO_ARG,
    max_workers: Optional[Union[conint(ge=0), _NO_ARG_TYPE]] = _NO_ARG,
    load_balancing: Union[LoadBalancingAlgorithm, _NO_ARG_TYPE] = _NO_ARG,
    on_demand: Union[bool, _NO_ARG_TYPE] = _NO_ARG,
    max_queued_tasks: Optional[Union[conint(ge=0), _NO_ARG_TYPE]] = _NO_ARG,
    # Retry parameters
    num_retries: Union[conint(ge=0), dict[str, conint(ge=0)], _NO_ARG_TYPE] = _NO_ARG,
    retry_on: Union[Any, dict[str, Any], _NO_ARG_TYPE] = _NO_ARG,
    retry_algorithm: Union[RetryAlgorithm, dict[str, RetryAlgorithm], _NO_ARG_TYPE] = _NO_ARG,
    retry_wait: Union[confloat(ge=0), dict[str, confloat(ge=0)], _NO_ARG_TYPE] = _NO_ARG,
    retry_jitter: Union[confloat(ge=0, le=1), dict[str, confloat(ge=0, le=1)], _NO_ARG_TYPE] = _NO_ARG,
    retry_until: Union[Any, dict[str, Any], _NO_ARG_TYPE] = _NO_ARG,
    # Worker-level configuration
    unwrap_futures: Union[bool, _NO_ARG_TYPE] = _NO_ARG,
    limits: Optional[Any] = None,
    # NEW: Control instantiation behavior
    auto_init: Union[bool, _NO_ARG_TYPE] = _NO_ARG,
    # Mode-specific options
    **kwargs: Any,
) -> Union[Callable[[Type[T]], Type[T]], Type[T]]:
    """Decorator to create a Worker class with pre-configured options.

    This decorator accepts all Worker.options() parameters and stores them
    for automatic application when the class is instantiated.

    Can be used with or without parameters:
    - `@worker` (no params)
    - `@worker(mode='thread', max_workers=4, auto_init=True)`

    Args:
        cls: The class to decorate (when used without parentheses)
        mode: Execution mode (sync, thread, process, asyncio, ray)
        blocking: Whether to return results directly
        max_workers: Number of workers for pool
        auto_init: Whether direct instantiation creates workers (default: True if any param)
        num_retries: Maximum number of retry attempts after initial failure
        retry_on: Exception types or callables that trigger retries
        retry_algorithm: Backoff strategy for wait times
        retry_wait: Minimum wait time between retries in seconds
        retry_jitter: Jitter factor between 0 and 1
        retry_until: Validation functions for output
        unwrap_futures: If True, automatically unwrap BaseFuture arguments
        limits: Resource protection and rate limiting
        load_balancing: Load balancing algorithm
        on_demand: If True, create workers on-demand per request
        max_queued_tasks: Maximum number of in-flight tasks per worker
        **kwargs: Mode-specific options

    Returns:
        Decorated class or decorator function

    Examples:
        Decorator Only:
            ```python
            @worker(mode='thread', max_workers=4, auto_init=True)
            class LLM:
                def __init__(self, model_name: str):
                    self.model_name = model_name

            # Direct instantiation creates worker
            llm = LLM(model_name='gpt-4')
            future = llm.call_llm("What is 1+1?")
            ```

        Without Parameters (Backward Compatible):
            ```python
            @worker
            class LLM(Worker):
                ...

            # Must use .options().init() (no auto_init)
            llm = LLM.options(mode='thread').init(...)
            ```

        Override at Instantiation:
            ```python
            @worker(mode='thread', max_workers=4)
            class LLM:
                ...

            # Override mode, keep max_workers
            llm = LLM.options(mode='process').init(...)
            ```

    Warnings:
        Mixing decorator and inheritance parameters is discouraged:
            ```python
            @worker(mode='process')  # Decorator
            class LLM(Worker, mode='thread'):  # Inheritance
                ...
            # UserWarning: Both decorator and inheritance config found
            ```
    """

    def decorator(target_cls: Type[T]) -> Type[T]:
        # 1. Make class inherit from Worker if needed
        if not issubclass(target_cls, Worker):
            target_cls = type(target_cls.__name__, (Worker, target_cls), dict(target_cls.__dict__))

        # 2. Collect decorator configuration
        decorator_config = {}

        def add_if_set(key, value):
            if value is not _NO_ARG:
                decorator_config[key] = value

        add_if_set("mode", mode)
        add_if_set("blocking", blocking)
        add_if_set("max_workers", max_workers)
        add_if_set("load_balancing", load_balancing)
        add_if_set("on_demand", on_demand)
        add_if_set("max_queued_tasks", max_queued_tasks)
        add_if_set("num_retries", num_retries)
        add_if_set("retry_on", retry_on)
        add_if_set("retry_algorithm", retry_algorithm)
        add_if_set("retry_wait", retry_wait)
        add_if_set("retry_jitter", retry_jitter)
        add_if_set("retry_until", retry_until)
        add_if_set("unwrap_futures", unwrap_futures)
        if limits is not None:
            decorator_config["limits"] = limits
        add_if_set("auto_init", auto_init)

        if len(kwargs) > 0:
            decorator_config["mode_options"] = kwargs

        # If any config provided but auto_init not specified, default to True
        if len(decorator_config) > 0 and "auto_init" not in decorator_config:
            decorator_config["auto_init"] = True

        # 3. Check for mixed decorator + inheritance (anti-pattern warning)
        # Only warn if BOTH decorator AND inheritance have actual configuration
        inheritance_config = getattr(target_cls, "_worker_inheritance_config", None)
        has_inheritance_config = inheritance_config is not None and len(inheritance_config) > 0
        has_decorator_config = len(decorator_config) > 0

        if has_inheritance_config and has_decorator_config:
            warnings.warn(
                f"Class {target_cls.__name__} uses both @worker decorator "
                f"and inheritance parameters (Worker subclass with kwargs). "
                f"This is an anti-pattern. Decorator parameters take precedence. "
                f"Recommend using one approach only.",
                UserWarning,
                stacklevel=2,
            )

        # 4. Store decorator configuration
        if len(decorator_config) > 0:
            target_cls._worker_decorator_config = decorator_config

        return target_cls

    # Support both @worker and @worker(...) syntax
    if cls is None:
        # Called with parameters: @worker(...)
        return decorator
    else:
        # Called without parameters: @worker
        return decorator(cls)
