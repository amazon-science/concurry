"""Ray-based worker implementation for concurry."""

import asyncio
import inspect
import threading
from typing import Any, Dict, Optional

from morphic.structs import map_collection
from pydantic import PrivateAttr

from ..future import RayFuture
from .base_worker import WorkerProxy, _create_worker_wrapper

# Note: Ray has native support for async methods in actors.
# When you define an async method in a Ray actor and call it with .remote(),
# Ray automatically handles the async execution.
#
# TODO: For optimal async performance with Ray, we could:
# 1. Detect async methods and use Ray's async actor APIs
# 2. Leverage Ray's asyncio integration for concurrent task execution
# 3. Use Ray's async/await support for better concurrency within actors
#
# For now, Ray's default behavior correctly executes async functions,
# though it may not provide the same level of concurrent execution as
# a dedicated event loop (like AsyncioWorkerProxy).


def _unwrap_future_for_ray(obj: Any) -> Any:
    """Unwrap futures for Ray, preserving ObjectRefs for zero-copy.

    RayFuture → ObjectRef (zero-copy for top-level args, resolved for nested)
    Other BaseFuture → .result() (materialize)
    Non-future → unchanged

    Note: Ray automatically unwraps top-level ObjectRef arguments but NOT
    ObjectRefs nested in collections. This function handles both cases.

    Args:
        obj: Object that might be a BaseFuture

    Returns:
        ObjectRef if obj is RayFuture (zero-copy),
        materialized value if obj is other BaseFuture,
        otherwise obj unchanged
    """
    from ..future import BaseFuture

    if isinstance(obj, RayFuture):
        # Zero-copy: pass ObjectRef directly
        return obj._object_ref
    elif isinstance(obj, BaseFuture):
        # Cross-worker: materialize value
        return obj.result()
    return obj


def _resolve_nested_objectrefs(obj: Any) -> Any:
    """Resolve ObjectRefs that are nested in collections.

    Ray automatically unwraps top-level ObjectRef arguments, but ObjectRefs
    nested in collections (lists, dicts, etc.) must be explicitly resolved.

    Args:
        obj: Object that might be an ObjectRef

    Returns:
        Resolved value if obj is an ObjectRef, otherwise obj unchanged
    """
    import ray

    if isinstance(obj, ray.ObjectRef):
        return ray.get(obj)
    return obj


def _unwrap_futures_for_ray(
    args: tuple,
    kwargs: dict,
    unwrap_futures: bool,
) -> tuple:
    """Ray-specific unwrapping with zero-copy optimization.

    Recursively traverses nested collections and:
    - Extracts ObjectRef from RayFuture (zero-copy)
    - Materializes other BaseFuture types via .result()
    - Resolves nested ObjectRefs (Ray limitation workaround)
    - Leaves non-future values unchanged

    Args:
        args: Positional arguments
        kwargs: Keyword arguments
        unwrap_futures: Whether to perform unwrapping

    Returns:
        Tuple of (unwrapped_args, unwrapped_kwargs)
    """
    if not unwrap_futures:
        return args, kwargs

    # Step 1: Unwrap BaseFuture instances to ObjectRefs or values
    unwrapped_args = tuple(map_collection(arg, _unwrap_future_for_ray, recurse=True) for arg in args)

    unwrapped_kwargs = {
        key: map_collection(value, _unwrap_future_for_ray, recurse=True) for key, value in kwargs.items()
    }

    # Step 2: Resolve any ObjectRefs that are nested in collections
    # Ray only auto-unwraps top-level ObjectRef args, not nested ones
    resolved_args = tuple(
        map_collection(arg, _resolve_nested_objectrefs, recurse=True) for arg in unwrapped_args
    )

    resolved_kwargs = {
        key: map_collection(value, _resolve_nested_objectrefs, recurse=True)
        for key, value in unwrapped_kwargs.items()
    }

    return resolved_args, resolved_kwargs


class RayWorkerProxy(WorkerProxy):
    """Worker proxy for Ray-based execution.

    This proxy uses Ray actors to execute methods in a distributed manner.

    **Default Resource Allocation:**

    - `num_cpus = 1`: Each Ray actor is allocated 1 CPU by default
    - `num_gpus = 0`: No GPU allocation by default
    - `resources = None`: No custom resources by default

    These defaults ensure Ray actors are created without requiring explicit
    resource specifications, while still allowing users to override as needed.

    **Exception Handling:**

    - Setup errors (e.g., `AttributeError` for non-existent methods) fail immediately
    - Execution errors are wrapped by Ray in `RayTaskError` (Ray's standard behavior)
    - Original exception information is preserved in the Ray error message

    **Async Function Support:**

    Ray has native support for async methods in actors - they work automatically.
    For TaskWorker with async functions, they are wrapped to execute correctly
    but won't provide the same concurrency benefits as `AsyncioWorkerProxy`.

    **Future Unwrapping with Zero-Copy Optimization:**

    When passing RayFuture instances from one Ray worker to another, the underlying
    ObjectRef is passed directly (zero-copy), avoiding data serialization. For futures
    from other worker types, values are materialized before passing. This optimization
    is automatic when `unwrap_futures=True` (default).

    **Example:**

        ```python
        import asyncio

        class MyWorker(Worker):
            async def async_method(self, x: int) -> int:
                await asyncio.sleep(0.01)
                return x * 2

        # Use defaults (1 CPU, 0 GPUs)
        w = MyWorker.options(mode="ray").init()
        result = w.async_method(5).result()  # Works with Ray's native async support

        # Override resources
        w = MyWorker.options(mode="ray", actor_options={"num_cpus": 2, "num_gpus": 1}).init()

        # Specify custom resources
        w = MyWorker.options(
            mode="ray",
            actor_options={"resources": {"special_hardware": 1}}
        ).init()

        w.stop()
        ```
    """

    actor_options: Optional[Dict[str, Any]] = None  # Ray actor resource options

    # Private attributes
    _ray_actor: Any = PrivateAttr()
    _futures: Dict[str, Any] = PrivateAttr()  # Maps future.uuid -> RayFuture
    _futures_lock: Any = PrivateAttr()

    def post_initialize(self) -> None:
        """Initialize private attributes after Typed validation."""
        super().post_initialize()

        # Initialize futures tracking
        self._futures = {}  # future.uuid -> RayFuture
        self._futures_lock = threading.Lock()

        # Create Ray actor (uses public fields directly)
        self._ray_actor = self._create_ray_actor()

    def _create_ray_actor(self):
        """Create a Ray actor from the worker class.

        Returns:
            Ray actor handle
        """
        try:
            import ray
        except ImportError:
            raise ImportError("Ray is required for RayWorker. Install with: pip install ray")

        # Check if Ray is initialized
        if not ray.is_initialized():
            raise RuntimeError("Ray is not initialized. Call ray.init() before creating Ray workers.")

        # Create worker wrapper with limits and retry logic if needed
        # (limits and retry_config already processed by WorkerBuilder)
        # Use for_ray=True to pre-wrap methods (Ray bypasses __getattribute__)
        worker_cls_to_use = _create_worker_wrapper(
            self.worker_cls, self.limits, self.retry_config, for_ray=True
        )

        # Create the Ray actor. Use actor_options if provided, otherwise use defaults.
        # Note: Ray 2.50+ doesn't accept ray.remote(**{}) with an empty dict
        # so we only pass actor_options if the dict is not empty
        if isinstance(self.actor_options, dict) and len(self.actor_options) > 0:
            ray_actor_cls = ray.remote(**self.actor_options)(worker_cls_to_use)
        else:
            ray_actor_cls = ray.remote(worker_cls_to_use)

        return ray_actor_cls.remote(*self.init_args, **self.init_kwargs)

    def _execute_method(self, method_name: str, *args: Any, **kwargs: Any):
        """Execute a method on the Ray actor.

        Args:
            method_name: Name of the method to invoke
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            RayFuture for the method execution

        Raises:
            AttributeError: If the method doesn't exist on the actor
            Exception: Any immediate errors during method invocation setup
        """
        # Unwrap futures with Ray zero-copy optimization
        args, kwargs = _unwrap_futures_for_ray(args, kwargs, self.unwrap_futures)

        # Don't catch exceptions - let them propagate immediately for fast failure
        # This ensures errors like AttributeError (method not found) fail immediately
        # rather than being wrapped in a future
        ray_method = getattr(self._ray_actor, method_name)
        object_ref = ray_method.remote(*args, **kwargs)
        future = RayFuture(object_ref=object_ref)

        # Store future for cancellation on stop()
        with self._futures_lock:
            self._futures[future.uuid] = future

        return future

    def _execute_task(self, fn, *args: Any, **kwargs: Any):
        """Execute an arbitrary function on the Ray actor with optional retry logic.

        This method applies retry logic for TaskWorker.submit() and TaskWorker.map().
        The retry logic is applied here (not in submit()) to avoid double-wrapping.

        Args:
            fn: Callable function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            RayFuture for the task execution

        Raises:
            ImportError: If Ray is not available
            Exception: Any immediate errors during task submission setup
        """
        # Unwrap futures with Ray zero-copy optimization
        args, kwargs = _unwrap_futures_for_ray(args, kwargs, self.unwrap_futures)

        # Don't catch exceptions - let them propagate immediately for fast failure
        import ray

        # Apply retry logic if configured (for TaskWorker functions)
        if self.retry_config is not None and self.retry_config.num_retries > 0:
            # Wrap the function with retry logic before making it remote
            # Important: Serialize retry_config to avoid Pydantic pickling issues with Ray
            import cloudpickle

            original_fn = fn
            retry_config_bytes = cloudpickle.dumps(self.retry_config)

            # Create a wrapper that deserializes config and uses execute_with_retry_auto
            def ray_retry_wrapper(*inner_args, **inner_kwargs):
                import cloudpickle

                from ..retry import execute_with_retry_auto

                # Deserialize retry config inside the Ray task
                r_config = cloudpickle.loads(retry_config_bytes)
                context = {
                    "method_name": original_fn.__name__
                    if hasattr(original_fn, "__name__")
                    else "anonymous_function",
                    "worker_class_name": "TaskWorker",
                }
                # execute_with_retry_auto handles both sync and async functions
                return execute_with_retry_auto(original_fn, inner_args, inner_kwargs, r_config, context)

            fn = ray_retry_wrapper
        else:
            # No retry configured - handle async functions normally
            if inspect.iscoroutinefunction(fn):
                # Capture the async function in a closure
                async_fn = fn

                # Wrap the async function in a sync wrapper
                def sync_wrapper(*args, **kwargs):
                    return asyncio.run(async_fn(*args, **kwargs))

                # Use the wrapper instead
                fn = sync_wrapper

        # Create a remote function and execute it on the actor's resources
        # We'll use ray.remote to make the function remote, then call it
        remote_fn = ray.remote(fn)

        # Execute with the same resources as the actor
        if isinstance(self.actor_options, dict) and len(self.actor_options) > 0:
            remote_fn = remote_fn.options(**self.actor_options)

        object_ref = remote_fn.remote(*args, **kwargs)
        future = RayFuture(object_ref=object_ref)

        # Store future for cancellation on stop()
        with self._futures_lock:
            self._futures[future.uuid] = future

        return future

    def stop(self, timeout: float = 30) -> None:
        """Stop the Ray actor.

        Args:
            timeout: Maximum time to wait for actor to stop (currently ignored for Ray)
        """
        super().stop(timeout)

        # Cancel all pending futures
        with self._futures_lock:
            for future in self._futures.values():
                future.cancel()
            self._futures.clear()

        try:
            import ray

            ray.kill(self._ray_actor)
        except Exception:
            pass
