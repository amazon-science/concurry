"""Ray-based worker implementation for concurry."""

from typing import Any, Dict, Optional, Union

from pydantic import PrivateAttr

from ..future import RayFuture, SyncFuture
from .base_worker import WorkerProxy


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

    **Example:**

        ```python
        # Use defaults (1 CPU, 0 GPUs)
        w = MyWorker.options(mode="ray").create()

        # Override resources
        w = MyWorker.options(mode="ray", num_cpus=2, num_gpus=1).create()

        # Specify custom resources
        w = MyWorker.options(
            mode="ray",
            resources={"special_hardware": 1}
        ).create()
        ```
    """

    num_cpus: float = 1  # Default to 1 CPU
    num_gpus: float = 0  # Default to 0 GPUs
    resources: Optional[Dict[str, Union[int, float]]] = None

    # Private attributes
    _ray_actor: Any = PrivateAttr()

    def post_initialize(self) -> None:
        """Initialize private attributes after Typed validation."""
        super().post_initialize()

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

        # Create Ray actor options using public fields
        actor_options = {}
        if self.num_cpus is not None:
            actor_options["num_cpus"] = self.num_cpus
        if self.num_gpus is not None:
            actor_options["num_gpus"] = self.num_gpus
        if self.resources is not None:
            actor_options["resources"] = self.resources

        # Add any additional options from kwargs
        for key, value in self._options.items():
            actor_options[key] = value

        # Create the Ray actor
        # Note: Ray 2.50+ doesn't accept ray.remote(**{}) with an empty dict
        # so we only pass options if the dict is not empty
        if actor_options:
            ray_actor_cls = ray.remote(**actor_options)(self.worker_cls)
        else:
            ray_actor_cls = ray.remote(self.worker_cls)

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
        # Don't catch exceptions - let them propagate immediately for fast failure
        # This ensures errors like AttributeError (method not found) fail immediately
        # rather than being wrapped in a future
        ray_method = getattr(self._ray_actor, method_name)
        object_ref = ray_method.remote(*args, **kwargs)
        return RayFuture(object_ref=object_ref)

    def _execute_task(self, fn, *args: Any, **kwargs: Any):
        """Execute an arbitrary function on the Ray actor.

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
        # Don't catch exceptions - let them propagate immediately for fast failure
        import ray

        # Create a remote function and execute it on the actor's resources
        # We'll use ray.remote to make the function remote, then call it
        remote_fn = ray.remote(fn)

        # Execute with the same resources as the actor (use public fields)
        options = {}
        if self.num_cpus is not None:
            options["num_cpus"] = self.num_cpus
        if self.num_gpus is not None:
            options["num_gpus"] = self.num_gpus
        if self.resources is not None:
            options["resources"] = self.resources

        if options:
            remote_fn = remote_fn.options(**options)

        object_ref = remote_fn.remote(*args, **kwargs)
        return RayFuture(object_ref=object_ref)

    def stop(self, timeout: float = 30) -> None:
        """Stop the Ray actor.

        Args:
            timeout: Maximum time to wait for actor to stop (currently ignored for Ray)
        """
        super().stop(timeout)

        try:
            import ray

            ray.kill(self._ray_actor)
        except Exception:
            pass
