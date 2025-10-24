from morphic import AutoEnum, auto


class RayContext(AutoEnum):
    Actor = auto()
    Task = auto()
    Driver = auto()
    Unknown = auto()


try:
    import ray

    _IS_RAY_INSTALLED = True

    def ray_context() -> RayContext:
        from ray._private.worker import (
            LOCAL_MODE,
            SCRIPT_MODE,
            WORKER_MODE,
            global_worker,
        )

        mode = global_worker.mode
        if mode == WORKER_MODE:
            # Inside a Ray worker (task or actor)
            actor_id = global_worker.actor_id
            if actor_id is not None and not actor_id.is_nil():
                return RayContext.Actor
            else:
                return RayContext.Task
        elif mode in (SCRIPT_MODE, LOCAL_MODE):
            return RayContext.Driver
        else:
            return RayContext.Unknown
except ImportError:
    _IS_RAY_INSTALLED = False
    ray = None

    def ray_context() -> RayContext:
        return RayContext.Unknown


# Check if ipywidgets is available and properly configured
try:
    import os
    import sys

    import ipywidgets
    from IPython import get_ipython

    # Allow users to force-disable ipywidgets support via environment variable
    # This is useful in environments with threading issues (e.g., SageMaker)
    if os.environ.get("CONCURRY_DISABLE_IPYWIDGETS", "").lower() in ("1", "true", "yes"):
        _IS_IPYWIDGETS_INSTALLED = False
        ipywidgets = None
    else:
        # Check if we're in a proper IPython/Jupyter environment
        ipython_instance = get_ipython()
        if ipython_instance is not None:
            # Additional check: verify the kernel has proper context variable support
            # This is needed to avoid threading issues with ipykernel's shell_parent context
            try:
                # Try to access the kernel - if this fails, ipywidgets won't work properly
                kernel = ipython_instance.kernel
                if kernel is not None and hasattr(kernel, "_shell_parent"):
                    # Kernel exists and has the context variable
                    # But we still need to be cautious - disable in SageMaker and similar environments
                    # where threading issues are common with ipywidgets + tqdm + background threads
                    if "sagemaker" in sys.modules or "sagemaker_containers" in sys.modules:
                        # In SageMaker, ipywidgets has threading issues with tqdm
                        _IS_IPYWIDGETS_INSTALLED = False
                    else:
                        _IS_IPYWIDGETS_INSTALLED = True
                else:
                    _IS_IPYWIDGETS_INSTALLED = False
            except (AttributeError, Exception):
                # Kernel access failed, ipywidgets won't work properly
                _IS_IPYWIDGETS_INSTALLED = False
        else:
            # ipywidgets is installed but we're not in a Jupyter environment
            _IS_IPYWIDGETS_INSTALLED = False
except (ImportError, Exception):
    _IS_IPYWIDGETS_INSTALLED = False
    ipywidgets = None
