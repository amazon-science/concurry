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
            # Check if we're in a proper kernel environment (Jupyter Notebook/Lab)
            # We use multiple detection methods for compatibility across ipykernel versions
            try:
                # Get shell class name for detection
                shell_class_name = ipython_instance.__class__.__name__

                # Check if kernel exists
                kernel = getattr(ipython_instance, "kernel", None)

                # Check for SageMaker first - disable ipywidgets there due to threading issues
                if "sagemaker" in sys.modules or "sagemaker_containers" in sys.modules:
                    _IS_IPYWIDGETS_INSTALLED = False
                # Method 1: Modern detection (ipykernel 6.x+)
                # Check for ZMQInteractiveShell which is used in Jupyter Notebook/Lab
                elif shell_class_name == "ZMQInteractiveShell":
                    _IS_IPYWIDGETS_INSTALLED = True
                # Method 2: Legacy detection (ipykernel < 6.0)
                # Check for _shell_parent attribute on kernel (older ipykernel versions)
                elif kernel is not None and hasattr(kernel, "_shell_parent"):
                    _IS_IPYWIDGETS_INSTALLED = True
                # Method 3: Fallback - check if we have any kernel at all
                # This handles edge cases where shell class name differs but kernel exists
                elif kernel is not None:
                    # Has a kernel, likely a Jupyter environment
                    _IS_IPYWIDGETS_INSTALLED = True
                # Method 4: Check for TerminalInteractiveShell (IPython terminal)
                # ipywidgets doesn't work well in plain terminal, but tqdm.auto handles it
                elif shell_class_name == "TerminalInteractiveShell":
                    _IS_IPYWIDGETS_INSTALLED = False
                else:
                    # Unknown shell type without kernel - assume not supported
                    _IS_IPYWIDGETS_INSTALLED = False
            except (AttributeError, Exception):
                # Detection failed, try a simple heuristic:
                # If we have an IPython instance and it has 'kernel' in its class hierarchy,
                # assume ipywidgets works
                try:
                    has_kernel = hasattr(ipython_instance, "kernel")
                    _IS_IPYWIDGETS_INSTALLED = has_kernel
                except Exception:
                    _IS_IPYWIDGETS_INSTALLED = False
        else:
            # ipywidgets is installed but we're not in an IPython/Jupyter environment
            _IS_IPYWIDGETS_INSTALLED = False
except (ImportError, Exception):
    _IS_IPYWIDGETS_INSTALLED = False
    ipywidgets = None
