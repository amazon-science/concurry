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
    import ipywidgets
    from IPython import get_ipython

    # Check if we're in a proper IPython/Jupyter environment
    ipython_instance = get_ipython()
    if ipython_instance is not None:
        # Additional check: see if the kernel is available
        # In properly configured Jupyter environments, this should work
        _IS_IPYWIDGETS_INSTALLED = True
    else:
        # ipywidgets is installed but we're not in a Jupyter environment
        _IS_IPYWIDGETS_INSTALLED = False
except (ImportError, Exception):
    _IS_IPYWIDGETS_INSTALLED = False
    ipywidgets = None
