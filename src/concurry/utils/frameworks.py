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


# Check if ipywidgets is available
try:
    import ipywidgets

    _IS_IPYWIDGETS_INSTALLED = True
except ImportError:
    _IS_IPYWIDGETS_INSTALLED = False
    ipywidgets = None
