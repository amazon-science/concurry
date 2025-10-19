"""Global configuration for Concurry library.

This module provides a global configuration system for setting default values
across the library. Users can customize defaults for each execution mode.
"""

from contextlib import contextmanager
from typing import Optional

from morphic import MutableTyped

from .core.constants import ExecutionMode, LoadBalancingAlgorithm


class ExecutionModeDefaults(MutableTyped):
    """Default configuration for a specific execution mode.

    Attributes:
        max_workers: Default number of workers for pools (None = single worker)
        max_queued_tasks: Default submission queue length (None = bypass queue)
        load_balancing: Default load balancing algorithm for pools
        load_balancing_on_demand: Default load balancing for on-demand pools
    """

    max_workers: Optional[int] = None
    max_queued_tasks: Optional[int] = None
    load_balancing: LoadBalancingAlgorithm = LoadBalancingAlgorithm.RoundRobin
    load_balancing_on_demand: LoadBalancingAlgorithm = LoadBalancingAlgorithm.Random

    def clone(self) -> "ExecutionModeDefaults":
        """Clone the execution mode defaults."""
        return ExecutionModeDefaults(**self.model_dump())


_SYNC_DEFAULTS = ExecutionModeDefaults(max_workers=1, max_queued_tasks=None)
_ASYNCIO_DEFAULTS = ExecutionModeDefaults(max_workers=1, max_queued_tasks=None)
_THREADS_DEFAULTS = ExecutionModeDefaults(max_workers=30, max_queued_tasks=1000)
_PROCESSES_DEFAULTS = ExecutionModeDefaults(max_workers=4, max_queued_tasks=100)
_RAY_DEFAULTS = ExecutionModeDefaults(max_workers=0, max_queued_tasks=3)


class ConcurryConfig(MutableTyped):
    """Global configuration for Concurry library.

    This configuration is mutable and can be updated at runtime. All changes
    are validated automatically via Typed.

    Example:
        ```python
        from concurry import config

        # Customize thread mode defaults
        config.thread.max_workers = 32
        config.thread.max_queued_tasks = 200

        # Customize Ray mode defaults
        config.ray.max_queued_tasks = 5

        # Reset to library defaults
        config.reset_to_defaults()
        ```
    """

    # Per-mode defaults
    sync: ExecutionModeDefaults = _SYNC_DEFAULTS.clone()
    asyncio: ExecutionModeDefaults = _ASYNCIO_DEFAULTS.clone()
    thread: ExecutionModeDefaults = _THREADS_DEFAULTS.clone()
    process: ExecutionModeDefaults = _PROCESSES_DEFAULTS.clone()
    ray: ExecutionModeDefaults = _RAY_DEFAULTS.clone()

    def get_defaults(self, mode: ExecutionMode) -> ExecutionModeDefaults:
        """Get defaults for a specific execution mode.

        Args:
            mode: The execution mode

        Returns:
            ExecutionModeDefaults for the mode
        """
        if mode == ExecutionMode.Sync:
            return self.sync
        elif mode == ExecutionMode.Asyncio:
            return self.asyncio
        elif mode == ExecutionMode.Threads:
            return self.thread
        elif mode == ExecutionMode.Processes:
            return self.process
        elif mode == ExecutionMode.Ray:
            return self.ray
        else:
            raise ValueError(f"Unknown execution mode: {mode}")

    def reset_to_defaults(self) -> None:
        """Reset all configuration to library defaults."""
        self.sync = _SYNC_DEFAULTS.clone()
        self.asyncio = _ASYNCIO_DEFAULTS.clone()
        self.thread = _THREADS_DEFAULTS.clone()
        self.process = _PROCESSES_DEFAULTS.clone()
        self.ray = _RAY_DEFAULTS.clone()

    def _snapshot(self) -> dict[str, ExecutionModeDefaults]:
        """Create a snapshot of current configuration.

        Returns:
            Dictionary mapping mode names to cloned defaults
        """
        return {
            "sync": self.sync.clone(),
            "asyncio": self.asyncio.clone(),
            "thread": self.thread.clone(),
            "process": self.process.clone(),
            "ray": self.ray.clone(),
        }

    def _restore(self, snapshot: dict[str, ExecutionModeDefaults]) -> None:
        """Restore configuration from a snapshot.

        Args:
            snapshot: Dictionary mapping mode names to defaults
        """
        self.sync = snapshot["sync"]
        self.asyncio = snapshot["asyncio"]
        self.thread = snapshot["thread"]
        self.process = snapshot["process"]
        self.ray = snapshot["ray"]


# Global configuration instance
global_config = ConcurryConfig()


@contextmanager
def temp_config(**overrides):
    """Temporarily override global configuration.

    This context manager allows you to temporarily modify the global configuration
    for the duration of a with block. All changes are automatically reverted when
    exiting the context.

    Args:
        **overrides: Keyword arguments specifying configuration overrides.
            Use dot notation in keys to specify nested attributes.
            Examples:
                - thread_max_workers=50
                - thread_max_queued_tasks=200
                - ray_max_queued_tasks=5

    Yields:
        The global_config instance with temporary overrides applied

    Example:
        ```python
        from concurry import Worker, temp_config

        # Normal defaults
        worker1 = MyWorker.options(mode="thread").init()
        print(worker1.max_queued_tasks)  # 1000 (default)
        worker1.stop()

        # Temporary override
        with temp_config(thread_max_queued_tasks=50, ray_max_queued_tasks=10):
            worker2 = MyWorker.options(mode="thread").init()
            print(worker2.max_queued_tasks)  # 50 (overridden)
            worker2.stop()

            worker3 = MyWorker.options(mode="ray").init()
            print(worker3.max_queued_tasks)  # 10 (overridden)
            worker3.stop()

        # Back to normal defaults
        worker4 = MyWorker.options(mode="thread").init()
        print(worker4.max_queued_tasks)  # 1000 (default restored)
        worker4.stop()
        ```

    Nested Context Managers:
        ```python
        # Nested overrides work correctly
        with temp_config(thread_max_queued_tasks=100):
            worker1 = MyWorker.options(mode="thread").init()
            print(worker1.max_queued_tasks)  # 100

            with temp_config(thread_max_queued_tasks=50):
                worker2 = MyWorker.options(mode="thread").init()
                print(worker2.max_queued_tasks)  # 50
                worker2.stop()

            worker3 = MyWorker.options(mode="thread").init()
            print(worker3.max_queued_tasks)  # 100 (restored to outer context)
            worker3.stop()
            worker1.stop()
        ```
    """
    # Create snapshot of current config
    snapshot = global_config._snapshot()

    try:
        # Apply overrides
        for key, value in overrides.items():
            # Parse key like "thread_max_queued_tasks" into ["thread", "max_queued_tasks"]
            parts = key.split("_", 1)
            if len(parts) != 2:
                raise ValueError(
                    f"Invalid override key: '{key}'. "
                    f"Expected format: '<mode>_<attribute>' (e.g., 'thread_max_queued_tasks')"
                )

            mode_name, attr_name = parts

            # Get the mode defaults object
            if mode_name == "sync":
                mode_defaults = global_config.sync
            elif mode_name == "asyncio":
                mode_defaults = global_config.asyncio
            elif mode_name == "thread":
                mode_defaults = global_config.thread
            elif mode_name == "process":
                mode_defaults = global_config.process
            elif mode_name == "ray":
                mode_defaults = global_config.ray
            else:
                raise ValueError(
                    f"Invalid mode in override key: '{mode_name}'. "
                    f"Valid modes: sync, asyncio, thread, process, ray"
                )

            # Set the attribute
            if not hasattr(mode_defaults, attr_name):
                raise ValueError(
                    f"Invalid attribute in override key: '{attr_name}'. "
                    f"Valid attributes: max_workers, max_queued_tasks, load_balancing, load_balancing_on_demand"
                )

            setattr(mode_defaults, attr_name, value)

        # Yield control to the with block
        yield global_config

    finally:
        # Restore original config
        global_config._restore(snapshot)
