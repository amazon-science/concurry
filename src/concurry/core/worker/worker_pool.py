"""Worker pool implementations for concurrent execution.

This module provides WorkerProxyPool classes that manage pools of workers
with load balancing, on-demand creation, and shared resource limits.
"""

import multiprocessing as mp
import threading
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional, Type

from morphic import Typed
from pydantic import PrivateAttr, confloat, conint

from ..algorithms.load_balancing import LoadBalancer
from ..constants import ExecutionMode, LoadBalancingAlgorithm
from ..future import BaseFuture
from ..retry import RetryConfig
from .base_worker import Worker, _transform_worker_limits


class WorkerProxyPool(Typed, ABC):
    """Abstract base class for worker pools.

    WorkerProxyPool manages a pool of worker instances and dispatches method calls
    to them based on a load balancing algorithm. It implements the same public API
    as WorkerProxy but does NOT inherit from it since the internal behavior is different.

    The pool can operate in two modes:
    1. **Persistent Pool**: Fixed number of workers created at initialization
    2. **On-Demand Pool**: Workers created dynamically for each request and destroyed after completion

    Key Features:
        - Load balancing across multiple workers
        - Shared resource limits across pool
        - Per-worker submission queues for overload protection
        - On-demand worker creation for bursty workloads
        - Monitoring and statistics
        - Clean shutdown of all workers

    Architecture:
        - Lives client-side (not a remote actor)
        - Manages multiple WorkerProxy instances
        - Dispatches method calls via load balancer
        - Wraps futures for tracking and cleanup

    Thread-Safety:
        WorkerProxyPool is thread-safe and can be used concurrently from
        multiple threads. Uses appropriate synchronization primitives.

    **Model Inheritance & Ray Support:**

    Worker pools support the same model inheritance as single workers:
    - ✅ morphic.Typed workers (ALL modes including Ray via automatic composition wrapper)
    - ✅ pydantic.BaseModel workers (ALL modes including Ray via automatic composition wrapper)
    - ✅ @validate/@validate_call decorators (ALL modes including Ray)
    - ✅ Typed/BaseModel workers with Ray mode (fully supported via automatic composition wrapper)

    All validation approaches now work seamlessly with Ray pools thanks to the automatic
    composition wrapper. No code changes required! See Worker docstring for details.

    Example:
        Basic Usage:
            ```python
            # Create a pool via Worker.options()
            pool = MyWorker.options(
                mode="thread",
                max_workers=10,
                load_balancing="round_robin",
                max_queued_tasks=5  # 5 in-flight tasks per worker
            ).init(arg1, arg2)

            # Use like a single worker
            # Total capacity: 10 workers × 5 queue = 50 concurrent tasks
            future = pool.my_method(x=5)
            result = future.result()

            # Get pool statistics
            stats = pool.get_pool_stats()
            print(f"Submission queue: {stats['max_queued_tasks']} per worker")

            # Stop all workers
            pool.stop()
            ```

        Context Manager (Recommended):
            ```python
            # Context manager automatically stops all workers
            with MyWorker.options(
                mode="thread",
                max_workers=10
            ).init(arg1, arg2) as pool:
                future = pool.my_method(x=5)
                result = future.result()
            # All workers automatically stopped here

            # Works with blocking mode
            with MyWorker.options(
                mode="thread",
                max_workers=5,
                blocking=True
            ).init() as pool:
                results = [pool.process(i) for i in range(10)]
            # Pool automatically stopped

            # Cleanup happens even on exceptions
            with MyWorker.options(mode="thread", max_workers=3).init() as pool:
                if error_condition:
                    raise ValueError("Error")
            # Pool still stopped despite exception
            ```

        Ray Pool with Validation:
            ```python
            # Ray pool with validation (use decorators, not inheritance)
            from morphic import validate

            class ValidatedWorker(Worker):
                @validate
                def process(self, x: int) -> int:
                    return x * 2

            # Works with Ray!
            with ValidatedWorker.options(
                mode="ray",
                max_workers=10
            ).init() as pool:
                result = pool.process(5).result()
            # Pool automatically stopped
            ```
    """

    # ========================================================================
    # PUBLIC ATTRIBUTES - NO DEFAULTS ALLOWED
    # All values must be passed from WorkerBuilder (with defafults picked from
    # global_config)
    # ========================================================================
    # CRITICAL: Public attributes MUST NOT have default values.
    # All values must be explicitly passed from WorkerBuilder, which resolves
    # defaults from global_config. This ensures all defaults are centralized
    # and can be overridden globally via temp_config().
    worker_cls: Type[Worker]
    mode: ExecutionMode
    max_workers: conint(ge=0)
    load_balancing: LoadBalancingAlgorithm
    on_demand: bool
    blocking: bool
    unwrap_futures: bool
    limits: Optional[Any]  # Shared LimitSet (processed by WorkerBuilder)
    retry_configs: Optional[
        dict[str, Optional[RetryConfig]]
    ]  # Per-method retry configs (None = no retry for that method)
    max_queued_tasks: Optional[conint(ge=0)]
    init_args: tuple
    init_kwargs: dict

    # On-demand configuration (values passed from WorkerBuilder via global config)
    on_demand_cleanup_timeout: confloat(ge=0)
    on_demand_slot_max_wait: confloat(ge=0)

    # Private attributes (mutable, type-checked)
    _load_balancer: Any = PrivateAttr()
    _workers: list[Any] = PrivateAttr()
    _stopped: bool = PrivateAttr()
    _method_cache: dict[str, Callable] = PrivateAttr()
    _on_demand_workers: list[Any] = PrivateAttr()
    _on_demand_lock: Any = PrivateAttr()
    _on_demand_counter: conint(ge=0) = PrivateAttr()  # Counter for on-demand worker indices

    def post_initialize(self) -> None:
        """Initialize private attributes after Typed validation."""
        # Initialize load balancer
        object.__setattr__(self, "_load_balancer", LoadBalancer(self.load_balancing))

        # Initialize worker lists
        object.__setattr__(self, "_workers", [])
        object.__setattr__(self, "_stopped", False)
        object.__setattr__(self, "_method_cache", {})

        # On-demand worker tracking
        object.__setattr__(self, "_on_demand_workers", [])
        object.__setattr__(self, "_on_demand_lock", threading.Lock())
        object.__setattr__(self, "_on_demand_counter", 0)  # Start at 0 for on-demand

        # Initialize the pool
        self._initialize_pool()

    @abstractmethod
    def _initialize_pool(self) -> None:
        """Initialize the worker pool.

        For persistent pools, this creates all workers upfront.
        For on-demand pools, this prepares the pool without creating workers.

        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def _create_worker(self) -> Any:
        """Create a single worker instance.

        Returns:
            WorkerProxy instance

        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def _get_on_demand_limit(self) -> Optional[int]:
        """Get the maximum number of concurrent on-demand workers.

        Returns:
            Maximum concurrent on-demand workers, or None for unlimited

        Must be implemented by subclasses.
        """
        pass

    def _wrap_future_with_tracking(self, future: BaseFuture, worker_idx: int) -> BaseFuture:
        """Wrap a future to track completion for load balancing.

        Args:
            future: The future to wrap
            worker_idx: Index of the worker that created the future

        Returns:
            Wrapped future that records completion
        """

        def on_complete(f):
            try:
                # Record completion for load balancer
                self._load_balancer.record_complete(worker_idx)
            except Exception:
                pass  # Ignore any errors in callback

        future.add_done_callback(on_complete)
        return future

    def _wrap_future_with_cleanup(self, future: BaseFuture, worker: Any) -> BaseFuture:
        """Wrap a future to cleanup on-demand worker after completion.

        Args:
            future: The future to wrap
            worker: The on-demand worker to cleanup

        Returns:
            Wrapped future that cleanups worker
        """

        def cleanup_callback(f):
            """Cleanup callback invoked when future completes."""
            try:
                # Remove from tracking first
                with self._on_demand_lock:
                    if worker in self._on_demand_workers:
                        self._on_demand_workers.remove(worker)

                # Schedule cleanup in a separate thread to avoid deadlock
                # Calling worker.stop() from within a callback can cause deadlocks
                # because stop() may try to cancel futures that are invoking this callback
                def deferred_cleanup():
                    try:
                        worker.stop(timeout=self.on_demand_cleanup_timeout)
                    except Exception:
                        pass  # Ignore cleanup errors

                cleanup_thread = threading.Thread(target=deferred_cleanup, daemon=True)
                cleanup_thread.start()

            except Exception:
                pass  # Ignore all cleanup errors

        # Use add_done_callback to ensure cleanup happens when future completes
        future.add_done_callback(cleanup_callback)
        return future

    def _wait_for_on_demand_slot(self) -> None:
        """Wait for an available on-demand worker slot if limit is enforced.

        Blocks until a slot is available or raises error if limit exceeded.
        """
        from ...config import global_config

        limit = self._get_on_demand_limit()
        if limit is None:
            return  # No limit

        # Wait for a slot to become available
        local_config = global_config.clone()
        sleep_time = local_config.defaults.worker_pool_cleanup_sleep
        max_wait = self.on_demand_slot_max_wait
        wait_time = 0.0
        while True:
            with self._on_demand_lock:
                if len(self._on_demand_workers) < limit:
                    return

            # Wait a bit and retry
            import time

            time.sleep(sleep_time)
            wait_time += sleep_time

            if wait_time >= max_wait:
                raise RuntimeError(
                    f"Timeout waiting for on-demand worker slot "
                    f"(limit={limit}, current={len(self._on_demand_workers)})"
                )

    def __getattr__(self, name: str) -> Callable:
        """Intercept method calls and dispatch them to pool workers.

        Args:
            name: Method name

        Returns:
            A callable that will execute the method on a pool worker

        Raises:
            AttributeError: If method starts with underscore
            RuntimeError: If pool is stopped
        """
        # Don't intercept private/dunder methods or Pydantic/Typed internal attributes
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")

        # Check cache first (safely, in case it doesn't exist yet during __init__)
        try:
            cache = object.__getattribute__(self, "_method_cache")
            if name in cache:
                return cache[name]
        except AttributeError:
            # _method_cache not initialized yet
            pass

        def method_wrapper(*args: Any, **kwargs: Any) -> Any:
            # On-demand mode: create new worker for this call
            if self.on_demand:
                # Wait for slot if limit enforced (can block)
                self._wait_for_on_demand_slot()

                # Check if stopped AFTER waiting for slot to avoid race condition
                if self._stopped:
                    raise RuntimeError("Worker pool is stopped")

                # Get next worker index and increment counter
                with self._on_demand_lock:
                    worker_index = self._on_demand_counter
                    self._on_demand_counter += 1

                # Create worker with unique index
                worker = self._create_worker(worker_index=worker_index)

                # Track worker
                with self._on_demand_lock:
                    self._on_demand_workers.append(worker)

                # Execute method
                future = getattr(worker, name)(*args, **kwargs)

                # Wrap future to cleanup worker after result
                wrapped_future = self._wrap_future_with_cleanup(future, worker)

                if self.blocking:
                    return wrapped_future.result()
                else:
                    return wrapped_future

            # Persistent pool mode: select worker via load balancer
            # Check workers exist before proceeding
            if len(self._workers) == 0:
                # Check if pool was stopped - give more specific error message
                if self._stopped:
                    raise RuntimeError("Worker pool is stopped")
                else:
                    raise RuntimeError("Worker pool has no workers")

            worker_idx = self._load_balancer.select_worker(len(self._workers))
            worker = self._workers[worker_idx]

            # Check if stopped
            if self._stopped:
                raise RuntimeError("Worker pool is stopped")

            # Record start for load balancer
            self._load_balancer.record_start(worker_idx)

            # Execute method
            result = getattr(worker, name)(*args, **kwargs)

            # If blocking mode, worker already returned result (not future)
            if self.blocking:
                # Record completion immediately
                self._load_balancer.record_complete(worker_idx)
                return result

            # Non-blocking: check if result is a future (has .result attribute)
            # Some methods like map() return iterators, not futures
            if hasattr(result, "result") and callable(getattr(result, "result")):
                # Wrap future to track completion and release semaphore
                wrapped_future = self._wrap_future_with_tracking(result, worker_idx)
                return wrapped_future
            else:
                # Not a future (e.g., iterator from map), return as-is
                # Record completion immediately since we don't track iterators
                self._load_balancer.record_complete(worker_idx)
                return result

        # Cache the wrapper (safely, in case it doesn't exist yet during __init__)
        try:
            cache = object.__getattribute__(self, "_method_cache")
            cache[name] = method_wrapper
        except AttributeError:
            # _method_cache not initialized yet, skip caching
            pass

        return method_wrapper

    def get_pool_stats(self) -> dict[str, Any]:
        """Get pool statistics.

        Returns:
            Dictionary with pool statistics including:
            - total_workers: Number of persistent workers
            - max_workers: Maximum pool size
            - on_demand: Whether on-demand mode is enabled
            - on_demand_active: Number of active on-demand workers
            - load_balancer: Load balancer statistics
            - stopped: Whether pool is stopped
            - max_queued_tasks: Per-worker submission queue capacity
        """
        with self._on_demand_lock:
            on_demand_active = len(self._on_demand_workers)

        return {
            "total_workers": len(self._workers),
            "max_workers": self.max_workers,
            "on_demand": self.on_demand,
            "on_demand_active": on_demand_active,
            "load_balancer": self._load_balancer.get_stats(),
            "stopped": self._stopped,
            "max_queued_tasks": self.max_queued_tasks,
        }

    def get_worker_stats(self, worker_id: int) -> dict[str, Any]:
        """Get statistics for a specific worker.

        Args:
            worker_id: Index of the worker (0-based)

        Returns:
            Dictionary with worker statistics

        Raises:
            IndexError: If worker_id is out of range
        """
        if worker_id < 0 or worker_id >= len(self._workers):
            raise IndexError(f"Worker ID {worker_id} out of range (pool size: {len(self._workers)})")

        return {
            "worker_id": worker_id,
            "stopped": self._workers[worker_id]._stopped,
        }

    def stop(self, timeout: float = 30) -> None:
        """Stop all workers in the pool.

        Args:
            timeout: Maximum time to wait for each worker to stop in seconds.
                Default value is determined by global_config.<mode>.stop_timeout
        """
        if self._stopped:
            return

        object.__setattr__(self, "_stopped", True)

        # Stop persistent workers
        for worker in self._workers:
            try:
                worker.stop(timeout=timeout)
            except Exception:
                pass  # Ignore errors during shutdown

        # Stop on-demand workers
        with self._on_demand_lock:
            for worker in self._on_demand_workers:
                try:
                    worker.stop(timeout=timeout)
                except Exception:
                    pass  # Ignore errors during shutdown
            self._on_demand_workers.clear()

        self._workers.clear()

    def __enter__(self) -> "WorkerProxyPool":
        """Enter context manager.

        Returns:
            Self for use in with statement
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager and stop all workers.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        self.stop()


class InMemoryWorkerProxyPool(WorkerProxyPool):
    """Worker pool for Sync, Asyncio, and Thread workers.

    Uses in-memory synchronization (threading.Lock) since all workers
    run in the same process.

    Supported Modes:
        - Sync: Single worker only (max_workers=1)
        - Asyncio: Single worker only (max_workers=1)
        - Thread: Multiple workers with thread-based concurrency
    """

    def _initialize_pool(self) -> None:
        """Initialize the worker pool."""
        # For on-demand mode, don't create workers upfront
        if self.on_demand:
            return

        # Create persistent workers with sequential indices
        for i in range(self.max_workers):
            worker = self._create_worker(worker_index=i)
            self._workers.append(worker)

    def _create_worker(self, worker_index: int = 0) -> Any:
        """Create a single worker instance.

        Args:
            worker_index: Index for round-robin load balancing in LimitPool
        """
        from .asyncio_worker import AsyncioWorkerProxy
        from .sync_worker import SyncWorkerProxy
        from .task_worker import TaskWorker, TaskWorkerMixin
        from .thread_worker import ThreadWorkerProxy

        # Select appropriate proxy class
        if self.mode == ExecutionMode.Sync:
            proxy_cls = SyncWorkerProxy
        elif self.mode == ExecutionMode.Threads:
            proxy_cls = ThreadWorkerProxy
        elif self.mode == ExecutionMode.Asyncio:
            proxy_cls = AsyncioWorkerProxy
        else:
            raise ValueError(f"Unsupported mode for InMemoryWorkerProxyPool: {self.mode}")

        # If this is TaskWorker, create a combined proxy class with TaskWorkerMixin
        if issubclass(self.worker_cls, TaskWorker):
            # Dynamically create a proxy class that includes TaskWorkerMixin
            class TaskWorkerProxyClass(TaskWorkerMixin, proxy_cls):  # type: ignore
                pass

            proxy_cls = TaskWorkerProxyClass

        # Process limits with worker_index
        worker_limits = _transform_worker_limits(
            limits=self.limits,
            mode=self.mode,
            is_pool=False,  # Each worker gets its own view
            worker_index=worker_index,
        )

        # On-demand workers don't need submission queuing since they're ephemeral
        # and the pool already limits concurrent on-demand workers
        worker_queue_length = 999999 if self.on_demand else self.max_queued_tasks

        # Get worker timeout configs from global config
        from ...config import global_config

        mode_defaults = global_config.get_defaults(self.mode)

        # Prepare worker options with timeout configs based on mode
        worker_options = {
            "worker_cls": self.worker_cls,
            "blocking": self.blocking,
            "unwrap_futures": self.unwrap_futures,
            "init_args": self.init_args,
            "init_kwargs": self.init_kwargs,
            "limits": worker_limits,
            "retry_configs": self.retry_configs,
            "max_queued_tasks": worker_queue_length,
        }

        # Add mode-specific timeout configs
        if self.mode == ExecutionMode.Threads:
            worker_options["command_queue_timeout"] = mode_defaults.worker_command_queue_timeout
        elif self.mode == ExecutionMode.Asyncio:
            worker_options["loop_ready_timeout"] = mode_defaults.worker_loop_ready_timeout
            worker_options["thread_ready_timeout"] = mode_defaults.worker_thread_ready_timeout
            worker_options["sync_queue_timeout"] = mode_defaults.worker_sync_queue_timeout

        # Create worker instance
        # Note: mode is NOT passed - it's a class variable set by each proxy subclass
        return proxy_cls(**worker_options)

    def _get_on_demand_limit(self) -> Optional[int]:
        """Get the maximum number of concurrent on-demand workers."""
        if self.mode == ExecutionMode.Threads:
            # Limit threads to cpu_count() - 1
            return max(1, mp.cpu_count() - 1)
        else:
            # Sync and Asyncio don't support on-demand
            return None


class MultiprocessWorkerProxyPool(WorkerProxyPool):
    """Worker pool for Process workers.

    Uses multiprocessing synchronization since workers run in
    separate processes.

    Supported Modes:
        - Process: Multiple workers with process-based concurrency
    """

    def _initialize_pool(self) -> None:
        """Initialize the worker pool."""
        # For on-demand mode, don't create workers upfront
        if self.on_demand:
            return

        # Create persistent workers with sequential indices
        for i in range(self.max_workers):
            worker = self._create_worker(worker_index=i)
            self._workers.append(worker)

    def _create_worker(self, worker_index: int = 0) -> Any:
        """Create a single worker instance.

        Args:
            worker_index: Index for round-robin load balancing in LimitPool
        """
        from .process_worker import ProcessWorkerProxy
        from .task_worker import TaskWorker, TaskWorkerMixin

        proxy_cls = ProcessWorkerProxy

        # If this is TaskWorker, create a combined proxy class with TaskWorkerMixin
        if issubclass(self.worker_cls, TaskWorker):
            # Dynamically create a proxy class that includes TaskWorkerMixin
            class TaskWorkerProxyClass(TaskWorkerMixin, proxy_cls):  # type: ignore
                pass

            proxy_cls = TaskWorkerProxyClass

        # Process limits with worker_index
        worker_limits = _transform_worker_limits(
            limits=self.limits,
            mode=self.mode,
            is_pool=False,  # Each worker gets its own view
            worker_index=worker_index,
        )

        # On-demand workers don't need submission queuing since they're ephemeral
        # and the pool already limits concurrent on-demand workers
        worker_queue_length = 999999 if self.on_demand else self.max_queued_tasks

        # Get worker timeout configs from global config
        from ...config import global_config

        mode_defaults = global_config.get_defaults(self.mode)

        # Prepare worker options with timeout configs for process mode
        worker_options = {
            "worker_cls": self.worker_cls,
            "blocking": self.blocking,
            "unwrap_futures": self.unwrap_futures,
            "init_args": self.init_args,
            "init_kwargs": self.init_kwargs,
            "limits": worker_limits,
            "retry_configs": self.retry_configs,
            "max_queued_tasks": worker_queue_length,
            "result_queue_timeout": mode_defaults.worker_result_queue_timeout,
            "result_queue_cleanup_timeout": mode_defaults.worker_result_queue_cleanup_timeout,
            "mp_context": mode_defaults.mp_context,
        }

        # Create worker instance
        # Note: mode is NOT passed - it's a class variable set by each proxy subclass
        return proxy_cls(**worker_options)

    def _get_on_demand_limit(self) -> Optional[int]:
        """Get the maximum number of concurrent on-demand workers."""
        # Limit processes to cpu_count() - 1
        return max(1, mp.cpu_count() - 1)


class RayWorkerProxyPool(WorkerProxyPool):
    """Worker pool for Ray workers.

    Lives client-side (not a Ray actor) and manages Ray actor workers.
    Uses threading.Lock for client-side synchronization.

    Supported Modes:
        - Ray: Multiple workers with Ray-based distributed execution
    """

    actor_options: Optional[dict[str, Any]] = None  # Ray actor resource options

    def _initialize_pool(self) -> None:
        """Initialize the worker pool."""
        # For on-demand mode, don't create workers upfront
        if self.on_demand:
            return

        # Create persistent workers with sequential indices
        for i in range(self.max_workers):
            worker = self._create_worker(worker_index=i)
            self._workers.append(worker)

    def _create_worker(self, worker_index: int = 0) -> Any:
        """Create a single worker instance.

        Args:
            worker_index: Index for round-robin load balancing in LimitPool
        """
        from .ray_worker import RayWorkerProxy
        from .task_worker import TaskWorker, TaskWorkerMixin

        proxy_cls = RayWorkerProxy

        # If this is TaskWorker, create a combined proxy class with TaskWorkerMixin
        if issubclass(self.worker_cls, TaskWorker):
            # Dynamically create a proxy class that includes TaskWorkerMixin
            class TaskWorkerProxyClass(TaskWorkerMixin, proxy_cls):  # type: ignore
                pass

            proxy_cls = TaskWorkerProxyClass

        # Process limits with worker_index
        worker_limits = _transform_worker_limits(
            limits=self.limits,
            mode=self.mode,
            is_pool=False,  # Each worker gets its own view
            worker_index=worker_index,
        )

        # On-demand workers don't need submission queuing since they're ephemeral
        # and the pool already limits concurrent on-demand workers
        worker_queue_length = 999999 if self.on_demand else self.max_queued_tasks

        # Create worker instance with Ray-specific options
        # Note: mode is NOT passed - it's a class variable set by each proxy subclass
        return proxy_cls(
            worker_cls=self.worker_cls,
            blocking=self.blocking,
            unwrap_futures=self.unwrap_futures,
            init_args=self.init_args,
            init_kwargs=self.init_kwargs,
            limits=worker_limits,
            retry_configs=self.retry_configs,
            max_queued_tasks=worker_queue_length,
            actor_options=self.actor_options,
        )

    def _get_on_demand_limit(self) -> Optional[int]:
        """Get the maximum number of concurrent on-demand workers."""
        # Ray supports unlimited on-demand workers
        return None
