"""Worker pool implementations for concurrent execution.

This module provides WorkerProxyPool classes that manage pools of workers
with load balancing, on-demand creation, and shared resource limits.
"""

import multiprocessing as mp
import threading
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Type

from morphic import Typed
from pydantic import PrivateAttr

from ..constants import ExecutionMode, LoadBalancingAlgorithm
from ..future import BaseFuture
from .base_worker import Worker
from ..algorithms.load_balancing import LoadBalancer


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

    **Model Inheritance & Ray Limitations:**

    Worker pools support the same model inheritance as single workers:
    - ✅ morphic.Typed workers (all modes EXCEPT Ray)
    - ✅ pydantic.BaseModel workers (all modes EXCEPT Ray)
    - ✅ @validate/@validate_call decorators (ALL modes including Ray)
    - ❌ Typed/BaseModel workers with Ray mode (raises ValueError)

    For Ray pools with validation, use @validate or @validate_call decorators
    instead of inheriting from Typed/BaseModel. See Worker docstring for details.

    Example:
        Basic Usage:
            ```python
            # Create a pool via Worker.options()
            pool = MyWorker.options(
                mode="thread",
                max_workers=10,
                load_balancing="round_robin"
            ).init(arg1, arg2)

            # Use like a single worker
            future = pool.my_method(x=5)
            result = future.result()

            # Get pool statistics
            stats = pool.get_pool_stats()

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

    # Public fields (immutable after creation)
    worker_cls: Type[Worker]
    mode: ExecutionMode
    max_workers: int
    load_balancing: LoadBalancingAlgorithm
    on_demand: bool
    blocking: bool
    unwrap_futures: bool
    limits: Optional[Any]  # Shared LimitSet (processed by WorkerBuilder)
    retry_config: Optional[Any] = None  # RetryConfig instance (processed by WorkerBuilder)
    init_args: tuple
    init_kwargs: dict

    # Private attributes (mutable, type-checked)
    _load_balancer: Any = PrivateAttr()
    _workers: List[Any] = PrivateAttr()
    _stopped: bool = PrivateAttr()
    _method_cache: Dict[str, Callable] = PrivateAttr()
    _on_demand_workers: List[Any] = PrivateAttr()
    _on_demand_lock: Any = PrivateAttr()
    _on_demand_counter: int = PrivateAttr()  # Counter for on-demand worker indices

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
        # Store original result method
        original_result = future.result

        def tracked_result(timeout: Optional[float] = None) -> Any:
            try:
                return original_result(timeout=timeout)
            finally:
                # Record completion for load balancer
                self._load_balancer.record_complete(worker_idx)

        # Replace result method
        future.result = tracked_result
        return future

    def _wrap_future_with_cleanup(self, future: BaseFuture, worker: Any) -> BaseFuture:
        """Wrap a future to cleanup on-demand worker after result is available.

        Args:
            future: The future to wrap
            worker: The on-demand worker to cleanup

        Returns:
            Wrapped future that cleanups worker
        """
        # Store original result method
        original_result = future.result

        def cleanup_result(timeout: Optional[float] = None) -> Any:
            try:
                return original_result(timeout=timeout)
            finally:
                # Cleanup on-demand worker
                try:
                    worker.stop(timeout=5)
                except Exception:
                    pass  # Ignore cleanup errors

                # Remove from tracking
                with self._on_demand_lock:
                    if worker in self._on_demand_workers:
                        self._on_demand_workers.remove(worker)

        # Replace result method
        future.result = cleanup_result
        return future

    def _wait_for_on_demand_slot(self) -> None:
        """Wait for an available on-demand worker slot if limit is enforced.

        Blocks until a slot is available or raises error if limit exceeded.
        """
        limit = self._get_on_demand_limit()
        if limit is None:
            return  # No limit

        # Wait for a slot to become available
        max_wait = 60  # 60 seconds max wait
        wait_time = 0.0
        while True:
            with self._on_demand_lock:
                if len(self._on_demand_workers) < limit:
                    return

            # Wait a bit and retry
            import time

            time.sleep(0.1)
            wait_time += 0.1

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
            if self._stopped:
                raise RuntimeError("Worker pool is stopped")

            # On-demand mode: create new worker for this call
            if self.on_demand:
                # Wait for slot if limit enforced
                self._wait_for_on_demand_slot()

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
            if len(self._workers) == 0:
                raise RuntimeError("Worker pool has no workers")

            worker_idx = self._load_balancer.select_worker(len(self._workers))
            worker = self._workers[worker_idx]

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
                # Wrap future to track completion
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

    def get_pool_stats(self) -> Dict[str, Any]:
        """Get pool statistics.

        Returns:
            Dictionary with pool statistics including:
            - total_workers: Number of persistent workers
            - max_workers: Maximum pool size
            - on_demand: Whether on-demand mode is enabled
            - on_demand_active: Number of active on-demand workers
            - load_balancer: Load balancer statistics
            - stopped: Whether pool is stopped
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
        }

    def get_worker_stats(self, worker_id: int) -> Dict[str, Any]:
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
            timeout: Maximum time to wait for each worker to stop in seconds
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
        from .base_worker import _transform_worker_limits

        worker_limits = _transform_worker_limits(
            limits=self.limits,
            mode=self.mode,
            is_pool=False,  # Each worker gets its own view
            worker_index=worker_index,
        )

        # Create worker instance
        return proxy_cls(
            worker_cls=self.worker_cls,
            blocking=self.blocking,
            unwrap_futures=self.unwrap_futures,
            init_args=self.init_args,
            init_kwargs=self.init_kwargs,
            limits=worker_limits,
            retry_config=self.retry_config,
        )

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
        from .base_worker import _transform_worker_limits

        worker_limits = _transform_worker_limits(
            limits=self.limits,
            mode=self.mode,
            is_pool=False,  # Each worker gets its own view
            worker_index=worker_index,
        )

        # Create worker instance
        return proxy_cls(
            worker_cls=self.worker_cls,
            blocking=self.blocking,
            unwrap_futures=self.unwrap_futures,
            init_args=self.init_args,
            init_kwargs=self.init_kwargs,
            limits=worker_limits,
            retry_config=self.retry_config,
        )

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

    actor_options: Optional[Dict[str, Any]] = None  # Ray actor resource options

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
        from .base_worker import _transform_worker_limits

        worker_limits = _transform_worker_limits(
            limits=self.limits,
            mode=self.mode,
            is_pool=False,  # Each worker gets its own view
            worker_index=worker_index,
        )

        # Create worker instance with Ray-specific options
        return proxy_cls(
            worker_cls=self.worker_cls,
            blocking=self.blocking,
            unwrap_futures=self.unwrap_futures,
            init_args=self.init_args,
            init_kwargs=self.init_kwargs,
            limits=worker_limits,
            retry_config=self.retry_config,
            actor_options=self.actor_options,
        )

    def _get_on_demand_limit(self) -> Optional[int]:
        """Get the maximum number of concurrent on-demand workers."""
        # Ray supports unlimited on-demand workers
        return None
