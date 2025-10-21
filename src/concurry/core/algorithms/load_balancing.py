"""Load balancing algorithms for worker pools."""

import random
import threading
from abc import ABC, abstractmethod
from typing import Any, Dict

from morphic import MutableTyped, Registry
from pydantic import ConfigDict, PrivateAttr

from ..constants import LoadBalancingAlgorithm


class _BaseLoadBalancer(Registry, MutableTyped, ABC):
    """Abstract base class for load balancing algorithms.

    **PRIVATE CLASS**: Do not use directly. Use the LoadBalancer() factory function instead.

    Provides a unified interface for different load balancing strategies
    used in worker pools. All algorithm implementations should inherit from this class.

    The load balancer is responsible for:
    - Selecting which worker should handle the next request
    - Tracking worker usage statistics
    - Providing statistics for monitoring

    Thread-Safety:
        Load balancers are thread-safe and can be used concurrently from
        multiple threads. Implementations use appropriate synchronization
        mechanisms (locks, atomic operations) as needed.

    Usage:
        Load balancers are typically created via the LoadBalancer() factory
        function and used internally by WorkerProxyPool:

        ```python
        balancer = LoadBalancer(LoadBalancingAlgorithm.RoundRobin)
        worker_idx = balancer.select_worker(workers)
        balancer.record_start(worker_idx)
        # ... execute task ...
        balancer.record_complete(worker_idx)
        ```
    """

    model_config = ConfigDict(extra="ignore")

    @abstractmethod
    def select_worker(self, num_workers: int) -> int:
        """Select a worker index from the pool.

        Args:
            num_workers: Total number of workers in the pool

        Returns:
            Index of the selected worker (0-based)

        Raises:
            ValueError: If num_workers <= 0
        """
        pass

    def record_start(self, worker_id: int) -> None:
        """Record that a call started on a worker.

        This is called when a task is dispatched to a worker. Some load
        balancing algorithms use this information to track active load.

        Args:
            worker_id: Index of the worker that received the task
        """
        pass

    def record_complete(self, worker_id: int) -> None:
        """Record that a call completed on a worker.

        This is called when a task finishes on a worker. Some load
        balancing algorithms use this information to update statistics.

        Args:
            worker_id: Index of the worker that completed the task
        """
        pass

    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Get load balancer statistics.

        Returns:
            Dictionary containing algorithm-specific statistics
        """
        pass


class _RoundRobinBalancer(_BaseLoadBalancer):
    """Round-robin load balancing algorithm with optional offset.

    **PRIVATE CLASS**: Do not use directly. Use the LoadBalancer() factory function instead.

    Distributes requests evenly across workers in a circular fashion.
    Each request goes to the next worker in sequence.

    Supports a configurable starting offset, enabling multiple workers to start
    at different positions in the round-robin cycle. This minimizes contention
    when many workers are competing for the same resources.

    The offset is added to the counter before modulo operation, so:
    - Balancer with offset 0: selects 0, 1, 2, 0, 1, 2, ...
    - Balancer with offset 1: selects 1, 2, 0, 1, 2, 0, ...
    - Balancer with offset 2: selects 2, 0, 1, 2, 0, 1, ...

    This distributes starting points evenly and reduces the probability of
    multiple workers selecting the same resource simultaneously.

    Characteristics:
        - Simple and fast (O(1) selection)
        - Fair distribution over time
        - No consideration of worker load or health
        - Thread-safe via atomic counter
        - Configurable offset for distributed starting points

    Best For:
        - Homogeneous workers with similar capabilities
        - Tasks with similar execution times
        - Multi-worker scenarios where offset helps reduce contention
        - When simplicity is preferred

    Attributes:
        offset: Starting position in the round-robin cycle (default: 0)

    Example:
        Basic usage (offset=0):
            ```python
            balancer = RoundRobinBalancer.of()
            # First call -> worker 0
            # Second call -> worker 1
            # Third call -> worker 2
            # Fourth call -> worker 0 (wraps around)
            ```

        With offset (for multi-worker scenarios):
            ```python
            # Worker 0 starts at index 0
            balancer0 = RoundRobinBalancer.of(offset=0)

            # Worker 1 starts at index 1
            balancer1 = RoundRobinBalancer.of(offset=1)

            # With 3 resources:
            # balancer0.select_worker(3) -> 0, 1, 2, 0, 1, 2, ...
            # balancer1.select_worker(3) -> 1, 2, 0, 1, 2, 0, ...
            ```
    """

    aliases = ["round_robin", "rr", LoadBalancingAlgorithm.RoundRobin]

    offset: int = 0  # Starting position in round-robin cycle
    _offset: int = PrivateAttr(default=0)
    _counter: int = PrivateAttr(default=0)
    _lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)

    def post_initialize(self) -> None:
        """Initialize private attributes after Typed validation."""
        # Store offset and set counter to offset
        self._offset = self.offset
        self._counter = self.offset

    def select_worker(self, num_workers: int) -> int:
        """Select next worker in round-robin fashion."""
        if num_workers <= 0:
            raise ValueError("num_workers must be positive")

        with self._lock:
            worker_idx = self._counter % num_workers
            self._counter += 1
            return worker_idx

    def get_stats(self) -> Dict[str, Any]:
        """Get round-robin statistics."""
        with self._lock:
            return {
                "algorithm": "RoundRobin",
                "offset": self._offset,
                "total_dispatched": self._counter - self._offset,
            }


class _LeastActiveLoadBalancer(_BaseLoadBalancer):
    """Least active load balancing algorithm.

    **PRIVATE CLASS**: Do not use directly. Use the LoadBalancer() factory function instead.

    Selects the worker with the fewest currently active (in-flight) calls.
    This balances load dynamically based on current worker utilization.

    Characteristics:
        - Adaptive to worker load
        - Good for tasks with varying execution times
        - Slightly more overhead than round-robin
        - Thread-safe via locks

    Best For:
        - Tasks with variable execution times
        - Heterogeneous workers with different capabilities
        - When you want to avoid overloading slow workers

    Example:
        ```python
        balancer = LeastActiveLoadBalancer.of()
        # Always selects worker with fewest active calls
        # Worker 0: 2 active, Worker 1: 1 active, Worker 2: 3 active
        # Next call -> Worker 1
        ```
    """

    aliases = ["least_active", "active", LoadBalancingAlgorithm.LeastActiveLoad]

    _active_calls: Dict[int, int] = PrivateAttr(default_factory=dict)  # worker_id -> active count
    _total_dispatched: int = PrivateAttr(default=0)  # total calls dispatched
    _lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)

    def select_worker(self, num_workers: int) -> int:
        """Select worker with least active calls."""
        if num_workers <= 0:
            raise ValueError("num_workers must be positive")

        with self._lock:
            # Initialize counts for any new workers
            for i in range(num_workers):
                if i not in self._active_calls:
                    self._active_calls[i] = 0

            # Find worker with minimum active calls
            min_active = min(self._active_calls.get(i, 0) for i in range(num_workers))
            for i in range(num_workers):
                if self._active_calls.get(i, 0) == min_active:
                    return i

            # Fallback (should never reach here)
            return 0

    def record_start(self, worker_id: int) -> None:
        """Record that a call started on worker."""
        with self._lock:
            self._active_calls[worker_id] = self._active_calls.get(worker_id, 0) + 1
            self._total_dispatched += 1

    def record_complete(self, worker_id: int) -> None:
        """Record that a call completed on worker."""
        with self._lock:
            if worker_id in self._active_calls:
                self._active_calls[worker_id] = max(0, self._active_calls[worker_id] - 1)

    def get_stats(self) -> Dict[str, Any]:
        """Get least active load statistics."""
        with self._lock:
            return {
                "algorithm": "LeastActiveLoad",
                "active_calls": dict(self._active_calls),
                "total_active": sum(self._active_calls.values()),
                "total_dispatched": self._total_dispatched,
            }


class _LeastTotalLoadBalancer(_BaseLoadBalancer):
    """Least total load balancing algorithm.

    **PRIVATE CLASS**: Do not use directly. Use the LoadBalancer() factory function instead.

    Selects the worker with the fewest total calls over its lifetime.
    This ensures even distribution of total work across workers.

    Characteristics:
        - Fair long-term distribution
        - Simple to implement and understand
        - Does not consider current load
        - Thread-safe via locks

    Best For:
        - Tasks with similar execution times
        - When you want to ensure even wear on workers
        - Monitoring total work distribution

    Example:
        ```python
        balancer = LeastTotalLoadBalancer.of()
        # Always selects worker with fewest total calls
        # Worker 0: 100 calls, Worker 1: 98 calls, Worker 2: 102 calls
        # Next call -> Worker 1
        ```
    """

    aliases = ["least_total", "total", LoadBalancingAlgorithm.LeastTotalLoad]

    _total_calls: Dict[int, int] = PrivateAttr(default_factory=dict)  # worker_id -> total count
    _lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)

    def select_worker(self, num_workers: int) -> int:
        """Select worker with least total calls."""
        if num_workers <= 0:
            raise ValueError("num_workers must be positive")

        with self._lock:
            # Initialize counts for any new workers
            for i in range(num_workers):
                if i not in self._total_calls:
                    self._total_calls[i] = 0

            # Find worker with minimum total calls
            min_total = min(self._total_calls.get(i, 0) for i in range(num_workers))
            for i in range(num_workers):
                if self._total_calls.get(i, 0) == min_total:
                    return i

            # Fallback (should never reach here)
            return 0

    def record_start(self, worker_id: int) -> None:
        """Record that a call started on worker."""
        with self._lock:
            self._total_calls[worker_id] = self._total_calls.get(worker_id, 0) + 1

    def get_stats(self) -> Dict[str, Any]:
        """Get least total load statistics."""
        with self._lock:
            total = sum(self._total_calls.values())
            return {
                "algorithm": "LeastTotalLoad",
                "total_calls": dict(self._total_calls),
                "sum_total_calls": total,
                "total_dispatched": total,  # Alias for consistency
            }


class _RandomBalancer(_BaseLoadBalancer):
    """Random load balancing algorithm.

    **PRIVATE CLASS**: Do not use directly. Use the LoadBalancer() factory function instead.

    Randomly selects a worker for each request. Simple and effective
    for many use cases, especially with stateless workers.

    Characteristics:
        - Simple and very fast
        - Good for stateless workers
        - Eventually fair distribution (law of large numbers)
        - No tracking overhead
        - Thread-safe (random.randint is thread-safe)

    Best For:
        - On-demand workers (ephemeral)
        - Stateless tasks
        - When simplicity is key
        - High-throughput scenarios

    Example:
        ```python
        balancer = RandomBalancer.of()
        # Each call randomly selects a worker
        # Good distribution over many requests
        ```
    """

    aliases = ["random", "rand", LoadBalancingAlgorithm.Random]

    _total_dispatched: int = PrivateAttr(default=0)
    _lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)

    def select_worker(self, num_workers: int) -> int:
        """Randomly select a worker."""
        if num_workers <= 0:
            raise ValueError("num_workers must be positive")

        with self._lock:
            self._total_dispatched += 1

        return random.randint(0, num_workers - 1)

    def get_stats(self) -> Dict[str, Any]:
        """Get random balancer statistics."""
        with self._lock:
            return {
                "algorithm": "Random",
                "total_dispatched": self._total_dispatched,
            }


def LoadBalancer(algorithm: LoadBalancingAlgorithm, **kwargs) -> _BaseLoadBalancer:
    """Factory function to create the appropriate load balancer using Registry pattern.

    This is the only public API for creating load balancers. Implementation
    classes are private and should not be used directly.

    Args:
        algorithm: The load balancing algorithm to use
        **kwargs: Additional keyword arguments to pass to the load balancer constructor

    Returns:
        Load balancer instance (private implementation class)

    Raises:
        ValueError: If algorithm is not recognized

    Example:
        ```python
        from concurry.core.constants import LoadBalancingAlgorithm

        balancer = LoadBalancer(LoadBalancingAlgorithm.RoundRobin)
        worker_idx = balancer.select_worker(num_workers=10)
        ```
    """
    return _BaseLoadBalancer.of(algorithm, **kwargs)
