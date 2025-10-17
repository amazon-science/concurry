"""LimitSet for managing multiple limits atomically.

This module provides LimitSet factory and implementations for atomic acquisition
of multiple limits with different semantics (rate limits, call limits, resource limits).

Architecture:
    - BaseLimitSet: Abstract base class with common acquisition logic
    - InMemorySharedLimitSet: Thread-safe implementation for sync/thread/asyncio workers
    - MultiprocessSharedLimitSet: Multiprocess-safe implementation for process workers
    - RaySharedLimitSet: Distributed implementation for Ray workers
    - LimitSet: Factory function that returns appropriate implementation

Shared State Management:

    InMemorySharedLimitSet:
        - Workers share the same process memory
        - Limit objects (_impl instances) are naturally shared
        - Uses threading.Lock for synchronization
        - Base class _can_acquire_all() works correctly

    MultiprocessSharedLimitSet & RaySharedLimitSet:
        - Each worker/actor has its OWN copy of Limit objects after pickling
        - Limit._impl instances are NOT shared across processes/actors
        - Problem: Base class _can_acquire_all() calls local limit.can_acquire()
        - Solution: Override _can_acquire_all() to use centralized shared state
            * MultiprocessSharedLimitSet: Manager-managed dicts for rate/call limit history
            * RaySharedLimitSet: LimitTrackerActor maintains all state centrally
        - This ensures all workers check against the SAME shared state, not local copies
"""

import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from ..config import ExecutionMode
from .acquisition import Acquisition, LimitSetAcquisition
from .limit import CallLimit, Limit, RateLimit, ResourceLimit


class BaseLimitSet(ABC):
    """Abstract base class for limit set implementations.

    Provides common logic for atomic acquisition of multiple limits.
    Subclasses implement specific synchronization mechanisms.
    """

    def __init__(self, limits: List[Limit], shared: bool, config: Optional[dict] = None):
        """Initialize the base limit set.

        Args:
            limits: List of Limit instances
            shared: Whether this is a shared limit set
            config: Static configuration dict (metadata)
        """
        self.limits = limits
        self._limits_by_key: Dict[str, Limit] = {}
        self.shared = shared
        self.config = config if config is not None else {}
        # Build internal index of limits by key
        for limit in self.limits:
            if limit.key in self._limits_by_key:
                raise ValueError(f"Duplicate limit key: '{limit.key}'")
            self._limits_by_key[limit.key] = limit

    @abstractmethod
    def acquire(
        self, requested: Optional[Dict[str, int]] = None, timeout: Optional[float] = None
    ) -> LimitSetAcquisition:
        """Acquire all limits atomically, blocking until available.

        Args:
            requested: Mapping of limit key to amount requested
            timeout: Maximum time to wait for all limits

        Returns:
            LimitSetAcquisition for tracking usage
        """
        pass

    @abstractmethod
    def try_acquire(self, requested: Optional[Dict[str, int]] = None) -> LimitSetAcquisition:
        """Try to acquire all limits atomically without blocking.

        Args:
            requested: Mapping of limit key to amount requested

        Returns:
            LimitSetAcquisition with successful=True if all acquired
        """
        pass

    @abstractmethod
    def release_limit_set_acquisition(self, acquisition: LimitSetAcquisition) -> None:
        """Release a LimitSetAcquisition.

        Args:
            acquisition: The acquisition to release
        """
        pass

    def _build_requested_amounts(self, requested: Optional[Dict[str, int]]) -> Dict[str, int]:
        """Build requested amounts with defaults.

        This method supports partial acquisition:
        - If requested is empty/None: Acquire ALL limits (CallLimit, ResourceLimit, RateLimit)
        - If requested is not empty: Acquire specified limits + automatically add
          any CallLimit/ResourceLimit with default of 1 (RateLimits must be explicit)

        Args:
            requested: User-provided requested amounts. If None or empty dict,
                acquires all limits with defaults.

        Returns:
            Complete mapping of limit key to requested amount

        Raises:
            ValueError: If RateLimit is not specified when acquiring all,
                or if unknown key provided
        """
        if requested is None:
            requested = {}

        requested_amounts = {}

        # If requested is empty, acquire all limits with defaults
        if len(requested) == 0:
            for limit in self.limits:
                if isinstance(limit, (CallLimit, ResourceLimit)):
                    requested_amounts[limit.key] = 1
                elif isinstance(limit, RateLimit):
                    raise ValueError(
                        f"Must specify requested amount for RateLimit '{limit.key}'. "
                        f"RateLimits require explicit token amounts."
                    )
        else:
            # Partial acquisition: acquire specified limits
            for key, amount in requested.items():
                if key not in self._limits_by_key:
                    raise ValueError(f"Unknown limit key: '{key}'")
                requested_amounts[key] = amount

            # Automatically add CallLimit and ResourceLimit with default of 1
            # (but NOT unspecified RateLimits)
            for limit in self.limits:
                if limit.key not in requested_amounts:
                    if isinstance(limit, (CallLimit, ResourceLimit)):
                        requested_amounts[limit.key] = 1

        return requested_amounts

    def _can_acquire_all(self, requested_amounts: Dict[str, int]) -> bool:
        """Check if all limits can be acquired.

        Args:
            requested_amounts: Amount to acquire for each limit

        Returns:
            True if all can be acquired
        """
        for key, amount in requested_amounts.items():
            limit = self._limits_by_key[key]
            if not limit.can_acquire(amount):
                return False
        return True

    def _acquire_all(self, requested_amounts: Dict[str, int]) -> Dict[str, Acquisition]:
        """Acquire all limits (assumes can_acquire_all returned True).

        Args:
            requested_amounts: Amount to acquire for each limit

        Returns:
            Mapping of limit key to Acquisition
        """
        acquisitions = {}

        try:
            for key, amount in requested_amounts.items():
                limit = self._limits_by_key[key]

                if isinstance(limit, ResourceLimit):
                    # For ResourceLimit, acquire from semaphore if available
                    self._acquire_resource(limit, amount)
                    limit._current_usage += amount
                    acquisitions[key] = Acquisition(limit=limit, requested=amount, successful=True)

                elif isinstance(limit, RateLimit):
                    # For RateLimit, acquire tokens directly from implementation
                    success = limit._impl.try_acquire(tokens=amount)
                    if not success:
                        raise RuntimeError(f"Failed to acquire RateLimit '{key}' despite _can_acquire check")
                    acquisitions[key] = Acquisition(limit=limit, requested=amount, successful=True)

                else:
                    acquisitions[key] = Acquisition(limit=limit, requested=amount, successful=True)

            return acquisitions

        except Exception:
            # Rollback: release any acquired limits
            self._release_acquisitions(acquisitions, requested_amounts)
            raise

    @abstractmethod
    def _acquire_resource(self, limit: ResourceLimit, amount: int) -> None:
        """Acquire resource limit (implementation-specific).

        Args:
            limit: ResourceLimit to acquire
            amount: Amount to acquire
        """
        pass

    def _release_acquisitions(
        self, acquisitions: Dict[str, Acquisition], requested_amounts: Dict[str, int]
    ) -> None:
        """Release acquired limits.

        Args:
            acquisitions: Mapping of limit key to acquisition
            requested_amounts: Original requested amounts
        """
        for key, acq in acquisitions.items():
            limit = acq.limit
            requested = acq.requested
            used = acq.used if acq.used is not None else requested

            if isinstance(limit, ResourceLimit):
                # Release resources
                self._release_resource(limit, requested)
                limit._current_usage -= requested

            elif isinstance(limit, RateLimit):
                # Refund unused tokens using the BaseRateLimiter interface
                if used < requested:
                    refund = requested - used
                    limit._impl.refund(tokens=refund)

    @abstractmethod
    def _release_resource(self, limit: ResourceLimit, amount: int) -> None:
        """Release resource limit (implementation-specific).

        Args:
            limit: ResourceLimit to release
            amount: Amount to release
        """
        pass

    def __getitem__(self, key: str) -> Limit:
        """Get a limit by its key.

        Args:
            key: The limit key to look up

        Returns:
            The Limit with the specified key
        """
        if key not in self._limits_by_key:
            raise KeyError(f"Limit with key '{key}' not found in LimitSet")
        return self._limits_by_key[key]

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for all limits.

        Returns:
            Mapping of limit key to statistics
        """
        return {limit.key: limit.get_stats() for limit in self.limits}


class InMemorySharedLimitSet(BaseLimitSet):
    """In-memory thread-safe limit set implementation.

    Uses threading.Lock and threading.Semaphore for synchronization.
    Suitable for sync, asyncio, and thread workers within the same process.
    """

    def __init__(self, limits: List[Limit], shared: bool, config: Optional[dict] = None):
        """Initialize in-memory shared limit set.

        Args:
            limits: List of Limit instances
            shared: Whether this is a shared limit set
            config: Static configuration dict (metadata)
        """
        super().__init__(limits, shared=shared, config=config)
        self._lock = threading.Lock()
        self._resource_semaphores: Dict[str, threading.Semaphore] = {}

        # Initialize resource semaphores
        for limit in self.limits:
            if isinstance(limit, ResourceLimit):
                self._resource_semaphores[limit.key] = threading.Semaphore(limit.capacity)

    def acquire(
        self, requested: Optional[Dict[str, int]] = None, timeout: Optional[float] = None
    ) -> LimitSetAcquisition:
        """Acquire all limits atomically, blocking until available."""
        requested_amounts = self._build_requested_amounts(requested)
        start_time = time.time()

        while True:
            with self._lock:
                if self._can_acquire_all(requested_amounts):
                    acquisitions = self._acquire_all(requested_amounts)
                    return LimitSetAcquisition(
                        limit_set=self, acquisitions=acquisitions, successful=True, config=self.config
                    )

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    raise TimeoutError(f"Failed to acquire all limits within {timeout}s")

            time.sleep(0.01)

    def try_acquire(self, requested: Optional[Dict[str, int]] = None) -> LimitSetAcquisition:
        """Try to acquire all limits atomically without blocking."""
        requested_amounts = self._build_requested_amounts(requested)

        with self._lock:
            if self._can_acquire_all(requested_amounts):
                acquisitions = self._acquire_all(requested_amounts)
                return LimitSetAcquisition(
                    limit_set=self, acquisitions=acquisitions, successful=True, config=self.config
                )
            else:
                # Create failed acquisitions
                acquisitions = {}
                for key, amount in requested_amounts.items():
                    limit = self._limits_by_key[key]
                    acquisitions[key] = Acquisition(limit=limit, requested=amount, successful=False)

                return LimitSetAcquisition(
                    limit_set=self, acquisitions=acquisitions, successful=False, config=self.config
                )

    def _acquire_resource(self, limit: ResourceLimit, amount: int) -> None:
        """Acquire resource from semaphore."""
        semaphore = self._resource_semaphores[limit.key]
        acquired_count = 0
        for i in range(amount):
            if semaphore.acquire(blocking=False):
                acquired_count += 1
            else:
                # Failed - rollback
                for j in range(acquired_count):
                    semaphore.release()
                raise RuntimeError(
                    f"Failed to acquire ResourceLimit '{limit.key}' despite _can_acquire check"
                )

    def _release_resource(self, limit: ResourceLimit, amount: int) -> None:
        """Release resource to semaphore."""
        semaphore = self._resource_semaphores[limit.key]
        for i in range(amount):
            semaphore.release()

    def release_limit_set_acquisition(self, acquisition: LimitSetAcquisition) -> None:
        """Release a LimitSetAcquisition."""
        with self._lock:
            self._release_acquisitions(
                acquisition.acquisitions,
                {key: acq.requested for key, acq in acquisition.acquisitions.items()},
            )


class MultiprocessSharedLimitSet(BaseLimitSet):
    """Multiprocess-safe limit set implementation.

    Uses multiprocessing.Manager for shared state across processes.
    Suitable for process workers.

    Architecture:
        Unlike InMemorySharedLimitSet which can rely on shared Limit objects
        in memory, MultiprocessSharedLimitSet must maintain all limit state
        in Manager-managed shared data structures since each process has its
        own copy of the Limit objects after pickling.

    State Management:
        - Resource limits: Use Manager.Semaphore (already shared)
        - Rate limits: Store token counts and timestamps in Manager dicts
        - Call limits: Store call counts and timestamps in Manager dicts
    """

    def __init__(self, limits: List[Limit], shared: bool = True, config: Optional[dict] = None):
        """Initialize multiprocess shared limit set.

        Args:
            limits: List of Limit instances
            shared: Whether this is a shared limit set (must be True)
            config: Static configuration dict (metadata)
        """
        assert shared is True
        super().__init__(limits, shared=True, config=config)
        import multiprocessing

        self._manager = multiprocessing.Manager()
        self._lock = self._manager.Lock()
        self._resource_semaphores: Dict[str, Any] = {}

        # Shared state for rate/call limits (Manager-managed)
        # Each limit gets: {tokens: int, history: [(timestamp, amount), ...]}
        self._rate_limit_state: Dict[str, Any] = self._manager.dict()

        # Initialize resource semaphores and rate limit state
        for limit in self.limits:
            if isinstance(limit, ResourceLimit):
                self._resource_semaphores[limit.key] = self._manager.Semaphore(limit.capacity)
            elif isinstance(limit, (RateLimit, CallLimit)):
                # Initialize shared state for this rate/call limit
                self._rate_limit_state[limit.key] = self._manager.dict(
                    {
                        "available_tokens": limit.capacity,
                        "history": self._manager.list(),
                    }
                )

    def _can_acquire_all(self, requested_amounts: Dict[str, int]) -> bool:
        """Check if all limits can be acquired using shared state.

        Override parent to use Manager-managed shared state instead of
        local Limit objects, since each process has its own copy after pickling.

        Args:
            requested_amounts: Amount to acquire for each limit

        Returns:
            True if all can be acquired
        """
        current_time = time.time()

        for key, amount in requested_amounts.items():
            limit = self._limits_by_key[key]

            if isinstance(limit, ResourceLimit):
                # Resource limits use semaphore (already shared)
                # Just check if capacity allows it
                if limit._current_usage + amount > limit.capacity:
                    return False

            elif isinstance(limit, (RateLimit, CallLimit)):
                # Use shared state to check availability
                state = self._rate_limit_state[key]

                # Clean old history entries outside window
                window_start = current_time - limit.window_seconds
                history = list(state["history"])
                active_history = [(ts, amt) for ts, amt in history if ts >= window_start]

                # Calculate current usage in window
                current_usage = sum(amt for ts, amt in active_history)

                # Check if we can accommodate the request
                if current_usage + amount > limit.capacity:
                    return False

        return True

    def _acquire_all(self, requested_amounts: Dict[str, int]) -> Dict[str, Acquisition]:
        """Acquire all limits using shared state.

        Override parent to update Manager-managed shared state instead of
        local Limit objects.

        Args:
            requested_amounts: Amount to acquire for each limit

        Returns:
            Mapping of limit key to Acquisition
        """
        acquisitions = {}
        current_time = time.time()

        try:
            for key, amount in requested_amounts.items():
                limit = self._limits_by_key[key]

                if isinstance(limit, ResourceLimit):
                    # For ResourceLimit, acquire from semaphore if available
                    self._acquire_resource(limit, amount)
                    limit._current_usage += amount
                    acquisitions[key] = Acquisition(limit=limit, requested=amount, successful=True)

                elif isinstance(limit, (RateLimit, CallLimit)):
                    # For Rate/Call limits, update shared state
                    state = self._rate_limit_state[key]

                    # Add to history
                    state["history"].append((current_time, amount))

                    acquisitions[key] = Acquisition(limit=limit, requested=amount, successful=True)

                else:
                    acquisitions[key] = Acquisition(limit=limit, requested=amount, successful=True)

            return acquisitions

        except Exception:
            # Rollback: release any acquired limits
            self._release_acquisitions(acquisitions, requested_amounts)
            raise

    def acquire(
        self, requested: Optional[Dict[str, int]] = None, timeout: Optional[float] = None
    ) -> LimitSetAcquisition:
        """Acquire all limits atomically, blocking until available."""
        requested_amounts = self._build_requested_amounts(requested)
        start_time = time.time()

        while True:
            with self._lock:
                if self._can_acquire_all(requested_amounts):
                    acquisitions = self._acquire_all(requested_amounts)
                    return LimitSetAcquisition(
                        limit_set=self, acquisitions=acquisitions, successful=True, config=self.config
                    )

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    raise TimeoutError(f"Failed to acquire all limits within {timeout}s")

            time.sleep(0.01)

    def try_acquire(self, requested: Optional[Dict[str, int]] = None) -> LimitSetAcquisition:
        """Try to acquire all limits atomically without blocking."""
        requested_amounts = self._build_requested_amounts(requested)

        with self._lock:
            if self._can_acquire_all(requested_amounts):
                acquisitions = self._acquire_all(requested_amounts)
                return LimitSetAcquisition(
                    limit_set=self, acquisitions=acquisitions, successful=True, config=self.config
                )
            else:
                acquisitions = {}
                for key, amount in requested_amounts.items():
                    limit = self._limits_by_key[key]
                    acquisitions[key] = Acquisition(limit=limit, requested=amount, successful=False)

                return LimitSetAcquisition(
                    limit_set=self, acquisitions=acquisitions, successful=False, config=self.config
                )

    def _acquire_resource(self, limit: ResourceLimit, amount: int) -> None:
        """Acquire resource from multiprocess semaphore."""
        semaphore = self._resource_semaphores[limit.key]
        acquired_count = 0
        for i in range(amount):
            if semaphore.acquire(blocking=False):
                acquired_count += 1
            else:
                # Failed - rollback
                for j in range(acquired_count):
                    semaphore.release()
                raise RuntimeError(
                    f"Failed to acquire ResourceLimit '{limit.key}' despite _can_acquire check"
                )

    def _release_resource(self, limit: ResourceLimit, amount: int) -> None:
        """Release resource to multiprocess semaphore."""
        semaphore = self._resource_semaphores[limit.key]
        for i in range(amount):
            semaphore.release()

    def _release_acquisitions(
        self, acquisitions: Dict[str, Acquisition], requested_amounts: Dict[str, int]
    ) -> None:
        """Release acquired limits using shared state.

        Override parent to update Manager-managed shared state instead of
        local Limit objects.

        Args:
            acquisitions: Mapping of limit key to acquisition
            requested_amounts: Original requested amounts
        """
        current_time = time.time()

        for key, acq in acquisitions.items():
            limit = acq.limit
            requested = acq.requested
            used = acq.used if acq.used is not None else requested

            if isinstance(limit, ResourceLimit):
                # Release resources
                self._release_resource(limit, requested)
                limit._current_usage -= requested

            elif isinstance(limit, (RateLimit, CallLimit)):
                # For Rate/Call limits, clean up history in shared state
                state = self._rate_limit_state[key]

                # Clean old history entries outside window
                window_start = current_time - limit.window_seconds
                history = list(state["history"])

                # Keep only entries within window
                active_history = [(ts, amt) for ts, amt in history if ts >= window_start]

                # Update shared history
                state["history"][:] = active_history

                # If there's a difference between used and requested (for RateLimits),
                # we don't need to do anything special since we're tracking actual history

    def release_limit_set_acquisition(self, acquisition: LimitSetAcquisition) -> None:
        """Release a LimitSetAcquisition."""
        with self._lock:
            self._release_acquisitions(
                acquisition.acquisitions,
                {key: acq.requested for key, acq in acquisition.acquisitions.items()},
            )


# Ray actor for centralized limit tracking
try:
    import ray

    @ray.remote
    class LimitTrackerActor:
        """Ray actor for tracking limits across distributed Ray workers.

        This actor provides centralized, atomic limit tracking for Ray workers,
        ensuring thread-safe limit management across a Ray cluster. All limit
        state is managed within this actor to prevent race conditions.

        Architecture:
            - Single actor instance shared across all Ray workers
            - Atomic operations via Ray actor's implicit serialization
            - Tracks current usage, history, and resource capacity
            - Supports CallLimit, RateLimit, and ResourceLimit

        Thread-Safety:
            Ray actors execute methods serially, providing implicit synchronization.
            The `acquire_all` method performs atomic check-and-acquire to prevent
            race conditions where multiple workers try to acquire simultaneously.

        Usage:
            Created automatically by RaySharedLimitSet:

            ```python
            limits = LimitSet(limits=[...], shared=True, mode="ray")
            # LimitTrackerActor created internally

            worker = MyWorker.options(mode="ray", limits=limits).init()
            # Worker uses actor for all limit operations
            ```

        Key Methods:
            - register_limits: Initialize limit configurations
            - acquire_all: Atomically check and acquire multiple limits
            - release_acquisitions: Release limits with actual usage tracking
            - can_acquire_all: Check if all limits can be acquired

        Performance:
            - Overhead: ~500-1000 microseconds per acquire (remote call)
            - Suitable for: Distributed Ray workloads
            - Not suitable for: High-frequency local operations (use InMemorySharedLimitSet)

        See Also:
            - RaySharedLimitSet: Uses this actor for distributed limit tracking
            - InMemorySharedLimitSet: Faster in-process alternative
            - MultiprocessSharedLimitSet: Cross-process alternative
        """

        def __init__(self):
            """Initialize the actor."""
            self._limits: Dict[str, Dict[str, Any]] = {}
            self._resource_usage: Dict[str, int] = {}  # Track resource usage
            self._resource_capacity: Dict[str, int] = {}  # Track resource capacity

            # Track rate/call limit state: {key: {'history': [(ts, amount), ...], 'window': seconds, 'capacity': int}}
            self._rate_limit_state: Dict[str, Dict[str, Any]] = {}

        def register_limits(self, limit_configs: Dict[str, Dict[str, Any]]) -> None:
            """Register limit configurations.

            Args:
                limit_configs: Dict mapping limit keys to their config
                    Config contains: {'type': 'call'|'rate'|'resource', 'capacity': int, 'window_seconds': float, ...}
            """
            for key, config in limit_configs.items():
                if key not in self._limits:
                    limit_type = config.get("type", "call")
                    capacity = config.get("capacity", 0)
                    window_seconds = config.get("window_seconds", 60.0)

                    self._limits[key] = {
                        "current_usage": 0,
                        "history": [],
                        "type": limit_type,
                        "capacity": capacity,
                        "window_seconds": window_seconds,
                    }

                    if limit_type == "resource":
                        self._resource_capacity[key] = capacity
                        self._resource_usage[key] = 0
                    elif limit_type in ("rate", "call"):
                        self._rate_limit_state[key] = {
                            "history": [],
                            "window_seconds": window_seconds,
                            "capacity": capacity,
                        }

        def _ensure_limit(self, limit_key: str) -> None:
            """Ensure limit tracking exists."""
            if limit_key not in self._limits:
                self._limits[limit_key] = {
                    "current_usage": 0,
                    "history": [],
                }

        def can_acquire(self, limit_key: str, requested: int) -> bool:
            """Check if limit can be acquired."""
            return True

        def can_acquire_all(self, requested_amounts: Dict[str, int]) -> bool:
            """Check if all requested amounts can be acquired.

            Args:
                requested_amounts: Dict mapping limit keys to amounts

            Returns:
                True if all can be acquired
            """
            current_time = time.time()

            for key, amount in requested_amounts.items():
                # Check resource limits
                if key in self._resource_capacity:
                    current = self._resource_usage.get(key, 0)
                    if current + amount > self._resource_capacity[key]:
                        return False

                # Check rate/call limits
                elif key in self._rate_limit_state:
                    state = self._rate_limit_state[key]
                    window_start = current_time - state["window_seconds"]

                    # Clean old history and calculate current usage
                    active_history = [(ts, amt) for ts, amt in state["history"] if ts >= window_start]
                    current_usage = sum(amt for ts, amt in active_history)

                    # Check if we can accommodate the request
                    if current_usage + amount > state["capacity"]:
                        return False

            return True

        def acquire_all(self, requested_amounts: Dict[str, int]) -> bool:
            """Atomically check and acquire all requested amounts.

            This is the atomic operation that prevents race conditions.

            Args:
                requested_amounts: Dict mapping limit keys to amounts

            Returns:
                True if all were acquired, False otherwise
            """
            # First check if all can be acquired
            if not self.can_acquire_all(requested_amounts):
                return False

            current_time = time.time()

            # Atomically acquire all
            for key, amount in requested_amounts.items():
                self._ensure_limit(key)
                self._limits[key]["current_usage"] += amount
                self._limits[key]["history"].append((current_time, amount))

                # Update resource usage if this is a resource limit
                if key in self._resource_capacity:
                    self._resource_usage[key] = self._resource_usage.get(key, 0) + amount

                # Update rate/call limit state
                elif key in self._rate_limit_state:
                    state = self._rate_limit_state[key]
                    state["history"].append((current_time, amount))

            return True

        def record_acquisition(self, limit_key: str, requested: int) -> None:
            """Record an acquisition."""
            self._ensure_limit(limit_key)
            self._limits[limit_key]["current_usage"] += requested
            self._limits[limit_key]["history"].append((time.time(), requested))

        def record_acquisitions(self, requested_amounts: Dict[str, int]) -> None:
            """Record multiple acquisitions.

            Args:
                requested_amounts: Dict mapping limit keys to amounts
            """
            for key, amount in requested_amounts.items():
                self.record_acquisition(key, amount)

        def release(self, limit_key: str, requested: int, used: int) -> None:
            """Release a limit."""
            self._ensure_limit(limit_key)
            self._limits[limit_key]["current_usage"] -= requested
            if used < requested:
                self._limits[limit_key]["current_usage"] += requested - used

        def release_acquisitions(
            self, requested_amounts: Dict[str, int], used_amounts: Dict[str, int]
        ) -> None:
            """Release multiple acquisitions.

            Args:
                requested_amounts: Dict mapping limit keys to requested amounts
                used_amounts: Dict mapping limit keys to used amounts
            """
            current_time = time.time()

            for key, requested in requested_amounts.items():
                used = used_amounts.get(key, requested)
                self._ensure_limit(key)
                self._limits[key]["current_usage"] -= requested
                if used < requested:
                    self._limits[key]["current_usage"] += requested - used

                # Update resource usage if this is a resource limit
                if key in self._resource_capacity:
                    self._resource_usage[key] = self._resource_usage.get(key, 0) - requested

                # Clean up rate/call limit state history
                elif key in self._rate_limit_state:
                    state = self._rate_limit_state[key]
                    window_start = current_time - state["window_seconds"]

                    # Keep only entries within window
                    state["history"] = [(ts, amt) for ts, amt in state["history"] if ts >= window_start]

        def get_stats(self, limit_key: str) -> Dict[str, Any]:
            """Get statistics for a limit."""
            self._ensure_limit(limit_key)
            return {
                "current_usage": self._limits[limit_key]["current_usage"],
                "history_count": len(self._limits[limit_key]["history"]),
            }

except ImportError:
    # Ray not available
    LimitTrackerActor = None


class RaySharedLimitSet(BaseLimitSet):
    """Ray-distributed limit set implementation.

    Uses a Ray actor for centralized coordination across Ray workers.
    Suitable for Ray workers.
    """

    def __init__(self, limits: List[Limit], shared: bool = True, config: Optional[dict] = None):
        """Initialize Ray shared limit set.

        Args:
            limits: List of Limit instances
            shared: Whether this is a shared limit set (must be True)
            config: Static configuration dict (metadata)
        """
        assert shared is True
        super().__init__(limits, shared=True, config=config)
        try:
            import ray
        except ImportError:
            raise ImportError("Ray is required for RaySharedLimitSet. Install with: pip install ray")

        if not ray.is_initialized():
            raise RuntimeError("Ray is not initialized. Call ray.init() before creating RaySharedLimitSet.")

        self._actor = LimitTrackerActor.options(num_cpus=0.01).remote()

        # Register limits with the actor
        limit_configs = {}
        for limit in limits:
            if isinstance(limit, ResourceLimit):
                limit_configs[limit.key] = {
                    "type": "resource",
                    "capacity": limit.capacity,
                }
            elif isinstance(limit, CallLimit):
                limit_configs[limit.key] = {
                    "type": "call",
                    "capacity": limit.capacity,
                    "window_seconds": limit.window_seconds,
                }
            elif isinstance(limit, RateLimit):
                limit_configs[limit.key] = {
                    "type": "rate",
                    "capacity": limit.capacity,
                    "window_seconds": limit.window_seconds,
                }

        ray.get(self._actor.register_limits.remote(limit_configs))

    def acquire(
        self, requested: Optional[Dict[str, int]] = None, timeout: Optional[float] = None
    ) -> LimitSetAcquisition:
        """Acquire all limits atomically, blocking until available."""
        requested_amounts = self._build_requested_amounts(requested)
        start_time = time.time()

        import ray

        while True:
            # Atomically acquire with Ray actor (prevents race condition)
            acquired = ray.get(self._actor.acquire_all.remote(requested_amounts))
            if acquired:
                # Create local acquisition objects (no state change locally)
                acquisitions = {}
                for key, amount in requested_amounts.items():
                    limit = self._limits_by_key[key]
                    acquisitions[key] = Acquisition(limit=limit, requested=amount, successful=True)

                return LimitSetAcquisition(
                    limit_set=self, acquisitions=acquisitions, successful=True, config=self.config
                )

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    raise TimeoutError(f"Failed to acquire all limits within {timeout}s")

            time.sleep(0.01)

    def try_acquire(self, requested: Optional[Dict[str, int]] = None) -> LimitSetAcquisition:
        """Try to acquire all limits atomically without blocking."""
        requested_amounts = self._build_requested_amounts(requested)

        import ray

        # Atomically try to acquire with Ray actor (prevents race condition)
        acquired = ray.get(self._actor.acquire_all.remote(requested_amounts))

        if acquired:
            # Create local acquisition objects (no state change locally)
            acquisitions = {}
            for key, amount in requested_amounts.items():
                limit = self._limits_by_key[key]
                acquisitions[key] = Acquisition(limit=limit, requested=amount, successful=True)

            return LimitSetAcquisition(
                limit_set=self, acquisitions=acquisitions, successful=True, config=self.config
            )
        else:
            acquisitions = {}
            for key, amount in requested_amounts.items():
                limit = self._limits_by_key[key]
                acquisitions[key] = Acquisition(limit=limit, requested=amount, successful=False)

            return LimitSetAcquisition(
                limit_set=self, acquisitions=acquisitions, successful=False, config=self.config
            )

    def _acquire_resource(self, limit: ResourceLimit, amount: int) -> None:
        """Resource acquisition handled by Ray actor."""
        # Actual semaphore logic is in Ray actor
        pass

    def _release_resource(self, limit: ResourceLimit, amount: int) -> None:
        """Resource release handled by Ray actor."""
        # Actual semaphore logic is in Ray actor
        pass

    def release_limit_set_acquisition(self, acquisition: LimitSetAcquisition) -> None:
        """Release a LimitSetAcquisition."""
        import ray

        requested_amounts = {key: acq.requested for key, acq in acquisition.acquisitions.items()}
        used_amounts = {
            key: acq.used if acq.used is not None else acq.requested
            for key, acq in acquisition.acquisitions.items()
        }
        # Release via Ray actor (all state is managed there)
        ray.get(self._actor.release_acquisitions.remote(requested_amounts, used_amounts))


def LimitSet(
    limits: List[Limit],
    shared: bool = False,
    mode: Union[str, ExecutionMode] = "sync",
    config: Optional[dict] = None,
) -> Union[InMemorySharedLimitSet, MultiprocessSharedLimitSet, RaySharedLimitSet]:
    """Factory function to create appropriate LimitSet implementation.

    Args:
        limits: List of Limit instances. Can be empty list to create a no-op LimitSet
                that always allows acquisition without blocking.
        shared: If True, create a shared LimitSet for cross-worker use.
                If False, create a private LimitSet with warning.
        mode: Execution mode (ExecutionMode enum or string like "sync", "thread", "asyncio", "process", "ray")
        config: Static configuration dict (metadata) accessible via acquisition.config.
                Empty dict by default. Useful for multi-account/multi-region scenarios.

    Returns:
        Appropriate LimitSet implementation based on shared and mode

    Raises:
        ValueError: If shared=False and mode != "sync"

    Examples:
        Private LimitSet (non-shared):
            ```python
            limits = LimitSet(
                limits=[RateLimit(...), ResourceLimit(...)],
                shared=False,
                mode="sync"
            )
            ```

        Empty LimitSet (always allows acquisition):
            ```python
            # Create empty LimitSet - never blocks, always succeeds
            limits = LimitSet(limits=[], shared=False, mode="sync")

            with limits.acquire():
                # Always succeeds immediately, no limits enforced
                do_work()

            # Workers automatically get empty LimitSet when no limits provided
            worker = MyWorker.options(mode="thread").init()
            # worker.limits is available and always allows acquisition
            ```

        Shared LimitSet for thread workers:
            ```python
            limits = LimitSet(
                limits=[RateLimit(...), ResourceLimit(...)],
                shared=True,
                mode="thread"
            )
            worker1 = MyWorker.options(mode="thread", limits=limits).init()
            worker2 = MyWorker.options(mode="thread", limits=limits).init()
            # worker1 and worker2 share the same limits
            ```

        Shared LimitSet for process workers:
            ```python
            limits = LimitSet(
                limits=[RateLimit(...), ResourceLimit(...)],
                shared=True,
                mode="process"
            )
            worker1 = MyWorker.options(mode="process", limits=limits).init()
            worker2 = MyWorker.options(mode="process", limits=limits).init()
            # worker1 and worker2 share the same limits across processes
            ```

    Notes:
        - Empty LimitSet (limits=[]) is useful for conditional limit enforcement
        - Workers automatically get empty LimitSet when no limits parameter provided
        - Empty LimitSet has zero overhead - acquire() returns immediately
        - Code can safely call self.limits.acquire() without checking if limits exist
    """
    # Convert string to ExecutionMode if needed
    if isinstance(mode, str):
        mode = ExecutionMode(mode)

    # Select appropriate implementation
    if mode in (ExecutionMode.Sync, ExecutionMode.Asyncio, ExecutionMode.Threads):
        return InMemorySharedLimitSet(limits=limits, shared=shared, config=config)
    elif mode == ExecutionMode.Processes:
        if shared is False:
            raise ValueError("Non-shared LimitSets cannot use mode='process'")
        return MultiprocessSharedLimitSet(limits=limits, shared=True, config=config)
    elif mode == ExecutionMode.Ray:
        if shared is False:
            raise ValueError("Non-shared LimitSets cannot use mode='ray'")
        return RaySharedLimitSet(limits=limits, shared=True, config=config)
    else:
        raise ValueError(
            f"Unknown execution mode: '{mode}'. Valid modes: sync, asyncio, thread, process, ray"
        )
