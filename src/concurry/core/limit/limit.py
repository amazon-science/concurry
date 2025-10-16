"""Base limit classes for resource protection and rate limiting.

This module provides the core limit abstractions for controlling resource usage
and enforcing rate limits in concurry applications. Limits are simple data containers
that define constraints but are NOT thread-safe and cannot be acquired directly.

Classes:
    Limit: Abstract base class for all limit types
    RateLimit: Time-based rate limiting with configurable algorithms
    CallLimit: Special RateLimit for counting calls (usage always 1)
    ResourceLimit: Semaphore-based resource limiting (e.g., connection pools)

Important:
    - Limits are NOT thread-safe - they are simple data containers
    - Limits cannot be acquired directly - use LimitSet for thread-safe acquisition
    - All limit state tracking is internal and not protected by locks
    - LimitSet is responsible for all acquisition, release, and thread-safety

Example:
    Basic limit definition (used within LimitSet)::

        from concurry import RateLimit, RateLimitAlgorithm, LimitSet

        # Define a rate limit
        limit = RateLimit(
            key="api_tokens",
            window_seconds=60,
            algorithm=RateLimitAlgorithm.TokenBucket,
            capacity=1000
        )

        # Use within a LimitSet (thread-safe)
        limits = LimitSet(limits=[limit])
        with limits.acquire(requested={"api_tokens": 100}) as acq:
            result = call_api()
            acq.update(usage={"api_tokens": result.actual_tokens})
"""

from abc import ABC
from typing import ClassVar, Dict, NoReturn

from morphic import Typed
from pydantic import confloat, conint

from .rate_limiting_algorithms import RateLimitAlgorithm, RateLimiter


class Limit(Typed, ABC):
    """Abstract base class for all limit types.

    A Limit is a simple data container that defines constraints on resource usage.
    Limits are NOT thread-safe and cannot be acquired directly. They must be used
    within a LimitSet for thread-safe acquisition and management.

    Attributes:
        key: Unique identifier for this limit within a LimitSet. Used to
            reference the limit when acquiring or updating usage.

    Thread-Safety:
        **Limits are NOT thread-safe.** All internal state (e.g., rate limiter
        implementations, current usage counters) is unprotected. LimitSet provides
        the necessary locking and synchronization for safe concurrent access.

    Usage:
        Limits should only be instantiated and used within a LimitSet:

        ```python
        # Create limit definitions
        limit = RateLimit(key="api", window_seconds=60, ...)

        # Use within thread-safe LimitSet
        limit_set = LimitSet(limits=[limit])
        with limit_set.acquire(requested={"api": 10}) as acq:
            # Safe concurrent access
            ...
        ```

    Subclass Requirements:
        Subclasses must implement:
        - can_acquire(requested): Check if limit can accommodate amount
        - validate_usage(requested, used): Validate actual usage
        - get_stats(): Return current statistics

    See Also:
        - RateLimit: Time-based rate limiting with configurable algorithms
        - CallLimit: Call counting (usage always 1)
        - ResourceLimit: Semaphore-based resource limiting
        - LimitSet: Thread-safe atomic multi-limit acquisition
    """

    key: str  # Unique identifier within a LimitSet

    def can_acquire(self, requested: int) -> bool:
        """Check if the limit can accommodate the requested amount.

        This is a non-blocking check that doesn't modify state. NOT thread-safe.

        Args:
            requested: Amount to check

        Returns:
            True if the requested amount can be acquired

        Warning:
            This method is NOT thread-safe. Only call from within LimitSet
            which provides proper synchronization.
        """
        raise NotImplementedError("Subclasses must implement can_acquire")

    def validate_usage(self, requested: int, used: int) -> None:
        """Validate that usage is valid for this limit type.

        Args:
            requested: Amount originally requested
            used: Actual amount used

        Raises:
            ValueError: If usage is invalid
        """
        raise NotImplementedError("Subclasses must implement validate_usage")

    def get_stats(self) -> dict:
        """Get current statistics for this limit.

        Returns:
            Dictionary of statistics

        Warning:
            This method is NOT thread-safe. For thread-safe stats,
            call via LimitSet.get_stats().
        """
        raise NotImplementedError("Subclasses must implement get_stats")


class RateLimit(Limit):
    """Rate-based limit using configurable rate limiting algorithms.

    RateLimits enforce time-based constraints on resource usage, such as API token
    consumption, bandwidth limits, or request rates. They support multiple algorithms
    with different performance and precision characteristics.

    Thread-Safety:
        **RateLimit is NOT thread-safe.** The internal rate limiter implementation
        (_impl) maintains unprotected state. Use within a LimitSet for thread-safe
        acquisition and management. LimitSet handles all token acquisition, refunding,
        and synchronization.

    Attributes:
        key: Unique identifier for this limit (e.g., "input_tokens", "api_calls")
        window_seconds: Time window in seconds over which the limit applies
        algorithm: Rate limiting algorithm (TokenBucket, LeakyBucket, SlidingWindow,
            FixedWindow, or GCRA)
        capacity: Maximum capacity (burst size for bucket algorithms, max count for
            window algorithms)

    Algorithms:
        - **TokenBucket**: Allows bursts up to capacity while maintaining average rate.
          Tokens refill continuously. Best for APIs that allow occasional bursts.
        - **LeakyBucket**: Processes requests at fixed rate, smoothing traffic.
          Best for predictable, steady-state traffic.
        - **SlidingWindow**: Precise rate limiting with rolling time window.
          More accurate than fixed window, higher memory usage.
        - **FixedWindow**: Simple rate limiting with fixed time buckets.
          Fastest but can allow 2x burst at window boundaries.
        - **GCRA** (Generic Cell Rate Algorithm): Most precise rate limiting using
          theoretical arrival time tracking. Best for strict rate control.

    Token Refunding:
        When actual usage is less than requested, unused tokens can be refunded back
        to the limit (up to capacity). This is algorithm-specific and handled by
        LimitSet:
        - TokenBucket and GCRA: Support refunding
        - Others: No refunding (reserved tokens count against limit)

    Example:
        Use within LimitSet::

            from concurry import RateLimit, RateLimitAlgorithm, LimitSet

            # Define rate limit
            limit = RateLimit(
                key="api_tokens",
                window_seconds=60,
                algorithm=RateLimitAlgorithm.TokenBucket,
                capacity=1000
            )

            # Use within LimitSet (thread-safe)
            limits = LimitSet(limits=[limit])
            with limits.acquire(requested={"api_tokens": 100}) as acq:
                result = call_api()
                acq.update(usage={"api_tokens": 80})  # Refund 20 tokens

    See Also:
        - CallLimit: Special case for call counting (usage always 1)
        - ResourceLimit: Non-time-based resource limiting
        - LimitSet: Thread-safe atomic multi-limit acquisition
    """

    window_seconds: confloat(gt=0)
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.SlidingWindow
    capacity: conint(gt=0)

    def post_initialize(self) -> NoReturn:
        """Initialize the rate limiter implementation."""
        # Convert max_rate from capacity per window to per second
        max_rate = self.capacity / self.window_seconds if self.window_seconds > 0 else 0

        # Use factory to create the appropriate limiter
        self._impl = RateLimiter(
            algorithm=self.algorithm,
            max_rate=max_rate,
            capacity=self.capacity,
            window_seconds=self.window_seconds,
        )

    def can_acquire(self, requested: int) -> bool:
        """Check if tokens can be acquired without consuming them.

        Warning:
            This method is NOT thread-safe. Only call from within LimitSet.
        """
        return self._impl.can_acquire(tokens=requested)

    def validate_usage(self, requested: int, used: int) -> None:
        """Validate that usage doesn't exceed requested."""
        if used > requested:
            raise ValueError(
                f"Usage ({used}) cannot exceed requested amount ({requested}) for limit '{self.key}'"
            )

    def get_stats(self) -> dict:
        """Get current rate limit statistics."""
        stats = self._impl.get_stats()
        stats["key"] = self.key
        stats["window_seconds"] = self.window_seconds
        stats["capacity"] = self.capacity
        return stats


class CallLimit(RateLimit):
    """Special RateLimit for counting individual calls.

    CallLimit is a specialized RateLimit that enforces a simpler semantic: counting
    the number of calls (acquisitions) rather than arbitrary token amounts. Usage
    must always be 1 per call, and the key is fixed to "call_count".

    Note:
        CallLimit is not thread-safe and cannot be acquired directly. Use within
        a LimitSet for thread-safe acquisition and management.

    Attributes:
        key: Always "call_count" (fixed, cannot be changed)
        window_seconds: Time window for call counting
        algorithm: Rate limiting algorithm to use
        capacity: Maximum calls allowed per window

    Characteristics:
        - Usage must always be 1 (validated on update)
        - Key is fixed to "call_count" for consistency
        - Inherits all RateLimit algorithm support
        - No need to call update() in LimitSet (handled automatically)

    Example:
        Use within LimitSet::

            from concurry import CallLimit, RateLimit, RateLimitAlgorithm, LimitSet

            limits = LimitSet(limits=[
                CallLimit(
                    window_seconds=60,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=100
                ),
                RateLimit(
                    key="tokens",
                    window_seconds=60,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=1000
                )
            ])

            # CallLimit doesn't need explicit requested or update
            with limits.acquire(requested={"tokens": 100}) as acq:
                result = do_work()
                acq.update(usage={"tokens": result.actual_tokens})
                # No need to update "call_count" - automatic!

    Notes:
        - Trying to set usage != 1 raises ValueError
        - Perfect for enforcing call rate limits independent of resource usage
        - Use RateLimit directly if you need custom keys or multi-token semantics

    See Also:
        - RateLimit: General-purpose rate limiting with custom keys
        - LimitSet: Combine CallLimit with other limits
    """

    CallLimit_key: ClassVar[str] = "call_count"

    @classmethod
    def pre_initialize(cls, data: Dict) -> NoReturn:
        data["key"] = cls.CallLimit_key  ## Force the key to be "call_count"

    def validate_usage(self, requested: int, used: int) -> None:
        """Validate that usage is always 1 for CallLimit."""
        if used != 1:
            raise ValueError(
                f"CallLimit usage must always be 1, got: {used}. "
                f"CallLimit '{self.key}' is for counting individual calls only."
            )
        super().validate_usage(requested, used)


class ResourceLimit(Limit):
    """Semaphore-based resource limiting for countable resources.

    ResourceLimits provide simple counting semantics for resources that exist in
    finite quantities, such as database connections, file handles, thread pool slots,
    or hardware devices. Unlike RateLimits, they have no time component and are
    automatically released when the context manager exits.

    Thread-Safety:
        **ResourceLimit is NOT thread-safe.** The internal _current_usage counter
        is unprotected. Use within a LimitSet for thread-safe acquisition and
        management. LimitSet handles all semaphore logic, acquisition tracking,
        and synchronization.

    Attributes:
        key: Unique identifier for this resource (e.g., "db_connections", "file_handles")
        capacity: Maximum number of resources available (must be >= 1)

    Characteristics:
        - No time component (unlike RateLimit)
        - Semaphore logic handled by LimitSet
        - Automatic release on context exit
        - No update() needed in LimitSet (handled automatically)

    Example:
        Use within LimitSet::

            from concurry import LimitSet, ResourceLimit, RateLimit, RateLimitAlgorithm

            limits = LimitSet(limits=[
                ResourceLimit(key="db_connections", capacity=5),
                ResourceLimit(key="file_handles", capacity=20),
                RateLimit(
                    key="api_tokens",
                    window_seconds=60,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=1000
                )
            ])

            # Acquire multiple resources atomically
            with limits.acquire(requested={
                "db_connections": 2,
                "file_handles": 5,
                "api_tokens": 100
            }) as acq:
                # Use resources
                acq.update(usage={"api_tokens": 80})
                # No need to update ResourceLimits - automatic!

        With Worker::

            from concurry import Worker

            class DatabaseWorker(Worker):
                def query(self, sql: str):
                    with self.limits.acquire(requested={"db_connections": 1}):
                        return execute_query(sql)

            worker = DatabaseWorker.options(
                mode="thread",
                limits=limits
            ).init()

    Notes:
        - Resources are released automatically on context exit
        - No need to call update() in LimitSet context
        - Capacity must be >= 1 (enforced at initialization)
        - Semaphore logic is managed by LimitSet for thread-safety
        - Perfect for connection pools, file handle limits, etc.

    See Also:
        - RateLimit: Time-based rate limiting
        - CallLimit: Call counting (time-based)
        - LimitSet: Combine multiple limits atomically with thread-safety
    """

    capacity: int  # Must be >= 1
    _current_usage: int = 0  # Track current usage (not thread-safe on its own)

    def post_initialize(self) -> NoReturn:
        """Validate capacity."""
        if self.capacity < 1:
            raise ValueError(f"ResourceLimit capacity must be >= 1, got: {self.capacity}")

    def can_acquire(self, requested: int) -> bool:
        """Check if resources can be acquired.

        Warning:
            This method is NOT thread-safe. Only call from within LimitSet.
        """
        return (self._current_usage + requested) <= self.capacity

    def validate_usage(self, requested: int, used: int) -> None:
        """Validate usage (not applicable for ResourceLimit).

        ResourceLimits don't have variable usage - they're automatically released.
        """
        pass

    def get_stats(self) -> dict:
        """Get current resource limit statistics.

        Note: This is not thread-safe. For thread-safe stats, call via LimitSet.
        """
        return {
            "key": self.key,
            "capacity": self.capacity,
            "current_usage": self._current_usage,
            "available": self.capacity - self._current_usage,
            "utilization": self._current_usage / self.capacity if self.capacity > 0 else 0,
        }
