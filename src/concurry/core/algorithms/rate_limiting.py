"""Rate limiting algorithms for resource protection."""

import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Deque, List, Optional

from morphic import MutableTyped, Registry
from pydantic import ConfigDict, PrivateAttr

from ..constants import RateLimitAlgorithm


class BaseRateLimiter(Registry, MutableTyped, ABC):
    """Abstract base class for rate limiting implementations.

    Provides a unified interface for different rate limiting algorithms.
    All algorithm implementations should inherit from this class.
    """

    model_config = ConfigDict(extra="ignore")

    @abstractmethod
    def acquire(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """Acquire tokens from the rate limiter.

        Args:
            tokens: Number of tokens to acquire
            timeout: Maximum time to wait for tokens

        Returns:
            True if tokens were acquired, False if timeout
        """
        pass

    @abstractmethod
    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens without blocking.

        Args:
            tokens: Number of tokens to acquire

        Returns:
            True if tokens were acquired immediately
        """
        pass

    @abstractmethod
    def can_acquire(self, tokens: int = 1) -> bool:
        """Check if tokens can be acquired without consuming them.

        This is a non-consuming check used by LimitSet to validate
        that all limits can be satisfied before atomically acquiring them.

        Args:
            tokens: Number of tokens to check

        Returns:
            True if tokens could be acquired
        """
        pass

    @abstractmethod
    def get_stats(self) -> dict:
        """Get current rate limiter statistics.

        Returns:
            Dictionary containing algorithm-specific statistics
        """
        pass

    @abstractmethod
    def refund(self, tokens: int) -> None:
        """Refund tokens back to the limiter.

        This is used when actual usage is less than requested.
        Not all algorithms support refunding.

        Args:
            tokens: Number of tokens to refund
        """
        pass


class TokenBucketLimiter(BaseRateLimiter):
    """Token Bucket rate limiting algorithm.

    Tokens are added to a bucket at a fixed rate. Requests consume tokens.
    Allows bursts up to bucket capacity while maintaining average rate.

    Best for: APIs that allow occasional bursts but need average rate control.
    """

    aliases = ["token_bucket", "token", RateLimitAlgorithm.TokenBucket]

    max_rate: float
    capacity: int

    _tokens: float = PrivateAttr()
    _last_update: float = PrivateAttr()

    def post_initialize(self) -> None:
        """Initialize private attributes after validation."""
        self._tokens = float(self.capacity)
        self._last_update = time.time()

    def _refill(self) -> None:
        """Refill the bucket based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_update

        # Add tokens based on elapsed time
        tokens_to_add = elapsed * self.max_rate
        self._tokens = min(self.capacity, self._tokens + tokens_to_add)
        self._last_update = now

    def can_acquire(self, tokens: int = 1) -> bool:
        """Check if tokens can be acquired without consuming them."""
        self._refill()
        return self._tokens >= tokens

    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens without blocking."""
        self._refill()

        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    def acquire(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """Acquire tokens, waiting if necessary."""
        start_time = time.time()

        while True:
            if self.try_acquire(tokens):
                return True

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False

            # Calculate wait time until we'll have enough tokens
            self._refill()
            tokens_needed = tokens - self._tokens
            wait_time = tokens_needed / self.max_rate if tokens_needed > 0 else 0.01

            if timeout is not None:
                wait_time = min(wait_time, timeout - (time.time() - start_time))

            if wait_time > 0:
                time.sleep(wait_time)

    def get_stats(self) -> dict:
        """Get current statistics."""
        self._refill()
        return {
            "algorithm": "token_bucket",
            "available_tokens": self._tokens,
            "capacity": self.capacity,
            "max_rate": self.max_rate,
            "utilization": 1.0 - (self._tokens / self.capacity),
        }

    def refund(self, tokens: int) -> None:
        """Refund tokens back to the bucket.

        TokenBucket supports refunding unused tokens.

        Args:
            tokens: Number of tokens to refund
        """
        self._tokens = min(self.capacity, self._tokens + tokens)


class LeakyBucketLimiter(BaseRateLimiter):
    """Leaky Bucket rate limiting algorithm.

    Requests are added to a queue and processed at a fixed rate.
    Smooths out traffic but may reject requests during bursts.

    Best for: Scenarios requiring smooth, predictable traffic flow.
    """

    aliases = ["leaky_bucket", "leaky", RateLimitAlgorithm.LeakyBucket]

    max_rate: float
    capacity: int

    _queue: Deque[float] = PrivateAttr()
    _last_leak: float = PrivateAttr()

    def post_initialize(self) -> None:
        """Initialize private attributes after validation."""
        self._queue = deque()
        self._last_leak = time.time()

    def _leak(self) -> None:
        """Process (leak) requests from the queue."""
        now = time.time()
        elapsed = now - self._last_leak

        # Calculate how many items to leak
        items_to_leak = int(elapsed * self.max_rate)

        for _ in range(min(items_to_leak, len(self._queue))):
            self._queue.popleft()

        self._last_leak = now

    def can_acquire(self, tokens: int = 1) -> bool:
        """Check if tokens can be acquired without consuming them."""
        self._leak()
        return len(self._queue) + tokens <= self.capacity

    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to add to the queue."""
        self._leak()

        if len(self._queue) + tokens <= self.capacity:
            for _ in range(tokens):
                self._queue.append(time.time())
            return True
        return False

    def acquire(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """Add to queue, waiting if necessary."""
        start_time = time.time()

        while True:
            if self.try_acquire(tokens):
                return True

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False

            # Wait for queue to drain
            self._leak()
            wait_time = 1.0 / self.max_rate if self.max_rate > 0 else 0.01

            if timeout is not None:
                remaining = timeout - (time.time() - start_time)
                wait_time = min(wait_time, remaining)

            if wait_time > 0:
                time.sleep(wait_time)

    def get_stats(self) -> dict:
        """Get current statistics."""
        self._leak()
        return {
            "algorithm": "leaky_bucket",
            "queue_size": len(self._queue),
            "capacity": self.capacity,
            "max_rate": self.max_rate,
            "utilization": len(self._queue) / self.capacity if self.capacity > 0 else 0,
        }

    def refund(self, tokens: int) -> None:
        """Refund tokens (no-op for LeakyBucket).

        LeakyBucket doesn't support refunding as it uses a queue-based approach.
        Once tokens are added to the queue, they count against the limit.

        Args:
            tokens: Number of tokens to refund (ignored)
        """
        # LeakyBucket doesn't support refunding
        pass


class SlidingWindowLimiter(BaseRateLimiter):
    """Sliding Window rate limiting algorithm.

    Maintains a rolling window of request timestamps.
    More accurate than fixed window but higher memory usage.

    Best for: Precise rate limiting without fixed window edge cases.
    """

    aliases = ["sliding_window", "sliding", RateLimitAlgorithm.SlidingWindow]

    max_rate: float
    window_seconds: float = 1.0

    _requests: List[float] = PrivateAttr()

    def post_initialize(self) -> None:
        """Initialize private attributes after validation."""
        self._requests = []

    def _cleanup_old_requests(self) -> None:
        """Remove requests outside the current window."""
        cutoff_time = time.time() - self.window_seconds
        self._requests = [ts for ts in self._requests if ts > cutoff_time]

    def can_acquire(self, tokens: int = 1) -> bool:
        """Check if tokens can be acquired without consuming them."""
        self._cleanup_old_requests()
        return len(self._requests) + tokens <= self.max_rate

    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire without blocking."""
        self._cleanup_old_requests()

        if len(self._requests) + tokens <= self.max_rate:
            now = time.time()
            for _ in range(tokens):
                self._requests.append(now)
            return True
        return False

    def acquire(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """Acquire, waiting if necessary."""
        start_time = time.time()

        while True:
            if self.try_acquire(tokens):
                return True

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False

            # Wait for oldest request to age out
            self._cleanup_old_requests()

            if len(self._requests) > 0:
                oldest = self._requests[0]
                wait_time = (oldest + self.window_seconds) - time.time()
                wait_time = max(0.01, wait_time)
            else:
                wait_time = 0.01

            if timeout is not None:
                remaining = timeout - (time.time() - start_time)
                wait_time = min(wait_time, remaining)

            if wait_time > 0:
                time.sleep(wait_time)

    def get_stats(self) -> dict:
        """Get current statistics."""
        self._cleanup_old_requests()
        return {
            "algorithm": "sliding_window",
            "current_requests": len(self._requests),
            "max_rate": self.max_rate,
            "window_seconds": self.window_seconds,
            "available": self.max_rate - len(self._requests),
            "utilization": len(self._requests) / self.max_rate if self.max_rate > 0 else 0,
        }

    def refund(self, tokens: int) -> None:
        """Refund tokens (no-op for SlidingWindow).

        SlidingWindow doesn't support refunding as it tracks timestamps.
        Once a request is recorded, it counts against the limit.

        Args:
            tokens: Number of tokens to refund (ignored)
        """
        # SlidingWindow doesn't support refunding
        pass


class FixedWindowLimiter(BaseRateLimiter):
    """Fixed Window rate limiting algorithm.

    Counts requests in fixed time windows. Simple but can have edge case issues
    where 2x max_rate requests occur around window boundary.

    Best for: Simple rate limiting where edge cases are acceptable.
    """

    aliases = ["fixed_window", "fixed", RateLimitAlgorithm.FixedWindow]

    max_rate: float
    window_seconds: float = 1.0

    _window_start: float = PrivateAttr()
    _request_count: int = PrivateAttr(default=0)

    def post_initialize(self) -> None:
        """Initialize private attributes after validation."""
        self._window_start = time.time()
        self._request_count = 0

    def _check_window_reset(self) -> None:
        """Reset counter if window has passed."""
        now = time.time()
        if now - self._window_start >= self.window_seconds:
            self._window_start = now
            self._request_count = 0

    def can_acquire(self, tokens: int = 1) -> bool:
        """Check if tokens can be acquired without consuming them."""
        self._check_window_reset()
        return self._request_count + tokens <= self.max_rate

    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire without blocking."""
        self._check_window_reset()

        if self._request_count + tokens <= self.max_rate:
            self._request_count += tokens
            return True
        return False

    def acquire(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """Acquire, waiting if necessary."""
        start_time = time.time()

        while True:
            if self.try_acquire(tokens):
                return True

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False

            # Wait for window to reset
            self._check_window_reset()
            wait_time = (self._window_start + self.window_seconds) - time.time()
            wait_time = max(0.01, wait_time)

            if timeout is not None:
                remaining = timeout - (time.time() - start_time)
                wait_time = min(wait_time, remaining)

            if wait_time > 0:
                time.sleep(wait_time)

    def get_stats(self) -> dict:
        """Get current statistics."""
        self._check_window_reset()
        return {
            "algorithm": "fixed_window",
            "current_requests": self._request_count,
            "max_rate": self.max_rate,
            "window_seconds": self.window_seconds,
            "available": self.max_rate - self._request_count,
            "utilization": self._request_count / self.max_rate if self.max_rate > 0 else 0,
        }

    def refund(self, tokens: int) -> None:
        """Refund tokens (no-op for FixedWindow).

        FixedWindow doesn't support refunding as it uses a counter approach.
        Once requests are counted, they count against the limit.

        Args:
            tokens: Number of tokens to refund (ignored)
        """
        # FixedWindow doesn't support refunding
        pass


class GCRALimiter(BaseRateLimiter):
    """Generic Cell Rate Algorithm (GCRA) rate limiter.

    Also known as Virtual Scheduling algorithm. Tracks a theoretical arrival
    time (TAT) to determine if requests arrive too early. More precise than
    token bucket for steady-state traffic.

    Best for: Precise rate limiting with better burst handling for steady streams.
    """

    aliases = ["gcra", RateLimitAlgorithm.GCRA]

    max_rate: float
    capacity: int

    _emission_interval: float = PrivateAttr()
    _tau: float = PrivateAttr()
    _tat: float = PrivateAttr(default=0.0)

    def post_initialize(self) -> None:
        """Initialize private attributes after validation."""
        # Time between requests (emission interval)
        self._emission_interval = 1.0 / self.max_rate if self.max_rate > 0 else 0

        # Maximum burst time (tau)
        self._tau = self.capacity * self._emission_interval

        # Theoretical Arrival Time - tracks when next request should arrive
        self._tat = 0.0

    def can_acquire(self, tokens: int = 1) -> bool:
        """Check if tokens can be acquired without consuming them."""
        now = time.time()
        new_tat = max(self._tat, now) + (tokens * self._emission_interval)
        return new_tat - now <= self._tau

    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens without blocking."""
        now = time.time()

        # Calculate new TAT if we were to accept this request
        # TAT' = max(TAT, now) + tokens * emission_interval
        new_tat = max(self._tat, now) + (tokens * self._emission_interval)

        # Check if request would exceed burst capacity
        # Allow if: new_tat - now <= tau (burst tolerance)
        if new_tat - now <= self._tau:
            self._tat = new_tat
            return True
        return False

    def acquire(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """Acquire tokens, waiting if necessary."""
        start_time = time.time()

        while True:
            if self.try_acquire(tokens):
                return True

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False

            # Calculate wait time
            now = time.time()
            new_tat = max(self._tat, now) + (tokens * self._emission_interval)
            wait_time = new_tat - now - self._tau

            if wait_time > 0:
                if timeout is not None:
                    remaining = timeout - (time.time() - start_time)
                    wait_time = min(wait_time, remaining)

                if wait_time > 0:
                    time.sleep(wait_time)
            else:
                time.sleep(0.01)

    def get_stats(self) -> dict:
        """Get current statistics."""
        now = time.time()

        # Calculate how many tokens are currently available
        # Available capacity = (TAT - now) / emission_interval
        if self._tat > now:
            used_capacity = (self._tat - now) / self._emission_interval
            available = max(0, self.capacity - used_capacity)
        else:
            available = self.capacity

        return {
            "algorithm": "gcra",
            "available_tokens": available,
            "capacity": self.capacity,
            "max_rate": self.max_rate,
            "emission_interval": self._emission_interval,
            "utilization": 1.0 - (available / self.capacity) if self.capacity > 0 else 0,
        }

    def refund(self, tokens: int) -> None:
        """Refund tokens by adjusting TAT backwards.

        GCRA supports refunding by moving the Theoretical Arrival Time backwards.

        Args:
            tokens: Number of tokens to refund
        """
        self._tat = max(time.time(), self._tat - (tokens * self._emission_interval))


def RateLimiter(
    algorithm: RateLimitAlgorithm,
    max_rate: float,
    capacity: int,
    window_seconds: Optional[float] = None,
) -> BaseRateLimiter:
    """Factory function to create the appropriate rate limiter using Registry pattern.

    Args:
        algorithm: The rate limiting algorithm to use
        max_rate: Maximum rate (requests per second for token/leaky bucket, total for window algorithms)
        capacity: Maximum capacity (burst size or window size)
        window_seconds: Window duration in seconds (for window-based algorithms)

    Returns:
        BaseRateLimiter instance of the appropriate type

    Raises:
        ValueError: If algorithm is not recognized

    Example:
        ```python
        limiter = RateLimiter(
            algorithm=RateLimitAlgorithm.TokenBucket,
            max_rate=10,
            capacity=20
        )
        ```
    """
    # For window-based algorithms, max_rate should be the total capacity within the window
    # For token/leaky bucket, max_rate is requests per second
    if algorithm in (RateLimitAlgorithm.SlidingWindow, RateLimitAlgorithm.FixedWindow):
        # Use capacity as max_rate for window algorithms
        return BaseRateLimiter.of(
            algorithm, max_rate=capacity, capacity=capacity, window_seconds=window_seconds
        )
    else:
        # Use max_rate as-is for token/leaky bucket
        return BaseRateLimiter.of(
            algorithm, max_rate=max_rate, capacity=capacity, window_seconds=window_seconds
        )
