"""Rate limiting algorithms for resource protection."""

import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Deque, List, Optional

from ..config import RateLimitAlgorithm


class BaseRateLimiter(ABC):
    """Abstract base class for rate limiting implementations.

    Provides a unified interface for different rate limiting algorithms.
    All algorithm implementations should inherit from this class.
    """

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

    def __init__(self, max_rate: float, capacity: int):
        """Initialize token bucket limiter.

        Args:
            max_rate: Token generation rate (tokens per second)
            capacity: Maximum bucket capacity (max burst size)
        """
        self.max_rate = max_rate
        self.capacity = capacity
        self.tokens = float(capacity)
        self.last_update = time.time()

    def _refill(self):
        """Refill the bucket based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_update

        # Add tokens based on elapsed time
        tokens_to_add = elapsed * self.max_rate
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_update = now

    def can_acquire(self, tokens: int = 1) -> bool:
        """Check if tokens can be acquired without consuming them."""
        self._refill()
        return self.tokens >= tokens

    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens without blocking."""
        self._refill()

        if self.tokens >= tokens:
            self.tokens -= tokens
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
            tokens_needed = tokens - self.tokens
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
            "available_tokens": self.tokens,
            "capacity": self.capacity,
            "max_rate": self.max_rate,
            "utilization": 1.0 - (self.tokens / self.capacity),
        }

    def refund(self, tokens: int) -> None:
        """Refund tokens back to the bucket.

        TokenBucket supports refunding unused tokens.

        Args:
            tokens: Number of tokens to refund
        """
        self.tokens = min(self.capacity, self.tokens + tokens)


class LeakyBucketLimiter(BaseRateLimiter):
    """Leaky Bucket rate limiting algorithm.

    Requests are added to a queue and processed at a fixed rate.
    Smooths out traffic but may reject requests during bursts.

    Best for: Scenarios requiring smooth, predictable traffic flow.
    """

    def __init__(self, max_rate: float, capacity: int):
        """Initialize leaky bucket limiter.

        Args:
            max_rate: Processing rate (requests per second)
            capacity: Maximum queue size
        """
        self.max_rate = max_rate
        self.capacity = capacity
        self.queue: Deque[float] = deque()
        self.last_leak = time.time()

    def _leak(self):
        """Process (leak) requests from the queue."""
        now = time.time()
        elapsed = now - self.last_leak

        # Calculate how many items to leak
        items_to_leak = int(elapsed * self.max_rate)

        for _ in range(min(items_to_leak, len(self.queue))):
            self.queue.popleft()

        self.last_leak = now

    def can_acquire(self, tokens: int = 1) -> bool:
        """Check if tokens can be acquired without consuming them."""
        self._leak()
        return len(self.queue) + tokens <= self.capacity

    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to add to the queue."""
        self._leak()

        if len(self.queue) + tokens <= self.capacity:
            for _ in range(tokens):
                self.queue.append(time.time())
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
            "queue_size": len(self.queue),
            "capacity": self.capacity,
            "max_rate": self.max_rate,
            "utilization": len(self.queue) / self.capacity if self.capacity > 0 else 0,
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

    def __init__(self, max_rate: float, window_seconds: float = 1.0):
        """Initialize sliding window limiter.

        Args:
            max_rate: Maximum requests per window
            window_seconds: Window duration in seconds
        """
        self.max_rate = max_rate
        self.window_seconds = window_seconds
        self.requests: List[float] = []

    def _cleanup_old_requests(self):
        """Remove requests outside the current window."""
        cutoff_time = time.time() - self.window_seconds
        self.requests = [ts for ts in self.requests if ts > cutoff_time]

    def can_acquire(self, tokens: int = 1) -> bool:
        """Check if tokens can be acquired without consuming them."""
        self._cleanup_old_requests()
        return len(self.requests) + tokens <= self.max_rate

    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire without blocking."""
        self._cleanup_old_requests()

        if len(self.requests) + tokens <= self.max_rate:
            now = time.time()
            for _ in range(tokens):
                self.requests.append(now)
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

            if self.requests:
                oldest = self.requests[0]
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
            "current_requests": len(self.requests),
            "max_rate": self.max_rate,
            "window_seconds": self.window_seconds,
            "available": self.max_rate - len(self.requests),
            "utilization": len(self.requests) / self.max_rate if self.max_rate > 0 else 0,
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

    def __init__(self, max_rate: float, window_seconds: float = 1.0):
        """Initialize fixed window limiter.

        Args:
            max_rate: Maximum requests per window
            window_seconds: Window duration in seconds
        """
        self.max_rate = max_rate
        self.window_seconds = window_seconds
        self.window_start = time.time()
        self.request_count = 0

    def _check_window_reset(self):
        """Reset counter if window has passed."""
        now = time.time()
        if now - self.window_start >= self.window_seconds:
            self.window_start = now
            self.request_count = 0

    def can_acquire(self, tokens: int = 1) -> bool:
        """Check if tokens can be acquired without consuming them."""
        self._check_window_reset()
        return self.request_count + tokens <= self.max_rate

    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire without blocking."""
        self._check_window_reset()

        if self.request_count + tokens <= self.max_rate:
            self.request_count += tokens
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
            wait_time = (self.window_start + self.window_seconds) - time.time()
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
            "current_requests": self.request_count,
            "max_rate": self.max_rate,
            "window_seconds": self.window_seconds,
            "available": self.max_rate - self.request_count,
            "utilization": self.request_count / self.max_rate if self.max_rate > 0 else 0,
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

    def __init__(self, max_rate: float, capacity: int):
        """Initialize GCRA limiter.

        Args:
            max_rate: Maximum rate (requests per second)
            capacity: Burst capacity (maximum tokens that can accumulate)
        """
        self.max_rate = max_rate
        self.capacity = capacity

        # Time between requests (emission interval)
        self.emission_interval = 1.0 / max_rate if max_rate > 0 else 0

        # Maximum burst time (tau)
        self.tau = capacity * self.emission_interval

        # Theoretical Arrival Time - tracks when next request should arrive
        self.tat = 0.0

    def can_acquire(self, tokens: int = 1) -> bool:
        """Check if tokens can be acquired without consuming them."""
        now = time.time()
        new_tat = max(self.tat, now) + (tokens * self.emission_interval)
        return new_tat - now <= self.tau

    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens without blocking."""
        now = time.time()

        # Calculate new TAT if we were to accept this request
        # TAT' = max(TAT, now) + tokens * emission_interval
        new_tat = max(self.tat, now) + (tokens * self.emission_interval)

        # Check if request would exceed burst capacity
        # Allow if: new_tat - now <= tau (burst tolerance)
        if new_tat - now <= self.tau:
            self.tat = new_tat
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
            new_tat = max(self.tat, now) + (tokens * self.emission_interval)
            wait_time = new_tat - now - self.tau

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
        if self.tat > now:
            used_capacity = (self.tat - now) / self.emission_interval
            available = max(0, self.capacity - used_capacity)
        else:
            available = self.capacity

        return {
            "algorithm": "gcra",
            "available_tokens": available,
            "capacity": self.capacity,
            "max_rate": self.max_rate,
            "emission_interval": self.emission_interval,
            "utilization": 1.0 - (available / self.capacity) if self.capacity > 0 else 0,
        }

    def refund(self, tokens: int) -> None:
        """Refund tokens by adjusting TAT backwards.

        GCRA supports refunding by moving the Theoretical Arrival Time backwards.

        Args:
            tokens: Number of tokens to refund
        """
        emission_interval = self.emission_interval
        self.tat = max(time.time(), self.tat - (tokens * emission_interval))


def RateLimiter(
    algorithm: RateLimitAlgorithm,
    max_rate: float,
    capacity: int,
    window_seconds: Optional[float] = None,
) -> BaseRateLimiter:
    """Factory function to create the appropriate rate limiter.

    Args:
        algorithm: The rate limiting algorithm to use
        max_rate: Maximum rate (requests per second)
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
    if algorithm == RateLimitAlgorithm.TokenBucket:
        return TokenBucketLimiter(max_rate=max_rate, capacity=capacity)
    elif algorithm == RateLimitAlgorithm.LeakyBucket:
        return LeakyBucketLimiter(max_rate=max_rate, capacity=capacity)
    elif algorithm == RateLimitAlgorithm.SlidingWindow:
        return SlidingWindowLimiter(max_rate=capacity, window_seconds=window_seconds or 1.0)
    elif algorithm == RateLimitAlgorithm.FixedWindow:
        return FixedWindowLimiter(max_rate=capacity, window_seconds=window_seconds or 1.0)
    elif algorithm == RateLimitAlgorithm.GCRA:
        return GCRALimiter(max_rate=max_rate, capacity=capacity)
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")
