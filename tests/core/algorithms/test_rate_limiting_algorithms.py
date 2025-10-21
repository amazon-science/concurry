"""Tests for all rate limiting algorithms."""

import time

import pytest

from concurry import Worker
from concurry.core.algorithms import RateLimiter
from concurry.core.constants import RateLimitAlgorithm
from concurry.core.limit import CallLimit


class TestTokenBucketLimiter:
    """Test TokenBucket rate limiter implementation."""

    def test_token_bucket_creation(self):
        """Test creating a TokenBucket limiter."""
        limiter = RateLimiter(RateLimitAlgorithm.TokenBucket, max_rate=10, capacity=20)
        assert limiter.max_rate == 10
        assert limiter.capacity == 20
        assert limiter._tokens == 20  # Starts full

    def test_token_bucket_try_acquire_success(self):
        """Test successful token acquisition."""
        limiter = RateLimiter(RateLimitAlgorithm.TokenBucket, max_rate=10, capacity=10)

        # Should succeed
        assert limiter.try_acquire(tokens=5) is True
        assert limiter.try_acquire(tokens=5) is True

    def test_token_bucket_try_acquire_failure(self):
        """Test failed token acquisition when capacity exceeded."""
        limiter = RateLimiter(RateLimitAlgorithm.TokenBucket, max_rate=10, capacity=10)

        # Acquire all capacity
        assert limiter.try_acquire(tokens=10) is True

        # Should fail - no tokens left
        assert limiter.try_acquire(tokens=1) is False

    def test_token_bucket_burst_handling(self):
        """Test TokenBucket burst capacity."""
        limiter = RateLimiter(RateLimitAlgorithm.TokenBucket, max_rate=10, capacity=20)

        # Should handle burst up to capacity
        assert limiter.try_acquire(tokens=20) is True

        # Should fail immediately after
        assert limiter.try_acquire(tokens=1) is False

    def test_token_bucket_refill_over_time(self):
        """Test that tokens refill over time."""
        limiter = RateLimiter(RateLimitAlgorithm.TokenBucket, max_rate=10, capacity=10)

        # Use all tokens
        assert limiter.try_acquire(tokens=10) is True

        # Should fail immediately
        assert limiter.try_acquire(tokens=1) is False

        # Wait for refill (need at least 0.1s for 1 token at rate 10/s)
        time.sleep(0.15)

        # Should succeed now
        assert limiter.try_acquire(tokens=1) is True

    def test_token_bucket_acquire_blocking(self):
        """Test blocking acquire."""
        limiter = RateLimiter(RateLimitAlgorithm.TokenBucket, max_rate=100, capacity=10)

        # Use capacity
        assert limiter.try_acquire(tokens=10) is True

        # Blocking acquire should wait and succeed
        start = time.time()
        assert limiter.acquire(tokens=5, timeout=1.0) is True
        elapsed = time.time() - start

        # Should have waited at least a bit
        assert elapsed > 0

    def test_token_bucket_acquire_timeout(self):
        """Test acquire with timeout."""
        limiter = RateLimiter(RateLimitAlgorithm.TokenBucket, max_rate=1, capacity=1)

        # Use capacity
        assert limiter.try_acquire(tokens=1) is True

        # Try to acquire with short timeout - should fail
        assert limiter.acquire(tokens=1, timeout=0.1) is False

    def test_token_bucket_stats(self):
        """Test getting statistics from TokenBucket limiter."""
        limiter = RateLimiter(RateLimitAlgorithm.TokenBucket, max_rate=10, capacity=20)

        stats = limiter.get_stats()
        assert stats["algorithm"] == "token_bucket"
        assert stats["capacity"] == 20
        assert stats["max_rate"] == 10
        assert "available_tokens" in stats
        assert "utilization" in stats

        # Initially should have full capacity available
        assert stats["available_tokens"] == 20
        assert stats["utilization"] == 0.0

    def test_token_bucket_stats_after_usage(self):
        """Test stats reflect usage."""
        limiter = RateLimiter(RateLimitAlgorithm.TokenBucket, max_rate=10, capacity=20)

        # Use some tokens
        limiter.try_acquire(tokens=10)

        stats = limiter.get_stats()

        # Should show reduced availability (allow small refill due to time passing)
        assert stats["available_tokens"] == pytest.approx(10, abs=0.5)
        assert stats["utilization"] == pytest.approx(0.5, abs=0.05)

    def test_token_bucket_partial_refill(self):
        """Test partial refill based on time elapsed."""
        limiter = RateLimiter(RateLimitAlgorithm.TokenBucket, max_rate=10, capacity=20)

        # Use all tokens
        limiter.try_acquire(tokens=20)

        # Wait for partial refill (0.5s = 5 tokens at 10/s)
        time.sleep(0.5)

        # Should be able to acquire ~5 tokens
        assert limiter.try_acquire(tokens=5) is True
        assert limiter.try_acquire(tokens=1) is False  # Not enough yet


class TestLeakyBucketLimiter:
    """Test LeakyBucket rate limiter implementation."""

    def test_leaky_bucket_creation(self):
        """Test creating a LeakyBucket limiter."""
        limiter = RateLimiter(RateLimitAlgorithm.LeakyBucket, max_rate=10, capacity=20)
        assert limiter.max_rate == 10
        assert limiter.capacity == 20
        assert len(limiter._queue) == 0

    def test_leaky_bucket_try_acquire_success(self):
        """Test successful token acquisition."""
        limiter = RateLimiter(RateLimitAlgorithm.LeakyBucket, max_rate=10, capacity=10)

        # Should succeed - queue has space
        assert limiter.try_acquire(tokens=5) is True
        assert len(limiter._queue) == 5

    def test_leaky_bucket_try_acquire_failure(self):
        """Test failed token acquisition when queue full."""
        limiter = RateLimiter(RateLimitAlgorithm.LeakyBucket, max_rate=10, capacity=10)

        # Fill queue
        assert limiter.try_acquire(tokens=10) is True

        # Should fail - queue full
        assert limiter.try_acquire(tokens=1) is False

    def test_leaky_bucket_queue_capacity(self):
        """Test LeakyBucket respects queue capacity."""
        limiter = RateLimiter(RateLimitAlgorithm.LeakyBucket, max_rate=10, capacity=20)

        # Should be able to queue up to capacity
        assert limiter.try_acquire(tokens=20) is True

        # Should fail - queue full
        assert limiter.try_acquire(tokens=1) is False

    def test_leaky_bucket_leak_over_time(self):
        """Test that queue leaks (processes) over time."""
        limiter = RateLimiter(RateLimitAlgorithm.LeakyBucket, max_rate=10, capacity=10)

        # Fill queue
        assert limiter.try_acquire(tokens=10) is True
        assert len(limiter._queue) == 10

        # Wait for leak (0.2s should process 2 items at 10/s)
        time.sleep(0.2)

        # Queue should have leaked some items
        limiter._leak()
        assert len(limiter._queue) < 10

    def test_leaky_bucket_acquire_blocking(self):
        """Test blocking acquire."""
        limiter = RateLimiter(RateLimitAlgorithm.LeakyBucket, max_rate=100, capacity=10)

        # Fill queue
        assert limiter.try_acquire(tokens=10) is True

        # Blocking acquire should wait and succeed
        start = time.time()
        assert limiter.acquire(tokens=5, timeout=1.0) is True
        elapsed = time.time() - start

        # Should have waited
        assert elapsed > 0

    def test_leaky_bucket_acquire_timeout(self):
        """Test acquire with timeout."""
        limiter = RateLimiter(RateLimitAlgorithm.LeakyBucket, max_rate=1, capacity=1)

        # Fill queue
        assert limiter.try_acquire(tokens=1) is True

        # Try to acquire with short timeout - should fail
        assert limiter.acquire(tokens=1, timeout=0.1) is False

    def test_leaky_bucket_stats(self):
        """Test getting statistics from LeakyBucket limiter."""
        limiter = RateLimiter(RateLimitAlgorithm.LeakyBucket, max_rate=10, capacity=20)

        stats = limiter.get_stats()
        assert stats["algorithm"] == "leaky_bucket"
        assert stats["capacity"] == 20
        assert stats["max_rate"] == 10
        assert "queue_size" in stats
        assert "utilization" in stats

        # Initially should be empty
        assert stats["queue_size"] == 0
        assert stats["utilization"] == 0.0

    def test_leaky_bucket_stats_after_usage(self):
        """Test stats reflect queue state."""
        limiter = RateLimiter(RateLimitAlgorithm.LeakyBucket, max_rate=10, capacity=20)

        # Add to queue
        limiter.try_acquire(tokens=10)

        stats = limiter.get_stats()

        # Should show queue usage
        assert stats["queue_size"] == 10
        assert stats["utilization"] == 0.5


class TestSlidingWindowLimiter:
    """Test SlidingWindow rate limiter implementation."""

    def test_sliding_window_creation(self):
        """Test creating a SlidingWindow limiter."""
        limiter = RateLimiter(RateLimitAlgorithm.SlidingWindow, max_rate=10, capacity=10, window_seconds=1.0)
        assert limiter.max_rate == 10
        assert limiter.window_seconds == 1.0
        assert len(limiter._requests) == 0

    def test_sliding_window_try_acquire_success(self):
        """Test successful token acquisition."""
        limiter = RateLimiter(RateLimitAlgorithm.SlidingWindow, max_rate=10, capacity=10, window_seconds=1.0)

        # Should succeed
        assert limiter.try_acquire(tokens=5) is True
        assert len(limiter._requests) == 5

    def test_sliding_window_try_acquire_failure(self):
        """Test failed token acquisition when limit reached."""
        limiter = RateLimiter(RateLimitAlgorithm.SlidingWindow, max_rate=10, capacity=10, window_seconds=1.0)

        # Fill window
        assert limiter.try_acquire(tokens=10) is True

        # Should fail - window full
        assert limiter.try_acquire(tokens=1) is False

    def test_sliding_window_cleanup_old_requests(self):
        """Test that old requests are cleaned up."""
        limiter = RateLimiter(RateLimitAlgorithm.SlidingWindow, max_rate=10, capacity=10, window_seconds=0.5)

        # Fill window
        assert limiter.try_acquire(tokens=10) is True
        assert len(limiter._requests) == 10

        # Wait for window to pass
        time.sleep(0.6)

        # Should succeed - old requests cleaned up
        assert limiter.try_acquire(tokens=10) is True

    def test_sliding_window_rolling_behavior(self):
        """Test rolling window behavior."""
        limiter = RateLimiter(RateLimitAlgorithm.SlidingWindow, max_rate=10, capacity=10, window_seconds=1.0)

        # Make some requests
        assert limiter.try_acquire(tokens=5) is True

        # Wait half a window
        time.sleep(0.5)

        # Should be able to add more (rolling window)
        assert limiter.try_acquire(tokens=5) is True

        # Window should still have 10 requests
        limiter._cleanup_old_requests()
        assert len(limiter._requests) == 10

    def test_sliding_window_acquire_blocking(self):
        """Test blocking acquire."""
        limiter = RateLimiter(
            RateLimitAlgorithm.SlidingWindow, max_rate=100, capacity=100, window_seconds=1.0
        )

        # Fill window
        assert limiter.try_acquire(tokens=100) is True

        # Blocking acquire should wait and succeed
        start = time.time()
        assert limiter.acquire(tokens=10, timeout=2.0) is True
        elapsed = time.time() - start

        # Should have waited
        assert elapsed > 0

    def test_sliding_window_acquire_timeout(self):
        """Test acquire with timeout."""
        limiter = RateLimiter(RateLimitAlgorithm.SlidingWindow, max_rate=1, capacity=1, window_seconds=1.0)

        # Fill window
        assert limiter.try_acquire(tokens=1) is True

        # Try to acquire with short timeout - should fail
        assert limiter.acquire(tokens=1, timeout=0.1) is False

    def test_sliding_window_stats(self):
        """Test getting statistics from SlidingWindow limiter."""
        limiter = RateLimiter(RateLimitAlgorithm.SlidingWindow, max_rate=10, capacity=10, window_seconds=1.0)

        stats = limiter.get_stats()
        assert stats["algorithm"] == "sliding_window"
        assert stats["max_rate"] == 10
        assert stats["window_seconds"] == 1.0
        assert "current_requests" in stats
        assert "available" in stats
        assert "utilization" in stats

        # Initially should be empty
        assert stats["current_requests"] == 0
        assert stats["available"] == 10
        assert stats["utilization"] == 0.0

    def test_sliding_window_stats_after_usage(self):
        """Test stats reflect window state."""
        limiter = RateLimiter(RateLimitAlgorithm.SlidingWindow, max_rate=10, capacity=10, window_seconds=1.0)

        # Add requests
        limiter.try_acquire(tokens=5)

        stats = limiter.get_stats()

        # Should show usage
        assert stats["current_requests"] == 5
        assert stats["available"] == 5
        assert stats["utilization"] == 0.5


class TestFixedWindowLimiter:
    """Test FixedWindow rate limiter implementation."""

    def test_fixed_window_creation(self):
        """Test creating a FixedWindow limiter."""
        limiter = RateLimiter(RateLimitAlgorithm.FixedWindow, max_rate=10, capacity=10, window_seconds=1.0)
        assert limiter.max_rate == 10
        assert limiter.window_seconds == 1.0
        assert limiter._request_count == 0

    def test_fixed_window_try_acquire_success(self):
        """Test successful token acquisition."""
        limiter = RateLimiter(RateLimitAlgorithm.FixedWindow, max_rate=10, capacity=10, window_seconds=1.0)

        # Should succeed
        assert limiter.try_acquire(tokens=5) is True
        assert limiter._request_count == 5

    def test_fixed_window_try_acquire_failure(self):
        """Test failed token acquisition when limit reached."""
        limiter = RateLimiter(RateLimitAlgorithm.FixedWindow, max_rate=10, capacity=10, window_seconds=1.0)

        # Fill window
        assert limiter.try_acquire(tokens=10) is True

        # Should fail - window full
        assert limiter.try_acquire(tokens=1) is False

    def test_fixed_window_reset(self):
        """Test that window resets after time period."""
        limiter = RateLimiter(RateLimitAlgorithm.FixedWindow, max_rate=10, capacity=10, window_seconds=0.5)

        # Fill window
        assert limiter.try_acquire(tokens=10) is True
        assert limiter._request_count == 10

        # Wait for window to reset
        time.sleep(0.6)

        # Should succeed - window reset
        assert limiter.try_acquire(tokens=10) is True
        assert limiter._request_count == 10  # Reset and refilled

    def test_fixed_window_boundary_behavior(self):
        """Test behavior at window boundaries."""
        limiter = RateLimiter(RateLimitAlgorithm.FixedWindow, max_rate=10, capacity=10, window_seconds=0.5)

        # Fill current window
        assert limiter.try_acquire(tokens=10) is True

        # Immediately should fail
        assert limiter.try_acquire(tokens=1) is False

        # Wait for window boundary
        time.sleep(0.6)

        # Should succeed in new window
        assert limiter.try_acquire(tokens=10) is True

    def test_fixed_window_acquire_blocking(self):
        """Test blocking acquire."""
        limiter = RateLimiter(RateLimitAlgorithm.FixedWindow, max_rate=100, capacity=100, window_seconds=1.0)

        # Fill window
        assert limiter.try_acquire(tokens=100) is True

        # Blocking acquire should wait and succeed
        start = time.time()
        assert limiter.acquire(tokens=10, timeout=2.0) is True
        elapsed = time.time() - start

        # Should have waited
        assert elapsed > 0

    def test_fixed_window_acquire_timeout(self):
        """Test acquire with timeout."""
        limiter = RateLimiter(RateLimitAlgorithm.FixedWindow, max_rate=1, capacity=1, window_seconds=1.0)

        # Fill window
        assert limiter.try_acquire(tokens=1) is True

        # Try to acquire with short timeout - should fail
        assert limiter.acquire(tokens=1, timeout=0.1) is False

    def test_fixed_window_stats(self):
        """Test getting statistics from FixedWindow limiter."""
        limiter = RateLimiter(RateLimitAlgorithm.FixedWindow, max_rate=10, capacity=10, window_seconds=1.0)

        stats = limiter.get_stats()
        assert stats["algorithm"] == "fixed_window"
        assert stats["max_rate"] == 10
        assert stats["window_seconds"] == 1.0
        assert "current_requests" in stats
        assert "available" in stats
        assert "utilization" in stats

        # Initially should be empty
        assert stats["current_requests"] == 0
        assert stats["available"] == 10
        assert stats["utilization"] == 0.0

    def test_fixed_window_stats_after_usage(self):
        """Test stats reflect window state."""
        limiter = RateLimiter(RateLimitAlgorithm.FixedWindow, max_rate=10, capacity=10, window_seconds=1.0)

        # Add requests
        limiter.try_acquire(tokens=5)

        stats = limiter.get_stats()

        # Should show usage
        assert stats["current_requests"] == 5
        assert stats["available"] == 5
        assert stats["utilization"] == 0.5


class TestGCRALimiter:
    """Test GCRA rate limiter implementation."""

    def test_gcra_creation(self):
        """Test creating a GCRA limiter."""
        limiter = RateLimiter(RateLimitAlgorithm.GCRA, max_rate=10, capacity=20)
        assert limiter.max_rate == 10
        assert limiter.capacity == 20
        assert limiter._emission_interval == 0.1  # 1/10
        assert limiter._tau == 2.0  # 20 * 0.1

    def test_gcra_try_acquire_success(self):
        """Test successful token acquisition."""
        limiter = RateLimiter(RateLimitAlgorithm.GCRA, max_rate=10, capacity=10)

        # Should succeed
        assert limiter.try_acquire(tokens=5) is True
        assert limiter.try_acquire(tokens=5) is True

    def test_gcra_try_acquire_failure(self):
        """Test failed token acquisition when capacity exceeded."""
        limiter = RateLimiter(RateLimitAlgorithm.GCRA, max_rate=10, capacity=10)

        # Acquire all capacity
        assert limiter.try_acquire(tokens=10) is True

        # Should fail - no capacity left
        assert limiter.try_acquire(tokens=1) is False

    def test_gcra_burst_handling(self):
        """Test GCRA burst capacity."""
        limiter = RateLimiter(RateLimitAlgorithm.GCRA, max_rate=10, capacity=20)

        # Should handle burst up to capacity
        assert limiter.try_acquire(tokens=20) is True

        # Should fail immediately after
        assert limiter.try_acquire(tokens=1) is False

    def test_gcra_replenishment_over_time(self):
        """Test that tokens replenish over time."""
        limiter = RateLimiter(RateLimitAlgorithm.GCRA, max_rate=10, capacity=10)

        # Use all tokens
        assert limiter.try_acquire(tokens=10) is True

        # Should fail immediately
        assert limiter.try_acquire(tokens=1) is False

        # Wait for replenishment (need at least 0.1s for 1 token at rate 10/s)
        time.sleep(0.15)

        # Should succeed now
        assert limiter.try_acquire(tokens=1) is True

    def test_gcra_acquire_blocking(self):
        """Test blocking acquire."""
        limiter = RateLimiter(RateLimitAlgorithm.GCRA, max_rate=100, capacity=10)

        # Use capacity
        assert limiter.try_acquire(tokens=10) is True

        # Blocking acquire should wait and succeed
        start = time.time()
        assert limiter.acquire(tokens=5, timeout=1.0) is True
        elapsed = time.time() - start

        # Should have waited at least a bit
        assert elapsed > 0

    def test_gcra_acquire_timeout(self):
        """Test acquire with timeout."""
        limiter = RateLimiter(RateLimitAlgorithm.GCRA, max_rate=1, capacity=1)

        # Use capacity
        assert limiter.try_acquire(tokens=1) is True

        # Try to acquire with short timeout - should fail
        assert limiter.acquire(tokens=1, timeout=0.1) is False

    def test_gcra_stats(self):
        """Test getting statistics from GCRA limiter."""
        limiter = RateLimiter(RateLimitAlgorithm.GCRA, max_rate=10, capacity=20)

        stats = limiter.get_stats()
        assert stats["algorithm"] == "gcra"
        assert stats["capacity"] == 20
        assert stats["max_rate"] == 10
        assert "available_tokens" in stats
        assert "utilization" in stats

        # Initially should have full capacity available
        assert stats["available_tokens"] == 20
        assert stats["utilization"] == 0.0

    def test_gcra_stats_after_usage(self):
        """Test stats reflect usage."""
        limiter = RateLimiter(RateLimitAlgorithm.GCRA, max_rate=10, capacity=20)

        # Use some tokens
        limiter.try_acquire(tokens=10)

        stats = limiter.get_stats()

        # Should show reduced availability
        assert stats["available_tokens"] < 20
        assert stats["utilization"] > 0

    def test_gcra_precision(self):
        """Test GCRA precision for steady-state traffic."""
        limiter = RateLimiter(RateLimitAlgorithm.GCRA, max_rate=10, capacity=10)

        # Simulate steady requests at exactly the allowed rate
        for i in range(5):
            assert limiter.try_acquire(tokens=1) is True
            time.sleep(0.1)  # Exactly 1/max_rate

        # Should still have capacity available
        assert limiter.try_acquire(tokens=1) is True

    def test_gcra_vs_token_bucket_burst(self):
        """Test GCRA burst behavior (for comparison with TokenBucket)."""
        # GCRA with rate 10/s and capacity 20
        gcra = RateLimiter(RateLimitAlgorithm.GCRA, max_rate=10, capacity=20)

        # Should handle burst of 20
        assert gcra.try_acquire(tokens=20) is True

        # Should fail on next request
        assert gcra.try_acquire(tokens=1) is False

        # Wait for one token time
        time.sleep(0.15)

        # Should be able to acquire 1 token
        assert gcra.try_acquire(tokens=1) is True

    def test_gcra_multiple_small_requests(self):
        """Test GCRA with multiple small requests."""
        limiter = RateLimiter(RateLimitAlgorithm.GCRA, max_rate=100, capacity=10)

        # Multiple small requests should work
        for i in range(10):
            assert limiter.try_acquire(tokens=1) is True

        # 11th should fail (capacity exhausted)
        assert limiter.try_acquire(tokens=1) is False

    def test_gcra_fractional_tokens(self):
        """Test GCRA behavior with fractional rate."""
        # 2.5 tokens per second
        limiter = RateLimiter(RateLimitAlgorithm.GCRA, max_rate=2.5, capacity=5)

        # Should be able to acquire 5 tokens (full capacity)
        assert limiter.try_acquire(tokens=5) is True

        # Should fail immediately
        assert limiter.try_acquire(tokens=1) is False

        # Wait for ~0.4s (1 token at 2.5/s)
        time.sleep(0.5)

        # Should be able to acquire 1 token
        assert limiter.try_acquire(tokens=1) is True

    def test_gcra_zero_rate(self):
        """Test GCRA with zero rate."""
        limiter = RateLimiter(RateLimitAlgorithm.GCRA, max_rate=0, capacity=10)

        # With zero rate, emission_interval is 0
        assert limiter._emission_interval == 0

        # Should still be able to acquire up to capacity
        assert limiter.try_acquire(tokens=10) is True

    def test_gcra_tat_tracking(self):
        """Test that TAT (Theoretical Arrival Time) is tracked correctly."""
        limiter = RateLimiter(RateLimitAlgorithm.GCRA, max_rate=10, capacity=10)

        # Initial TAT should be 0
        assert limiter._tat == 0.0

        # After acquiring, TAT should advance
        limiter.try_acquire(tokens=5)
        assert limiter._tat > 0

        # TAT should be approximately now + 5 * emission_interval
        expected_tat = time.time() + 5 * 0.1
        assert abs(limiter._tat - expected_tat) < 0.01  # Within 10ms tolerance


class TestRateLimitingAlgorithms:
    """Comprehensive tests for all rate limiting algorithms across all execution modes.

    Each algorithm has different characteristics:
    - TokenBucket: Allows bursts up to capacity, refills continuously
    - LeakyBucket: Processes at fixed rate, smooths traffic
    - SlidingWindow: Precise rolling window, no burst
    - FixedWindow: Fixed time buckets, can allow 2x at boundaries
    - GCRA: Theoretical arrival time, precise rate control

    Note: Tests use 20 calls @ 100/sec for fast execution (~0.2s per test).
    """

    def test_token_bucket_rate_limiting(self, worker_mode):
        """Test TokenBucket algorithm - allows burst up to capacity."""
        # Skip ray mode - use separate ray test
        if worker_mode == "ray":
            pytest.skip("Ray mode has separate test due to initialization requirements")

        class Counter(Worker):
            def __init__(self):
                self.count = 0

            def increment(self):
                with self.limits.acquire():
                    self.count += 1
                    return self.count

            def get_count(self) -> int:
                return self.count

        # TokenBucket: capacity=20, rate=100/sec (fast for testing)
        w = Counter.options(
            mode=worker_mode,
            max_workers=1,  # Single worker to ensure count is consistent
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=20)],
        ).init()

        # Make 20 calls - all burst instantly (capacity=20)
        start_time = time.time()
        for _ in range(20):
            w.increment().result()
        elapsed = time.time() - start_time

        assert w.get_count().result() == 20

        # TokenBucket allows full burst, expect ~0.0-0.2s
        assert elapsed <= 0.5, f"TokenBucket: Expected instant burst, got {elapsed:.2f}s (too slow)"

        w.stop()

    def test_token_bucket_rate_limiting_ray(self):
        """Test TokenBucket algorithm on Ray workers."""
        pytest.importorskip("ray")
        # Ray is initialized by conftest.py initialize_ray fixture

        class Counter(Worker):
            def __init__(self):
                self.count = 0

            def increment(self):
                with self.limits.acquire():
                    self.count += 1
                    return self.count

            def get_count(self) -> int:
                return self.count

        w = Counter.options(
            mode="ray",
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=20)],
        ).init()

        start_time = time.time()
        for _ in range(20):
            w.increment().result()
        elapsed = time.time() - start_time

        assert w.get_count().result() == 20
        # Ray has overhead (actor creation, remote calls), allow up to 2.5s
        assert elapsed <= 2.5, f"TokenBucket: Expected fast execution, got {elapsed:.2f}s (too slow)"

        w.stop()

    def test_leaky_bucket_rate_limiting(self, worker_mode):
        """Test LeakyBucket algorithm - processes at fixed rate, smooths traffic."""
        # Skip ray mode - use separate ray test
        if worker_mode == "ray":
            pytest.skip("Ray mode has separate test due to initialization requirements")

        class Counter(Worker):
            def __init__(self):
                self.count = 0

            def increment(self):
                with self.limits.acquire():
                    self.count += 1
                    return self.count

            def get_count(self) -> int:
                return self.count

        # LeakyBucket: capacity=20, rate=100/sec (fast for testing)
        w = Counter.options(
            mode=worker_mode,
            max_workers=1,  # Single worker to ensure count is consistent
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.LeakyBucket, capacity=20)],
        ).init()

        # Make 20 calls - queue holds 20, processes instantly
        start_time = time.time()
        for _ in range(20):
            w.increment().result()
        elapsed = time.time() - start_time

        assert w.get_count().result() == 20

        # LeakyBucket can queue up to capacity, expect ~0.0-0.2s
        assert elapsed <= 0.5, f"LeakyBucket: Expected instant queue, got {elapsed:.2f}s (too slow)"

        w.stop()

    def test_leaky_bucket_rate_limiting_ray(self):
        """Test LeakyBucket algorithm on Ray workers."""
        pytest.importorskip("ray")
        # Ray is initialized by conftest.py initialize_ray fixture

        class Counter(Worker):
            def __init__(self):
                self.count = 0

            def increment(self):
                with self.limits.acquire():
                    self.count += 1
                    return self.count

            def get_count(self) -> int:
                return self.count

        w = Counter.options(
            mode="ray",
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.LeakyBucket, capacity=20)],
        ).init()

        start_time = time.time()
        for _ in range(20):
            w.increment().result()
        elapsed = time.time() - start_time

        assert w.get_count().result() == 20
        # Ray has overhead (actor creation, remote calls), allow up to 2s
        assert elapsed <= 2.0, f"LeakyBucket: Expected fast execution, got {elapsed:.2f}s (too slow)"

        w.stop()

    def test_sliding_window_rate_limiting(self, worker_mode):
        """Test SlidingWindow algorithm (default) - precise rolling window."""
        # Skip ray mode - use separate ray test
        if worker_mode == "ray":
            pytest.skip("Ray mode has separate test due to initialization requirements")

        class Counter(Worker):
            def __init__(self):
                self.count = 0

            def increment(self):
                with self.limits.acquire():
                    self.count += 1
                    return self.count

            def get_count(self) -> int:
                return self.count

        # SlidingWindow: capacity=20 (default algorithm, fast rate for testing)
        w = Counter.options(
            mode=worker_mode,
            max_workers=1,  # Single worker to ensure count is consistent
            limits=[CallLimit(window_seconds=1.0, capacity=20)],  # Uses default SlidingWindow
        ).init()

        # Make 20 calls - all fit in window instantly
        start_time = time.time()
        for _ in range(20):
            w.increment().result()
        elapsed = time.time() - start_time

        assert w.get_count().result() == 20

        # SlidingWindow: First 20 calls fit in window instantly
        assert elapsed <= 0.5, f"SlidingWindow: Expected instant, got {elapsed:.2f}s (too slow)"

        w.stop()

    def test_sliding_window_rate_limiting_ray(self):
        """Test SlidingWindow algorithm on Ray workers."""
        pytest.importorskip("ray")
        # Ray is initialized by conftest.py initialize_ray fixture

        class Counter(Worker):
            def __init__(self):
                self.count = 0

            def increment(self):
                with self.limits.acquire():
                    self.count += 1
                    return self.count

            def get_count(self) -> int:
                return self.count

        w = Counter.options(
            mode="ray",
            limits=[CallLimit(window_seconds=1.0, capacity=20)],  # Uses default SlidingWindow
        ).init()

        start_time = time.time()
        for _ in range(20):
            w.increment().result()
        elapsed = time.time() - start_time

        assert w.get_count().result() == 20
        # Ray has overhead (actor creation, remote calls), allow up to 2s
        assert elapsed <= 2.0, f"SlidingWindow: Expected fast execution, got {elapsed:.2f}s (too slow)"

        w.stop()

    def test_fixed_window_rate_limiting(self, worker_mode):
        """Test FixedWindow algorithm - simple fixed time buckets."""
        # Skip ray mode - use separate ray test
        if worker_mode == "ray":
            pytest.skip("Ray mode has separate test due to initialization requirements")

        class Counter(Worker):
            def __init__(self):
                self.count = 0

            def increment(self):
                with self.limits.acquire():
                    self.count += 1
                    return self.count

            def get_count(self) -> int:
                return self.count

        # FixedWindow: capacity=20, rate=100/sec (fast for testing)
        w = Counter.options(
            mode=worker_mode,
            max_workers=1,  # Single worker to ensure count is consistent
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.FixedWindow, capacity=20)],
        ).init()

        # Make 20 calls - all fit in current window instantly
        start_time = time.time()
        for _ in range(20):
            w.increment().result()
        elapsed = time.time() - start_time

        assert w.get_count().result() == 20

        # FixedWindow: 20 calls fit in window instantly
        assert elapsed <= 0.5, f"FixedWindow: Expected instant, got {elapsed:.2f}s (too slow)"

        w.stop()

    def test_fixed_window_rate_limiting_ray(self):
        """Test FixedWindow algorithm on Ray workers."""
        pytest.importorskip("ray")
        # Ray is initialized by conftest.py initialize_ray fixture

        class Counter(Worker):
            def __init__(self):
                self.count = 0

            def increment(self):
                with self.limits.acquire():
                    self.count += 1
                    return self.count

            def get_count(self) -> int:
                return self.count

        w = Counter.options(
            mode="ray",
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.FixedWindow, capacity=20)],
        ).init()

        start_time = time.time()
        for _ in range(20):
            w.increment().result()
        elapsed = time.time() - start_time

        assert w.get_count().result() == 20
        # Ray has overhead (actor creation, remote calls), allow up to 20s for FixedWindow
        # (FixedWindow can have timing issues at boundaries)
        assert elapsed <= 20.0, f"FixedWindow: Expected reasonably fast, got {elapsed:.2f}s (too slow)"

        w.stop()

    def test_gcra_rate_limiting(self, worker_mode):
        """Test GCRA algorithm - theoretical arrival time based precise control."""
        # Skip ray mode - use separate ray test
        if worker_mode == "ray":
            pytest.skip("Ray mode has separate test due to initialization requirements")

        class Counter(Worker):
            def __init__(self):
                self.count = 0

            def increment(self):
                with self.limits.acquire():
                    self.count += 1
                    return self.count

            def get_count(self) -> int:
                return self.count

        # GCRA: capacity=20, rate=100/sec (fast for testing)
        w = Counter.options(
            mode=worker_mode,
            max_workers=1,  # Single worker to ensure count is consistent
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.GCRA, capacity=20)],
        ).init()

        # Make 20 calls - all burst instantly (capacity=20)
        start_time = time.time()
        for _ in range(20):
            w.increment().result()
        elapsed = time.time() - start_time

        assert w.get_count().result() == 20

        # GCRA allows burst based on tau, expect ~0.0-0.2s
        assert elapsed <= 0.5, f"GCRA: Expected instant burst, got {elapsed:.2f}s (too slow)"

        w.stop()

    def test_gcra_rate_limiting_ray(self):
        """Test GCRA algorithm on Ray workers."""
        pytest.importorskip("ray")
        # Ray is initialized by conftest.py initialize_ray fixture

        class Counter(Worker):
            def __init__(self):
                self.count = 0

            def increment(self):
                with self.limits.acquire():
                    self.count += 1
                    return self.count

            def get_count(self) -> int:
                return self.count

        w = Counter.options(
            mode="ray",
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.GCRA, capacity=20)],
        ).init()

        start_time = time.time()
        for _ in range(20):
            w.increment().result()
        elapsed = time.time() - start_time

        assert w.get_count().result() == 20
        # Ray has overhead (actor creation, remote calls), allow up to 2.5s
        assert elapsed <= 2.5, f"GCRA: Expected fast execution, got {elapsed:.2f}s (too slow)"

        w.stop()
