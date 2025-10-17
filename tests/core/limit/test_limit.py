"""Tests for basic Limit functionality.

These tests verify that Limit classes are simple data containers that define
constraints. All acquisition logic is handled by LimitSet (see test_limit_set.py).
"""

import pytest

from concurry import CallLimit, LimitSet, RateLimit, RateLimitAlgorithm, ResourceLimit


class TestRateLimit:
    """Test RateLimit class as a simple data container."""

    def test_rate_limit_creation(self):
        """Test creating a RateLimit."""
        limit = RateLimit(
            key="test_tokens", window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100
        )
        assert limit.key == "test_tokens"
        assert limit.window_seconds == 60
        assert limit.capacity == 100

    def test_rate_limit_can_acquire(self):
        """Test can_acquire check (non-blocking, doesn't modify state)."""
        limit = RateLimit(
            key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10
        )

        # Initially should be able to acquire
        assert limit.can_acquire(5) is True
        assert limit.can_acquire(10) is True

        # can_acquire doesn't modify state
        assert limit.can_acquire(10) is True

    def test_rate_limit_validate_usage(self):
        """Test usage validation."""
        limit = RateLimit(
            key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10
        )

        # Valid usage (used <= requested)
        limit.validate_usage(requested=10, used=8)  # Should not raise
        limit.validate_usage(requested=10, used=10)  # Should not raise

        # Invalid usage (used > requested)
        with pytest.raises(ValueError, match="cannot exceed requested"):
            limit.validate_usage(requested=10, used=11)

    def test_rate_limit_get_stats(self):
        """Test getting statistics."""
        limit = RateLimit(
            key="tokens", window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100
        )

        stats = limit.get_stats()
        assert "key" in stats
        assert stats["key"] == "tokens"
        assert "capacity" in stats
        assert "window_seconds" in stats

    def test_rate_limit_algorithms(self):
        """Test different rate limiting algorithms."""
        algorithms = [
            RateLimitAlgorithm.TokenBucket,
            RateLimitAlgorithm.LeakyBucket,
            RateLimitAlgorithm.SlidingWindow,
            RateLimitAlgorithm.FixedWindow,
            RateLimitAlgorithm.GCRA,
        ]

        for algo in algorithms:
            limit = RateLimit(key="tokens", window_seconds=1, algorithm=algo, capacity=10)
            assert limit.algorithm == algo
            # Should be able to check acquisition
            assert limit.can_acquire(5) is True

    def test_rate_limit_must_use_limitset(self):
        """Test that RateLimits must be used within LimitSet for acquisition."""
        limit = RateLimit(
            key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10
        )

        # Direct acquisition not supported
        with pytest.raises(AttributeError):
            limit.acquire(requested=5)

        # Must use LimitSet
        limit_set = LimitSet(limits=[limit])
        with limit_set.acquire(requested={"tokens": 5}) as acq:
            assert acq.successful is True
            acq.update(usage={"tokens": 5})


class TestCallLimit:
    """Test CallLimit class."""

    def test_call_limit_creation(self):
        """Test creating a CallLimit."""
        limit = CallLimit(window_seconds=60, algorithm=RateLimitAlgorithm.SlidingWindow, capacity=100)
        # Key is automatically set to "call_count"
        assert limit.key == "call_count"
        assert limit.window_seconds == 60
        assert limit.capacity == 100

    def test_call_limit_key_is_fixed(self):
        """Test that CallLimit key is always 'call_count'."""
        limit = CallLimit(window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10)
        assert limit.key == "call_count"

    def test_call_limit_validate_usage(self):
        """Test that CallLimit usage must always be 1."""
        limit = CallLimit(window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10)

        # Valid usage (must be 1)
        limit.validate_usage(requested=1, used=1)  # Should not raise

        # Invalid usage (not 1)
        with pytest.raises(ValueError, match="must always be 1"):
            limit.validate_usage(requested=1, used=2)

        with pytest.raises(ValueError, match="must always be 1"):
            limit.validate_usage(requested=1, used=0)

    def test_call_limit_with_limitset(self):
        """Test CallLimit usage within LimitSet."""
        limit = CallLimit(window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=5)

        limit_set = LimitSet(limits=[limit])

        # CallLimit defaults to 1 in LimitSet
        with limit_set.acquire() as acq:
            assert acq.successful is True
            # No need to update CallLimit - it's automatic


class TestResourceLimit:
    """Test ResourceLimit class."""

    def test_resource_limit_creation(self):
        """Test creating a ResourceLimit."""
        limit = ResourceLimit(key="connections", capacity=10)
        assert limit.key == "connections"
        assert limit.capacity == 10
        assert limit._current_usage == 0

    def test_resource_limit_capacity_validation(self):
        """Test that capacity must be >= 1."""
        # Valid capacity
        limit = ResourceLimit(key="connections", capacity=1)
        assert limit.capacity == 1

        # Invalid capacity
        with pytest.raises(ValueError, match="must be >= 1"):
            ResourceLimit(key="connections", capacity=0)

    def test_resource_limit_can_acquire(self):
        """Test can_acquire check."""
        limit = ResourceLimit(key="connections", capacity=5)

        # Initially should be able to acquire
        assert limit.can_acquire(1) is True
        assert limit.can_acquire(5) is True

        # Can't acquire more than capacity
        assert limit.can_acquire(6) is False

        # Simulate usage (internal tracking - NOT thread-safe)
        limit._current_usage = 3
        assert limit.can_acquire(2) is True
        assert limit.can_acquire(3) is False

    def test_resource_limit_validate_usage(self):
        """Test that ResourceLimit doesn't validate usage (automatic release)."""
        limit = ResourceLimit(key="connections", capacity=5)

        # validate_usage is a no-op for ResourceLimit
        limit.validate_usage(requested=3, used=3)  # Should not raise
        limit.validate_usage(requested=3, used=0)  # Should not raise
        limit.validate_usage(requested=3, used=10)  # Should not raise

    def test_resource_limit_get_stats(self):
        """Test getting statistics."""
        limit = ResourceLimit(key="connections", capacity=10)
        limit._current_usage = 3

        stats = limit.get_stats()
        assert stats["key"] == "connections"
        assert stats["capacity"] == 10
        assert stats["current_usage"] == 3
        assert stats["available"] == 7
        assert stats["utilization"] == 0.3

    def test_resource_limit_must_use_limitset(self):
        """Test that ResourceLimits must be used within LimitSet for acquisition."""
        limit = ResourceLimit(key="connections", capacity=5)

        # Direct acquisition not supported
        with pytest.raises(AttributeError):
            limit.acquire(requested=2)

        # Must use LimitSet
        limit_set = LimitSet(limits=[limit])
        with limit_set.acquire(requested={"connections": 2}) as acq:
            assert acq.successful is True
            # No need to update ResourceLimit - automatic release


class TestLimitThreadSafety:
    """Test that Limits are NOT thread-safe (as documented)."""

    def test_ratelimit_not_thread_safe(self):
        """Verify that RateLimit internal state is not thread-safe."""
        limit = RateLimit(
            key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100
        )

        # Direct manipulation of internal state (not thread-safe)
        # This is just demonstrating that Limit doesn't protect its state
        assert hasattr(limit, "_impl")

        # Thread-safety is provided by LimitSet, not by Limit
        # For actual acquisition, use LimitSet

    def test_resourcelimit_not_thread_safe(self):
        """Verify that ResourceLimit internal state is not thread-safe."""
        limit = ResourceLimit(key="connections", capacity=10)

        # Direct manipulation of internal state (not thread-safe)
        limit._current_usage = 5
        assert limit._current_usage == 5

        # Thread-safety is provided by LimitSet, not by Limit
        # For actual acquisition, use LimitSet


class TestEmptyLimitSet:
    """Test empty LimitSet behavior (no limits configured)."""

    def test_empty_limitset_creation(self):
        """Test creating an empty LimitSet."""
        limit_set = LimitSet(limits=[], shared=False, mode="sync")
        assert len(limit_set.limits) == 0

    def test_empty_limitset_acquire_always_succeeds(self):
        """Test that empty LimitSet always allows acquisition."""
        limit_set = LimitSet(limits=[], shared=False, mode="sync")

        # Acquire without arguments
        with limit_set.acquire() as acq:
            assert acq.successful is True
            assert len(acq.acquisitions) == 0

    def test_empty_limitset_try_acquire_always_succeeds(self):
        """Test that empty LimitSet try_acquire always succeeds."""
        limit_set = LimitSet(limits=[], shared=False, mode="sync")

        acq = limit_set.try_acquire()
        assert acq.successful is True
        assert len(acq.acquisitions) == 0

    def test_empty_limitset_acquire_with_empty_requested(self):
        """Test empty LimitSet acquire with empty requested dict."""
        limit_set = LimitSet(limits=[], shared=False, mode="sync")

        with limit_set.acquire(requested={}) as acq:
            assert acq.successful is True
            assert len(acq.acquisitions) == 0

    def test_empty_limitset_multiple_acquires(self):
        """Test multiple acquisitions on empty LimitSet (never blocks)."""
        limit_set = LimitSet(limits=[], shared=False, mode="sync")

        # Multiple sequential acquisitions - all should succeed immediately
        for i in range(10):
            with limit_set.acquire() as acq:
                assert acq.successful is True

    def test_empty_limitset_get_stats(self):
        """Test get_stats on empty LimitSet."""
        limit_set = LimitSet(limits=[], shared=False, mode="sync")
        stats = limit_set.get_stats()
        assert stats == {}

    def test_empty_limitset_shared_mode(self):
        """Test empty LimitSet in shared mode."""
        limit_set = LimitSet(limits=[], shared=True, mode="thread")
        assert limit_set.shared is True

        with limit_set.acquire() as acq:
            assert acq.successful is True
