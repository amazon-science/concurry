"""Tests for basic Limit functionality.

These tests verify that Limit classes are simple data containers that define
constraints. All acquisition logic is handled by LimitSet (see test_limit_set.py).
"""

import logging

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

    def test_rate_limit_validate_usage(self, caplog):
        """Test usage validation.

        validate_usage now warns instead of raising error when used > requested,
        since the spend has already occurred and cannot be undone.
        """
        limit = RateLimit(
            key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10
        )

        # Valid usage (used <= requested)
        limit.validate_usage(requested=10, used=8)  # Should not raise
        limit.validate_usage(requested=10, used=10)  # Should not raise

        # Usage exceeds requested - should warn but not raise
        with caplog.at_level(logging.WARNING):
            limit.validate_usage(requested=10, used=11)  # Now warns instead of error
            assert "exceeds requested amount" in caplog.text
            assert "tokens" in caplog.text

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
        """Test CallLimit usage validation for both implicit and explicit acquisition.

        - Implicit (requested=1): usage must be 1
        - Explicit (requested>1): usage must be in range [0, requested]
        """
        limit = CallLimit(window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10)

        # Implicit acquisition (requested=1): usage must be 1
        limit.validate_usage(requested=1, used=1)  # Should not raise

        # Invalid implicit usage (not 1 when requested=1)
        with pytest.raises(ValueError, match="implicit acquisition"):
            limit.validate_usage(requested=1, used=2)

        with pytest.raises(ValueError, match="implicit acquisition"):
            limit.validate_usage(requested=1, used=0)

        # Explicit acquisition (requested>1): usage can be 0 to requested
        limit.validate_usage(requested=5, used=5)  # Should not raise (full usage)
        limit.validate_usage(requested=5, used=3)  # Should not raise (partial usage)
        limit.validate_usage(requested=5, used=0)  # Should not raise (no usage on error)

        # Invalid explicit usage (negative)
        with pytest.raises(ValueError, match="cannot be negative"):
            limit.validate_usage(requested=5, used=-1)

        # Usage exceeding requested logs warning but doesn't raise
        # (This is inherited from RateLimit.validate_usage behavior)
        # Note: The warning is logged via Python's logging, not pytest warnings
        limit.validate_usage(requested=5, used=6)  # Should not raise

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


class TestLimitValidationEdgeCases:
    """Test edge cases for limit validation and acquisition."""

    def test_rate_limit_used_exceeds_requested_warns(self, caplog):
        """Test that RateLimit warns (doesn't error) when usage exceeds requested.

        This test verifies that when actual usage exceeds the requested amount,
        a warning is logged but no exception is raised. This is because the
        spend has already occurred and cannot be undone.
        """
        limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens", window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=1000
                )
            ]
        )

        # Request 100 tokens but use 150
        with caplog.at_level(logging.WARNING):
            with limits.acquire(requested={"tokens": 100}) as acq:
                # Update with more than requested - should warn but not error
                acq.update(usage={"tokens": 150})
                assert "exceeds requested amount" in caplog.text
                assert acq.acquisitions["tokens"].used == 150

    def test_call_limit_explicit_request_requires_update(self):
        """Test that CallLimit with explicit request > 1 requires update().

        When CallLimit is explicitly requested with value > 1, the user MUST
        call update() with usage matching the requested amount.
        """
        limits = LimitSet(
            limits=[CallLimit(window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100)]
        )

        # Explicit request for 5 calls - MUST update with 5
        with limits.acquire(requested={"call_count": 5}) as acq:
            # Must update when explicitly requested > 1
            acq.update(usage={"call_count": 5})
            assert acq.acquisitions["call_count"].requested == 5
            assert acq.acquisitions["call_count"].used == 5

    def test_call_limit_explicit_request_missing_update_fails(self):
        """Test that missing update() for explicit CallLimit request raises error.

        When CallLimit is explicitly requested with value > 1, failing to call
        update() should raise RuntimeError.
        """
        limits = LimitSet(
            limits=[CallLimit(window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100)]
        )

        # Explicit request for 5 calls without update() should fail
        with pytest.raises(RuntimeError, match="Not all limits.*were updated.*call_count"):
            with limits.acquire(requested={"call_count": 5}):
                pass  # No update() called - should raise on exit

    def test_call_limit_explicit_request_partial_usage_allowed(self):
        """Test that partial usage for explicit CallLimit request is allowed.

        When CallLimit is explicitly requested with value > 1, update() can
        specify any value in range [0, requested] to support error scenarios.
        """
        limits = LimitSet(
            limits=[CallLimit(window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100)]
        )

        # Explicit request for 10 calls, update with partial completion
        with limits.acquire(requested={"call_count": 10}) as acq:
            acq.update(usage={"call_count": 5})  # Partial - OK

        # Explicit request, update with 0 (error scenario)
        with limits.acquire(requested={"call_count": 10}) as acq:
            acq.update(usage={"call_count": 0})  # No calls succeeded - OK

        # Explicit request, update with full count
        with limits.acquire(requested={"call_count": 10}) as acq:
            acq.update(usage={"call_count": 10})  # Full usage - OK

        # Negative usage should fail
        with limits.acquire(requested={"call_count": 5}) as acq:
            with pytest.raises(ValueError, match="cannot be negative"):
                acq.update(usage={"call_count": -1})
            # Provide valid update after the failed one for clean exit
            acq.update(usage={"call_count": 5})

    def test_call_limit_implicit_no_update_needed(self):
        """Test that CallLimit with implicit request (default=1) doesn't need update().

        When CallLimit is implicitly acquired with default value of 1,
        update() is not required.
        """
        limits = LimitSet(
            limits=[
                CallLimit(window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100),
                RateLimit(
                    key="tokens", window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=1000
                ),
            ]
        )

        # Implicit CallLimit (not in requested dict) - no update needed
        with limits.acquire(requested={"tokens": 100}) as acq:
            assert acq.acquisitions["call_count"].requested == 1
            acq.update(usage={"tokens": 80})
            # No call_count update needed - automatic!

    def test_requested_exceeds_capacity_rate_limit(self):
        """Test that requesting more than capacity raises ValueError immediately.

        When requested amount exceeds limit capacity, acquisition should fail
        immediately with ValueError rather than blocking forever.
        """
        limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens", window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=1000
                )
            ]
        )

        # Request more than capacity - should raise immediately
        with pytest.raises(ValueError, match="exceeds capacity.*can never be fulfilled"):
            limits.acquire(requested={"tokens": 1500})

    def test_requested_exceeds_capacity_call_limit(self):
        """Test that CallLimit request exceeding capacity raises ValueError immediately."""
        limits = LimitSet(
            limits=[CallLimit(window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10)]
        )

        # Request more than capacity
        with pytest.raises(ValueError, match="exceeds capacity.*can never be fulfilled"):
            limits.acquire(requested={"call_count": 15})

    def test_requested_exceeds_capacity_resource_limit(self):
        """Test that ResourceLimit request exceeding capacity raises ValueError immediately."""
        limits = LimitSet(limits=[ResourceLimit(key="connections", capacity=5)])

        # Request more than capacity
        with pytest.raises(ValueError, match="exceeds capacity.*can never be fulfilled"):
            limits.acquire(requested={"connections": 10})

    def test_mixed_limits_requested_exceeds_one_capacity(self):
        """Test that exceeding capacity on one limit fails entire acquisition."""
        limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens", window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=1000
                ),
                ResourceLimit(key="connections", capacity=5),
            ]
        )

        # tokens is fine, but connections exceeds capacity
        with pytest.raises(ValueError, match="exceeds capacity.*connections"):
            limits.acquire(requested={"tokens": 100, "connections": 10})

    def test_requested_equals_capacity_succeeds(self):
        """Test that requesting exactly the capacity succeeds (edge case)."""
        limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens", window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=1000
                ),
                ResourceLimit(key="connections", capacity=5),
            ]
        )

        # Request exactly the capacity - should work
        with limits.acquire(requested={"tokens": 1000, "connections": 5}) as acq:
            assert acq.successful is True
            assert acq.acquisitions["tokens"].requested == 1000
            assert acq.acquisitions["connections"].requested == 5
            acq.update(usage={"tokens": 1000})

    def test_multiple_explicit_call_limit_requests(self):
        """Test multiple explicit CallLimit acquisitions with different amounts."""
        limits = LimitSet(
            limits=[CallLimit(window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100)]
        )

        # First acquisition: request 3
        with limits.acquire(requested={"call_count": 3}) as acq:
            acq.update(usage={"call_count": 3})

        # Second acquisition: request 5
        with limits.acquire(requested={"call_count": 5}) as acq:
            acq.update(usage={"call_count": 5})

        # Third acquisition: request 1 (implicit behavior)
        with limits.acquire(requested={"call_count": 1}) as acq:
            # requested=1 is implicit behavior, usage must be 1
            acq.update(usage={"call_count": 1})

    def test_rate_limit_usage_exceeds_with_mixed_limits(self, caplog):
        """Test RateLimit usage exceeding requested with mixed limit types."""
        limits = LimitSet(
            limits=[
                CallLimit(window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100),
                RateLimit(
                    key="tokens", window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=1000
                ),
                ResourceLimit(key="connections", capacity=5),
            ]
        )

        with caplog.at_level(logging.WARNING):
            with limits.acquire(requested={"tokens": 100, "connections": 2}) as acq:
                # Update tokens with more than requested - should warn
                acq.update(usage={"tokens": 150})
                assert "exceeds requested amount" in caplog.text
                # CallLimit is implicit (no update needed)
                # ResourceLimit is automatic (no update needed)
