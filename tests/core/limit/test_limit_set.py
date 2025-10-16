"""Tests for LimitSet functionality."""

import pytest

from concurry import (
    CallLimit,
    LimitSet,
    RateLimit,
    RateLimitAlgorithm,
    ResourceLimit,
)


class TestLimitSet:
    """Test LimitSet class."""

    def test_limit_set_creation(self):
        """Test creating a LimitSet."""
        limits = LimitSet(
            limits=[
                CallLimit(window_seconds=60, algorithm=RateLimitAlgorithm.SlidingWindow, capacity=100),
                RateLimit(
                    key="input_tokens",
                    window_seconds=60,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=1000,
                ),
                ResourceLimit(key="connections", capacity=10),
            ]
        )

        assert len(limits.limits) == 3
        assert len(limits._limits_by_key) == 3

    def test_limit_set_duplicate_keys(self):
        """Test that duplicate keys raise error."""
        with pytest.raises(ValueError, match="Duplicate limit key"):
            LimitSet(
                limits=[
                    RateLimit(
                        key="tokens",
                        window_seconds=60,
                        algorithm=RateLimitAlgorithm.TokenBucket,
                        capacity=100,
                    ),
                    RateLimit(
                        key="tokens",
                        window_seconds=60,
                        algorithm=RateLimitAlgorithm.TokenBucket,
                        capacity=200,
                    ),
                ]
            )

    def test_limit_set_acquire_with_defaults(self):
        """Test acquiring LimitSet with default values."""
        limits = LimitSet(
            limits=[
                CallLimit(window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100),
                ResourceLimit(key="connections", capacity=10),
            ]
        )

        # Should use default of 1 for CallLimit and ResourceLimit
        with limits.acquire() as acq:
            assert len(acq.acquisitions) == 2
            assert acq.acquisitions["call_count"].requested == 1
            assert acq.acquisitions["connections"].requested == 1

    def test_limit_set_acquire_explicit_requested(self):
        """Test acquiring LimitSet with explicit requested amounts."""
        limits = LimitSet(
            limits=[
                RateLimit(
                    key="input_tokens",
                    window_seconds=1,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=1000,
                ),
                ResourceLimit(key="connections", capacity=10),
            ]
        )

        with limits.acquire(requested={"input_tokens": 100, "connections": 2}) as acq:
            assert acq.acquisitions["input_tokens"].requested == 100
            assert acq.acquisitions["connections"].requested == 2

            # Update the rate limit
            acq.update(usage={"input_tokens": 80})

    def test_limit_set_missing_rate_limit_requested(self):
        """Test that missing RateLimit in requested raises error."""
        limits = LimitSet(
            limits=[
                RateLimit(
                    key="input_tokens",
                    window_seconds=60,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=1000,
                ),
            ]
        )

        with pytest.raises(ValueError, match="Must specify requested amount for RateLimit"):
            limits.acquire()

    def test_limit_set_atomic_acquisition(self):
        """Test that all limits are acquired atomically."""
        limits = LimitSet(
            limits=[ResourceLimit(key="conn1", capacity=1), ResourceLimit(key="conn2", capacity=1)]
        )

        # Acquire conn1 only
        acq1 = limits.acquire(requested={"conn1": 1})

        # Try to acquire both - should fail because conn1 is taken
        acq_set = limits.try_acquire()
        assert acq_set.successful is False

        # Release conn1
        acq1.release()

        # Now should succeed
        acq_set2 = limits.try_acquire()
        assert acq_set2.successful is True
        acq_set2.release()

    def test_limit_set_update_validation(self):
        """Test that update validates keys."""
        limits = LimitSet(
            limits=[
                RateLimit(
                    key="input_tokens",
                    window_seconds=1,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=1000,
                ),
            ]
        )

        with limits.acquire(requested={"input_tokens": 100}) as acq:
            # Valid update
            acq.update(usage={"input_tokens": 80})

            # Invalid key should raise error
            with pytest.raises(ValueError, match="Cannot update limit.*not acquired"):
                acq.update(usage={"output_tokens": 50})

    def test_limit_set_missing_updates(self):
        """Test that missing updates raise error on exit."""
        limits = LimitSet(
            limits=[
                RateLimit(
                    key="input_tokens",
                    window_seconds=1,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=1000,
                ),
                RateLimit(
                    key="output_tokens",
                    window_seconds=1,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=500,
                ),
            ]
        )

        with pytest.raises(RuntimeError, match="Not all limits in the LimitSet were updated"):
            with limits.acquire(requested={"input_tokens": 100, "output_tokens": 50}) as acq:
                # Only update input_tokens, not output_tokens
                acq.update(usage={"input_tokens": 80})
                # exit will raise error

    def test_limit_set_no_update_needed_for_resources(self):
        """Test that ResourceLimits don't need updates."""
        limits = LimitSet(limits=[ResourceLimit(key="connections", capacity=10)])

        # Should not raise error even without update
        with limits.acquire(requested={"connections": 2}) as acq:
            pass  # No update needed for ResourceLimit

    def test_limit_set_no_update_needed_for_call_limit(self):
        """Test that CallLimits don't need explicit updates."""
        limits = LimitSet(
            limits=[CallLimit(window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100)]
        )

        # Should not raise error even without update
        with limits.acquire() as acq:
            pass  # No update needed for CallLimit

    def test_limit_set_mixed_limits_update_requirements(self):
        """Test update requirements with mixed limit types."""
        limits = LimitSet(
            limits=[
                CallLimit(window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100),
                RateLimit(
                    key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=1000
                ),
                ResourceLimit(key="connections", capacity=10),
            ]
        )

        # Only RateLimit (not CallLimit) needs update
        with limits.acquire(requested={"tokens": 100, "connections": 1}) as acq:
            acq.update(usage={"tokens": 80})
            # CallLimit and ResourceLimit don't need updates

    def test_limit_set_try_acquire(self):
        """Test try_acquire for LimitSet."""
        limits = LimitSet(limits=[ResourceLimit(key="connections", capacity=1)])

        # First try should succeed
        acq1 = limits.try_acquire()
        assert acq1.successful is True

        # Second try should fail
        acq2 = limits.try_acquire()
        assert acq2.successful is False

        # Release
        acq1.release()

    def test_limit_set_stats(self):
        """Test getting stats from LimitSet."""
        limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=1000
                ),
                ResourceLimit(key="connections", capacity=10),
            ]
        )

        stats = limits.get_stats()
        assert "tokens" in stats
        assert "connections" in stats
        assert stats["connections"]["capacity"] == 10

    def test_limit_set_timeout(self):
        """Test that acquire with timeout raises TimeoutError."""
        limits = LimitSet(limits=[ResourceLimit(key="connections", capacity=1)])

        # Acquire the only connection
        acq1 = limits.acquire()

        # Try to acquire with short timeout - should fail
        with pytest.raises(TimeoutError):
            limits.acquire(timeout=0.1)

        acq1.release()

    def test_limit_set_context_manager_on_failed_try_acquire(self):
        """Test using context manager with failed try_acquire."""
        limits = LimitSet(limits=[ResourceLimit(key="connections", capacity=1)])

        # Acquire the connection
        acq1 = limits.acquire()

        # Try acquire should fail, but context manager should work
        with limits.try_acquire() as acq2:
            assert acq2.successful is False

        acq1.release()

    def test_limit_set_update_after_release(self):
        """Test that updating after release raises error."""
        limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=1000
                ),
            ]
        )

        acq = limits.acquire(requested={"tokens": 100})
        acq.update(usage={"tokens": 80})
        acq.release()

        with pytest.raises(RuntimeError, match="Cannot update an already released"):
            acq.update(usage={"tokens": 70})

    def test_limit_set_nested_acquisition(self):
        """Test nested acquisition pattern."""
        limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=1000
                ),
                ResourceLimit(key="connections", capacity=10),
            ]
        )

        # Outer: acquire resources
        with limits.acquire(requested={"connections": 2}) as outer_acq:
            # Inner: acquire rate limits
            with limits.acquire(requested={"tokens": 100}) as inner_acq:
                inner_acq.update(usage={"tokens": 80})
            # outer_acq (resources) releases automatically without update


class TestLimitSetSharedModes:
    """Test LimitSet shared and mode parameters."""

    def test_limitset_default_not_shared(self):
        """Test that LimitSet defaults to shared=False, mode='sync'."""
        from concurry.core.limit.limit_set import InMemorySharedLimitSet

        limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100
                )
            ]
        )

        # Default is shared=False, mode="sync" which creates InMemorySharedLimitSet
        assert isinstance(limits, InMemorySharedLimitSet)

    def test_limitset_non_shared_must_be_sync(self):
        """Test that non-shared LimitSets must have mode='sync'."""
        from concurry.core.limit.limit_set import InMemorySharedLimitSet

        # Valid: shared=False, mode="sync"
        limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100
                )
            ],
            shared=False,
            mode="sync",
        )
        assert isinstance(limits, InMemorySharedLimitSet)

        # Invalid: shared=False, mode="thread"
        with pytest.raises(ValueError, match="Non-shared LimitSets cannot use mode='process'"):
            LimitSet(
                limits=[
                    RateLimit(
                        key="tokens",
                        window_seconds=1,
                        algorithm=RateLimitAlgorithm.TokenBucket,
                        capacity=100,
                    )
                ],
                shared=False,
                mode="process",
            )

    def test_limitset_shared_sync_mode(self):
        """Test creating shared LimitSet with sync/thread/asyncio mode."""
        from concurry.core.limit.limit_set import InMemorySharedLimitSet

        # All these should create InMemorySharedLimitSet
        for mode in ["sync", "thread", "asyncio"]:
            limits = LimitSet(
                limits=[
                    RateLimit(
                        key="tokens",
                        window_seconds=1,
                        algorithm=RateLimitAlgorithm.TokenBucket,
                        capacity=100,
                    )
                ],
                shared=True,
                mode=mode,
            )
            assert isinstance(limits, InMemorySharedLimitSet)

    def test_limitset_shared_process_mode(self):
        """Test creating shared LimitSet with process mode."""
        from concurry.core.limit.limit_set import MultiprocessSharedLimitSet

        limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100
                )
            ],
            shared=True,
            mode="process",
        )
        assert isinstance(limits, MultiprocessSharedLimitSet)

    def test_limitset_shared_invalid_mode(self):
        """Test that invalid mode raises error."""
        with pytest.raises(ValueError, match="Could not find enum with value"):
            LimitSet(
                limits=[
                    RateLimit(
                        key="tokens",
                        window_seconds=1,
                        algorithm=RateLimitAlgorithm.TokenBucket,
                        capacity=100,
                    )
                ],
                shared=True,
                mode="invalid",
            )

    def test_limitset_thread_safety_non_shared(self):
        """Test that non-shared LimitSet has threading.Lock."""
        from concurry.core.limit.limit_set import InMemorySharedLimitSet

        limits = LimitSet(limits=[ResourceLimit(key="connections", capacity=5)], shared=False, mode="sync")

        # Should be InMemorySharedLimitSet with a lock
        assert isinstance(limits, InMemorySharedLimitSet)
        assert limits._lock is not None

        # Should be able to acquire
        with limits.acquire() as acq:
            assert acq.successful is True

    def test_limitset_acquire_works_across_modes(self):
        """Test that acquisition works regardless of shared/mode."""
        from concurry.core.limit.limit_set import InMemorySharedLimitSet, MultiprocessSharedLimitSet

        test_cases = [
            (False, "sync", InMemorySharedLimitSet),
            (True, "sync", InMemorySharedLimitSet),
            (True, "thread", InMemorySharedLimitSet),
            (True, "asyncio", InMemorySharedLimitSet),
            (True, "process", MultiprocessSharedLimitSet),
        ]

        for shared, mode, expected_type in test_cases:
            limits = LimitSet(
                limits=[
                    RateLimit(
                        key="tokens",
                        window_seconds=1,
                        algorithm=RateLimitAlgorithm.TokenBucket,
                        capacity=100,
                    ),
                    ResourceLimit(key="connections", capacity=5),
                ],
                shared=shared,
                mode=mode,
            )

            # Check correct implementation type
            assert isinstance(limits, expected_type)

            # Should be able to acquire
            with limits.acquire(requested={"tokens": 10, "connections": 1}) as acq:
                assert acq.successful is True
                acq.update(usage={"tokens": 10})
