"""Tests for LimitSet functionality."""

import logging

import pytest

from concurry import (
    CallLimit,
    LimitSet,
    RateLimit,
    RateLimitAlgorithm,
    ResourceLimit,
)
from concurry.core.limit.limit_set import InMemorySharedLimitSet, MultiprocessSharedLimitSet


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

    def test_limit_set_update_validation(self, caplog):
        """Test that update validates keys with warnings for unknown keys."""
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

        with caplog.at_level(logging.WARNING):
            caplog.clear()
            with limits.acquire(requested={"input_tokens": 100}) as acq:
                # Valid update
                acq.update(usage={"input_tokens": 80})

                # Unknown key should warn but not raise error
                acq.update(usage={"output_tokens": 50})

            # Should have warning for unknown key
            assert len(caplog.records) == 1
            assert "Cannot update limit 'output_tokens'" in caplog.records[0].message
            assert "not acquired" in caplog.records[0].message

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

        limits = LimitSet(limits=[ResourceLimit(key="connections", capacity=5)], shared=False, mode="sync")

        # Should be InMemorySharedLimitSet with a lock
        assert isinstance(limits, InMemorySharedLimitSet)
        assert limits._lock is not None

        # Should be able to acquire
        with limits.acquire() as acq:
            assert acq.successful is True

    def test_limitset_acquire_works_across_modes(self):
        """Test that acquisition works regardless of shared/mode."""

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


class TestUnknownLimitKeys:
    """Test handling of unknown limit keys in acquisition requests."""

    def test_unknown_key_warns_once(self, caplog):
        """Test that unknown key logs warning once per key."""
        import logging

        limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens",
                    window_seconds=1,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=100,
                ),
                ResourceLimit(key="connections", capacity=5),
            ]
        )

        # First acquisition with unknown key - should warn
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            with limits.acquire(requested={"tokens": 10, "unknown_key": 50}) as acq:
                acq.update(usage={"tokens": 10})
                assert acq.successful is True

            # Should have one warning about unknown_key
            assert len(caplog.records) == 1
            assert "Unknown limit key 'unknown_key'" in caplog.records[0].message
            assert "Available limit keys:" in caplog.records[0].message

        # Second acquisition with same unknown key - should NOT warn again
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            with limits.acquire(requested={"tokens": 10, "unknown_key": 50}) as acq:
                acq.update(usage={"tokens": 10})
                assert acq.successful is True

            # Should have NO warnings (already warned)
            assert len(caplog.records) == 0

        # Third acquisition with different unknown key - should warn
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            with limits.acquire(requested={"tokens": 10, "another_unknown": 25}) as acq:
                acq.update(usage={"tokens": 10})
                assert acq.successful is True

            # Should have one warning about another_unknown
            assert len(caplog.records) == 1
            assert "Unknown limit key 'another_unknown'" in caplog.records[0].message

    def test_unknown_key_does_not_raise_error(self):
        """Test that unknown key does not raise ValueError."""
        limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens",
                    window_seconds=1,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=100,
                ),
                ResourceLimit(key="connections", capacity=5),
            ]
        )

        # Should NOT raise ValueError for unknown key
        with limits.acquire(requested={"tokens": 10, "nonexistent": 100}) as acq:
            acq.update(usage={"tokens": 10})
            assert acq.successful is True
            # Only known keys should be in acquisitions
            assert "tokens" in acq.acquisitions
            assert "nonexistent" not in acq.acquisitions

    def test_mixed_known_unknown_keys(self):
        """Test partial acquisition with mix of known and unknown keys."""
        limits = LimitSet(
            limits=[
                CallLimit(window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100),
                RateLimit(
                    key="tokens",
                    window_seconds=1,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=1000,
                ),
                ResourceLimit(key="connections", capacity=10),
            ]
        )

        # Mix of known and unknown keys
        with limits.acquire(
            requested={
                "tokens": 100,  # Known
                "gpu_memory": 500,  # Unknown
                "connections": 2,  # Known
                "premium_quota": 50,  # Unknown
            }
        ) as acq:
            acq.update(usage={"tokens": 80})
            assert acq.successful is True

            # Only known keys + auto-added CallLimit should be acquired
            assert len(acq.acquisitions) == 3
            assert "tokens" in acq.acquisitions
            assert "connections" in acq.acquisitions
            assert "call_count" in acq.acquisitions  # Auto-added
            assert "gpu_memory" not in acq.acquisitions
            assert "premium_quota" not in acq.acquisitions

            # Check requested amounts for known keys
            assert acq.acquisitions["tokens"].requested == 100
            assert acq.acquisitions["connections"].requested == 2
            assert acq.acquisitions["call_count"].requested == 1  # Auto-added

    def test_all_unknown_keys(self):
        """Test that all unknown keys still acquires CallLimit/ResourceLimit."""
        limits = LimitSet(
            limits=[
                CallLimit(window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100),
                RateLimit(
                    key="tokens",
                    window_seconds=1,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=1000,
                ),
                ResourceLimit(key="connections", capacity=10),
            ]
        )

        # All requested keys are unknown
        with limits.acquire(requested={"unknown1": 100, "unknown2": 50, "unknown3": 25}) as acq:
            # Tokens RateLimit was not requested, so it's not in acquisitions
            # No need to update it (only CallLimit and ResourceLimit auto-added)
            assert acq.successful is True

            # Should still acquire auto-added CallLimit and ResourceLimit
            assert len(acq.acquisitions) == 2
            assert "call_count" in acq.acquisitions
            assert "connections" in acq.acquisitions
            assert acq.acquisitions["call_count"].requested == 1
            assert acq.acquisitions["connections"].requested == 1

            # Unknown keys should not be acquired
            assert "unknown1" not in acq.acquisitions
            assert "unknown2" not in acq.acquisitions
            assert "unknown3" not in acq.acquisitions

            # Tokens RateLimit should not be acquired (not requested, not auto-added)
            assert "tokens" not in acq.acquisitions

    def test_warning_shows_available_keys(self, caplog):
        """Test that warning message shows available limit keys."""
        import logging

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
                ResourceLimit(key="db_connections", capacity=10),
            ]
        )

        with caplog.at_level(logging.WARNING):
            caplog.clear()
            with limits.acquire(requested={"input_tokens": 100, "typo_key": 50}) as acq:
                acq.update(usage={"input_tokens": 100})

            # Check warning message contains available keys
            assert len(caplog.records) == 1
            warning_msg = caplog.records[0].message
            assert "Unknown limit key 'typo_key'" in warning_msg
            assert "Available limit keys:" in warning_msg
            # Should list all available keys
            assert "input_tokens" in warning_msg or "'input_tokens'" in warning_msg
            assert "output_tokens" in warning_msg or "'output_tokens'" in warning_msg
            assert "db_connections" in warning_msg or "'db_connections'" in warning_msg


class TestUnknownUpdateKeys:
    """Test behavior when trying to update limits that weren't acquired."""

    def test_update_unknown_key_warns_once(self, caplog):
        """Test that updating unknown key logs warning once per key."""
        limits = LimitSet(limits=[RateLimit(key="tokens", window_seconds=1, capacity=1000)])

        with caplog.at_level(logging.WARNING):
            caplog.clear()
            with limits.acquire(requested={"tokens": 100}) as acq:
                # Try to update unknown key multiple times
                acq.update(usage={"tokens": 80, "unknown_key": 50})
                acq.update(usage={"tokens": 80, "unknown_key": 50})  # Should not warn again
                acq.update(usage={"tokens": 80, "another_unknown": 30})  # New key, should warn

            # Should have exactly 2 warnings (one for each unique unknown key)
            warning_msgs = [r.message for r in caplog.records if r.levelname == "WARNING"]
            assert len(warning_msgs) == 2
            assert any("unknown_key" in msg for msg in warning_msgs)
            assert any("another_unknown" in msg for msg in warning_msgs)

    def test_update_unknown_key_does_not_raise_error(self):
        """Test that updating unknown key does not raise an error."""
        limits = LimitSet(limits=[RateLimit(key="tokens", window_seconds=1, capacity=1000)])

        # Should not raise ValueError
        with limits.acquire(requested={"tokens": 100}) as acq:
            acq.update(usage={"tokens": 80, "unknown_key": 50})
            # Test passes if no exception is raised

    def test_update_mixed_known_unknown_keys(self):
        """Test that updating mix of known and unknown keys works correctly."""
        limits = LimitSet(
            limits=[
                RateLimit(key="input_tokens", window_seconds=1, capacity=1000),
                RateLimit(key="output_tokens", window_seconds=1, capacity=500),
            ]
        )

        with limits.acquire(requested={"input_tokens": 100, "output_tokens": 50}) as acq:
            # Update with mix of known and unknown keys
            acq.update(
                usage={
                    "input_tokens": 80,
                    "output_tokens": 40,
                    "unknown_key1": 10,
                    "unknown_key2": 20,
                }
            )
            # Known keys should be updated, unknown keys skipped with warning

    def test_update_all_unknown_keys(self):
        """Test that updating only unknown keys doesn't break anything."""
        limits = LimitSet(limits=[RateLimit(key="tokens", window_seconds=1, capacity=1000)])

        with limits.acquire(requested={"tokens": 100}) as acq:
            # Update with only unknown keys
            acq.update(usage={"unknown_key": 50})
            # Still need to update the acquired limit
            acq.update(usage={"tokens": 80})

    def test_warning_shows_available_keys(self, caplog):
        """Test that warning message lists available keys."""
        limits = LimitSet(
            limits=[
                RateLimit(key="input_tokens", window_seconds=1, capacity=1000),
                RateLimit(key="output_tokens", window_seconds=1, capacity=500),
            ]
        )

        with caplog.at_level(logging.WARNING):
            caplog.clear()
            with limits.acquire(requested={"input_tokens": 100, "output_tokens": 50}) as acq:
                acq.update(usage={"input_tokens": 80, "output_tokens": 40, "typo_key": 10})

            # Check warning message contains available keys
            assert len(caplog.records) == 1
            warning_msg = caplog.records[0].message
            assert "Cannot update limit 'typo_key'" in warning_msg
            assert "Available keys:" in warning_msg
            # Should list all acquired keys
            assert "input_tokens" in warning_msg or "'input_tokens'" in warning_msg
            assert "output_tokens" in warning_msg or "'output_tokens'" in warning_msg
