"""Test LimitPool functionality."""

import time

import pytest
from pydantic_core import ValidationError

from concurry import (
    CallLimit,
    LimitSet,
    LoadBalancingAlgorithm,
    RateLimit,
    ResourceLimit,
)
from concurry.core.limit.limit_pool import LimitPool


class TestLimitPoolCreation:
    """Test LimitPool creation and validation."""

    def test_limitpool_creation(self):
        """Test basic LimitPool creation."""
        ls1 = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=100)], shared=True, mode="sync"
        )
        ls2 = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=100)], shared=True, mode="sync"
        )
        pool = LimitPool(
            limit_sets=[ls1, ls2], load_balancing=LoadBalancingAlgorithm.RoundRobin, worker_index=0
        )
        assert len(pool.limit_sets) == 2
        assert pool.load_balancing == LoadBalancingAlgorithm.RoundRobin
        assert pool.worker_index == 0

    def test_limitpool_with_single_limitset(self):
        """Test LimitPool with single LimitSet."""
        ls = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=100)], shared=True, mode="sync"
        )
        pool = LimitPool(limit_sets=[ls], load_balancing=LoadBalancingAlgorithm.Random)
        assert len(pool.limit_sets) == 1

    def test_limitpool_empty_raises_error(self):
        """Test that empty limit_sets raises ValueError."""
        with pytest.raises(ValueError, match="at least one LimitSet"):
            LimitPool(limit_sets=[], load_balancing=LoadBalancingAlgorithm.RoundRobin)

    def test_limitpool_unsupported_algorithm_raises_error(self):
        """Test that unsupported load balancing algorithm raises ValueError."""
        ls = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=100)], shared=True, mode="sync"
        )
        # LimitPool only supports Random and RoundRobin, not LeastActiveLoad
        with pytest.raises(ValueError, match="Unsupported load balancing algorithm"):
            LimitPool(limit_sets=[ls], load_balancing=LoadBalancingAlgorithm.LeastActiveLoad)

    def test_limitpool_immutability(self):
        """Test that LimitPool is immutable (Typed subclass)."""

        ls = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=100)], shared=True, mode="sync"
        )
        pool = LimitPool(limit_sets=[ls], load_balancing=LoadBalancingAlgorithm.RoundRobin)

        # Try to modify public attributes (should fail due to frozen=True)
        with pytest.raises(ValidationError):
            pool.worker_index = 999

        with pytest.raises(ValidationError):
            pool.load_balancing = LoadBalancingAlgorithm.Random


class TestLimitPoolLoadBalancing:
    """Test load balancing strategies."""

    def test_limitpool_round_robin_selection(self):
        """Test round-robin selection cycles through LimitSets."""
        limitsets = [
            LimitSet(
                limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
                shared=True,
                mode="sync",
                config={"index": i},
            )
            for i in range(3)
        ]

        pool = LimitPool(
            limit_sets=limitsets, load_balancing=LoadBalancingAlgorithm.RoundRobin, worker_index=0
        )

        # Make multiple acquisitions and track which LimitSets were selected
        selected_indices = []
        for _ in range(9):  # 3 full cycles
            acq = pool.try_acquire(requested={"tokens": 10})
            assert acq.successful
            selected_indices.append(acq.config["index"])
            with acq:
                acq.update(usage={"tokens": 10})

        # Should cycle through 0, 1, 2, 0, 1, 2, 0, 1, 2
        assert selected_indices == [0, 1, 2, 0, 1, 2, 0, 1, 2]

    def test_limitpool_round_robin_with_offset(self):
        """Test round-robin with different worker offsets."""
        limitsets = [
            LimitSet(
                limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
                shared=True,
                mode="sync",
                config={"index": i},
            )
            for i in range(3)
        ]

        # Worker 0 starts at index 0
        pool0 = LimitPool(
            limit_sets=limitsets, load_balancing=LoadBalancingAlgorithm.RoundRobin, worker_index=0
        )

        # Worker 1 starts at index 1
        pool1 = LimitPool(
            limit_sets=limitsets, load_balancing=LoadBalancingAlgorithm.RoundRobin, worker_index=1
        )

        # Worker 2 starts at index 2
        pool2 = LimitPool(
            limit_sets=limitsets, load_balancing=LoadBalancingAlgorithm.RoundRobin, worker_index=2
        )

        # First selection from each pool should be different
        acq0 = pool0.try_acquire(requested={"tokens": 10})
        acq1 = pool1.try_acquire(requested={"tokens": 10})
        acq2 = pool2.try_acquire(requested={"tokens": 10})

        assert acq0.config["index"] == 0  # Worker 0 starts at 0
        assert acq1.config["index"] == 1  # Worker 1 starts at 1
        assert acq2.config["index"] == 2  # Worker 2 starts at 2

        with acq0:
            acq0.update(usage={"tokens": 10})
        with acq1:
            acq1.update(usage={"tokens": 10})
        with acq2:
            acq2.update(usage={"tokens": 10})

    def test_limitpool_random_selection(self):
        """Test random selection distributes across LimitSets."""
        limitsets = [
            LimitSet(
                limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
                shared=True,
                mode="sync",
                config={"index": i},
            )
            for i in range(3)
        ]

        pool = LimitPool(limit_sets=limitsets, load_balancing=LoadBalancingAlgorithm.Random, worker_index=0)

        # Make many acquisitions and verify all LimitSets are selected
        selected_indices = set()
        for _ in range(30):
            acq = pool.try_acquire(requested={"tokens": 10})
            assert acq.successful
            selected_indices.add(acq.config["index"])
            with acq:
                acq.update(usage={"tokens": 10})

        # With 30 selections across 3 LimitSets, we should see all of them
        assert selected_indices == {0, 1, 2}

    def test_limitpool_balancer_stats(self):
        """Test that balancer statistics are tracked."""
        ls = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)], shared=True, mode="sync"
        )
        pool = LimitPool(limit_sets=[ls], load_balancing=LoadBalancingAlgorithm.RoundRobin, worker_index=5)

        # Make some acquisitions
        for _ in range(3):
            acq = pool.try_acquire(requested={"tokens": 10})
            with acq:
                acq.update(usage={"tokens": 10})

        stats = pool.get_stats()
        assert "balancer_stats" in stats
        assert stats["balancer_stats"]["algorithm"] == "RoundRobin"
        assert stats["balancer_stats"]["offset"] == 5
        assert stats["balancer_stats"]["total_dispatched"] == 3


class TestLimitPoolAcquisition:
    """Test that LimitPool properly delegates to LimitSet and returns LimitSetAcquisition."""

    def test_limitpool_acquire_delegates_to_limitset(self):
        """Test that acquire() properly delegates to selected LimitSet."""
        limitset = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
            shared=True,
            mode="sync",
            config={"region": "us-east-1"},
        )
        pool = LimitPool(limit_sets=[limitset], load_balancing=LoadBalancingAlgorithm.RoundRobin)

        # Acquire should return LimitSetAcquisition with config
        with pool.acquire(requested={"tokens": 100}) as acq:
            assert acq.successful
            assert acq.config["region"] == "us-east-1"
            acq.update(usage={"tokens": 100})

    def test_limitpool_try_acquire(self):
        """Test non-blocking try_acquire()."""
        limitset = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=1, capacity=10)], shared=True, mode="sync"
        )
        pool = LimitPool(limit_sets=[limitset], load_balancing=LoadBalancingAlgorithm.RoundRobin)

        # First try_acquire should succeed
        acq1 = pool.try_acquire(requested={"tokens": 10})
        assert acq1.successful

        # Second try_acquire should fail (no tokens available)
        acq2 = pool.try_acquire(requested={"tokens": 1})
        assert not acq2.successful

        # Release first acquisition
        with acq1:
            acq1.update(usage={"tokens": 10})

        # Wait for tokens to refill
        time.sleep(1.1)

        # Now try_acquire should succeed
        acq3 = pool.try_acquire(requested={"tokens": 5})
        assert acq3.successful
        with acq3:
            acq3.update(usage={"tokens": 5})

    def test_limitpool_acquire_with_timeout(self):
        """Test acquire() with timeout."""
        limitset = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=10, capacity=10)], shared=True, mode="sync"
        )
        pool = LimitPool(limit_sets=[limitset], load_balancing=LoadBalancingAlgorithm.RoundRobin)

        # Exhaust the tokens
        acq1 = pool.try_acquire(requested={"tokens": 10})
        assert acq1.successful
        with acq1:
            acq1.update(usage={"tokens": 10})

        # Try to acquire more with short timeout (should fail)
        with pytest.raises(TimeoutError):
            pool.acquire(requested={"tokens": 5}, timeout=0.1)


class TestLimitPoolStats:
    """Test statistics gathering from LimitPool."""

    def test_limitpool_get_stats(self):
        """Test get_stats() returns comprehensive statistics."""
        limitsets = [
            LimitSet(
                limits=[RateLimit(key="tokens", window_seconds=60, capacity=100 * (i + 1))],
                shared=True,
                mode="sync",
            )
            for i in range(3)
        ]

        pool = LimitPool(
            limit_sets=limitsets, load_balancing=LoadBalancingAlgorithm.RoundRobin, worker_index=5
        )

        stats = pool.get_stats()

        # Check top-level stats
        assert stats["num_limit_sets"] == 3
        assert stats["load_balancing"] == "RoundRobin"  # Enum value, not lowercase
        assert stats["worker_index"] == 5

        # Check balancer stats
        assert "balancer_stats" in stats
        assert stats["balancer_stats"]["algorithm"] == "RoundRobin"

        # Check per-LimitSet stats
        assert len(stats["limit_sets"]) == 3
        assert stats["limit_sets"][0]["tokens"]["capacity"] == 100
        assert stats["limit_sets"][1]["tokens"]["capacity"] == 200
        assert stats["limit_sets"][2]["tokens"]["capacity"] == 300


class TestLimitPoolGetItem:
    """Test __getitem__ access to individual LimitSets."""

    def test_limitpool_getitem_by_integer_index(self):
        """Test accessing LimitSet by integer index."""
        limitsets = [
            LimitSet(
                limits=[RateLimit(key="tokens", window_seconds=60, capacity=100 * (i + 1))],
                shared=True,
                mode="sync",
            )
            for i in range(3)
        ]

        pool = LimitPool(limit_sets=limitsets, load_balancing=LoadBalancingAlgorithm.RoundRobin)

        # Access by integer index
        ls0 = pool[0]
        ls1 = pool[1]
        ls2 = pool[2]

        assert ls0.get_stats()["tokens"]["capacity"] == 100
        assert ls1.get_stats()["tokens"]["capacity"] == 200
        assert ls2.get_stats()["tokens"]["capacity"] == 300

    def test_limitpool_getitem_chained_with_limit_key(self):
        """Test chained access: pool[index][key] to get Limit."""
        limitset = LimitSet(
            limits=[
                CallLimit(window_seconds=60, capacity=100),
                RateLimit(key="tokens", window_seconds=60, capacity=1000),
            ],
            shared=True,
            mode="sync",
        )

        pool = LimitPool(limit_sets=[limitset], load_balancing=LoadBalancingAlgorithm.RoundRobin)

        # Access Limit via pool[0]["key"]
        call_limit = pool[0]["call_count"]
        token_limit = pool[0]["tokens"]

        assert call_limit.capacity == 100
        assert token_limit.capacity == 1000

    def test_limitpool_getitem_string_key_raises_error(self):
        """Test that string key access raises TypeError with helpful message."""
        limitset = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=100)], shared=True, mode="sync"
        )

        pool = LimitPool(limit_sets=[limitset], load_balancing=LoadBalancingAlgorithm.RoundRobin)

        # String key access should raise TypeError
        with pytest.raises(TypeError, match="must be integers.*not str"):
            _ = pool["tokens"]

    def test_limitpool_getitem_out_of_range(self):
        """Test that out-of-range index raises IndexError."""
        limitset = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=100)], shared=True, mode="sync"
        )

        pool = LimitPool(limit_sets=[limitset], load_balancing=LoadBalancingAlgorithm.RoundRobin)

        # Out of range access
        with pytest.raises(IndexError):
            _ = pool[5]


class TestLimitPoolWithSharedLimitSets:
    """Test that LimitPool works correctly with shared LimitSets."""

    def test_limitpool_with_shared_limitsets(self):
        """Test that multiple workers can share LimitSets via LimitPool."""
        # Create shared LimitSets
        limitsets = [
            LimitSet(
                limits=[RateLimit(key="tokens", window_seconds=60, capacity=100)], shared=True, mode="sync"
            )
            for _ in range(2)
        ]

        # Create two LimitPools (simulating two workers)
        pool1 = LimitPool(
            limit_sets=limitsets, load_balancing=LoadBalancingAlgorithm.RoundRobin, worker_index=0
        )
        pool2 = LimitPool(
            limit_sets=limitsets, load_balancing=LoadBalancingAlgorithm.RoundRobin, worker_index=1
        )

        # Both pools can acquire from the shared LimitSets
        acq1 = pool1.try_acquire(requested={"tokens": 50})
        acq2 = pool2.try_acquire(requested={"tokens": 50})

        assert acq1.successful
        assert acq2.successful

        with acq1:
            acq1.update(usage={"tokens": 50})
        with acq2:
            acq2.update(usage={"tokens": 50})


class TestLimitPoolWithDifferentLimitKeys:
    """Test LimitPool behavior when LimitSets have different keys."""

    def test_limitpool_with_different_keys_in_limitsets(self):
        """Test that LimitPool works when LimitSets have different limit keys."""
        # LimitSet 1 has "tokens" limit
        ls1 = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
            shared=True,
            mode="sync",
            config={"service": "service-a"},
        )

        # LimitSet 2 has "requests" limit (different key)
        ls2 = LimitSet(
            limits=[RateLimit(key="requests", window_seconds=60, capacity=100)],
            shared=True,
            mode="sync",
            config={"service": "service-b"},
        )

        pool = LimitPool(limit_sets=[ls1, ls2], load_balancing=LoadBalancingAlgorithm.RoundRobin)

        # First acquisition should use "tokens" limit from ls1
        with pool.acquire(requested={"tokens": 100}) as acq1:
            assert acq1.config["service"] == "service-a"
            acq1.update(usage={"tokens": 100})

        # Second acquisition should use "requests" limit from ls2
        with pool.acquire(requested={"requests": 10}) as acq2:
            assert acq2.config["service"] == "service-b"
            acq2.update(usage={"requests": 10})

    def test_limitpool_cannot_use_string_key_with_different_keys(self):
        """Test that string key access is not supported (different keys in LimitSets)."""
        # LimitSet 1 has "tokens" limit
        ls1 = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)], shared=True, mode="sync"
        )

        # LimitSet 2 has "requests" limit (different key)
        ls2 = LimitSet(
            limits=[RateLimit(key="requests", window_seconds=60, capacity=100)], shared=True, mode="sync"
        )

        pool = LimitPool(limit_sets=[ls1, ls2], load_balancing=LoadBalancingAlgorithm.RoundRobin)

        # String key access should raise TypeError
        with pytest.raises(TypeError, match="must be integers"):
            _ = pool["tokens"]

        # But integer index works
        assert pool[0]["tokens"].capacity == 1000
        assert pool[1]["requests"].capacity == 100


class TestLimitPoolEdgeCases:
    """Test edge cases and error handling."""

    def test_limitpool_with_empty_limitset(self):
        """Test LimitPool with empty LimitSet (no limits)."""
        empty_ls = LimitSet(limits=[], shared=False, mode="sync")

        pool = LimitPool(limit_sets=[empty_ls], load_balancing=LoadBalancingAlgorithm.RoundRobin)

        # Should always succeed (no limits to check)
        acq = pool.try_acquire()
        assert acq.successful
        acq.release()

    def test_limitpool_with_mixed_limit_types(self):
        """Test LimitPool with LimitSets containing different limit types."""
        ls1 = LimitSet(
            limits=[
                CallLimit(window_seconds=60, capacity=100),
                RateLimit(key="tokens", window_seconds=60, capacity=1000),
            ],
            shared=True,
            mode="sync",
            config={"type": "full"},
        )

        ls2 = LimitSet(
            limits=[ResourceLimit(key="connections", capacity=10)],
            shared=True,
            mode="sync",
            config={"type": "resource-only"},
        )

        pool = LimitPool(limit_sets=[ls1, ls2], load_balancing=LoadBalancingAlgorithm.RoundRobin)

        # First acquisition from ls1
        with pool.acquire(requested={"tokens": 100}) as acq1:
            assert acq1.config["type"] == "full"
            acq1.update(usage={"tokens": 100})

        # Second acquisition from ls2
        with pool.acquire(requested={"connections": 2}) as acq2:
            assert acq2.config["type"] == "resource-only"
            # No update needed for ResourceLimit

    def test_limitpool_large_number_of_limitsets(self):
        """Test LimitPool with many LimitSets."""
        num_limitsets = 50
        limitsets = [
            LimitSet(
                limits=[RateLimit(key="tokens", window_seconds=60, capacity=100)],
                shared=True,
                mode="sync",
                config={"index": i},
            )
            for i in range(num_limitsets)
        ]

        pool = LimitPool(
            limit_sets=limitsets, load_balancing=LoadBalancingAlgorithm.RoundRobin, worker_index=0
        )

        # Verify round-robin cycles through all LimitSets
        selected_indices = []
        for _ in range(num_limitsets):
            acq = pool.try_acquire(requested={"tokens": 1})
            assert acq.successful
            selected_indices.append(acq.config["index"])
            with acq:
                acq.update(usage={"tokens": 1})

        # Should have selected 0, 1, 2, ..., 49
        assert selected_indices == list(range(num_limitsets))
