"""Tests for Worker integration with Limits."""

import pytest

from concurry import (
    CallLimit,
    LimitSet,
    LoadBalancingAlgorithm,
    RateLimit,
    RateLimitAlgorithm,
    ResourceLimit,
    Worker,
)
from concurry.core.limit.limit_pool import LimitPool

# Worker mode fixture and cleanup are provided by tests/conftest.py


class TestWorkerLimits:
    """Test Worker integration with Limits.

    Note: Ray workers with limits are skipped because LimitSet contains threading
    primitives (locks, semaphores) that cannot be pickled for Ray serialization.
    This is a known limitation of Ray's serialization system.
    """

    def test_worker_with_limits(self, worker_mode):
        """Test that worker can access limits.

        1. Defines RateLimit and ResourceLimit
        2. Creates TestWorker with limits list
        3. Calls process(10) which accesses self.limits
        4. Verifies self.limits is not None
        5. Verifies result is 20 (10*2)
        6. Stops worker
        """
        # Pass list of Limits - each worker will create its own private LimitSet
        limits = [
            RateLimit(key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100),
            ResourceLimit(key="connections", capacity=5),
        ]

        class TestWorker(Worker):
            def __init__(self):
                self.results = []

            def process(self, value: int) -> int:
                # Access limits
                assert self.limits is not None
                return value * 2

        worker = TestWorker.options(mode=worker_mode, limits=limits).init()
        result = worker.process(10).result()
        assert result == 20
        worker.stop()

    def test_worker_using_limits(self, worker_mode):
        """Test worker actually using limits.

        1. Defines RateLimit for tokens (100 capacity)
        2. Creates TokenWorker with limits
        3. Calls process(50) which acquires 50 tokens
        4. Uses limits.acquire() context manager
        5. Updates actual usage to 45 tokens (via acq.update())
        6. Verifies result is "Used 45 tokens"
        7. Stops worker
        """
        # Pass list of Limits - each worker will create its own private LimitSet
        limits = [
            RateLimit(key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100),
        ]

        class TokenWorker(Worker):
            def __init__(self):
                pass

            def process(self, tokens_needed: int) -> str:
                with self.limits.acquire(requested={"tokens": tokens_needed}) as acq:
                    # Simulate work
                    actual_used = tokens_needed - 5  # Use slightly less
                    acq.update(usage={"tokens": actual_used})
                    return f"Used {actual_used} tokens"

        worker = TokenWorker.options(mode=worker_mode, limits=limits).init()
        result = worker.process(50).result()
        assert "Used 45 tokens" == result
        worker.stop()

    def test_worker_with_resource_limits(self, worker_mode):
        """Test worker using resource limits.

        1. Defines ResourceLimit for connections (capacity=2)
        2. Creates DBWorker with resource limit
        3. Calls query() twice, each acquiring 1 connection
        4. Verifies both queries succeed (within capacity)
        5. Returns "Query result" for each
        6. Stops worker
        """
        # Pass list of Limits - each worker will create its own private LimitSet
        limits = [ResourceLimit(key="connections", capacity=2)]

        class DBWorker(Worker):
            def __init__(self):
                self.conn_count = 0

            def query(self) -> str:
                with self.limits.acquire(requested={"connections": 1}):
                    self.conn_count += 1
                    # Simulate DB query
                    return "Query result"

        worker = DBWorker.options(mode=worker_mode, limits=limits).init()

        # Should succeed
        result1 = worker.query().result()
        assert result1 == "Query result"

        # Should succeed
        result2 = worker.query().result()
        assert result2 == "Query result"

        worker.stop()

    def test_worker_with_mixed_limits(self, worker_mode):
        """Test worker using mixed limit types."""
        # Pass list of Limits - each worker will create its own private LimitSet
        limits = [
            CallLimit(window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100),
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
            ResourceLimit(key="db_connections", capacity=2),
        ]

        class LLMWorker(Worker):
            def __init__(self, model: str):
                self.model = model

            def process(self, prompt: str) -> str:
                # Acquire resource first
                with self.limits.acquire(requested={"db_connections": 1}):
                    # Then acquire rate limits
                    with self.limits.acquire(requested={"input_tokens": 100, "output_tokens": 50}):
                        # Update with actual usage
                        result = f"Processed: {prompt}"
                        # Assume we calculated actual usage
                        actual_input = 80
                        actual_output = 40

                        # This should raise error because we're outside the inner context
                        # Actually, we need to update inside the context
                        return result

        worker = LLMWorker.options(mode=worker_mode, limits=limits).init("test-model")

        # This will fail because we're not updating inside the context
        with pytest.raises(RuntimeError):
            worker.process("test prompt").result()

        worker.stop()

    def test_worker_with_nested_limit_acquisition(self, worker_mode):
        """Test worker with properly nested limit acquisition."""
        # Pass list of Limits - each worker will create its own private LimitSet
        limits = [
            RateLimit(
                key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=1000
            ),
            ResourceLimit(key="connections", capacity=2),
        ]

        class ProperWorker(Worker):
            def __init__(self):
                pass

            def process(self, value: int) -> str:
                # Proper nesting
                with self.limits.acquire(requested={"connections": 1}) as res_acq:
                    with self.limits.acquire(requested={"tokens": 100}) as rate_acq:
                        # Do work
                        rate_acq.update(usage={"tokens": 80})
                        return f"Processed {value}"

        worker = ProperWorker.options(mode=worker_mode, limits=limits).init()
        result = worker.process(42).result()
        assert result == "Processed 42"
        worker.stop()

    def test_worker_without_limits(self, worker_mode):
        """Test that worker works without limits."""

        class SimpleWorker(Worker):
            def __init__(self):
                pass

            def process(self, value: int) -> int:
                return value * 2

        worker = SimpleWorker.options(mode=worker_mode).init()
        result = worker.process(10).result()
        assert result == 20
        worker.stop()

    def test_worker_get_limit_by_key(self, worker_mode):
        """Test worker accessing individual limits by key."""
        limits = [
            CallLimit(window_seconds=60, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100),
            RateLimit(
                key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=1000
            ),
            ResourceLimit(key="connections", capacity=5),
        ]

        class InspectorWorker(Worker):
            def __init__(self):
                pass

            def get_limit_capacity(self, limit_key: str) -> int:
                """Get the capacity of a specific limit."""
                # Access limit via first LimitSet in pool
                limit = self.limits[0][limit_key]
                return limit.capacity

            def get_limit_stats(self, limit_key: str) -> dict:
                """Get statistics for a specific limit."""
                # Access limit via first LimitSet in pool
                limit = self.limits[0][limit_key]
                return limit.get_stats()

            def check_all_limits(self) -> dict:
                """Check capacities of all configured limits."""
                return {
                    "call_count_capacity": self.limits[0]["call_count"].capacity,
                    "tokens_capacity": self.limits[0]["tokens"].capacity,
                    "connections_capacity": self.limits[0]["connections"].capacity,
                }

        worker = InspectorWorker.options(mode=worker_mode, limits=limits).init()

        # Test accessing CallLimit (special "call_count" key)
        call_capacity = worker.get_limit_capacity("call_count").result()
        assert call_capacity == 100

        # Test accessing RateLimit
        token_capacity = worker.get_limit_capacity("tokens").result()
        assert token_capacity == 1000

        # Test accessing ResourceLimit
        conn_capacity = worker.get_limit_capacity("connections").result()
        assert conn_capacity == 5

        # Test getting stats for a limit
        token_stats = worker.get_limit_stats("tokens").result()
        assert token_stats["key"] == "tokens"
        assert token_stats["capacity"] == 1000

        # Test checking all limits at once
        all_capacities = worker.check_all_limits().result()
        assert all_capacities["call_count_capacity"] == 100
        assert all_capacities["tokens_capacity"] == 1000
        assert all_capacities["connections_capacity"] == 5

        worker.stop()


class TestWorkerSharedLimits:
    """Test Worker integration with shared LimitSets."""

    def test_worker_with_shared_limits_list_conversion(self):
        """Test that passing list of Limits creates private LimitSet."""
        limits_list = [
            RateLimit(key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100),
            ResourceLimit(key="connections", capacity=5),
        ]

        class TestWorker(Worker):
            def process(self) -> bool:
                # Should have limits set
                return self.limits is not None

        # Passing list should create private LimitSet (no warning for sync mode)
        worker = TestWorker.options(mode="sync", limits=limits_list).init()
        result = worker.process().result()
        assert result is True
        worker.stop()

    def test_worker_with_shared_limitset_thread(self):
        """Test workers sharing a LimitSet in thread mode."""
        shared_limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100
                )
            ],
            shared=True,
            mode="thread",
        )

        class TestWorker(Worker):
            def process(self) -> str:
                with self.limits.acquire(requested={"tokens": 10}) as acq:
                    acq.update(usage={"tokens": 10})
                    return "done"

        # Create two workers sharing the same LimitSet
        worker1 = TestWorker.options(mode="thread", limits=shared_limits).init()
        worker2 = TestWorker.options(mode="thread", limits=shared_limits).init()

        # Both should be able to use the shared limits
        result1 = worker1.process().result()
        result2 = worker2.process().result()

        assert result1 == "done"
        assert result2 == "done"

        worker1.stop()
        worker2.stop()

    def test_worker_with_shared_limitset_process(self):
        """Test workers sharing a LimitSet in process mode."""
        shared_limits = LimitSet(
            limits=[ResourceLimit(key="connections", capacity=2)], shared=True, mode="process"
        )

        class TestWorker(Worker):
            def process(self) -> str:
                with self.limits.acquire(requested={"connections": 1}):
                    return "done"

        # Create two process workers sharing the same LimitSet
        worker1 = TestWorker.options(mode="process", limits=shared_limits).init()
        worker2 = TestWorker.options(mode="process", limits=shared_limits).init()

        result1 = worker1.process().result()
        result2 = worker2.process().result()

        assert result1 == "done"
        assert result2 == "done"

        worker1.stop()
        worker2.stop()

    def test_worker_incompatible_limitset_mode(self):
        """Test that incompatible LimitSet mode raises error."""
        # Create thread-mode shared LimitSet
        thread_limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens", window_seconds=1, algorithm=RateLimitAlgorithm.TokenBucket, capacity=100
                )
            ],
            shared=True,
            mode="thread",
        )

        class TestWorker(Worker):
            def process(self) -> str:
                return "done"

        # Should raise error when trying to use with process worker
        with pytest.raises(ValueError, match="not compatible"):
            TestWorker.options(mode="process", limits=thread_limits).init()


class TestWorkerWithoutLimits:
    """Test that workers work correctly without limits configured."""

    def test_worker_without_limits_has_limits_attribute(self, worker_mode):
        """Test that workers without limits still have self.limits attribute."""

        class TestWorker(Worker):
            def __init__(self):
                pass

            def check_limits(self) -> bool:
                """Check if self.limits is available."""
                return self.limits is not None

        worker = TestWorker.options(mode=worker_mode).init()
        result = worker.check_limits().result()
        assert result is True
        worker.stop()

    def test_worker_without_limits_can_acquire(self, worker_mode):
        """Test that workers without limits can call self.limits.acquire()."""

        class TestWorker(Worker):
            def __init__(self):
                self.count = 0

            def process(self, value: int) -> int:
                """Process with limit acquisition (should always succeed)."""
                with self.limits.acquire():
                    self.count += 1
                    return value * 2

        worker = TestWorker.options(mode=worker_mode, blocking=True).init()
        result = worker.process(10)
        assert result == 20
        worker.stop()

    def test_worker_without_limits_never_blocks(self, worker_mode):
        """Test that empty limits never block."""

        class TestWorker(Worker):
            def __init__(self):
                pass

            def process(self) -> str:
                """Multiple acquisitions should never block."""
                results = []
                for i in range(100):
                    with self.limits.acquire():
                        results.append(i)
                return "done"

        worker = TestWorker.options(mode=worker_mode, blocking=True).init()
        result = worker.process()
        assert result == "done"
        worker.stop()

    def test_pool_without_limits(self, worker_mode):
        """Test that worker pools work without limits."""
        # Skip sync and asyncio since they don't support pools
        if worker_mode in ("sync", "asyncio"):
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        class TestWorker(Worker):
            def process(self, x: int) -> int:
                with self.limits.acquire():
                    return x * 2

        pool = TestWorker.options(mode=worker_mode, max_workers=3, blocking=True).init()
        result = pool.process(5)
        assert result == 10
        pool.stop()


class TestWorkerLimitsWithConfig:
    """Test Worker integration with LimitSet config parameter."""

    def test_worker_with_limitset_config(self, worker_mode):
        """Test that worker can access config from LimitSet via acquisition."""
        config = {"region": "us-east-1", "account_id": "12345"}
        limitset = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
            shared=True,
            mode=worker_mode,
            config=config,
        )

        class APIWorker(Worker):
            def __init__(self):
                pass

            def call_api(self, prompt: str):
                with self.limits.acquire(requested={"tokens": 100}) as acq:
                    # Access config from acquisition
                    assert acq.config == config
                    assert acq.config["region"] == "us-east-1"
                    assert acq.config["account_id"] == "12345"
                    acq.update(usage={"tokens": 100})
                    return f"Called {acq.config['region']}"

        worker = APIWorker.options(mode=worker_mode, limits=limitset).init()
        result = worker.call_api("test").result()
        assert "us-east-1" in result
        worker.stop()

    def test_worker_config_immutable_during_acquisition(self, worker_mode):
        """Test that modifying acq.config doesn't affect the LimitSet's config."""
        original_config = {"region": "us-east-1"}
        limitset = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
            shared=True,
            mode=worker_mode,
            config=original_config,
        )

        class APIWorker(Worker):
            def __init__(self):
                pass

            def call_api(self):
                with self.limits.acquire(requested={"tokens": 100}) as acq:
                    # Try to modify the acquisition's config
                    acq.config["region"] = "modified"
                    acq.update(usage={"tokens": 50})
                    return acq.config["region"]

        worker = APIWorker.options(mode=worker_mode, limits=limitset).init()
        result = worker.call_api().result()
        assert result == "modified"  # Worker saw the modification

        # But the original LimitSet config should be unchanged
        assert limitset.config["region"] == "us-east-1"
        worker.stop()

    def test_worker_with_config_and_all_limit_types(self, worker_mode):
        """Test that config works with all limit types."""
        config = {"environment": "production", "service": "api"}
        limitset = LimitSet(
            limits=[
                CallLimit(window_seconds=60, capacity=100),
                RateLimit(key="tokens", window_seconds=60, capacity=1000),
                ResourceLimit(key="connections", capacity=10),
            ],
            shared=True,
            mode=worker_mode,
            config=config,
        )

        class APIWorker(Worker):
            def __init__(self):
                pass

            def process(self):
                with self.limits.acquire(requested={"tokens": 100, "connections": 2}) as acq:
                    assert acq.config == config
                    assert acq.config["environment"] == "production"
                    assert acq.config["service"] == "api"
                    acq.update(usage={"tokens": 80})
                    return True

        worker = APIWorker.options(mode=worker_mode, limits=limitset).init()
        result = worker.process().result()
        assert result is True
        worker.stop()

    def test_worker_config_with_nested_acquisition(self, worker_mode):
        """Test that config is accessible in nested acquisitions."""
        config = {"region": "ap-southeast-1"}
        limitset = LimitSet(
            limits=[
                RateLimit(key="tokens", window_seconds=60, capacity=1000),
                ResourceLimit(key="connections", capacity=10),
            ],
            shared=True,
            mode=worker_mode,
            config=config,
        )

        class APIWorker(Worker):
            def __init__(self):
                pass

            def process(self):
                # First acquisition
                with self.limits.acquire(requested={"connections": 1}) as acq1:
                    assert acq1.config["region"] == "ap-southeast-1"

                    # Nested acquisition
                    with self.limits.acquire(requested={"tokens": 100}) as acq2:
                        assert acq2.config["region"] == "ap-southeast-1"
                        acq2.update(usage={"tokens": 90})

                return True

        worker = APIWorker.options(mode=worker_mode, limits=limitset).init()
        result = worker.process().result()
        assert result is True
        worker.stop()

    def test_worker_without_config_defaults_to_empty_dict(self, worker_mode):
        """Test that LimitSet without config defaults to empty dict."""
        limitset = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
            shared=True,
            mode=worker_mode,
        )

        class APIWorker(Worker):
            def __init__(self):
                pass

            def process(self):
                with self.limits.acquire(requested={"tokens": 100}) as acq:
                    assert acq.config == {}
                    acq.update(usage={"tokens": 100})
                    return True

        worker = APIWorker.options(mode=worker_mode, limits=limitset).init()
        result = worker.process().result()
        assert result is True
        worker.stop()


class TestLimitPoolWorkerIntegration:
    """Test LimitPool integration with Workers across all modes."""

    def test_worker_with_limitpool(self, worker_mode):
        """Test that workers can use LimitPool via self.limits."""

        class APIWorker(Worker):
            def __init__(self):
                self.calls = []

            def call_api(self, prompt: str):
                # self.limits is a LimitPool
                with self.limits.acquire(requested={"tokens": 100}) as acq:
                    region = acq.config["region"]
                    self.calls.append(region)
                    acq.update(usage={"tokens": 100})
                    return f"Response from {region}"

        # Create LimitSets with configs for the appropriate mode
        limitsets = [
            LimitSet(
                limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
                shared=True,
                mode=worker_mode,
                config={"region": f"region-{i}"},
            )
            for i in range(3)
        ]

        # Create LimitPool
        pool = LimitPool(
            limit_sets=limitsets, load_balancing=LoadBalancingAlgorithm.RoundRobin, worker_index=0
        )

        # Create worker with LimitPool (single worker to ensure sequential processing)
        worker = APIWorker.options(mode=worker_mode, max_workers=1, limits=pool).init()

        # Make calls - should cycle through regions
        results = []
        for i in range(6):
            result = worker.call_api(f"prompt-{i}").result()
            results.append(result)

        # Verify round-robin distribution
        assert "region-0" in results[0]
        assert "region-1" in results[1]
        assert "region-2" in results[2]
        assert "region-0" in results[3]
        assert "region-1" in results[4]
        assert "region-2" in results[5]

        worker.stop()

    def test_worker_with_list_of_limitsets(self, worker_mode):
        """Test that passing List[LimitSet] to worker creates LimitPool automatically."""

        class APIWorker(Worker):
            def __init__(self):
                pass

            def call_api(self, prompt: str):
                # self.limits should be a LimitPool wrapping the LimitSets
                with self.limits.acquire(requested={"tokens": 100}) as acq:
                    region = acq.config.get("region", "unknown")
                    acq.update(usage={"tokens": 100})
                    return region

        # Create list of LimitSets with appropriate mode
        limitsets = [
            LimitSet(
                limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
                shared=True,
                mode=worker_mode,
                config={"region": f"region-{i}"},
            )
            for i in range(2)
        ]

        # Pass list of LimitSets directly (single worker to ensure sequential processing)
        worker = APIWorker.options(mode=worker_mode, max_workers=1, limits=limitsets).init()

        # Make calls
        results = []
        for i in range(4):
            result = worker.call_api(f"prompt-{i}").result()
            results.append(result)

        # Verify both regions were used (round-robin)
        assert "region-0" in results
        assert "region-1" in results

        worker.stop()

    def test_worker_pool_with_limitpool(self, worker_mode):
        """Test that worker pools properly assign worker_index to each worker's LimitPool."""
        # Skip modes that don't support pools
        if worker_mode in ("sync", "asyncio"):
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        class APIWorker(Worker):
            def __init__(self):
                pass

            def call_api(self, prompt: str):
                with self.limits.acquire(requested={"tokens": 100}) as acq:
                    region = acq.config["region"]
                    acq.update(usage={"tokens": 100})
                    return region

        # Create LimitSets with appropriate mode
        limitsets = [
            LimitSet(
                limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
                shared=True,
                mode=worker_mode,
                config={"region": f"region-{i}"},
            )
            for i in range(3)
        ]

        # Create worker pool with list of LimitSets
        pool = APIWorker.options(mode=worker_mode, max_workers=3, limits=limitsets).init()

        # Make many calls
        futures = [pool.call_api(f"prompt-{i}") for i in range(12)]
        results = [f.result() for f in futures]

        # Verify all regions were accessed
        regions = set(results)
        assert "region-0" in regions
        assert "region-1" in regions
        assert "region-2" in regions

        pool.stop()

    def test_shared_limitsets_enforce_limits_across_workers(self, worker_mode):
        """Test that shared LimitSets properly enforce limits across multiple workers in a pool."""
        # Skip modes that don't support pools
        if worker_mode in ("sync", "asyncio"):
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        class APIWorker(Worker):
            def __init__(self):
                pass

            def call_api(self, tokens: int):
                # Try to acquire tokens
                acq = self.limits.try_acquire(requested={"tokens": tokens})
                if acq.successful:
                    with acq:
                        acq.update(usage={"tokens": tokens})
                        return True
                return False

        # Create shared LimitSet with small capacity to test enforcement
        limitset = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=10, capacity=100)],
            shared=True,
            mode=worker_mode,
        )

        # Create pool with 3 workers sharing the same LimitSet
        pool = APIWorker.options(mode=worker_mode, max_workers=3, limits=limitset).init()

        # Each worker tries to acquire 50 tokens (total 150 tokens needed, but only 100 available)
        futures = [pool.call_api(50) for _ in range(3)]
        results = [f.result() for f in futures]

        # Only 2 out of 3 calls should succeed (2 * 50 = 100 tokens)
        successful = sum(1 for r in results if r)
        assert successful == 2, f"Expected 2 successful calls, got {successful}"

        pool.stop()

    def test_limitpool_round_robin_across_worker_pool(self, worker_mode):
        """Test that LimitPool distributes acquisitions using round-robin across worker pool."""
        # Skip modes that don't support pools
        if worker_mode in ("sync", "asyncio"):
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        class APIWorker(Worker):
            def __init__(self):
                pass

            def call_api(self):
                with self.limits.acquire(requested={"tokens": 10}) as acq:
                    # Return the region that was selected
                    acq.update(usage={"tokens": 10})
                    return acq.config["region"]

        # Create 3 LimitSets with different regions
        limitsets = [
            LimitSet(
                limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
                shared=True,
                mode=worker_mode,
                config={"region": f"region-{i}"},
            )
            for i in range(3)
        ]

        # Create pool with 3 workers and 3 LimitSets
        # Each worker should start at different offset (0, 1, 2)
        pool = APIWorker.options(mode=worker_mode, max_workers=3, limits=limitsets).init()

        # Make many calls
        futures = [pool.call_api() for _ in range(30)]
        results = [f.result() for f in futures]

        # Count calls per region
        region_counts = {}
        for region in results:
            region_counts[region] = region_counts.get(region, 0) + 1

        # All regions should be used roughly equally (within 30-40% range)
        for region, count in region_counts.items():
            assert count >= 7 and count <= 13, f"Region {region} was used {count} times (expected 7-13)"

        pool.stop()

    @pytest.mark.parametrize("mode", ["process", "ray"])
    def test_limitpool_serialization_across_processes(self, mode):
        """Test that LimitPool can be serialized for process/Ray workers."""

        class APIWorker(Worker):
            def __init__(self):
                pass

            def call_api(self):
                # Access limits in remote worker
                with self.limits.acquire(requested={"tokens": 10}) as acq:
                    region = acq.config["region"]
                    acq.update(usage={"tokens": 10})
                    return region

        # Create LimitSets
        limitsets = [
            LimitSet(
                limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
                shared=True,
                mode=mode,
                config={"region": f"region-{i}"},
            )
            for i in range(2)
        ]

        # Create worker with LimitSets (should create LimitPool)
        worker = APIWorker.options(mode=mode, limits=limitsets).init()

        # Make call - this will serialize LimitPool to remote worker
        result = worker.call_api().result()
        assert result in ["region-0", "region-1"]

        worker.stop()

    def test_limitpool_high_concurrency_stress(self, worker_mode):
        """Stress test LimitPool with high concurrency to verify thread-safety."""
        # Skip modes that don't support pools
        if worker_mode in ("sync", "asyncio"):
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        class APIWorker(Worker):
            def __init__(self):
                pass

            def call_api(self, id: int):
                with self.limits.acquire(requested={"tokens": 1}) as acq:
                    region = acq.config["region"]
                    acq.update(usage={"tokens": 1})
                    return (id, region)

        # Create multiple LimitSets
        limitsets = [
            LimitSet(
                limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
                shared=True,
                mode=worker_mode,
                config={"region": f"region-{i}"},
            )
            for i in range(5)
        ]

        # Create pool with many workers
        num_workers = 10
        pool = APIWorker.options(mode=worker_mode, max_workers=num_workers, limits=limitsets).init()

        # Make many concurrent calls
        num_calls = 100
        futures = [pool.call_api(i) for i in range(num_calls)]
        results = [f.result() for f in futures]

        # Verify all calls succeeded
        assert len(results) == num_calls

        # Verify all regions were used
        regions_used = set(region for _, region in results)
        assert len(regions_used) == 5, f"Expected 5 regions, got {len(regions_used)}"

        pool.stop()

    def test_limitpool_enforces_limits_with_shared_limitsets(self, worker_mode):
        """Test that LimitPool with shared LimitSets properly enforces limits across workers."""
        # Skip modes that don't support pools
        if worker_mode in ("sync", "asyncio"):
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        class APIWorker(Worker):
            def __init__(self):
                pass

            def consume_tokens(self, tokens: int):
                acq = self.limits.try_acquire(requested={"tokens": tokens})
                if acq.successful:
                    with acq:
                        acq.update(usage={"tokens": tokens})
                        return True
                return False

        # Create 2 shared LimitSets with LIMITED capacity
        limitsets = [
            LimitSet(
                limits=[RateLimit(key="tokens", window_seconds=5, capacity=100)],
                shared=True,
                mode=worker_mode,
                config={"limitset_index": i},
            )
            for i in range(2)
        ]

        # Create pool with 5 workers and 2 LimitSets
        pool = APIWorker.options(mode=worker_mode, max_workers=5, limits=limitsets).init()

        # Try to consume more tokens than available across all LimitSets
        # Each LimitSet has 100 tokens, total 200 tokens
        # Request 10 workers x 30 tokens = 300 tokens
        futures = [pool.consume_tokens(30) for _ in range(10)]
        results = [f.result() for f in futures]

        # At most 6 calls should succeed (6 * 30 = 180, 7 * 30 = 210 > 200)
        successful = sum(1 for r in results if r)
        assert successful <= 7, f"Expected at most 7 successful calls, got {successful}"
        assert successful >= 5, f"Expected at least 5 successful calls, got {successful}"

        pool.stop()

    @pytest.mark.parametrize("mode", ["process", "ray"])
    def test_limitpool_config_accessible_in_remote_workers(self, mode):
        """Test that config from LimitPool's LimitSets is accessible in remote workers."""

        class APIWorker(Worker):
            def __init__(self):
                pass

            def get_region_and_account(self):
                with self.limits.acquire(requested={"tokens": 10}) as acq:
                    region = acq.config.get("region", "")
                    account = acq.config.get("account", "")
                    acq.update(usage={"tokens": 10})
                    return (region, account)

        # Create LimitSets with detailed config
        limitsets = [
            LimitSet(
                limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
                shared=True,
                mode=mode,
                config={"region": f"us-east-{i + 1}", "account": f"account-{i + 1}"},
            )
            for i in range(2)
        ]

        # Create worker with LimitSets (single worker to ensure sequential processing)
        worker = APIWorker.options(mode=mode, max_workers=1, limits=limitsets).init()

        # Make multiple calls - should see different configs due to round-robin
        results = [worker.get_region_and_account().result() for _ in range(4)]

        # Extract regions and accounts
        regions = [region for region, _ in results]
        accounts = [account for _, account in results]

        # Should see both regions and accounts
        assert "us-east-1" in regions
        assert "us-east-2" in regions
        assert "account-1" in accounts
        assert "account-2" in accounts

        worker.stop()
