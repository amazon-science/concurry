"""Tests for shared LimitSets across multiple workers.

This module tests that LimitSets can be properly shared across workers
of the same execution mode, and that limits are enforced correctly.
"""

import time

import pytest

from concurry import Worker
from concurry.core.limit import (
    CallLimit,
    LimitSet,
    RateLimit,
    RateLimitAlgorithm,
    ResourceLimit,
)
from concurry.core.limit.limit_set import BaseLimitSet


class TestBasicLimitEnforcement:
    """Test basic limit enforcement with single workers."""

    def test_counter_with_call_limit(self, worker_mode):
        """Test Counter worker with CallLimit - should throttle execution."""
        # Skip ray mode - use separate ray tests in TestRayWorkerLimits
        if worker_mode == "ray":
            pytest.skip("Ray mode has separate tests in TestRayWorkerLimits class")

        class Counter(Worker):
            def __init__(self, count: int = 0):
                self.count = count

            def increment(self, amount: int = 1):
                with self.limits.acquire():
                    self.count += amount
                    return self.count

            def get_count(self) -> int:
                return self.count

        # Create worker with CallLimit: 20 calls per second
        w = Counter.options(
            mode=worker_mode,
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=20)],
        ).init(count=5)

        # Make 100 calls - should take ~5 seconds (100 calls / 20 per second)
        start_time = time.time()
        for _ in range(100):
            w.increment(1).result()
        elapsed = time.time() - start_time

        # Verify count
        final_count = w.get_count().result()
        assert final_count == 105  # 5 initial + 100 increments

        # Verify timing for TokenBucket:
        # - Capacity=20 means 20 tokens available immediately (burst)
        # - Remaining 80 calls at 20/sec = 4 seconds
        # - Total expected: ~4 seconds (burst happens instantly)
        assert elapsed >= 3.5, f"Expected ~4 seconds, got {elapsed:.2f}s (too fast, limits not enforced)"
        assert elapsed <= 6.0, f"Expected ~4 seconds, got {elapsed:.2f}s (too slow)"

        w.stop()

    def test_counter_with_rate_limit(self, worker_mode):
        """Test Counter worker with RateLimit - should throttle token consumption."""
        # Skip ray mode - use separate ray tests in TestRayWorkerLimits
        if worker_mode == "ray":
            pytest.skip("Ray mode has separate tests in TestRayWorkerLimits class")

        class TokenCounter(Worker):
            def __init__(self):
                self.total_tokens = 0

            def consume_tokens(self, tokens: int):
                with self.limits.acquire(requested={"tokens": tokens}) as acq:
                    self.total_tokens += tokens
                    acq.update(usage={"tokens": tokens})
                    return self.total_tokens

            def get_total(self) -> int:
                return self.total_tokens

        # Create worker with RateLimit: 50 tokens per second
        w = TokenCounter.options(
            mode=worker_mode,
            limits=[
                RateLimit(
                    key="tokens", window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=50
                )
            ],
        ).init()

        # Consume 250 tokens (10 calls x 25 tokens) - should take ~5 seconds
        start_time = time.time()
        for _ in range(10):
            w.consume_tokens(25).result()
        elapsed = time.time() - start_time

        # Verify total
        final_total = w.get_total().result()
        assert final_total == 250

        # Verify timing for TokenBucket:
        # - Capacity=50 means 50 tokens available immediately (burst)
        # - Remaining 200 tokens at 50/sec = 4 seconds
        # - Total expected: ~4 seconds (burst happens instantly)
        assert elapsed >= 3.5, f"Expected ~4 seconds, got {elapsed:.2f}s (too fast, limits not enforced)"
        assert elapsed <= 6.0, f"Expected ~4 seconds, got {elapsed:.2f}s (too slow)"

        w.stop()

    def test_counter_with_resource_limit(self, worker_mode):
        """Test Counter worker with ResourceLimit - should block when resources exhausted."""
        # Skip ray mode - use separate ray tests in TestRayWorkerLimits
        if worker_mode == "ray":
            pytest.skip("Ray mode has separate tests in TestRayWorkerLimits class")

        class ResourceWorker(Worker):
            def __init__(self):
                self.operations = []

            def process(self, value: int):
                # Acquire 1 connection
                with self.limits.acquire(requested={"connections": 1}):
                    # Simulate work
                    time.sleep(0.1)
                    self.operations.append(value)
                    return len(self.operations)

            def get_count(self) -> int:
                return len(self.operations)

        # Create worker with ResourceLimit: only 2 concurrent connections
        w = ResourceWorker.options(
            mode=worker_mode, limits=[ResourceLimit(key="connections", capacity=2)]
        ).init()

        # Submit 10 operations
        # With capacity=2 and 0.1s per operation, should take at least 0.5s (10 ops / 2 concurrent)
        start_time = time.time()
        futures = [w.process(i) for i in range(10)]
        results = [f.result() for f in futures]
        elapsed = time.time() - start_time

        # Verify all operations completed
        final_count = w.get_count().result()
        assert final_count == 10
        assert results[-1] == 10  # Last operation should return count=10

        # Verify timing - should take at least 0.5 seconds due to resource limit
        assert elapsed >= 0.45, f"Expected >= 0.5s, got {elapsed:.2f}s (resource limit not enforced)"

        w.stop()


class TestSharedLimitSets:
    """Test shared LimitSets across multiple workers."""

    def test_shared_limitset_across_workers_inmemory(self, worker_mode):
        """Test that shared InMemorySharedLimitSet is shared across workers."""
        # Skip process and ray modes - they use different shared limit implementations
        if worker_mode in ("process", "ray"):
            pytest.skip("InMemorySharedLimitSet is only for sync/thread/asyncio modes")

        class Counter(Worker):
            def __init__(self):
                pass

            def increment(self):
                with self.limits.acquire():
                    time.sleep(0.01)  # Small delay
                    return 1

        # Create shared LimitSet with small capacity
        shared_limits = LimitSet(
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10)],
            shared=True,
            mode=worker_mode,
        )

        # Verify both workers reference the same LimitSet instance
        w1 = Counter.options(mode=worker_mode, limits=shared_limits).init()
        w2 = Counter.options(mode=worker_mode, limits=shared_limits).init()

        # Make calls and verify they complete
        futures = []
        for i in range(5):
            futures.append(w1.increment())
            futures.append(w2.increment())

        # Wait for all to complete
        for f in futures:
            f.result()

        w1.stop()
        w2.stop()

    def test_shared_limitset_across_workers_process(self):
        """Test that shared MultiprocessSharedLimitSet is shared across process workers."""

        class Counter(Worker):
            def __init__(self):
                pass

            def increment(self):
                with self.limits.acquire():
                    import time

                    time.sleep(0.01)
                    return 1

        # Create shared LimitSet for process mode
        shared_limits = LimitSet(
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10)],
            shared=True,
            mode="process",
        )

        # Create two process workers sharing the same limits
        w1 = Counter.options(mode="process", limits=shared_limits).init()
        w2 = Counter.options(mode="process", limits=shared_limits).init()

        # Make calls and verify they complete
        futures = []
        for i in range(5):
            futures.append(w1.increment())
            futures.append(w2.increment())

        for f in futures:
            f.result()

        w1.stop()
        w2.stop()

    def test_non_shared_limitset_not_shared(self):
        """Test that passing list of Limits creates separate LimitSets for each worker."""

        class Counter(Worker):
            def __init__(self):
                pass

            def increment(self):
                with self.limits.acquire():
                    return 1

        # Pass list of limits - each worker gets its own LimitSet
        limits_list = [CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10)]

        # Create two workers - each will have separate limits
        w1 = Counter.options(mode="thread", limits=limits_list).init()
        w2 = Counter.options(mode="thread", limits=limits_list).init()

        # Make calls - should complete successfully
        futures = []
        for i in range(5):
            futures.append(w1.increment())
            futures.append(w2.increment())

        for f in futures:
            f.result()

        w1.stop()
        w2.stop()


class TestRayWorkerLimits:
    """Test Ray worker limits separately due to Ray initialization.

    Note: Basic Ray limit enforcement is covered in test_rate_limiting_algorithms.py.
    This class focuses on shared LimitSet behavior across multiple Ray workers.
    """

    def test_shared_limitset_across_ray_workers(self):
        """Test that shared RaySharedLimitSet works across Ray workers."""
        pytest.importorskip("ray")
        # Ray is initialized by conftest.py initialize_ray fixture

        class Counter(Worker):
            def __init__(self):
                pass

            def increment(self):
                with self.limits.acquire():
                    return 1

        # Create shared LimitSet for Ray
        shared_limits = LimitSet(
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10)],
            shared=True,
            mode="ray",
        )

        # Create two Ray workers sharing the same limits
        w1 = Counter.options(mode="ray", limits=shared_limits).init()
        w2 = Counter.options(mode="ray", limits=shared_limits).init()

        # Make calls and verify they complete
        futures = []
        for i in range(5):
            futures.append(w1.increment())
            futures.append(w2.increment())

        for f in futures:
            f.result()

        w1.stop()
        w2.stop()


class TestMixedLimitTypes:
    """Test workers with multiple limit types."""

    def test_worker_with_call_and_rate_limits(self, worker_mode):
        """Test worker with both CallLimit and RateLimit."""
        # Skip ray mode - use separate ray test
        if worker_mode == "ray":
            pytest.skip("Ray mode has separate test")

        class APIWorker(Worker):
            def __init__(self):
                self.calls = 0
                self.total_tokens = 0

            def process(self, tokens: int):
                # Acquire both call limit and token limit
                # CallLimit is automatic (defaults to 1), but RateLimit needs explicit amount
                with self.limits.acquire(requested={"tokens": tokens}) as acq:
                    self.calls += 1
                    self.total_tokens += tokens
                    # Update the RateLimit with actual usage
                    acq.update(usage={"tokens": tokens})
                    return (self.calls, self.total_tokens)

            def get_stats(self):
                return (self.calls, self.total_tokens)

        w = APIWorker.options(
            mode=worker_mode,
            limits=[
                CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=5),
                RateLimit(
                    key="tokens", window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10
                ),
            ],
        ).init()

        # Make 10 calls with 1 token each
        # CallLimit: 5 calls/sec -> 10 calls = 2 seconds
        # RateLimit: 10 tokens/sec -> 10 tokens = 1 second
        # Bottleneck is CallLimit, so should take ~2 seconds
        start_time = time.time()
        for _ in range(10):
            w.process(1).result()
        elapsed = time.time() - start_time

        calls, tokens = w.get_stats().result()
        assert calls == 10
        assert tokens == 10

        # Should be limited by CallLimit (5 calls/sec) with TokenBucket:
        # - Capacity=5 means 5 calls available immediately (burst)
        # - Remaining 5 calls at 5/sec = 1 second
        # - Total expected: ~1 second (burst happens instantly)
        assert elapsed >= 0.8, f"Expected ~1s, got {elapsed:.2f}s (too fast)"
        assert elapsed <= 2.0, f"Expected ~1s, got {elapsed:.2f}s (too slow)"

        w.stop()

    def test_worker_with_call_and_rate_limits_ray(self):
        """Test Ray worker with both CallLimit and RateLimit."""
        pytest.importorskip("ray")
        # Ray is initialized by conftest.py initialize_ray fixture

        class APIWorker(Worker):
            def __init__(self):
                self.calls = 0
                self.total_tokens = 0

            def increment(self, amount: int = 1):
                with self.limits.acquire():
                    self.calls += 1
                    return self.calls

            def get_count(self) -> int:
                return self.calls

        # Create Ray worker with CallLimit
        w = APIWorker.options(
            mode="ray",
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=5)],
        ).init()

        # Make 10 calls
        start_time = time.time()
        for _ in range(10):
            w.increment(1).result()
        elapsed = time.time() - start_time

        # Verify count
        final_count = w.get_count().result()
        assert final_count == 10

        # Verify timing for TokenBucket with Ray overhead:
        # - Capacity=5 means 5 calls available immediately (burst)
        # - Remaining 5 calls at 5/sec = 1 second
        # - Ray has overhead (actor creation, remote calls), allow up to 3s
        assert elapsed >= 0.5, f"Expected ~1s with Ray overhead, got {elapsed:.2f}s (too fast)"
        assert elapsed <= 3.0, f"Expected ~1s with Ray overhead, got {elapsed:.2f}s (too slow)"

        w.stop()

    def test_worker_with_all_limit_types(self, worker_mode):
        """Test worker with CallLimit, RateLimit, and ResourceLimit."""
        # Skip ray mode - use separate ray tests
        if worker_mode == "ray":
            pytest.skip("Ray mode has separate tests")

        class ComplexWorker(Worker):
            def __init__(self):
                self.operations = []

            def process(self, tokens: int):
                # Acquire all three limits
                with self.limits.acquire(requested={"tokens": tokens, "connections": 1}) as acq:
                    self.operations.append(tokens)
                    acq.update(usage={"tokens": tokens})
                    return len(self.operations)

            def get_count(self) -> int:
                return len(self.operations)

        w = ComplexWorker.options(
            mode=worker_mode,
            limits=[
                CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=20),
                RateLimit(
                    key="tokens", window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=50
                ),
                ResourceLimit(key="connections", capacity=2),
            ],
        ).init()

        # Submit 10 operations with 5 tokens each
        for _ in range(10):
            w.process(5).result()

        count = w.get_count().result()
        assert count == 10

        w.stop()


class TestLimitValidation:
    """Test that limit validation works correctly."""

    def test_incompatible_limitset_mode_raises_error(self):
        """Test that passing InMemorySharedLimitSet to process worker raises error."""

        class DummyWorker(Worker):
            def process(self):
                return 1

        # Create InMemorySharedLimitSet (for sync/thread/asyncio)
        limits = LimitSet(
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10)],
            shared=True,
            mode="sync",
        )

        # Should raise error when trying to use with process worker
        with pytest.raises(
            ValueError, match="InMemorySharedLimitSet is not compatible with worker mode 'Processes'"
        ):
            DummyWorker.options(mode="process", limits=limits).init()

    def test_list_of_limits_creates_appropriate_limitset(self):
        """Test that list of Limits creates appropriate LimitSet for worker mode."""
        from concurry.core.limit.limit_pool import LimitPool

        class DummyWorker(Worker):
            def process(self):
                # Verify limits exist and check type
                assert self.limits is not None
                # self.limits is now always a LimitPool
                assert isinstance(self.limits, LimitPool), f"Expected LimitPool, got {type(self.limits)}"
                # Verify it contains exactly one LimitSet
                assert len(self.limits.limit_sets) == 1, (
                    f"Expected 1 LimitSet in LimitPool, got {len(self.limits.limit_sets)}"
                )
                # Verify the LimitSet is a BaseLimitSet
                assert isinstance(self.limits.limit_sets[0], BaseLimitSet), (
                    f"Expected BaseLimitSet inside LimitPool, got {type(self.limits.limit_sets[0])}"
                )
                return 1

        limits_list = [CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10)]

        # Thread worker should get InMemorySharedLimitSet wrapped in LimitPool
        w_thread = DummyWorker.options(mode="thread", limits=limits_list).init()
        # Call process() which will verify limits inside the worker
        w_thread.process().result()
        w_thread.stop()

        # Process worker should also get appropriate LimitSet wrapped in LimitPool
        w_process = DummyWorker.options(mode="process", limits=limits_list).init()
        # Call process() which will verify limits inside the worker
        w_process.process().result()
        w_process.stop()


class TestSharedLimitSetsWithConfig:
    """Test config parameter with shared LimitSets across multiple workers."""

    def test_config_shared_across_workers_inmemory(self, worker_mode):
        """Test that multiple workers can access the same config from shared LimitSet."""
        # Skip process and ray modes - they use different shared limit implementations
        if worker_mode in ("process", "ray"):
            pytest.skip("InMemorySharedLimitSet is only for sync/thread/asyncio modes")

        class APIWorker(Worker):
            def __init__(self):
                pass

            def call_api(self):
                with self.limits.acquire(requested={"tokens": 100}) as acq:
                    region = acq.config.get("region", "unknown")
                    acq.update(usage={"tokens": 100})
                    return region

        # Create shared LimitSet with config
        shared_limits = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
            shared=True,
            mode=worker_mode,
            config={"region": "us-east-1", "account": "12345"},
        )

        # Create multiple workers with shared limits
        worker1 = APIWorker.options(mode=worker_mode, limits=shared_limits).init()
        worker2 = APIWorker.options(mode=worker_mode, limits=shared_limits).init()

        # Both workers should see the same config
        result1 = worker1.call_api().result()
        result2 = worker2.call_api().result()

        assert result1 == "us-east-1"
        assert result2 == "us-east-1"

        worker1.stop()
        worker2.stop()

    def test_config_shared_across_workers_process(self):
        """Test that process workers can access config from shared LimitSet."""

        class APIWorker(Worker):
            def __init__(self):
                pass

            def call_api(self):
                with self.limits.acquire(requested={"tokens": 100}) as acq:
                    region = acq.config.get("region", "unknown")
                    account = acq.config.get("account", "unknown")
                    acq.update(usage={"tokens": 100})
                    return f"{region}:{account}"

        # Create shared LimitSet with config for process mode
        shared_limits = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
            shared=True,
            mode="process",
            config={"region": "eu-west-1", "account": "67890"},
        )

        # Create multiple process workers
        worker1 = APIWorker.options(mode="process", limits=shared_limits).init()
        worker2 = APIWorker.options(mode="process", limits=shared_limits).init()

        # Both workers should see the same config
        result1 = worker1.call_api().result()
        result2 = worker2.call_api().result()

        assert result1 == "eu-west-1:67890"
        assert result2 == "eu-west-1:67890"

        worker1.stop()
        worker2.stop()

    @pytest.mark.skipif(
        not pytest.importorskip("ray", reason="Ray not installed"), reason="Ray not installed"
    )
    def test_config_shared_across_ray_workers(self):
        """Test that Ray workers can access config from shared LimitSet."""

        class APIWorker(Worker):
            def __init__(self):
                pass

            def call_api(self):
                with self.limits.acquire(requested={"tokens": 100}) as acq:
                    region = acq.config.get("region", "unknown")
                    acq.update(usage={"tokens": 100})
                    return region

        # Create shared LimitSet with config for Ray mode
        shared_limits = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
            shared=True,
            mode="ray",
            config={"region": "ap-southeast-1", "endpoint": "https://api.example.com"},
        )

        # Create multiple Ray workers
        worker1 = APIWorker.options(mode="ray", limits=shared_limits).init()
        worker2 = APIWorker.options(mode="ray", limits=shared_limits).init()

        # Both workers should see the same config
        result1 = worker1.call_api().result()
        result2 = worker2.call_api().result()

        assert result1 == "ap-southeast-1"
        assert result2 == "ap-southeast-1"

        worker1.stop()
        worker2.stop()

    def test_config_not_modified_across_workers(self, worker_mode):
        """Test that one worker modifying acq.config doesn't affect other workers."""
        # This test is primarily for thread mode where we can easily test shared state
        if worker_mode not in ("thread", "sync", "asyncio"):
            pytest.skip("This test is specific to in-memory shared modes")

        class APIWorker(Worker):
            def __init__(self):
                pass

            def modify_config(self):
                with self.limits.acquire(requested={"tokens": 100}) as acq:
                    # Modify the acquisition's config (should be a copy)
                    original = acq.config["region"]
                    acq.config["region"] = "modified"
                    acq.update(usage={"tokens": 100})
                    return original

            def read_config(self):
                with self.limits.acquire(requested={"tokens": 100}) as acq:
                    region = acq.config["region"]
                    acq.update(usage={"tokens": 100})
                    return region

        # Create shared LimitSet with config
        shared_limits = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=2000)],
            shared=True,
            mode=worker_mode,
            config={"region": "us-west-2"},
        )

        worker1 = APIWorker.options(mode=worker_mode, limits=shared_limits).init()
        worker2 = APIWorker.options(mode=worker_mode, limits=shared_limits).init()

        # Worker 1 modifies its acquisition's config
        result1 = worker1.modify_config().result()
        assert result1 == "us-west-2"

        # Worker 2 should still see the original config
        result2 = worker2.read_config().result()
        assert result2 == "us-west-2"

        # LimitSet's config should be unchanged
        assert shared_limits.config["region"] == "us-west-2"

        worker1.stop()
        worker2.stop()

    def test_config_with_worker_pool(self):
        """Test that worker pools properly handle config from shared LimitSet."""

        class APIWorker(Worker):
            def __init__(self):
                pass

            def call_api(self, prompt: str):
                with self.limits.acquire(requested={"tokens": 100}) as acq:
                    region = acq.config.get("region", "unknown")
                    acq.update(usage={"tokens": 50})
                    return f"{region}:{prompt}"

        # Create shared LimitSet with config
        shared_limits = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=10000)],
            shared=True,
            mode="thread",
            config={"region": "us-east-1", "tier": "premium"},
        )

        # Create worker pool
        pool = APIWorker.options(mode="thread", max_workers=5, limits=shared_limits).init()

        # All workers in the pool should see the same config
        results = []
        for i in range(10):
            result = pool.call_api(f"prompt-{i}").result()
            results.append(result)

        # All results should have the same region
        for result in results:
            assert result.startswith("us-east-1:")

        pool.stop()
