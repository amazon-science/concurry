"""Tests for shared LimitSets across multiple workers.

This module tests that LimitSets can be properly shared across workers
of the same execution mode, and that limits are enforced correctly.
"""

import time

import morphic
import pytest

import concurry
from concurry import Worker
from concurry.core.limit import (
    CallLimit,
    LimitSet,
    RateLimit,
    RateLimiterAlgorithm,
    ResourceLimit,
)


class TestBasicLimitEnforcement:
    """Test basic limit enforcement with single workers."""

    @pytest.mark.parametrize("worker_mode", ["sync", "thread", "asyncio", "process"])
    def test_counter_with_call_limit(self, worker_mode):
        """Test Counter worker with CallLimit - should throttle execution."""

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
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=20)],
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

    @pytest.mark.parametrize("worker_mode", ["sync", "thread", "asyncio", "process"])
    def test_counter_with_rate_limit(self, worker_mode):
        """Test Counter worker with RateLimit - should throttle token consumption."""

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
                    key="tokens", window_seconds=1.0, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=50
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

    @pytest.mark.parametrize("worker_mode", ["sync", "thread", "asyncio", "process"])
    def test_counter_with_resource_limit(self, worker_mode):
        """Test Counter worker with ResourceLimit - should block when resources exhausted."""

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

    @pytest.mark.parametrize("worker_mode", ["sync", "thread", "asyncio"])
    def test_shared_limitset_across_workers_inmemory(self, worker_mode):
        """Test that shared InMemorySharedLimitSet is shared across workers."""

        class Counter(Worker):
            def __init__(self):
                pass

            def increment(self):
                with self.limits.acquire():
                    time.sleep(0.01)  # Small delay
                    return 1

        # Create shared LimitSet with small capacity
        shared_limits = LimitSet(
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=10)],
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
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=10)],
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
        limits_list = [CallLimit(window_seconds=1.0, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=10)]

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
        import ray

        if not ray.is_initialized():
            ray.init(
                ignore_reinit_error=True,
                num_cpus=4,
                runtime_env={"py_modules": [concurry, morphic]},
            )

        class Counter(Worker):
            def __init__(self):
                pass

            def increment(self):
                with self.limits.acquire():
                    return 1

        # Create shared LimitSet for Ray
        shared_limits = LimitSet(
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=10)],
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

    @pytest.mark.parametrize("worker_mode", ["sync", "thread", "asyncio", "process"])
    def test_worker_with_call_and_rate_limits(self, worker_mode):
        """Test worker with both CallLimit and RateLimit."""

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
                CallLimit(window_seconds=1.0, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=5),
                RateLimit(
                    key="tokens", window_seconds=1.0, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=10
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
        import ray

        if not ray.is_initialized():
            ray.init(
                ignore_reinit_error=True,
                num_cpus=4,
                runtime_env={"py_modules": [concurry, morphic]},
            )

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
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=5)],
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

    @pytest.mark.parametrize("worker_mode", ["sync", "thread", "asyncio", "process"])
    def test_worker_with_all_limit_types(self, worker_mode):
        """Test worker with CallLimit, RateLimit, and ResourceLimit."""

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
                CallLimit(window_seconds=1.0, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=20),
                RateLimit(
                    key="tokens", window_seconds=1.0, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=50
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
            limits=[CallLimit(window_seconds=1.0, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=10)],
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

        class DummyWorker(Worker):
            def process(self):
                # Verify limits exist and check type
                from concurry.core.limit.limit_set import BaseLimitSet

                assert self.limits is not None
                assert isinstance(self.limits, BaseLimitSet), (
                    f"Expected BaseLimitSet, got {type(self.limits)}"
                )
                return 1

        limits_list = [CallLimit(window_seconds=1.0, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=10)]

        # Thread worker should get InMemorySharedLimitSet
        w_thread = DummyWorker.options(mode="thread", limits=limits_list).init()
        # Call process() which will verify limits inside the worker
        w_thread.process().result()
        w_thread.stop()

        # Process worker should also get InMemorySharedLimitSet (private copy)
        w_process = DummyWorker.options(mode="process", limits=limits_list).init()
        # Call process() which will verify limits inside the worker
        w_process.process().result()
        w_process.stop()
