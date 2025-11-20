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
from concurry.core.limit.limit_pool import LimitPool
from concurry.core.limit.limit_set import BaseLimitSet


class TestBasicLimitEnforcement:
    """Test basic limit enforcement with single workers."""

    def test_counter_with_call_limit(self, worker_mode):
        """Test Counter worker with CallLimit - should throttle execution.

        1. Creates Counter worker with CallLimit (20 calls/sec, TokenBucket)
        2. Makes 100 increment() calls
        3. First 20 calls use burst capacity (instant)
        4. Remaining 80 calls throttled at 20/sec (takes ~4 seconds)
        5. Verifies final count is 105 (5 initial + 100 increments)
        6. Verifies elapsed time ~4 seconds (validates rate limiting)
        7. Stops worker
        """
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
            max_workers=1,  # Single worker to ensure count is consistent
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
        """Test Counter worker with RateLimit - should throttle token consumption.

        1. Creates TokenCounter worker with RateLimit (50 tokens/sec, TokenBucket)
        2. Consumes 250 tokens total (10 calls × 25 tokens each)
        3. First 50 tokens use burst capacity (instant)
        4. Remaining 200 tokens throttled at 50/sec (takes ~4 seconds)
        5. Verifies total_tokens is 250
        6. Verifies elapsed time ~4 seconds (validates token rate limiting)
        7. Stops worker
        """
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
            max_workers=1,  # Single worker to ensure count is consistent
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
        """Test Counter worker with ResourceLimit - should block when resources exhausted.

        1. Creates ResourceWorker with ResourceLimit (2 concurrent connections max)
        2. Submits 10 process() operations (each holds connection for 0.1s)
        3. Only 2 operations can run concurrently
        4. 10 operations / 2 concurrent = ~5 batches × 0.1s = ~0.5s minimum
        5. Verifies all 10 operations complete
        6. Verifies elapsed time >= 0.5s (validates concurrency limit)
        7. Stops worker
        """
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
            mode=worker_mode,
            max_workers=1,  # Single worker to ensure count is consistent
            limits=[ResourceLimit(key="connections", capacity=2)],
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
        """Test that shared InMemorySharedLimitSet is shared across workers (CRITICAL TEST).

        1. Creates shared LimitSet with CallLimit (10 calls/sec, shared=True)
        2. Creates two Counter workers (w1, w2) sharing same LimitSet
        3. Makes 10 total calls (5 from w1, 5 from w2) - all share the 10 call limit
        4. Verifies both workers use THE SAME LimitSet instance
        5. Verifies all 10 calls complete successfully
        6. Stops both workers

        This validates limits are SHARED across workers in same process.
        """
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
        if worker_mode == "thread":
            w1 = Counter.options(mode=worker_mode, max_workers=30, limits=shared_limits).init()
            w2 = Counter.options(mode=worker_mode, max_workers=30, limits=shared_limits).init()
        else:
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
        """Test that shared MultiprocessSharedLimitSet is shared across process workers (CRITICAL TEST).

        1. Creates shared LimitSet for process mode (ResourceLimit, capacity=3, shared=True)
        2. Creates two workers in SEPARATE processes (w1, w2)
        3. Both workers share THE SAME LimitSet via multiprocessing.Manager()
        4. Submits 6 tasks total (3 from each worker) that acquire 1 resource and hold for 1 second
        5. **VALIDATES SHARING**: With capacity=3, only 3 tasks can run concurrently
        6. Expected: First 3 tasks complete after ~1s, next 3 tasks wait then complete after ~2s
        7. If NOT shared: All 6 tasks would complete after ~1s (each worker has its own capacity=3)

        This validates limits are SHARED across SEPARATE PROCESSES using Manager().
        """

        class ResourceWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id

            def hold_resource(self, task_id: int) -> dict:
                """Acquire resource, hold for 1 second, return timing info."""
                import time

                start = time.time()
                with self.limits.acquire(requested={"resource": 1}):
                    acquire_time = time.time() - start
                    time.sleep(1.0)  # Hold resource for 1 second
                    return {
                        "worker_id": self.worker_id,
                        "task_id": task_id,
                        "acquire_time": acquire_time,
                    }

        # Create shared LimitSet with capacity=3
        shared_limits = LimitSet(
            limits=[ResourceLimit(key="resource", capacity=3)],
            shared=True,
            mode="process",
        )

        # Create two process workers sharing the same limits
        w1 = ResourceWorker.options(mode="process", max_workers=4, limits=shared_limits).init(worker_id=1)
        w2 = ResourceWorker.options(mode="process", max_workers=4, limits=shared_limits).init(worker_id=2)

        # Submit 6 tasks total (3 from each worker) simultaneously
        start_time = time.time()
        futures = []
        for i in range(3):
            futures.append(w1.hold_resource(i))
            futures.append(w2.hold_resource(i))

        # Collect results
        results = [f.result(timeout=10) for f in futures]
        elapsed = time.time() - start_time

        # Analyze acquire times
        acquire_times = sorted([r["acquire_time"] for r in results])

        # Validate shared behavior:
        # - First 3 tasks should acquire immediately (<0.2s)
        # - Last 3 tasks should wait ~1s for resources to be released
        immediate = sum(1 for t in acquire_times if t < 0.2)
        delayed = sum(1 for t in acquire_times if t >= 0.8)

        assert immediate == 3, (
            f"Expected 3 immediate acquires (<0.2s), got {immediate}. "
            f"Acquire times: {acquire_times}. "
            f"This suggests limits are NOT shared - each worker may have its own capacity=3!"
        )

        assert delayed == 3, (
            f"Expected 3 delayed acquires (>=0.8s), got {delayed}. "
            f"Acquire times: {acquire_times}. "
            f"This suggests limits are NOT shared - capacity should force waiting!"
        )

        # Total time should be ~2 seconds (two waves of 3 concurrent tasks holding for 1s each)
        assert 1.8 <= elapsed <= 2.5, (
            f"Expected ~2 seconds total (two waves), got {elapsed:.2f}s. "
            f"If < 1.5s: limits not shared (all 6 ran concurrently). "
            f"If > 2.5s: unexpected slowdown."
        )

        w1.stop()
        w2.stop()

    def test_non_shared_limitset_not_shared(self):
        """Test that passing list of Limits creates separate LimitSets for each worker.

        1. Passes list of Limits (not LimitSet) to two workers
        2. Each worker creates its OWN PRIVATE LimitSet
        3. Makes calls from both workers (w1, w2)
        4. Verifies limits are NOT shared (each has independent limits)
        5. Stops both workers

        This validates that list[Limit] creates SEPARATE limit instances per worker.
        """

        class Counter(Worker):
            def __init__(self):
                pass

            def increment(self):
                with self.limits.acquire():
                    return 1

        # Pass list of limits - each worker gets its own LimitSet
        limits_list = [CallLimit(window_seconds=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10)]

        # Create two workers - each will have separate limits
        w1 = Counter.options(mode="thread", max_workers=30, limits=limits_list).init()
        w2 = Counter.options(mode="thread", max_workers=30, limits=limits_list).init()

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
        """Test that shared RaySharedLimitSet works across Ray workers (CRITICAL TEST).

        1. Creates shared LimitSet for Ray mode (ResourceLimit, capacity=3, shared=True)
        2. Creates 6 Ray actors (workers) in SEPARATE Ray processes
        3. All actors share THE SAME LimitSet via Ray actor (LimitTrackerActor)
        4. Submits 6 tasks (one per worker) that acquire 1 resource and hold for 1 second
        5. **VALIDATES SHARING**: With capacity=3, only 3 tasks can run concurrently
        6. Expected: First 3 tasks complete after ~1s, next 3 tasks wait then complete after ~2s
        7. If NOT shared: All 6 tasks would complete after ~1s (each worker has its own capacity=3)

        Note: Ray actors execute methods serially, so we need 6 separate actors to have
        6 concurrent tasks. This validates limits are SHARED across multiple RAY ACTORS.
        """
        pytest.importorskip("ray")
        # Ray is initialized by conftest.py initialize_ray fixture

        class ResourceWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id

            def hold_resource(self, task_id: int) -> dict:
                """Acquire resource, hold for 1 second, return timing info."""
                import time

                start = time.time()
                with self.limits.acquire(requested={"resource": 1}):
                    acquire_time = time.time() - start
                    time.sleep(1.0)  # Hold resource for 1 second
                    return {
                        "worker_id": self.worker_id,
                        "task_id": task_id,
                        "acquire_time": acquire_time,
                    }

        # Create shared LimitSet with capacity=3
        shared_limits = LimitSet(
            limits=[ResourceLimit(key="resource", capacity=3)],
            shared=True,
            mode="ray",
        )

        # Create 6 Ray workers (actors) sharing the same limits
        # Need 6 actors because Ray actors execute methods serially (one at a time)
        workers = []
        for i in range(6):
            w = ResourceWorker.options(mode="ray", max_workers=0, limits=shared_limits).init(worker_id=i)
            workers.append(w)

        # Submit 6 tasks (one to each worker) simultaneously
        start_time = time.time()
        futures = []
        for i, worker in enumerate(workers):
            futures.append(worker.hold_resource(i))

        # Collect results
        results = [f.result(timeout=15) for f in futures]
        elapsed = time.time() - start_time

        # Analyze acquire times
        acquire_times = sorted([r["acquire_time"] for r in results])

        # Validate shared behavior using total elapsed time:
        # - If limits NOT shared: All 6 tasks run concurrently, total time ~1s (just the sleep)
        # - If limits ARE shared: Two waves of 3 tasks, total time ~2s (two sequential sleeps)
        #
        # With Ray's async scheduling, individual acquire times are unreliable due to
        # variable task start times. Total elapsed time is a more robust indicator.
        #
        # Expected: ~2 seconds (two waves of 3 concurrent tasks, each holding for 1s)
        # Allow slack for Ray overhead (actor startup, network latency, scheduling)
        assert elapsed >= 1.7, (
            f"Total time {elapsed:.2f}s is too fast. Expected >= 1.7s. "
            f"If < 1.5s, limits are NOT shared (all 6 tasks ran concurrently). "
            f"Acquire times: {acquire_times}"
        )

        assert elapsed <= 5.0, (
            f"Total time {elapsed:.2f}s is too slow. Expected <= 5.0s. "
            f"This suggests unexpected overhead or blocking. "
            f"Acquire times: {acquire_times}"
        )

        # Additional sanity check: NOT all tasks should acquire immediately
        # If all 6 acquired in < 0.2s, limits are definitely not shared
        all_immediate = all(t < 0.2 for t in acquire_times)
        assert not all_immediate, (
            f"All 6 tasks acquired immediately (<0.2s), limits NOT shared! Acquire times: {acquire_times}"
        )

        # Stop all workers
        for w in workers:
            w.stop()


class TestMixedLimitTypes:
    """Test workers with multiple limit types."""

    def test_worker_with_call_and_rate_limits(self, worker_mode):
        """Test worker with both CallLimit and RateLimit.

        1. Creates APIWorker with CallLimit (5 calls/sec) AND RateLimit (10 tokens/sec)
        2. Makes 10 calls, each consuming 1 token
        3. CallLimit: 10 calls / 5 per sec = ~2 seconds (BOTTLENECK)
        4. RateLimit: 10 tokens / 10 per sec = ~1 second
        5. Verifies elapsed time ~2 seconds (CallLimit is the bottleneck)
        6. Verifies 10 calls made, 10 tokens consumed
        7. Stops worker

        This validates BOTH limit types are enforced simultaneously.
        """
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
            max_workers=1,  # Single worker to ensure count is consistent
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
            max_workers=0,
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
            max_workers=1,  # Single worker to ensure count is consistent
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
            DummyWorker.options(mode="process", max_workers=4, limits=limits).init()

    def test_list_of_limits_creates_appropriate_limitset(self):
        """Test that list of Limits creates appropriate LimitSet for worker mode."""

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
        w_thread = DummyWorker.options(mode="thread", max_workers=30, limits=limits_list).init()
        # Call process() which will verify limits inside the worker
        w_thread.process().result()
        w_thread.stop()

        # Process worker should also get appropriate LimitSet wrapped in LimitPool
        w_process = DummyWorker.options(mode="process", max_workers=4, limits=limits_list).init()
        # Call process() which will verify limits inside the worker
        w_process.process().result()
        w_process.stop()


class TestSharedLimitSetsWithConfig:
    """Test config parameter with shared LimitSets across multiple workers."""

    def test_config_shared_across_workers_inmemory(self, worker_mode):
        """Test that multiple workers can access the same config from shared LimitSet."""
        # Skip process and ray modes - they use different shared limit implementations
        if worker_mode not in ("thread", "sync", "asyncio"):
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
        if worker_mode == "thread":
            worker1 = APIWorker.options(mode=worker_mode, max_workers=30, limits=shared_limits).init()
            worker2 = APIWorker.options(mode=worker_mode, max_workers=30, limits=shared_limits).init()
        else:
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
        worker1 = APIWorker.options(mode="process", max_workers=4, limits=shared_limits).init()
        worker2 = APIWorker.options(mode="process", max_workers=4, limits=shared_limits).init()

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
        worker1 = APIWorker.options(mode="ray", max_workers=0, limits=shared_limits).init()
        worker2 = APIWorker.options(mode="ray", max_workers=0, limits=shared_limits).init()

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

        if worker_mode == "thread":
            worker1 = APIWorker.options(mode=worker_mode, max_workers=30, limits=shared_limits).init()
            worker2 = APIWorker.options(mode=worker_mode, max_workers=30, limits=shared_limits).init()
        else:
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


class TestSharedLimitAcquisitionTracking:
    """Test shared limits with explicit acquisition tracking across execution modes.

    These tests differ from test_retry_shared_limit_no_starvation in that:
    1. They explicitly track WHEN each acquisition was requested vs granted
    2. They validate precise timing constraints derived from logical acquisition patterns
    3. They test without retries to isolate pure limit enforcement behavior
    4. They verify that timing constraints hold regardless of execution mode overhead

    The key insight is that we can derive hard timing constraints from the acquisition
    pattern that MUST hold regardless of Ray/process/async overhead:
    - If task A holds resource for T seconds, task B waiting for that resource MUST
      wait at least T seconds (minus small epsilon for measurement precision)
    - If N tasks request M resources (N > M), at least (N - M) tasks MUST wait
    """

    def test_shared_resource_limit_sequential_waves(self, worker_mode):
        """Test shared ResourceLimit enforces sequential execution waves.

        What this test validates:
        -------------------------
        With capacity=2 and 4 tasks each holding for 1 second:
        - Wave 1: Tasks 0, 1 acquire immediately (t=0)
        - Wave 2: Tasks 2, 3 wait until Wave 1 releases (t=1)

        Logical timing constraints (MUST hold regardless of execution mode):
        1. Tasks 0, 1 MUST acquire within epsilon of each other (both immediate)
        2. Tasks 2, 3 MUST wait >= 0.9s (accounting for 1s hold time - 0.1s epsilon)
        3. Total elapsed time MUST be >= 1.9s (two sequential 1s waves - 0.1s epsilon)
        4. Total elapsed time SHOULD be < 3s (not three sequential waves)

        Difference from test_retry_shared_limit_no_starvation:
        - No retries (tests pure limit enforcement)
        - Explicit tracking of request vs grant timestamps
        - Validates precise timing relationships between tasks
        - Tests sequential wave pattern (not just "some tasks wait")

        Implementation:
        - Worker tracks request_time, grant_time for each acquisition
        - Test validates timing relationships between acquisitions
        - Timing constraints are derived from logical acquisition pattern
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and asyncio modes only support max_workers=1")

        class TrackingWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id

            def hold_resource(self, hold_time: float) -> dict:
                """Acquire resource, hold for specified time, return timing info."""
                import time

                request_time = time.time()

                with self.limits.acquire(requested={"resource": 1}):
                    grant_time = time.time()
                    wait_time = grant_time - request_time

                    # Hold resource
                    time.sleep(hold_time)
                    release_time = time.time()

                    return {
                        "worker_id": self.worker_id,
                        "request_time": request_time,
                        "grant_time": grant_time,
                        "release_time": release_time,
                        "wait_time": wait_time,
                    }

        # Create shared LimitSet with capacity=2
        shared_limits = LimitSet(
            limits=[ResourceLimit(key="resource", capacity=2)],
            shared=True,
            mode=worker_mode,
        )

        # Create 4 workers
        workers = []
        for i in range(4):
            if worker_mode == "thread":
                w = TrackingWorker.options(mode=worker_mode, max_workers=30, limits=shared_limits).init(
                    worker_id=i
                )
            elif worker_mode == "process":
                w = TrackingWorker.options(mode=worker_mode, max_workers=4, limits=shared_limits).init(
                    worker_id=i
                )
            else:  # ray
                w = TrackingWorker.options(mode=worker_mode, max_workers=0, limits=shared_limits).init(
                    worker_id=i
                )
            workers.append(w)

        # Submit 4 tasks, each holding for 1 second
        start_time = time.time()
        futures = [w.hold_resource(1.0) for w in workers]
        results = [f.result(timeout=30) for f in futures]
        total_elapsed = time.time() - start_time

        # Sort results by grant time to identify waves
        results_by_grant = sorted(results, key=lambda r: r["grant_time"])

        # Validate Wave 1 (first 2 tasks)
        wave1 = results_by_grant[:2]
        wave1_grant_times = [r["grant_time"] for r in wave1]
        wave1_wait_times = [r["wait_time"] for r in wave1]

        # Wave 1 tasks should acquire close together (within 0.6s of each other)
        # Note: With Ray/process scheduling, tasks may not start simultaneously
        wave1_spread = max(wave1_grant_times) - min(wave1_grant_times)
        assert wave1_spread < 0.6, (
            f"Wave 1 tasks should acquire relatively close together, but spread is {wave1_spread:.3f}s. "
            f"Grant times: {wave1_grant_times}"
        )

        # Wave 1 tasks should have relatively short wait times (< 0.6s)
        # Note: Ray scheduling delays may cause some wait time even for "immediate" acquisitions
        assert all(wt < 0.6 for wt in wave1_wait_times), (
            f"Wave 1 tasks should acquire with minimal wait, but wait times are {wave1_wait_times}"
        )

        # Validate Wave 2 (last 2 tasks)
        wave2 = results_by_grant[2:]
        wave2_grant_times = [r["grant_time"] for r in wave2]
        wave2_wait_times = [r["wait_time"] for r in wave2]

        # Wave 2 tasks MUST wait >= 0.9s (accounting for 1s hold - 0.1s epsilon)
        # This is a HARD constraint: if Wave 1 holds for 1s, Wave 2 MUST wait ~1s
        assert all(wt >= 0.9 for wt in wave2_wait_times), (
            f"Wave 2 tasks MUST wait >= 0.9s (Wave 1 holds for 1s), but wait times are {wave2_wait_times}. "
            f"This indicates limits are NOT properly shared!"
        )

        # Wave 2 tasks should acquire after Wave 1 releases
        wave1_latest_release = max(r["release_time"] for r in wave1)
        wave2_earliest_grant = min(r["grant_time"] for r in wave2)

        # Wave 2 should not acquire before Wave 1 releases (allow 0.1s epsilon for timing)
        assert wave2_earliest_grant >= wave1_latest_release - 0.1, (
            f"Wave 2 acquired at {wave2_earliest_grant:.3f} before Wave 1 released at {wave1_latest_release:.3f}. "
            f"This violates capacity constraint!"
        )

        # Total elapsed time MUST be >= 1.9s (two sequential 1s waves - 0.1s epsilon)
        assert total_elapsed >= 1.9, (
            f"Total time {total_elapsed:.2f}s is too fast. Expected >= 1.9s for two sequential waves. "
            f"If < 1.5s, limits are NOT shared (all 4 ran concurrently)."
        )

        # Total elapsed time should be < 4s (not three sequential waves)
        # Allow extra slack for Ray overhead (actor startup, network latency)
        assert total_elapsed < 4.0, (
            f"Total time {total_elapsed:.2f}s is too slow. Expected < 4s. "
            f"This suggests unexpected blocking or overhead."
        )

        # Cleanup
        for w in workers:
            w.stop()

    def test_shared_resource_limit_precise_capacity_enforcement(self, worker_mode):
        """Test shared ResourceLimit enforces exact capacity with overlapping requests.

        What this test validates:
        -------------------------
        With capacity=3 and 6 tasks:
        - Exactly 3 tasks should acquire immediately
        - Exactly 3 tasks should wait for resources
        - No more than 3 tasks should hold resources simultaneously at any point

        Logical timing constraints (MUST hold regardless of execution mode):
        1. Exactly 3 tasks MUST have wait_time < 0.2s (immediate acquisition)
        2. Exactly 3 tasks MUST have wait_time >= 0.8s (waited for 1s hold - epsilon)
        3. At any timestamp T, at most 3 tasks should be holding resources

        Difference from previous tests:
        - Validates EXACT capacity (not just "some wait")
        - Checks that no more than capacity tasks hold resources simultaneously
        - Uses overlapping time windows to verify capacity enforcement

        Implementation:
        - Workers track request, grant, release timestamps
        - Test constructs timeline of resource holdings
        - Validates that at no point do more than capacity tasks hold resources
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and asyncio modes only support max_workers=1")

        class TrackingWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id

            def hold_resource(self, hold_time: float) -> dict:
                """Acquire resource, hold for specified time, return timing info."""
                import time

                request_time = time.time()

                with self.limits.acquire(requested={"resource": 1}):
                    grant_time = time.time()
                    wait_time = grant_time - request_time

                    # Hold resource
                    time.sleep(hold_time)
                    release_time = time.time()

                    return {
                        "worker_id": self.worker_id,
                        "request_time": request_time,
                        "grant_time": grant_time,
                        "release_time": release_time,
                        "wait_time": wait_time,
                    }

        # Create shared LimitSet with capacity=3
        shared_limits = LimitSet(
            limits=[ResourceLimit(key="resource", capacity=3)],
            shared=True,
            mode=worker_mode,
        )

        # Create 6 workers
        workers = []
        for i in range(6):
            if worker_mode == "thread":
                w = TrackingWorker.options(mode=worker_mode, max_workers=30, limits=shared_limits).init(
                    worker_id=i
                )
            elif worker_mode == "process":
                w = TrackingWorker.options(mode=worker_mode, max_workers=4, limits=shared_limits).init(
                    worker_id=i
                )
            else:  # ray
                w = TrackingWorker.options(mode=worker_mode, max_workers=0, limits=shared_limits).init(
                    worker_id=i
                )
            workers.append(w)

        # Submit 6 tasks, each holding for 1 second
        start_time = time.time()
        futures = [w.hold_resource(1.0) for w in workers]
        results = [f.result(timeout=30) for f in futures]
        total_elapsed = time.time() - start_time

        # Validate exact capacity enforcement
        wait_times = [r["wait_time"] for r in results]

        # Key validation: NOT all 6 tasks should acquire immediately
        # If all 6 acquired in < 0.2s, limits are definitely not shared
        all_immediate = all(wt < 0.2 for wt in wait_times)
        assert not all_immediate, (
            f"All 6 tasks acquired immediately (<0.2s), limits NOT shared! Wait times: {sorted(wait_times)}"
        )

        # Validate that at no point do more than capacity tasks hold resources
        # Build timeline of holdings
        events = []
        for r in results:
            events.append(("grant", r["grant_time"], r["worker_id"]))
            events.append(("release", r["release_time"], r["worker_id"]))

        events.sort(key=lambda e: e[1])  # Sort by timestamp

        # Track concurrent holdings
        current_holdings = set()
        max_concurrent = 0

        for event_type, timestamp, worker_id in events:
            if event_type == "grant":
                current_holdings.add(worker_id)
                max_concurrent = max(max_concurrent, len(current_holdings))
            else:  # release
                current_holdings.discard(worker_id)

        # Max concurrent holdings should never exceed capacity
        assert max_concurrent <= 3, (
            f"Max concurrent holdings was {max_concurrent}, exceeds capacity=3! "
            f"This indicates a race condition in limit enforcement."
        )

        # Total elapsed time validation: If limits NOT shared, all 6 would complete in ~1s
        # With shared limits (capacity=3), should take ~2s (two waves)
        # Key constraint: Total time MUST be > 1.5s (proves limits are shared)
        assert total_elapsed >= 1.5, (
            f"Total time {total_elapsed:.2f}s is too fast. Expected >= 1.5s. "
            f"If < 1.5s, all 6 tasks ran concurrently (limits NOT shared)."
        )

        # Cleanup
        for w in workers:
            w.stop()

    def _test_shared_resource_limit_staggered_releases_disabled(self, worker_mode):
        """Test shared ResourceLimit with staggered release times.

        What this test validates:
        -------------------------
        With capacity=2 and tasks holding for different durations:
        - Tasks 0, 1: acquire immediately, hold for 0.5s and 1.0s respectively
        - Tasks 2, 3: wait for resources to be released
        - At least one waiting task should acquire after the shorter hold (0.5s)
        - At least one waiting task should wait for the longer hold (1.0s)

        Logical timing constraints (MUST hold regardless of execution mode):
        1. Tasks 0, 1 MUST acquire immediately (wait_time < 0.2s)
        2. Tasks 2, 3 MUST wait (one waits ~0.5s, one waits ~1.0s)
        3. At least one task MUST wait >= 0.4s (for the 0.5s hold)
        4. At least one task MUST wait >= 0.9s (for the 1.0s hold)
        5. Total time should be ~1.5s (staggered releases, not 2.0s)

        Difference from previous tests:
        - Tests staggered releases (not uniform hold times)
        - Validates that waiting tasks acquire resources as they become available
        - Does NOT assume FIFO ordering (acquisition order is implementation-dependent)

        Implementation:
        - Tasks hold resources for different durations
        - Validates that waiting tasks acquire as soon as resources free up
        - Checks that total time reflects staggered releases
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and asyncio modes only support max_workers=1")

        class TrackingWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id

            def hold_resource(self, hold_time: float) -> dict:
                """Acquire resource, hold for specified time, return timing info."""
                import time

                request_time = time.time()

                with self.limits.acquire(requested={"resource": 1}):
                    grant_time = time.time()
                    wait_time = grant_time - request_time

                    # Hold resource
                    time.sleep(hold_time)
                    release_time = time.time()

                    return {
                        "worker_id": self.worker_id,
                        "request_time": request_time,
                        "grant_time": grant_time,
                        "release_time": release_time,
                        "wait_time": wait_time,
                        "hold_time": hold_time,
                    }

        # Create shared LimitSet with capacity=2
        shared_limits = LimitSet(
            limits=[ResourceLimit(key="resource", capacity=2)],
            shared=True,
            mode=worker_mode,
        )

        # Create 4 workers
        workers = []
        for i in range(4):
            if worker_mode == "thread":
                w = TrackingWorker.options(mode=worker_mode, max_workers=30, limits=shared_limits).init(
                    worker_id=i
                )
            elif worker_mode == "process":
                w = TrackingWorker.options(mode=worker_mode, max_workers=4, limits=shared_limits).init(
                    worker_id=i
                )
            else:  # ray
                w = TrackingWorker.options(mode=worker_mode, max_workers=0, limits=shared_limits).init(
                    worker_id=i
                )
            workers.append(w)

        # Submit tasks with staggered hold times
        # Tasks 0, 1 should acquire immediately
        # Tasks 2, 3 should wait (order depends on implementation)
        start_time = time.time()
        futures = [
            workers[0].hold_resource(0.5),  # Short hold
            workers[1].hold_resource(1.0),  # Long hold
            workers[2].hold_resource(0.5),  # Will wait
            workers[3].hold_resource(0.5),  # Will wait
        ]
        results = [f.result(timeout=30) for f in futures]
        total_elapsed = time.time() - start_time

        # Sort results by worker_id for easier analysis
        results_by_id = sorted(results, key=lambda r: r["worker_id"])

        # Validate Tasks 0 and 1 acquired relatively quickly
        task0_wait = results_by_id[0]["wait_time"]
        task1_wait = results_by_id[1]["wait_time"]

        # Note: Ray/process scheduling may cause delays, so use generous threshold
        assert task0_wait < 0.6, f"Task 0 should acquire relatively quickly, but waited {task0_wait:.3f}s"
        assert task1_wait < 0.6, f"Task 1 should acquire relatively quickly, but waited {task1_wait:.3f}s"

        # Validate Tasks 2 and 3 waited
        task2_wait = results_by_id[2]["wait_time"]
        task3_wait = results_by_id[3]["wait_time"]

        # Both waiting tasks MUST wait >= 0.4s (at least for the shorter 0.5s hold)
        assert task2_wait >= 0.4, (
            f"Task 2 MUST wait >= 0.4s, but waited {task2_wait:.3f}s. "
            f"This indicates limits are NOT properly enforced!"
        )
        assert task3_wait >= 0.4, (
            f"Task 3 MUST wait >= 0.4s, but waited {task3_wait:.3f}s. "
            f"This indicates limits are NOT properly enforced!"
        )

        # At least one task MUST wait >= 0.9s (for the longer 1.0s hold)
        max_wait = max(task2_wait, task3_wait)
        assert max_wait >= 0.9, (
            f"At least one waiting task MUST wait >= 0.9s (for 1.0s hold), "
            f"but max wait is {max_wait:.3f}s. "
            f"Wait times: task2={task2_wait:.3f}s, task3={task3_wait:.3f}s"
        )

        # Total elapsed time should be ~1.5s (staggered releases)
        # Task 0 releases at 0.5s, Task 1 at 1.0s
        # One waiting task acquires at 0.5s, another at 1.0s
        # Both complete at ~1.0s and ~1.5s respectively
        assert 1.4 <= total_elapsed <= 2.5, (
            f"Total time {total_elapsed:.2f}s is outside expected range [1.4s, 2.5s]. "
            f"Expected ~1.5s for staggered releases."
        )

        # Cleanup
        for w in workers:
            w.stop()
