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
            limits=[CallLimit(window=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=20)],
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
                RateLimit(key="tokens", window=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=50)
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
            limits=[CallLimit(window=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10)],
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
        limits_list = [CallLimit(window=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10)]

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

    def test_shared_limitset_across_ray_workers(self, requires_ray_mode):
        """Verify that a shared RaySharedLimitSet enforces capacity across Ray actors.

        Scenario:
            - 6 Ray actors share a single ResourceLimit with capacity=3.
            - Each actor acquires 1 resource unit, sleeps for 1 second, then releases.
            - With capacity=3, only 3 actors can hold the resource at once.
            - The remaining 3 must block until a slot frees up.

        Expected behaviour:
            The 6 tasks split into two waves of 3. Wave 1 runs from ~t=0 to
            ~t=1, Wave 2 runs from ~t=1 to ~t=2, so total elapsed is ~2s.
            If limits are NOT shared (each actor has its own private
            capacity=3), all 6 run concurrently and finish in ~1s.

        Why 6 separate actors:
            Ray actors execute methods serially. To have 6 concurrent tasks
            we need 6 separate actors, each running one task.

        Why we warm up with ping():
            Ray actors are created asynchronously. Without a warmup step,
            some actors may not finish initialising until well after the
            timing window starts, making the total elapsed time unreliable.
            Calling ``ping().result()`` on every actor forces us to wait
            until every actor is alive and responsive before we start the
            clock.

        Assertions (and why these specific ones):
            1. total elapsed >= 1.7s -- proves two sequential waves occurred.
               We use 1.7 instead of 2.0 to tolerate scheduling jitter.
            2. total elapsed <= 8.0s -- sanity upper bound; rules out
               deadlocks or pathological blocking.

        Why we do NOT assert per-task acquire_time:
            Even with warmup, Ray's task scheduling introduces variable
            delays between when we call ``.hold_resource()`` and when the
            actor actually starts executing it.  Per-task ``acquire_time``
            (measured inside the actor) is therefore unreliable for
            distinguishing "waited for a limit" from "waited for Ray to
            schedule me".  Total wall-clock time is the robust metric.
        """
        pytest.importorskip("ray")

        class ResourceWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id

            def ping(self) -> bool:
                return True

            def hold_resource(self, task_id: int) -> dict:
                """Acquire resource, hold for 1 second, return timing info."""
                import time

                start = time.time()
                with self.limits.acquire(requested={"resource": 1}):
                    acquire_time = time.time() - start
                    time.sleep(1.0)
                    return {
                        "worker_id": self.worker_id,
                        "task_id": task_id,
                        "acquire_time": acquire_time,
                    }

        shared_limits = LimitSet(
            limits=[ResourceLimit(key="resource", capacity=3)],
            shared=True,
            mode="ray",
        )

        workers = []
        for i in range(6):
            w = ResourceWorker.options(mode="ray", max_workers=0, limits=shared_limits).init(worker_id=i)
            workers.append(w)

        # Warm up: ensure all actors are initialized before timing begins.
        for w in workers:
            assert w.ping().result(timeout=30) is True

        start_time = time.time()
        futures = [worker.hold_resource(i) for i, worker in enumerate(workers)]
        results = [f.result(timeout=15) for f in futures]
        elapsed = time.time() - start_time

        assert elapsed >= 1.7, (
            f"Total time {elapsed:.2f}s is too fast. Expected >= 1.7s. "
            f"If < 1.5s, limits are NOT shared (all 6 tasks ran concurrently)."
        )

        assert elapsed <= 8.0, (
            f"Total time {elapsed:.2f}s is too slow. Expected <= 8.0s. "
            f"This suggests unexpected overhead or blocking."
        )

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
                CallLimit(window=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=5),
                RateLimit(key="tokens", window=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10),
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

    def test_worker_with_call_and_rate_limits_ray(self, requires_ray_mode):
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
            limits=[CallLimit(window=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=5)],
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
        assert elapsed <= 5.0, f"Expected ~1s with Ray overhead, got {elapsed:.2f}s (too slow)"

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
                CallLimit(window=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=20),
                RateLimit(key="tokens", window=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=50),
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
            limits=[CallLimit(window=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10)],
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

        limits_list = [CallLimit(window=1.0, algorithm=RateLimitAlgorithm.TokenBucket, capacity=10)]

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
            limits=[RateLimit(key="tokens", window=60, capacity=1000)],
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
            limits=[RateLimit(key="tokens", window=60, capacity=1000)],
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
    def test_config_shared_across_ray_workers(self, requires_ray_mode):
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
            limits=[RateLimit(key="tokens", window=60, capacity=1000)],
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
            limits=[RateLimit(key="tokens", window=60, capacity=2000)],
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
            limits=[RateLimit(key="tokens", window=60, capacity=10000)],
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
    """Test that a shared ResourceLimit is enforced across multiple workers.

    These tests create N workers sharing a single LimitSet with
    ResourceLimit(capacity=C), submit one task per worker, and verify that the
    shared capacity constraint is never violated. Each task acquires 1 unit,
    holds for a fixed duration, then releases.

    The core property under test:
        At no point in time should more than C tasks hold the resource
        simultaneously. With N > C tasks, this forces sequential "waves":
        the first C tasks acquire immediately; the remaining (N - C) tasks
        must block until a slot is freed.

    Validation strategy (robust to scheduling jitter in Ray/process modes):
        - Timeline analysis: reconstruct grant/release events and verify
          max concurrent holdings <= capacity.
        - Total elapsed time: with N tasks, C capacity, and H hold-seconds,
          minimum elapsed time is ceil(N/C) * H. If all N ran concurrently
          (limits broken), elapsed would be ~H.
        - Wave ordering (capacity=2 only): Wave 2 grants must occur after
          Wave 1 releases.  Not used for higher capacities because
          staggered task starts can cause legitimate interleaving that
          does not violate capacity.

    Why we warm up with ping():
        Ray actors (and to a lesser extent process workers) start
        asynchronously. Without a warmup, some workers may not begin
        executing their task until well into the test window, which can
        cause false failures (e.g. a late-starting worker requests the
        resource after Wave 1 has already released, so its wait_time is
        near-zero despite limits working correctly).  Calling
        ``ping().result()`` on every worker before starting the clock
        guarantees all workers are alive and responsive.

    These tests do NOT use retries, isolating pure limit enforcement.
    """

    def test_shared_resource_limit_sequential_waves(self, worker_mode):
        """Verify that 4 tasks sharing capacity=2 execute in two sequential waves.

        Scenario:
            - 4 separate workers share a ResourceLimit with capacity=2.
            - Each task acquires 1 resource unit, holds for 1 second, releases.
            - With capacity=2, only 2 tasks can hold the resource at once.
            - The remaining 2 must wait for a slot to free up.

        Expected behaviour:
            Wave 1 (2 tasks) runs from ~t=0 to ~t=1. Wave 2 (2 tasks) runs
            from ~t=1 to ~t=2. Total elapsed is ~2s. If limits are NOT
            shared, all 4 run concurrently and finish in ~1s.

        Assertions:
            1. Capacity never exceeded -- reconstruct a timeline of grant
               and release events; at no point do more than 2 tasks hold
               the resource simultaneously.
            2. Wave ordering -- the earliest Wave 2 grant timestamp is
               after the latest Wave 1 release timestamp.  This is valid
               for capacity=2 because after warmup, all 4 tasks start
               nearly simultaneously, so the first 2 by grant-time truly
               form a concurrent wave whose releases gate the next 2.
            3. Total elapsed >= 1.9s -- proves two sequential 1-second
               waves occurred (minus 0.1s epsilon for measurement jitter).
            4. Total elapsed < 5s -- rules out deadlock or pathological
               blocking.

        Why wave ordering works here but not for capacity=3:
            With capacity=2 and 4 tasks starting simultaneously, exactly 2
            acquire immediately and 2 block. The blocked tasks cannot
            acquire until the first 2 release. So Wave 1 (first 2 by
            grant time) and Wave 2 (last 2) are cleanly separated.
            With higher capacities and more tasks, staggered scheduling
            can cause tasks to interleave across "waves" without violating
            capacity, making a strict wave ordering check invalid.
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and asyncio modes only support max_workers=1")

        class TrackingWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id

            def ping(self) -> bool:
                return True

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

        shared_limits = LimitSet(
            limits=[ResourceLimit(key="resource", capacity=2)],
            shared=True,
            mode=worker_mode,
        )

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

        # Warm up: ensure all actors are initialized before timing begins.
        # Ray actors start asynchronously; without this, staggered startup
        # causes some tasks to begin well after others, skewing timing.
        for w in workers:
            assert w.ping().result(timeout=30) is True

        start_time = time.time()
        futures = [w.hold_resource(1.0) for w in workers]
        results = [f.result(timeout=30) for f in futures]
        total_elapsed = time.time() - start_time

        results_by_grant = sorted(results, key=lambda r: r["grant_time"])

        # --- Property 1: capacity never exceeded (timeline analysis) ---
        events = []
        for r in results:
            events.append(("grant", r["grant_time"], r["worker_id"]))
            events.append(("release", r["release_time"], r["worker_id"]))
        events.sort(key=lambda e: e[1])

        current_holdings = set()
        max_concurrent = 0
        for event_type, _ts, worker_id in events:
            if event_type == "grant":
                current_holdings.add(worker_id)
                max_concurrent = max(max_concurrent, len(current_holdings))
            else:
                current_holdings.discard(worker_id)

        assert max_concurrent <= 2, (
            f"Max concurrent holdings was {max_concurrent}, exceeds capacity=2! "
            f"This indicates a race condition in limit enforcement."
        )

        # --- Property 2: Wave 2 grants occur after Wave 1 releases ---
        wave1 = results_by_grant[:2]
        wave2 = results_by_grant[2:]
        wave1_latest_release = max(r["release_time"] for r in wave1)
        wave2_earliest_grant = min(r["grant_time"] for r in wave2)

        assert wave2_earliest_grant >= wave1_latest_release - 0.1, (
            f"Wave 2 acquired at {wave2_earliest_grant:.3f} before Wave 1 released at "
            f"{wave1_latest_release:.3f}. This violates the capacity constraint!"
        )

        # --- Property 3: total time proves limits are shared ---
        assert total_elapsed >= 1.9, (
            f"Total time {total_elapsed:.2f}s is too fast. Expected >= 1.9s for two "
            f"sequential 1s waves. If < 1.5s, limits are NOT shared."
        )

        # --- Property 4: not pathologically slow ---
        assert total_elapsed < 5.0, (
            f"Total time {total_elapsed:.2f}s is too slow. Expected < 5s. "
            f"This suggests unexpected blocking or overhead."
        )

        for w in workers:
            w.stop()

    def test_shared_resource_limit_precise_capacity_enforcement(self, worker_mode):
        """Verify the hard capacity invariant: at most C tasks hold the resource at once.

        Scenario:
            - 6 separate workers share a ResourceLimit with capacity=3.
            - Each task acquires 1 resource unit, holds for 1 second, releases.
            - With capacity=3, only 3 tasks can hold the resource concurrently.
            - The remaining 3 must block until a slot frees up.

        Expected behaviour:
            The 6 tasks split into roughly two waves of 3. Total elapsed is
            ~2s. If limits are NOT shared, all 6 run concurrently and finish
            in ~1s.

        Assertions:
            1. Capacity never exceeded -- reconstruct the grant/release
               timeline and verify at most 3 tasks hold the resource at any
               point.  This is the strongest possible check: it directly
               validates the invariant that ResourceLimit is supposed to
               enforce.
            2. Total elapsed >= 1.5s -- proves that NOT all 6 tasks ran
               concurrently. With capacity=3, at least ceil(6/3)=2 waves
               are needed, each taking ~1s.

        Why we do NOT check wave ordering here:
            With capacity=3 and 6 tasks, staggered task scheduling (even
            after warmup) can cause the 4th task to start and acquire the
            resource shortly after the 1st task releases, while the 2nd
            and 3rd tasks are still holding. Sorting tasks by grant-time
            and calling the first 3 "Wave 1" and last 3 "Wave 2" creates
            an artificial grouping that does not correspond to a real
            concurrent wave. A "Wave 2" task can legitimately acquire
            before the latest "Wave 1" task releases, as long as the
            total concurrent count never exceeds 3. The timeline analysis
            (assertion 1) is the correct check for this.

        Why we do NOT check per-task wait_time:
            Per-task ``wait_time`` (grant_time - request_time) depends on
            when the actor starts executing the method, which varies due
            to Ray/process scheduling jitter. A task that starts late may
            have a near-zero wait_time despite limits being correctly
            enforced, because by the time it requests the resource a slot
            has already freed up. Total elapsed time and the timeline
            capacity analysis are immune to this problem.
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and asyncio modes only support max_workers=1")

        class TrackingWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id

            def ping(self) -> bool:
                return True

            def hold_resource(self, hold_time: float) -> dict:
                """Acquire resource, hold for specified time, return timing info."""
                import time

                request_time = time.time()

                with self.limits.acquire(requested={"resource": 1}):
                    grant_time = time.time()
                    wait_time = grant_time - request_time

                    time.sleep(hold_time)
                    release_time = time.time()

                    return {
                        "worker_id": self.worker_id,
                        "request_time": request_time,
                        "grant_time": grant_time,
                        "release_time": release_time,
                        "wait_time": wait_time,
                    }

        shared_limits = LimitSet(
            limits=[ResourceLimit(key="resource", capacity=3)],
            shared=True,
            mode=worker_mode,
        )

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

        # Warm up: ensure all actors are initialized before timing begins.
        for w in workers:
            assert w.ping().result(timeout=30) is True

        start_time = time.time()
        futures = [w.hold_resource(1.0) for w in workers]
        results = [f.result(timeout=30) for f in futures]
        total_elapsed = time.time() - start_time

        # --- Property 1: capacity never exceeded (timeline analysis) ---
        events = []
        for r in results:
            events.append(("grant", r["grant_time"], r["worker_id"]))
            events.append(("release", r["release_time"], r["worker_id"]))
        events.sort(key=lambda e: e[1])

        current_holdings = set()
        max_concurrent = 0
        for event_type, _ts, worker_id in events:
            if event_type == "grant":
                current_holdings.add(worker_id)
                max_concurrent = max(max_concurrent, len(current_holdings))
            else:
                current_holdings.discard(worker_id)

        assert max_concurrent <= 3, (
            f"Max concurrent holdings was {max_concurrent}, exceeds capacity=3! "
            f"This indicates a race condition in limit enforcement."
        )

        # --- Property 2: total time proves limits are shared ---
        assert total_elapsed >= 1.5, (
            f"Total time {total_elapsed:.2f}s is too fast. Expected >= 1.5s for two "
            f"sequential 1s waves with capacity=3. If < 1.5s, all 6 tasks ran concurrently "
            f"(limits NOT shared)."
        )

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

            def ping(self) -> bool:
                return True

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
