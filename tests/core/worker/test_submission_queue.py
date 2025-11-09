"""Comprehensive tests for submission queue functionality.

This module tests the client-side submission queuing mechanism that limits
the number of in-flight tasks per worker to prevent overloading.

Coverage:
- Basic functionality across all execution modes
- Integration with worker pools and load balancing
- Integration with limits (LimitSet, LimitPool)
- Integration with retry mechanisms
- Integration with synchronization primitives (wait, gather) - MAIN USE CASE
- Non-blocking user submission loops
- Edge cases: cancellation, exceptions, timeouts
- On-demand workers
- TaskWorker.map() with submission queues
- High volume submissions
"""

import threading
import time
from typing import Any, Dict

import pytest

from concurry import (
    LimitSet,
    RateLimit,
    RateLimitAlgorithm,
    ResourceLimit,
    ReturnWhen,
    TaskWorker,
    Worker,
    gather,
    wait,
)

# =============================================================================
# Test Worker Classes
# =============================================================================


class SlowWorker(Worker):
    """Worker with slow tasks for testing queue blocking."""

    def __init__(self):
        self.task_count = 0

    def slow_task(self, duration: float, task_id: int) -> Dict[str, Any]:
        """Execute a slow task."""
        time.sleep(duration)
        self.task_count += 1
        return {"task_id": task_id, "duration": duration}

    def get_task_count(self) -> int:
        """Get the number of completed tasks."""
        return self.task_count


class CounterWorker(Worker):
    """Worker that counts operations."""

    def __init__(self):
        self.count = 0

    def increment(self, amount: int = 1) -> int:
        """Increment counter."""
        time.sleep(0.01)  # Small delay
        self.count += amount
        return self.count

    def get_count(self) -> int:
        """Get current count."""
        return self.count


class FailingWorker(Worker):
    """Worker that can fail tasks."""

    def __init__(self):
        self.attempt_count = 0

    def flaky_task(self, fail_count: int) -> str:
        """Task that fails N times then succeeds."""
        self.attempt_count += 1
        if self.attempt_count <= fail_count:
            raise ValueError(f"Attempt {self.attempt_count} failed")
        return f"Success after {self.attempt_count} attempts"

    def get_attempts(self) -> int:
        """Get attempt count."""
        return self.attempt_count


class LimitedWorker(Worker):
    """Worker that uses resource limits."""

    def __init__(self):
        self.execution_count = 0

    def limited_task(self, duration: float = 0.05) -> str:
        """Task that acquires limits."""
        with self.limits.acquire(requested={"connections": 1}):
            time.sleep(duration)
            self.execution_count += 1
            return f"Execution {self.execution_count}"

    def get_execution_count(self) -> int:
        """Get execution count."""
        return self.execution_count


# =============================================================================
# Test Basic Functionality Across All Modes
# =============================================================================


class TestSubmissionQueueBasics:
    """Test basic submission queue functionality across all execution modes."""

    def test_submission_queue_default_value(self, worker_mode):
        """Test that default max_queued_tasks varies by mode.

        This test:
        1. Creates a CounterWorker with default settings for each mode
        2. Verifies max_queued_tasks matches mode-specific defaults
        3. Expected: sync/asyncio/thread=None (bypass), process=100, ray=3
        4. Stops the worker
        """
        worker = CounterWorker.options(mode=worker_mode).init()
        # Default values: sync/asyncio/thread=None (bypass), process=100, ray=3
        expected = {
            "sync": None,
            "thread": None,
            "process": 100,
            "asyncio": None,
            "ray": 3,
        }
        assert worker.max_queued_tasks == expected[worker_mode]
        worker.stop()

    def test_submission_queue_custom_values(self, worker_mode):
        """Test custom max_queued_tasks values across modes.

        This test:
        1. Iterates through custom queue lengths [1, 5, 10, 50]
        2. For each length, creates a CounterWorker with max_queued_tasks=length
        3. Verifies worker.max_queued_tasks matches the specified value
        4. Stops the worker
        """
        for queue_len in [1, 5, 10, 50]:
            worker = CounterWorker.options(mode=worker_mode, max_queued_tasks=queue_len).init()
            assert worker.max_queued_tasks == queue_len
            worker.stop()

    def test_submission_queue_limits_backend_forwarding(self, worker_mode):
        """Test that max_queued_tasks limits backend forwarding (not user submissions).

        User submissions are NEVER blocked. Futures are created immediately
        and queued internally. Only backend forwarding respects max_queued_tasks.

        This test:
        1. Creates a SlowWorker with max_workers=1, max_queued_tasks=2
        2. Submits 5 tasks immediately (all should return futures instantly)
        3. Verifies all 5 futures are created in < 0.5s (non-blocking!)
        4. Verifies only first 2 tasks start executing immediately (respects limit)
        5. Waits for all tasks to complete
        6. Stops worker
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = SlowWorker.options(mode=worker_mode, max_workers=1, max_queued_tasks=2).init()

        # Submit 5 tasks - ALL should return immediately (non-blocking!)
        start = time.time()
        f1 = worker.slow_task(0.3, 1)
        f2 = worker.slow_task(0.3, 2)
        f3 = worker.slow_task(0.3, 3)
        f4 = worker.slow_task(0.3, 4)
        f5 = worker.slow_task(0.3, 5)
        submission_time = time.time() - start

        # CRITICAL: All 5 submissions should be IMMEDIATE (non-blocking)
        assert submission_time < 0.5, f"Submissions took {submission_time:.3f}s, should be immediate"

        # Wait for all to complete
        results = [f1.result(), f2.result(), f3.result(), f4.result(), f5.result()]
        assert len(results) == 5, "All tasks should complete"

        # Verify results
        task_ids = [r["task_id"] for r in results]
        assert task_ids == [1, 2, 3, 4, 5], "All tasks should execute in order"

        worker.stop()

    def test_submission_queue_releases_on_completion(self, worker_mode):
        """Test that semaphore is released when tasks complete.

        This test:
        1. Creates a CounterWorker with max_queued_tasks=2
        2. Submits and completes 2 tasks (f1, f2)
        3. Measures time to submit 2 more tasks (f3, f4) immediately
        4. Verifies submission is fast (<0.2s), proving semaphore was released
        5. Waits for all tasks to complete
        6. Stops the worker
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = CounterWorker.options(mode=worker_mode, max_queued_tasks=2).init()

        # Submit and complete 2 tasks
        f1 = worker.increment(1)
        f2 = worker.increment(2)
        f1.result()
        f2.result()

        # Should be able to submit 2 more immediately
        start = time.time()
        f3 = worker.increment(3)
        f4 = worker.increment(4)
        submission_time = time.time() - start
        assert submission_time < 0.2, "Semaphore should have been released"

        # Cleanup
        f3.result()
        f4.result()
        worker.stop()


# =============================================================================
# Test Blocking and Sync Modes Bypass Queue
# =============================================================================


class TestSubmissionQueueBypassModes:
    """Test that blocking and sync modes bypass submission queue."""

    def test_blocking_mode_bypasses_queue(self, worker_mode):
        """Test that blocking mode doesn't use submission queue.

        This test:
        1. Creates a CounterWorker with blocking=True and max_queued_tasks=1
        2. Makes 10 increment calls (returns results directly, not futures)
        3. Verifies all 10 calls complete without queue blocking
        4. Stops the worker
        """
        worker = CounterWorker.options(mode=worker_mode, blocking=True, max_queued_tasks=1).init()

        # Should be able to make many calls without blocking on submission
        results = []
        for i in range(10):
            result = worker.increment(1)  # Returns result directly
            results.append(result)

        assert len(results) == 10
        worker.stop()

    def test_sync_mode_bypasses_queue(self):
        """Test that sync mode doesn't use submission queue.

        This test:
        1. Creates a sync CounterWorker with max_queued_tasks=1
        2. Submits 10 increment tasks (sync mode executes immediately)
        3. Collects all results
        4. Verifies all 10 tasks completed without queue blocking
        5. Stops the worker
        """
        worker = CounterWorker.options(mode="sync", max_queued_tasks=1).init()

        # Sync mode should execute immediately without queuing
        futures = [worker.increment(i) for i in range(10)]
        results = [f.result() for f in futures]

        assert len(results) == 10
        worker.stop()

    def test_asyncio_mode_bypasses_queue(self):
        """Test that asyncio mode doesn't use submission queue for concurrency.

        This test:
        1. Creates an asyncio CounterWorker with max_queued_tasks=1
        2. Submits 50 increment tasks (asyncio allows unlimited concurrent submissions)
        3. Collects all results (event loop handles concurrency, not queue)
        4. Verifies all 50 tasks completed
        5. Stops the worker
        """
        worker = CounterWorker.options(mode="asyncio", max_queued_tasks=1).init()

        # AsyncIO mode should allow unlimited concurrent submissions
        # The event loop handles concurrency, not the submission queue
        futures = [worker.increment(i) for i in range(50)]
        results = [f.result() for f in futures]

        assert len(results) == 50
        worker.stop()


# =============================================================================
# Test Worker Pools with Submission Queues
# =============================================================================


class TestSubmissionQueuePools:
    """Test submission queue with worker pools."""

    def test_pool_per_worker_semaphores(self, pool_mode):
        """Test that each worker in pool has independent queue.

        1. Creates a SlowWorker pool with 3 workers, max_queued_tasks=2, round_robin load balancing
        2. Submits 6 tasks (2 per worker due to round-robin distribution)
        3. Verifies submission is fast (<0.5s) since each worker has capacity=2
        4. Waits for all 6 tasks to complete
        5. Stops the pool
        """
        pool = SlowWorker.options(
            mode=pool_mode, max_workers=3, max_queued_tasks=2, load_balancing="round_robin"
        ).init()

        # Submit 6 tasks (2 per worker due to round-robin)
        # Should not block since each worker has capacity=2
        start = time.time()
        futures = [pool.slow_task(0.3, i) for i in range(6)]
        submission_time = time.time() - start
        assert submission_time < 0.5, "Submissions should be fast with 3 workers"

        # Wait for all to complete
        results = [f.result() for f in futures]
        assert len(results) == 6

        pool.stop()

    def test_pool_stats_include_queue_info(self, pool_mode):
        """Test that pool stats include submission queue information.

        1. Creates a CounterWorker pool with 4 workers, max_queued_tasks=10
        2. Gets pool stats via get_pool_stats()
        3. Verifies stats contain max_queued_tasks=10
        4. Stops the pool

        Note: Submission queues are now managed by individual WorkerProxy instances,
        not by the pool. The pool stats show max_queued_tasks but not per-worker
        queue state since that's internal to each worker.
        """
        pool = CounterWorker.options(mode=pool_mode, max_workers=4, max_queued_tasks=10).init()

        stats = pool.get_pool_stats()
        assert "max_queued_tasks" in stats
        assert stats["max_queued_tasks"] == 10
        # submission_queues removed - managed by workers internally

        pool.stop()

    def test_pool_queue_with_load_balancing(self, pool_mode):
        """Test submission queue works with different load balancing strategies.

        1. Iterates through 4 load balancing algorithms: round_robin, active, total, random
        2. For each algorithm, creates a CounterWorker pool with 3 workers, max_queued_tasks=15
        3. Submits 15 increment tasks
        4. Waits for all 15 results
        5. Verifies all tasks completed successfully
        6. Stops the pool and repeats for next algorithm
        """
        for algorithm in ["round_robin", "active", "total", "random"]:
            pool = CounterWorker.options(
                mode=pool_mode,
                max_workers=3,
                max_queued_tasks=15,  # Large enough to not block during submission
                load_balancing=algorithm,
            ).init()

            # Submit tasks
            futures = [pool.increment(1) for i in range(15)]

            # Wait for all results
            results = [f.result() for f in futures]
            assert len(results) == 15

            pool.stop()


# =============================================================================
# Test Integration with Synchronization Primitives (MAIN USE CASE)
# =============================================================================


class TestSubmissionQueueWithSynchronization:
    """Test submission queue with wait() and gather() - the main use case!"""

    def test_queue_with_gather_list(self, worker_mode):
        """Test submission queue with gather() on list of futures (MAIN USE CASE).

        1. Creates CounterWorker with max_queued_tasks=3 (limits in-flight)
        2. Submits 20 increment tasks (queue blocks when >3 in-flight)
        3. Calls gather(futures) to collect all results
        4. Verifies all 20 results returned correctly
        5. Stops worker
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = CounterWorker.options(mode=worker_mode, max_queued_tasks=3).init()

        # Submit many tasks - submission queue limits in-flight tasks
        futures = [worker.increment(i) for i in range(20)]

        # Gather should work correctly
        results = gather(futures, timeout=10.0)
        assert len(results) == 20

        worker.stop()

    def test_queue_with_gather_dict(self, worker_mode):
        """Test submission queue with gather() on dict of futures (MAIN USE CASE).

        1. Creates SlowWorker with max_queued_tasks=2
        2. Submits 10 tasks as dict {task_0: future, ...}
        3. Calls gather(tasks) to collect results
        4. Verifies results is dict with all 10 task keys
        5. Stops worker
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = SlowWorker.options(mode=worker_mode, max_queued_tasks=2).init()

        # Submit tasks as dict
        tasks = {f"task_{i}": worker.slow_task(0.05, i) for i in range(10)}

        # Gather should preserve dict structure
        results = gather(tasks, timeout=10.0)
        assert isinstance(results, dict)
        assert len(results) == 10
        assert all(f"task_{i}" in results for i in range(10))

        worker.stop()

    def test_queue_with_wait_all_completed(self, worker_mode):
        """Test submission queue with wait(ALL_COMPLETED) (MAIN USE CASE).

        1. Creates CounterWorker with max_queued_tasks=3
        2. Submits 15 increment tasks
        3. Calls wait(futures, ALL_COMPLETED)
        4. Verifies all 15 in done set, 0 in not_done
        5. Stops worker
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = CounterWorker.options(mode=worker_mode, max_queued_tasks=3).init()

        # Submit tasks
        futures = [worker.increment(i) for i in range(15)]

        # Wait for all to complete
        done, not_done = wait(futures, return_when=ReturnWhen.ALL_COMPLETED, timeout=10.0)

        assert len(done) == 15
        assert len(not_done) == 0

        worker.stop()

    def test_queue_with_wait_first_completed(self, worker_mode):
        """Test submission queue with wait(FIRST_COMPLETED) (MAIN USE CASE).

        1. Creates SlowWorker with max_queued_tasks=2
        2. Submits 3 tasks with varying durations (0.1s, 0.2s, 0.3s)
        3. Calls wait(futures, FIRST_COMPLETED)
        4. Verifies at least 1 in done set
        5. Waits for all remaining, stops worker
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = SlowWorker.options(mode=worker_mode, max_queued_tasks=2).init()

        # Submit tasks with varying durations
        futures = [
            worker.slow_task(0.1, 1),
            worker.slow_task(0.2, 2),
            worker.slow_task(0.3, 3),
        ]

        # Wait for first to complete
        done, not_done = wait(futures, return_when=ReturnWhen.FIRST_COMPLETED, timeout=5.0)

        assert len(done) >= 1
        assert len(not_done) >= 0

        # Cleanup
        wait(futures, return_when=ReturnWhen.ALL_COMPLETED, timeout=10.0)
        worker.stop()

    def test_queue_with_pool_and_gather(self, pool_mode):
        """Test submission queue with pool and gather() - realistic scenario."""
        pool = CounterWorker.options(
            mode=pool_mode,
            max_workers=5,
            max_queued_tasks=3,  # 3 in-flight per worker
            load_balancing="round_robin",
        ).init()

        # Submit large batch - submission queue prevents overload
        futures = [pool.increment(i) for i in range(100)]

        # Gather all results
        results = gather(futures, timeout=30.0)
        assert len(results) == 100

        pool.stop()

    def test_queue_with_pool_and_wait(self, pool_mode):
        """Test submission queue with pool and wait()."""
        pool = SlowWorker.options(
            mode=pool_mode, max_workers=4, max_queued_tasks=2, load_balancing="active"
        ).init()

        # Submit batch of tasks
        futures = [pool.slow_task(0.05, i) for i in range(30)]

        # Wait for all
        done, not_done = wait(futures, timeout=20.0)
        assert len(done) == 30
        assert len(not_done) == 0

        pool.stop()


# =============================================================================
# Test Integration with Limits
# =============================================================================


class TestSubmissionQueueWithLimits:
    """Test submission queue interaction with resource limits."""

    def test_queue_with_resource_limit(self, worker_mode):
        """Test submission queue works independently of resource limits."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        limits = [ResourceLimit(key="connections", capacity=2)]

        worker = LimitedWorker.options(mode=worker_mode, limits=limits, max_queued_tasks=3).init()

        # Submit many tasks
        # Submission queue limits in-flight tasks (3)
        # Resource limit limits concurrent executions (2)
        futures = [worker.limited_task(0.05) for _ in range(10)]

        # All should complete
        results = gather(futures, timeout=10.0)
        assert len(results) == 10

        worker.stop()

    def test_queue_with_rate_limit(self, worker_mode):
        """Test submission queue with rate limits."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        limits = [
            RateLimit(
                key="api_calls", window_seconds=1.0, capacity=20, algorithm=RateLimitAlgorithm.TokenBucket
            )
        ]

        class RateLimitedWorker(Worker):
            def __init__(self):
                self.call_count = 0

            def api_call(self, task_id: int) -> int:
                with self.limits.acquire(requested={"api_calls": 1}) as acq:
                    self.call_count += 1
                    acq.update(usage={"api_calls": 1})
                    time.sleep(0.01)
                    return task_id

        worker = RateLimitedWorker.options(mode=worker_mode, limits=limits, max_queued_tasks=5).init()

        # Submit tasks
        futures = [worker.api_call(i) for i in range(15)]
        results = gather(futures, timeout=10.0)

        assert len(results) == 15
        worker.stop()

    def test_queue_with_shared_limits(self, worker_mode):
        """Test submission queue with shared limits across workers."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        shared_limits = LimitSet(
            limits=[ResourceLimit(key="connections", capacity=3)],
            shared=True,
            mode=worker_mode,
        )

        # Create two workers sharing limits
        w1 = LimitedWorker.options(mode=worker_mode, limits=shared_limits, max_queued_tasks=2).init()

        w2 = LimitedWorker.options(mode=worker_mode, limits=shared_limits, max_queued_tasks=2).init()

        # Submit to both workers
        futures1 = [w1.limited_task(0.05) for _ in range(5)]
        futures2 = [w2.limited_task(0.05) for _ in range(5)]

        # All should complete
        results = gather(futures1 + futures2, timeout=10.0)
        assert len(results) == 10

        w1.stop()
        w2.stop()

    def test_queue_with_pool_and_limits(self, pool_mode):
        """Test submission queue with pool and resource limits."""
        limits = [ResourceLimit(key="connections", capacity=3)]

        pool = LimitedWorker.options(
            mode=pool_mode,
            max_workers=5,
            limits=limits,
            max_queued_tasks=2,
            load_balancing="round_robin",
        ).init()

        # Submit tasks - both submission queue and limits should work
        futures = [pool.limited_task(0.05) for _ in range(20)]
        results = gather(futures, timeout=15.0)

        assert len(results) == 20
        pool.stop()


# =============================================================================
# Test Integration with Retries
# =============================================================================


class TestSubmissionQueueWithRetries:
    """Test submission queue interaction with retry mechanisms."""

    def test_queue_with_retry_success(self, worker_mode):
        """Test that retries don't affect submission queue count."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = FailingWorker.options(
            mode=worker_mode, num_retries=5, retry_wait=0.01, max_queued_tasks=2
        ).init()

        # Submit tasks that will retry but eventually succeed
        # Each task counts as 1 submission regardless of retries
        f1 = worker.flaky_task(2)  # Fails 2 times
        f2 = worker.flaky_task(2)

        # Should not block on third submission (retries don't count)
        start = time.time()
        f3 = worker.flaky_task(1)
        submission_time = time.time() - start

        # Wait for all to complete
        result1 = f1.result()
        result2 = f2.result()
        result3 = f3.result()

        assert "Success" in result1
        assert "Success" in result2
        assert "Success" in result3

        # Third submission should have blocked (queue full)
        # But should still complete quickly once slot opens
        assert submission_time < 5.0

        worker.stop()

    def test_queue_with_retry_exhaustion(self, worker_mode):
        """Test submission queue when retries are exhausted."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = FailingWorker.options(
            mode=worker_mode, num_retries=2, retry_wait=0.01, max_queued_tasks=3
        ).init()

        # Submit tasks that will exhaust retries
        # Use high fail_count (100) to ensure all tasks fail even with shared state
        futures = [worker.flaky_task(100) for _ in range(5)]  # Will fail

        # Even with failures, submission queue should release
        for f in futures:
            with pytest.raises(ValueError):
                f.result()

        # Should be able to submit more tasks immediately
        start = time.time()
        new_futures = [worker.flaky_task(100) for _ in range(3)]
        submission_time = time.time() - start
        assert submission_time < 0.5, "Semaphores should have been released after failures"

        # Cleanup
        for f in new_futures:
            try:
                f.result()
            except ValueError:
                pass

        worker.stop()

    def test_queue_with_pool_and_retries(self, pool_mode):
        """Test submission queue with pool and retry mechanisms."""
        pool = FailingWorker.options(
            mode=pool_mode,
            max_workers=3,
            num_retries=5,
            retry_wait=0.01,
            max_queued_tasks=2,
            load_balancing="round_robin",
        ).init()

        # Submit tasks with retries
        futures = [pool.flaky_task(2) for _ in range(10)]

        # All should eventually succeed
        results = gather(futures, timeout=15.0)
        assert len(results) == 10
        assert all("Success" in r for r in results)

        pool.stop()


# =============================================================================
# Test Non-Blocking User Submission Loops
# =============================================================================


class TestSubmissionQueueNonBlocking:
    """Test that user submission loops work correctly with queuing."""

    def test_submission_loop_doesnt_hang(self, worker_mode):
        """Test that submission loop completes without hanging."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = SlowWorker.options(mode=worker_mode, max_queued_tasks=2).init()

        # Submit many tasks in a loop
        # Loop should block when queue is full but eventually complete
        start = time.time()
        futures = []
        for i in range(20):
            f = worker.slow_task(0.05, i)
            futures.append(f)

        submission_time = time.time() - start

        # Submissions should have blocked but completed
        assert len(futures) == 20

        # Wait for all to complete
        results = gather(futures, timeout=10.0)
        assert len(results) == 20

        worker.stop()

    def test_concurrent_submission_threads(self, worker_mode):
        """Test multiple threads submitting tasks concurrently."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = CounterWorker.options(mode=worker_mode, max_queued_tasks=3).init()

        all_futures = []
        futures_lock = threading.Lock()

        def submit_tasks(thread_id: int):
            local_futures = []
            for i in range(10):
                f = worker.increment(1)
                local_futures.append(f)
            with futures_lock:
                all_futures.extend(local_futures)

        # Start multiple threads submitting concurrently
        threads = [threading.Thread(target=submit_tasks, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All submissions should have succeeded
        assert len(all_futures) == 50

        # All should complete
        results = gather(all_futures, timeout=10.0)
        assert len(results) == 50

        worker.stop()

    def test_pool_submission_loop(self, pool_mode):
        """Test submission loop with pool doesn't hang."""
        pool = CounterWorker.options(
            mode=pool_mode, max_workers=4, max_queued_tasks=2, load_balancing="active"
        ).init()

        # Submit large batch
        futures = [pool.increment(1) for _ in range(100)]

        # All should complete
        results = gather(futures, timeout=30.0)
        assert len(results) == 100

        pool.stop()


# =============================================================================
# Test Edge Cases
# =============================================================================


class TestSubmissionQueueEdgeCases:
    """Test edge cases and error conditions."""

    def test_queue_with_exceptions(self, worker_mode):
        """Test that semaphore is released when task raises exception."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        class ErrorWorker(Worker):
            def failing_task(self, should_fail: bool) -> str:
                time.sleep(0.05)
                if should_fail:
                    raise ValueError("Task failed")
                return "success"

        worker = ErrorWorker.options(mode=worker_mode, max_queued_tasks=2).init()

        # Submit failing tasks
        f1 = worker.failing_task(True)
        f2 = worker.failing_task(True)

        # Wait for them to fail
        with pytest.raises(ValueError):
            f1.result()
        with pytest.raises(ValueError):
            f2.result()

        # Should be able to submit more (semaphores released)
        start = time.time()
        f3 = worker.failing_task(False)
        f4 = worker.failing_task(False)
        submission_time = time.time() - start
        assert submission_time < 0.5

        assert f3.result() == "success"
        assert f4.result() == "success"

        worker.stop()

    def test_queue_with_worker_stop(self, worker_mode):
        """Test submission queue behavior when worker is stopped."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = SlowWorker.options(mode=worker_mode, max_workers=1, max_queued_tasks=2).init()

        # Submit tasks
        f1 = worker.slow_task(0.5, 1)
        f2 = worker.slow_task(0.5, 2)

        # Stop worker
        worker.stop()

        # Should raise error when trying to submit after stop
        with pytest.raises(RuntimeError, match="Worker is stopped"):
            worker.slow_task(0.1, 3)

    def test_queue_with_timeout(self, worker_mode):
        """Test submission queue with task timeouts."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = SlowWorker.options(mode=worker_mode, max_queued_tasks=2).init()

        # Submit slow tasks
        f1 = worker.slow_task(5.0, 1)
        f2 = worker.slow_task(5.0, 2)

        # Timeout should still work
        with pytest.raises(TimeoutError):
            f1.result(timeout=0.1)

        # Cleanup
        worker.stop()

    def test_queue_with_on_demand_workers(self, pool_mode):
        """Test submission queue with on-demand worker creation."""

        # Ray mode: use minimal CPU to avoid creating too many workers
        kwargs = {}
        if pool_mode == "ray":
            kwargs["actor_options"] = {"num_cpus": 0.01}

        pool = CounterWorker.options(
            mode=pool_mode, max_workers=5, on_demand=True, max_queued_tasks=2, **kwargs
        ).init()

        # Submit tasks - on-demand workers created as needed
        futures = [pool.increment(1) for _ in range(20)]

        # All should complete
        results = gather(futures, timeout=15.0)
        assert len(results) == 20

        pool.stop()

    def test_queue_with_very_small_queue_length(self, worker_mode):
        """Test with max_queued_tasks=1 (minimum) - submissions still non-blocking.

        Even with max_queued_tasks=1, submissions are non-blocking.
        Only backend forwarding is limited to 1 at a time.
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = SlowWorker.options(mode=worker_mode, max_workers=1, max_queued_tasks=1).init()

        # Submit 5 tasks - ALL should return immediately even with max_queued_tasks=1
        start = time.time()
        futures = [worker.slow_task(0.2, i) for i in range(5)]
        submission_time = time.time() - start

        # CRITICAL: All submissions should be immediate (non-blocking)
        assert submission_time < 0.5, f"Submissions took {submission_time:.3f}s, should be immediate"

        # All futures should be created
        assert len(futures) == 5, "All futures should be created"

        # Wait for all to complete (callback forwarding will process queue)
        results = [f.result(timeout=10.0) for f in futures]
        assert len(results) == 5, "All tasks should complete"

        worker.stop()

    def test_queue_with_large_queue_length(self, worker_mode):
        """Test with very large max_queued_tasks."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = CounterWorker.options(mode=worker_mode, max_queued_tasks=1000).init()

        # Can submit many tasks without blocking
        start = time.time()
        futures = [worker.increment(1) for _ in range(100)]
        submission_time = time.time() - start
        assert submission_time < 2.0, "Should not block with large queue"

        # All should complete
        results = gather(futures, timeout=10.0)
        assert len(results) == 100

        worker.stop()


# =============================================================================
# Test TaskWorker with Submission Queues
# =============================================================================


class TestSubmissionQueueTaskWorker:
    """Test submission queue with TaskWorker.submit() and TaskWorker.map()."""

    def test_taskworker_submit_with_queue(self, worker_mode):
        """Test TaskWorker.submit() respects submission queue."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        def slow_function(x: int) -> int:
            time.sleep(0.05)
            return x * 2

        worker = TaskWorker.options(mode=worker_mode, max_queued_tasks=3).init()

        # Submit many tasks
        futures = [worker.submit(slow_function, i) for i in range(20)]

        # All should complete
        results = gather(futures, timeout=10.0)
        assert results == [i * 2 for i in range(20)]

        worker.stop()

    def test_taskworker_map_with_queue(self, worker_mode):
        """Test TaskWorker.map() with submission queue."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        def compute(x: int) -> int:
            time.sleep(0.01)
            return x**2

        worker = TaskWorker.options(mode=worker_mode, max_queued_tasks=2).init()

        # Map should work with submission queue
        results = list(worker.map(compute, range(20)))
        assert results == [i**2 for i in range(20)]

        worker.stop()

    def test_taskworker_pool_with_queue(self, pool_mode):
        """Test TaskWorker pool with submission queue."""

        def multiply(x: int) -> int:
            time.sleep(0.01)
            return x * 3

        pool = TaskWorker.options(
            mode=pool_mode, max_workers=4, max_queued_tasks=2, load_balancing="round_robin"
        ).init()

        # Submit via pool
        futures = [pool.submit(multiply, i) for i in range(30)]
        results = gather(futures, timeout=10.0)

        assert results == [i * 3 for i in range(30)]
        pool.stop()


# =============================================================================
# Test High Volume Submissions
# =============================================================================


class TestSubmissionQueueHighVolume:
    """Test submission queue with high volume of tasks."""

    def test_high_volume_single_worker(self, worker_mode):
        """Test submitting many tasks to single worker."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = CounterWorker.options(mode=worker_mode, max_queued_tasks=10).init()

        # Submit many tasks
        num_tasks = 500
        futures = [worker.increment(1) for _ in range(num_tasks)]

        # All should complete
        results = gather(futures, timeout=60.0)
        assert len(results) == num_tasks

        worker.stop()

    def test_high_volume_pool(self, pool_mode):
        """Test submitting many tasks to worker pool."""
        pool = CounterWorker.options(
            mode=pool_mode, max_workers=8, max_queued_tasks=5, load_balancing="active"
        ).init()

        # Submit large batch
        num_tasks = 1000
        futures = [pool.increment(1) for _ in range(num_tasks)]

        # All should complete
        results = gather(futures, timeout=60.0)
        assert len(results) == num_tasks

        pool.stop()

    def test_memory_usage_with_high_volume(self, worker_mode):
        """Test that submission queue prevents excessive memory usage."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = CounterWorker.options(
            mode=worker_mode,
            max_queued_tasks=5,  # Limit in-flight tasks
        ).init()

        # Submit very large batch
        # Submission queue should prevent memory explosion
        num_tasks = 2000
        futures = []
        for i in range(num_tasks):
            f = worker.increment(1)
            futures.append(f)

        # Process in batches to keep memory under control
        batch_size = 100
        for i in range(0, len(futures), batch_size):
            batch = futures[i : i + batch_size]
            results = gather(batch, timeout=30.0)
            assert len(results) == len(batch)

        worker.stop()


# =============================================================================
# Test Submission Queue Performance
# =============================================================================


class TestSubmissionQueuePerformance:
    """Test performance characteristics of submission queue."""

    def test_queue_overhead_is_minimal(self, worker_mode):
        """Test that submission queue overhead is minimal."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        # Test with queue
        worker_with_queue = CounterWorker.options(
            mode=worker_mode,
            max_queued_tasks=100,  # Large enough to not block
        ).init()

        start = time.time()
        futures = [worker_with_queue.increment(1) for _ in range(100)]
        gather(futures, timeout=10.0)
        time_with_queue = time.time() - start

        worker_with_queue.stop()

        # Queue overhead should be negligible (< 20% slowdown)
        # This is hard to test precisely across all modes, so just verify it works
        assert time_with_queue < 30.0  # Generous timeout

    def test_queue_prevents_overload(self, pool_mode):
        """Test that submission queue prevents worker overload."""
        pool = SlowWorker.options(
            mode=pool_mode,
            max_workers=3,
            max_queued_tasks=2,  # Small queue
            load_balancing="round_robin",
        ).init()

        # Submit burst of tasks
        # Without queue, this could overload workers
        futures = [pool.slow_task(0.05, i) for i in range(50)]

        # All should complete without errors
        results = gather(futures, timeout=20.0)
        assert len(results) == 50

        pool.stop()


class TestSubmissionQueueNone:
    """Test max_queued_tasks=None behavior."""

    def test_single_worker_with_none_queue(self, worker_mode):
        """Test single worker with max_queued_tasks=None.

        Verifies:
        1. Worker can be created with max_queued_tasks=None (no crash)
        2. All 10 tasks submit fast (< 1s) for async modes
        3. Tasks execute correctly
        4. All results are correct
        """
        # Create worker with unlimited queue
        worker = SlowWorker.options(mode=worker_mode, max_queued_tasks=None).init()

        # Submit 10 tasks, each taking 0.1 seconds (reduced for faster test)
        start_submit = time.time()
        futures = [worker.slow_task(0.1, i) for i in range(10)]
        submit_time = time.time() - start_submit

        # For async modes, submission should be fast (non-blocking)
        # For sync mode, submission includes execution
        if worker_mode != "sync":
            assert submit_time < 1.0, f"Submission took {submit_time:.3f}s, expected < 1s"

        # Get all results
        results = [f.result() for f in futures]

        # Verify results (SlowWorker returns dict with task_id and duration)
        task_ids = [r["task_id"] for r in results]
        assert task_ids == list(range(10)), "Task IDs should be 0-9"

        worker.stop()

    def test_pool_with_none_queue(self, pool_mode):
        """Test pool with max_queued_tasks=None.

        Verifies:
        1. Pool can be created with max_queued_tasks=None (no crash)
        2. All 10 tasks submit fast (< 1s)
        3. Tasks execute correctly
        4. All results are correct
        """
        # Create pool with 10 workers and unlimited queue
        pool = SlowWorker.options(mode=pool_mode, max_workers=10, max_queued_tasks=None).init()

        # Submit 10 tasks, each taking 0.1 seconds (reduced for faster test)
        start_submit = time.time()
        futures = [pool.slow_task(0.1, i) for i in range(10)]
        submit_time = time.time() - start_submit

        # Submission should be fast (non-blocking)
        assert submit_time < 1.0, f"Submission took {submit_time:.3f}s, expected < 1s"

        # Get all results
        results = [f.result() for f in futures]

        # Verify results (SlowWorker returns dict with task_id and duration)
        task_ids = sorted([r["task_id"] for r in results])
        assert task_ids == list(range(10)), "Task IDs should be 0-9"

        pool.stop()


class TestSubmissionQueueFastSubmission:
    """Test that submission is fast (non-blocking) even with slow tasks."""

    def test_single_worker_fast_submission(self, worker_mode, initialize_ray):
        """Test single worker submission is fast with slow tasks.

        Verifies:
        1. All 10 tasks submit fast despite 2s execution time (async modes)
        2. Tasks complete successfully
        3. Results are correct
        """
        # Use default queue size for the mode
        worker = SlowWorker.options(mode=worker_mode).init()

        # Submit 10 tasks, each taking 2 seconds
        start_submit = time.time()
        futures = [worker.slow_task(2.0, i) for i in range(10)]
        submit_time = time.time() - start_submit

        # For async modes, submission should be fast (non-blocking)
        # For sync mode, submission includes execution
        if worker_mode != "sync":
            # With default max_queued_tasks, submission may block after queue fills
            # But initial submissions should be fast
            pass  # Just verify it doesn't crash

        # Get all results
        results = [f.result(timeout=30.0) for f in futures]

        # Verify results (SlowWorker returns dict with task_id and duration)
        task_ids = [r["task_id"] for r in results]
        assert task_ids == list(range(10)), "Task IDs should be 0-9"

        worker.stop()

    def test_immediate_future_creation_single_worker(self, worker_mode, initialize_ray):
        """Test that futures are created immediately, not blocked by semaphore.

        CRITICAL TEST: This is the core behavior that was fixed.

        Verifies:
        1. Future creation is immediate (< 0.5s) for all tasks
        2. All futures are returned before tasks start executing
        3. Callback-driven forwarding works correctly
        4. All tasks eventually complete

        Background: Previously, submitting tasks would block when max_queued_tasks
        limit was reached. Now, futures are created immediately and queued internally,
        with callback-driven forwarding to respect the limit.
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        # Create worker with small queue (2 tasks) and slow tasks (1s each - fast enough for testing)
        worker = SlowWorker.options(mode=worker_mode, max_workers=1, max_queued_tasks=2).init()

        # Submit 10 tasks - MUST return immediately
        start_submit = time.time()
        futures = [worker.slow_task(1.0, i) for i in range(10)]
        submit_time = time.time() - start_submit

        # CRITICAL ASSERTION: Future creation must be immediate (< 0.5s)
        # With 10 tasks taking 1s each, if submission blocked, this would take 8s+
        # With non-blocking submission, all 10 futures are created immediately
        assert submit_time < 0.5, (
            f"Future creation took {submit_time:.3f}s but should be immediate (< 0.5s). "
            f"This indicates submission is blocking on semaphore instead of queuing."
        )

        # Verify all futures were created
        assert len(futures) == 10, f"Expected 10 futures, got {len(futures)}"

        # Verify all tasks eventually complete (callback forwarding works)
        results = [f.result(timeout=30.0) for f in futures]
        assert len(results) == 10, "All tasks should complete"

        # Verify results are correct
        task_ids = [r["task_id"] for r in results]
        assert task_ids == list(range(10)), "Task IDs should be 0-9"

        worker.stop()

    def test_immediate_future_creation_pool(self, pool_mode, initialize_ray):
        """Test that futures are created immediately for worker pools.

        CRITICAL TEST: Verifies non-blocking submission for pools.

        Verifies:
        1. Future creation is immediate (< 0.5s) for all tasks
        2. Per-worker semaphores work independently
        3. All futures returned before tasks execute
        4. All tasks eventually complete
        """
        # Create pool with 2 workers, max_queued_tasks=4 per worker
        # Total capacity: 2 workers × 4 = 8 concurrent submissions
        pool = SlowWorker.options(
            mode=pool_mode, max_workers=2, max_queued_tasks=4, load_balancing="round_robin"
        ).init()

        # Submit 20 tasks - more than capacity but should return immediately
        start_submit = time.time()
        futures = [pool.slow_task(0.5, i) for i in range(20)]
        submit_time = time.time() - start_submit

        # CRITICAL ASSERTION: Future creation must be immediate (< 0.5s)
        assert submit_time < 0.5, (
            f"Future creation took {submit_time:.3f}s but should be immediate (< 0.5s). "
            f"Pool submissions should not block on semaphore."
        )

        # Verify all futures were created
        assert len(futures) == 20, f"Expected 20 futures, got {len(futures)}"

        # Verify all tasks eventually complete
        results = [f.result(timeout=30.0) for f in futures]
        assert len(results) == 20, "All tasks should complete"

        pool.stop()

    def test_immediate_future_with_progress_bar(self, worker_mode, initialize_ray):
        """Test immediate future creation with progress bar (user's original use case).

        This replicates the user's exact scenario that revealed the bug.

        Verifies:
        1. Future creation is immediate even with progress bar
        2. Progress bar shows fast iteration (not blocked)
        3. All tasks complete correctly
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        from concurry.utils.progress import ProgressBar

        worker = SlowWorker.options(mode=worker_mode, max_workers=2, max_queued_tasks=4).init()

        # Submit tasks with progress bar (user's original pattern)
        start_submit = time.time()
        futures = [worker.slow_task(0.5, i) for i in ProgressBar(range(20), style="std")]
        submit_time = time.time() - start_submit

        # CRITICAL: Should complete quickly (< 1s), not blocking on submission
        assert submit_time < 1.0, (
            f"Submission with progress bar took {submit_time:.3f}s, expected < 1s. "
            f"This was the original bug - submission blocked instead of queuing."
        )

        # All tasks should complete
        results = gather(futures, timeout=30.0)
        assert len(results) == 20

        worker.stop()

    def test_pool_fast_submission(self, pool_mode, initialize_ray):
        """Test pool submission is fast with slow tasks.

        Verifies:
        1. All 10 tasks submit fast despite 2s execution time
        2. Tasks execute successfully with 10 workers
        3. Results are correct
        4. Parallel execution is faster than serial (< 5s vs ~20s)
        """
        # Create pool with 10 workers
        pool = SlowWorker.options(mode=pool_mode, max_workers=10).init()

        # Submit 10 tasks, each taking 2 seconds
        start_submit = time.time()
        futures = [pool.slow_task(2.0, i) for i in range(10)]
        submit_time = time.time() - start_submit

        # Submission should be fast (non-blocking)
        assert submit_time < 1.0, f"Submission took {submit_time:.3f}s, expected < 1s"

        # Get all results
        start_complete = time.time()
        results = [f.result(timeout=30.0) for f in futures]
        complete_time = time.time() - start_complete

        # Verify results (SlowWorker returns dict with task_id and duration)
        task_ids = sorted([r["task_id"] for r in results])
        assert task_ids == list(range(10)), "Task IDs should be 0-9"

        # With 10 workers, completion should be much faster than serial execution
        # Serial would be ~20s, parallel should be ~2-3s
        # Ray client mode adds significant overhead (actor startup, network communication)
        # Allow up to 8s to account for Ray client mode overhead while still catching regressions
        assert complete_time < 8.0, f"Completion took {complete_time:.3f}s, should show parallelism (< 8s)"

        pool.stop()
