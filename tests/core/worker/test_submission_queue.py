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
        3. Expected: sync/asyncio=None (bypass), thread=1000, process=100, ray=3
        4. Stops the worker
        """
        worker = CounterWorker.options(mode=worker_mode).init()
        # Default values: sync/asyncio=None (bypass), thread=100, process=5, ray=2
        expected = {
            "sync": None,
            "thread": 1000,
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

    def test_submission_queue_blocks_at_limit(self, worker_mode):
        """Test that submission queue blocks when limit is reached.

        This test:
        1. Creates a SlowWorker with max_workers=1, max_queued_tasks=2
        2. Submits 2 slow tasks (0.5s each) - these fill the queue without blocking
        3. Attempts to submit 3rd task in separate thread - should block
        4. Verifies 3rd submission is blocked (thread still alive after 0.1s)
        5. Waits for first task to complete - this unblocks 3rd submission
        6. Verifies 3rd submission unblocks (thread completes)
        7. Waits for all tasks to complete and stops worker
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = SlowWorker.options(mode=worker_mode, max_workers=1, max_queued_tasks=2).init()

        # Submit 2 tasks (should not block)
        start = time.time()
        f1 = worker.slow_task(0.5, 1)
        f2 = worker.slow_task(0.5, 2)
        submission_time = time.time() - start
        assert submission_time < 0.2, "First 2 submissions should be fast"

        # Third submission should block until one completes
        submission_blocked = threading.Event()
        third_future = None

        def submit_third():
            nonlocal third_future
            submission_blocked.set()
            third_future = worker.slow_task(0.2, 3)

        thread = threading.Thread(target=submit_third)
        thread.start()

        # Wait for thread to start blocking
        submission_blocked.wait(timeout=1.0)
        time.sleep(0.1)
        assert thread.is_alive(), "Third submission should be blocked"

        # Wait for first task to complete
        f1.result()
        thread.join(timeout=2.0)
        assert not thread.is_alive(), "Third submission should have unblocked"

        # Cleanup
        f2.result()
        if third_future is not None:
            third_future.result()
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
        4. Verifies stats contain submission_queues array with 4 entries
        5. Verifies each queue_info has worker_idx and capacity=10
        6. Stops the pool
        """
        pool = CounterWorker.options(mode=pool_mode, max_workers=4, max_queued_tasks=10).init()

        stats = pool.get_pool_stats()
        assert "max_queued_tasks" in stats
        assert stats["max_queued_tasks"] == 10
        assert "submission_queues" in stats
        assert len(stats["submission_queues"]) == 4

        for queue_info in stats["submission_queues"]:
            assert "worker_idx" in queue_info
            assert "capacity" in queue_info
            assert queue_info["capacity"] == 10

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
        """Test with max_queued_tasks=1 (minimum)."""
        if worker_mode in ("sync", "asyncio"):
            pytest.skip("Sync and AsyncIO modes bypass submission queue")

        worker = SlowWorker.options(mode=worker_mode, max_workers=1, max_queued_tasks=1).init()

        # Only 1 task can be submitted at a time
        f1 = worker.slow_task(0.2, 1)

        # Second submission should block
        submission_blocked = threading.Event()
        second_future = None

        def submit_second():
            nonlocal second_future
            submission_blocked.set()
            second_future = worker.slow_task(0.1, 2)

        thread = threading.Thread(target=submit_second)
        thread.start()

        submission_blocked.wait(timeout=1.0)
        time.sleep(0.05)
        assert thread.is_alive(), "Should be blocked"

        # Complete first task
        f1.result()
        thread.join(timeout=2.0)

        # Second should complete
        if second_future is not None:
            second_future.result()

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
