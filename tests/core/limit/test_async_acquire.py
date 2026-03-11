"""Tests for async_acquire() on LimitSet implementations.

Validates that async_acquire() prevents event-loop deadlocks that occur when
synchronous acquire() is used from async contexts with more coroutines than
limit capacity.
"""

import asyncio
import time

import pytest

from concurry import CallLimit, LimitSet, RateLimit, RateLimitAlgorithm, ResourceLimit
from concurry.core.limit.limit_set import InMemorySharedLimitSet, NoOpLimitSet


class TestAsyncAcquireBasicCorrectness:
    """Test basic async_acquire() behavior on InMemorySharedLimitSet."""

    @pytest.mark.asyncio
    async def test_async_acquire_succeeds_when_capacity_available(self):
        """Test that async_acquire returns a successful LimitSetAcquisition
        when capacity is available.

        Steps:
        1. Create InMemorySharedLimitSet with CallLimit capacity=10
        2. Call async_acquire once
        3. Verify acquisition is successful
        4. Release acquisition
        """
        limit_set = InMemorySharedLimitSet(
            limits=[CallLimit(window_seconds=60, capacity=10)],
            shared=True,
        )
        acq = await limit_set.async_acquire(requested={"call_count": 1})
        assert acq.successful is True
        assert len(acq.acquisitions) == 1
        assert acq.acquisitions["call_count"].requested == 1
        acq.release()

    @pytest.mark.asyncio
    async def test_async_acquire_returns_immediately_below_capacity(self):
        """Test that N async_acquire calls where N < capacity all complete quickly.

        Steps:
        1. Create limit set with capacity=10
        2. Fire 5 async_acquire calls concurrently
        3. Verify all succeed within a short timeout
        4. Release all acquisitions
        """
        limit_set = InMemorySharedLimitSet(
            limits=[CallLimit(window_seconds=60, capacity=10)],
            shared=True,
        )
        N = 5

        async def acquire_and_hold(i: int):
            acq = await limit_set.async_acquire(requested={"call_count": 1})
            assert acq.successful is True
            return acq

        acquisitions = await asyncio.wait_for(
            asyncio.gather(*[acquire_and_hold(i) for i in range(N)]),
            timeout=5.0,
        )
        assert len(acquisitions) == N
        for acq in acquisitions:
            acq.release()

    @pytest.mark.asyncio
    async def test_async_acquire_at_exact_capacity(self):
        """Test that N async_acquire calls where N = capacity all succeed.

        Steps:
        1. Create limit set with capacity=5
        2. Fire exactly 5 concurrent async_acquire calls
        3. Verify all succeed
        4. Release all
        """
        limit_set = InMemorySharedLimitSet(
            limits=[CallLimit(window_seconds=60, capacity=5)],
            shared=True,
        )
        N = 5

        async def acquire_and_hold(i: int):
            acq = await limit_set.async_acquire(requested={"call_count": 1})
            assert acq.successful is True
            return acq

        acquisitions = await asyncio.wait_for(
            asyncio.gather(*[acquire_and_hold(i) for i in range(N)]),
            timeout=5.0,
        )
        assert len(acquisitions) == N
        for acq in acquisitions:
            acq.release()

    @pytest.mark.asyncio
    async def test_async_acquire_timeout_raises(self):
        """Test that async_acquire raises TimeoutError when capacity is exhausted
        and the timeout expires.

        Steps:
        1. Create limit set with capacity=1
        2. Acquire the single slot (hold it)
        3. Attempt a second async_acquire with a short timeout
        4. Verify TimeoutError is raised
        5. Release the first acquisition
        """
        limit_set = InMemorySharedLimitSet(
            limits=[CallLimit(window_seconds=60, capacity=1)],
            shared=True,
        )
        acq1 = await limit_set.async_acquire(requested={"call_count": 1})

        with pytest.raises(TimeoutError, match="Failed to acquire all limits within"):
            await limit_set.async_acquire(requested={"call_count": 1}, timeout=0.3)

        acq1.release()

    @pytest.mark.asyncio
    async def test_async_acquire_update_and_release(self):
        """Test the full lifecycle: async_acquire -> update -> release.

        Steps:
        1. Create limit set with RateLimit
        2. async_acquire with requested tokens
        3. Call update with actual usage
        4. Release the acquisition
        5. Verify a new acquire succeeds (capacity restored)
        """
        limit_set = InMemorySharedLimitSet(
            limits=[
                RateLimit(
                    key="tokens",
                    window_seconds=60,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=100,
                )
            ],
            shared=True,
        )
        acq = await limit_set.async_acquire(requested={"tokens": 50})
        assert acq.successful is True
        acq.update(usage={"tokens": 30})
        acq.release()

        acq2 = await limit_set.async_acquire(requested={"tokens": 50})
        assert acq2.successful is True
        acq2.update(usage={"tokens": 50})
        acq2.release()


class TestAsyncAcquireDeadlockPrevention:
    """The critical tests: prove that async_acquire prevents event-loop deadlocks
    that occur with sync acquire() when N > capacity in async context."""

    @pytest.mark.asyncio
    async def test_async_acquire_no_deadlock_over_capacity_call_limit(self):
        """THE critical test. N coroutines where N > capacity, using async_acquire
        with ResourceLimit (release-based). All should complete (no deadlock).

        With sync acquire(), this same scenario deadlocks because time.sleep()
        blocks the event loop, preventing coroutines holding limits from completing
        and releasing them.

        Steps:
        1. Create ResourceLimit with capacity=5
        2. Launch 10 coroutines concurrently (N > capacity)
        3. Each coroutine: async_acquire -> await asyncio.sleep(0.1) -> release
        4. Verify all 10 complete within a generous timeout
        """
        limit_set = InMemorySharedLimitSet(
            limits=[ResourceLimit(key="slots", capacity=5)],
            shared=True,
        )
        N = 10
        completed = []

        async def worker(i: int):
            acq = await limit_set.async_acquire(requested={"slots": 1})
            try:
                await asyncio.sleep(0.1)
                completed.append(i)
            finally:
                acq.release()

        await asyncio.wait_for(
            asyncio.gather(*[worker(i) for i in range(N)]),
            timeout=10.0,
        )
        assert len(completed) == N

    @pytest.mark.asyncio
    async def test_sync_acquire_deadlocks_over_capacity(self):
        """Prove that sync acquire() deadlocks when N > capacity in async context.

        This is the negative test that documents the bug async_acquire fixes.
        Using sync acquire() on the event loop, coroutine 6+ call time.sleep()
        which blocks the loop, so coroutines 1-5 can never complete and release.

        We use a sync acquire WITH timeout so the blocking sleep eventually
        unblocks and lets the event loop tick. The key assertion: when using
        sync acquire, coroutines that exceed capacity timeout waiting, proving
        the deadlock condition.

        Steps:
        1. Create ResourceLimit with capacity=3
        2. Launch 6 coroutines using SYNC acquire() with a short timeout
        3. Verify that some raise TimeoutError (proving deadlock)
        """
        limit_set = InMemorySharedLimitSet(
            limits=[ResourceLimit(key="slots", capacity=3)],
            shared=True,
        )
        N = 6
        completed = []
        timed_out = []

        async def worker(i: int):
            try:
                acq = limit_set.acquire(requested={"slots": 1}, timeout=1.0)
            except TimeoutError:
                timed_out.append(i)
                return
            try:
                await asyncio.sleep(0.2)
                completed.append(i)
            finally:
                acq.release()

        await asyncio.wait_for(
            asyncio.gather(*[worker(i) for i in range(N)]),
            timeout=15.0,
        )
        # The first 3 coroutines acquire immediately; coroutines 4-6 call
        # time.sleep() in the polling loop, blocking the event loop. The first 3
        # cannot complete their await asyncio.sleep(). Eventually the sync
        # acquire timeouts fire, unblocking the loop. Key assertion: not all
        # coroutines completed, proving the deadlock condition.
        assert len(timed_out) > 0

    @pytest.mark.asyncio
    async def test_async_acquire_with_resource_limit_no_deadlock(self):
        """ResourceLimit (semaphore-based) with N > capacity using async_acquire.
        All coroutines should complete.

        ResourceLimit is the most deadlock-prone because capacity is only freed
        when calls COMPLETE (not by time window expiry).

        Steps:
        1. Create ResourceLimit with capacity=5
        2. Launch 10 coroutines
        3. Each holds the resource for 0.1s of async work
        4. Verify all 10 complete
        """
        limit_set = InMemorySharedLimitSet(
            limits=[ResourceLimit(key="connections", capacity=5)],
            shared=True,
        )
        N = 10
        completed = []

        async def worker(i: int):
            acq = await limit_set.async_acquire(requested={"connections": 1})
            try:
                await asyncio.sleep(0.1)
                completed.append(i)
            finally:
                acq.release()

        await asyncio.wait_for(
            asyncio.gather(*[worker(i) for i in range(N)]),
            timeout=10.0,
        )
        assert len(completed) == N

    @pytest.mark.asyncio
    async def test_async_acquire_with_rate_limit_no_deadlock(self):
        """RateLimit with N <= capacity using async_acquire. All coroutines
        should complete without deadlock.

        RateLimit is window-based (tokens are consumed and only refill after
        the window expires), so N must be <= capacity for a single-window test.

        Steps:
        1. Create RateLimit with capacity=10, window=60s
        2. Launch 10 coroutines (N == capacity)
        3. Each acquires 1 token and does async work
        4. Verify all 10 complete
        """
        limit_set = InMemorySharedLimitSet(
            limits=[
                RateLimit(
                    key="tokens",
                    window_seconds=60,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=10,
                )
            ],
            shared=True,
        )
        N = 10
        completed = []

        async def worker(i: int):
            acq = await limit_set.async_acquire(requested={"tokens": 1})
            try:
                await asyncio.sleep(0.05)
                acq.update(usage={"tokens": 1})
                completed.append(i)
            finally:
                acq.release()

        await asyncio.wait_for(
            asyncio.gather(*[worker(i) for i in range(N)]),
            timeout=10.0,
        )
        assert len(completed) == N


class TestAsyncAcquireCancellationAndErrors:
    """Test cancellation and error handling with async_acquire."""

    @pytest.mark.asyncio
    async def test_async_acquire_cancelled_during_wait(self):
        """Start async_acquire that will block, cancel the task. Verify
        CancelledError propagates and no limits are leaked.

        Steps:
        1. Create ResourceLimit with capacity=1, exhaust it
        2. Start a second async_acquire as a task
        3. Cancel the task after a brief delay
        4. Verify CancelledError and no leaked resources
        """
        limit_set = InMemorySharedLimitSet(
            limits=[ResourceLimit(key="slots", capacity=1)],
            shared=True,
        )
        acq1 = await limit_set.async_acquire(requested={"slots": 1})

        async def blocked_acquire():
            return await limit_set.async_acquire(requested={"slots": 1})

        task = asyncio.create_task(blocked_acquire())
        await asyncio.sleep(0.2)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        acq1.release()

        acq3 = await limit_set.async_acquire(requested={"slots": 1}, timeout=1.0)
        assert acq3.successful is True
        acq3.release()

    @pytest.mark.asyncio
    async def test_async_acquire_exception_in_work_releases_limits(self):
        """Acquire succeeds, work raises exception. Verify limits are released
        in finally block so subsequent acquires succeed.

        Steps:
        1. Create ResourceLimit with capacity=1
        2. async_acquire, simulate exception, release in finally
        3. Verify a new acquire succeeds immediately
        """
        limit_set = InMemorySharedLimitSet(
            limits=[ResourceLimit(key="slots", capacity=1)],
            shared=True,
        )

        with pytest.raises(ValueError, match="simulated"):
            acq = await limit_set.async_acquire(requested={"slots": 1})
            try:
                raise ValueError("simulated")
            finally:
                acq.release()

        acq2 = await limit_set.async_acquire(requested={"slots": 1}, timeout=1.0)
        assert acq2.successful is True
        acq2.release()


class TestAsyncAcquireConcurrentCorrectness:
    """Test concurrent correctness of async_acquire."""

    @pytest.mark.asyncio
    async def test_async_acquire_concurrent_fairness(self):
        """Multiple coroutines waiting for capacity. Verify all eventually
        get served (no starvation).

        Steps:
        1. Create ResourceLimit with capacity=2
        2. Launch 8 coroutines, each holding the resource for 0.1s
        3. Verify all 8 complete
        4. Verify total time shows sequential waves (>= 0.3s for 4 waves)
        """
        limit_set = InMemorySharedLimitSet(
            limits=[ResourceLimit(key="slots", capacity=2)],
            shared=True,
        )
        N = 8
        completed = []

        async def worker(i: int):
            acq = await limit_set.async_acquire(requested={"slots": 1})
            try:
                await asyncio.sleep(0.1)
                completed.append(i)
            finally:
                acq.release()

        start = time.time()
        await asyncio.wait_for(
            asyncio.gather(*[worker(i) for i in range(N)]),
            timeout=10.0,
        )
        elapsed = time.time() - start

        assert len(completed) == N
        assert elapsed >= 0.3

    @pytest.mark.asyncio
    async def test_async_acquire_mixed_limit_types(self):
        """LimitSet with CallLimit + ResourceLimit. Both acquired atomically
        via async_acquire.

        Steps:
        1. Create LimitSet with CallLimit(capacity=10) + ResourceLimit(capacity=3)
        2. Launch 6 coroutines
        3. Verify all complete (ResourceLimit is the bottleneck at 3)
        """
        limit_set = InMemorySharedLimitSet(
            limits=[
                CallLimit(window_seconds=60, capacity=10),
                ResourceLimit(key="connections", capacity=3),
            ],
            shared=True,
        )
        N = 6
        completed = []

        async def worker(i: int):
            acq = await limit_set.async_acquire(
                requested={"call_count": 1, "connections": 1}
            )
            try:
                await asyncio.sleep(0.1)
                completed.append(i)
            finally:
                acq.release()

        await asyncio.wait_for(
            asyncio.gather(*[worker(i) for i in range(N)]),
            timeout=10.0,
        )
        assert len(completed) == N


class TestAsyncAcquireNoOpLimitSet:
    """Test async_acquire on NoOpLimitSet."""

    @pytest.mark.asyncio
    async def test_noop_async_acquire_succeeds_immediately(self):
        """NoOp async_acquire always succeeds with empty acquisitions.

        Steps:
        1. Create NoOpLimitSet
        2. Call async_acquire
        3. Verify successful with no acquisitions
        """
        limit_set = NoOpLimitSet(shared=True)
        acq = await limit_set.async_acquire()
        assert acq.successful is True
        assert len(acq.acquisitions) == 0
        acq.release()


class TestAsyncAcquireLimitSetFactory:
    """Test async_acquire via the LimitSet factory function."""

    @pytest.mark.asyncio
    async def test_limitset_factory_asyncio_mode_async_acquire(self):
        """LimitSet created with mode='asyncio' supports async_acquire.

        Steps:
        1. Create LimitSet via factory with mode='asyncio'
        2. Call async_acquire
        3. Verify successful
        """
        limits = LimitSet(
            limits=[CallLimit(window_seconds=60, capacity=10)],
            shared=True,
            mode="asyncio",
        )
        acq = await limits.async_acquire(requested={"call_count": 1})
        assert acq.successful is True
        acq.release()

    @pytest.mark.asyncio
    async def test_empty_limitset_async_acquire(self):
        """Empty LimitSet (NoOpLimitSet) supports async_acquire.

        Steps:
        1. Create empty LimitSet
        2. Call async_acquire
        3. Verify succeeds
        """
        limits = LimitSet(limits=[], shared=True, mode="asyncio")
        acq = await limits.async_acquire()
        assert acq.successful is True
        acq.release()


class TestAsyncContextManager:
    """Test __aenter__/__aexit__ on LimitSetAcquisition."""

    @pytest.mark.asyncio
    async def test_async_with_pattern(self):
        """Test the 'async with await' pattern for async_acquire.

        Steps:
        1. Create ResourceLimit with capacity=1
        2. Use 'async with await limit_set.async_acquire(...)' pattern
        3. Verify acquisition works inside the block
        4. Verify released after block exits
        """
        limit_set = InMemorySharedLimitSet(
            limits=[ResourceLimit(key="slots", capacity=1)],
            shared=True,
        )

        async with await limit_set.async_acquire(requested={"slots": 1}) as acq:
            assert acq.successful is True
            assert not acq._released

        assert acq._released

        acq2 = await limit_set.async_acquire(requested={"slots": 1}, timeout=1.0)
        assert acq2.successful is True
        acq2.release()

    @pytest.mark.asyncio
    async def test_async_with_exception_releases(self):
        """Test that async with releases limits even when exception occurs.

        Steps:
        1. Create ResourceLimit with capacity=1
        2. Use async with, raise inside
        3. Verify limits released (next acquire succeeds)
        """
        limit_set = InMemorySharedLimitSet(
            limits=[ResourceLimit(key="slots", capacity=1)],
            shared=True,
        )

        with pytest.raises(ValueError, match="boom"):
            async with await limit_set.async_acquire(requested={"slots": 1}) as acq:
                raise ValueError("boom")

        acq2 = await limit_set.async_acquire(requested={"slots": 1}, timeout=1.0)
        assert acq2.successful is True
        acq2.release()

    @pytest.mark.asyncio
    async def test_async_with_no_deadlock_pattern(self):
        """End-to-end test: async with + N > capacity, no deadlock.

        Steps:
        1. Create ResourceLimit with capacity=3
        2. Launch 9 coroutines using async with pattern
        3. Verify all complete (no deadlock)
        """
        limit_set = InMemorySharedLimitSet(
            limits=[ResourceLimit(key="slots", capacity=3)],
            shared=True,
        )
        N = 9
        completed = []

        async def worker(i: int):
            async with await limit_set.async_acquire(requested={"slots": 1}) as acq:
                await asyncio.sleep(0.05)
                completed.append(i)

        await asyncio.wait_for(
            asyncio.gather(*[worker(i) for i in range(N)]),
            timeout=10.0,
        )
        assert len(completed) == N


class TestAsyncAcquireLimitPool:
    """Test async_acquire on LimitPool."""

    @pytest.mark.asyncio
    async def test_limit_pool_async_acquire_delegates(self):
        """LimitPool.async_acquire() selects a LimitSet and delegates.

        Steps:
        1. Create two InMemorySharedLimitSets
        2. Create LimitPool wrapping them
        3. Call async_acquire
        4. Verify acquisition succeeds
        """
        from concurry.core.limit.limit_pool import LimitPool

        ls1 = InMemorySharedLimitSet(
            limits=[CallLimit(window_seconds=60, capacity=10)],
            shared=True,
            config={"account": "a"},
        )
        ls2 = InMemorySharedLimitSet(
            limits=[CallLimit(window_seconds=60, capacity=10)],
            shared=True,
            config={"account": "b"},
        )
        pool = LimitPool(limit_sets=[ls1, ls2])

        acq = await pool.async_acquire(requested={"call_count": 1})
        assert acq.successful is True
        assert "account" in acq.config
        acq.release()

    @pytest.mark.asyncio
    async def test_limit_pool_async_acquire_no_deadlock(self):
        """LimitPool.async_acquire with N > capacity doesn't deadlock.

        Steps:
        1. Create LimitPool with a single LimitSet (capacity=3)
        2. Launch 6 coroutines via pool.async_acquire
        3. Verify all complete
        """
        from concurry.core.limit.limit_pool import LimitPool

        ls = InMemorySharedLimitSet(
            limits=[ResourceLimit(key="slots", capacity=3)],
            shared=True,
        )
        pool = LimitPool(limit_sets=[ls])

        N = 6
        completed = []

        async def worker(i: int):
            async with await pool.async_acquire(requested={"slots": 1}):
                await asyncio.sleep(0.1)
                completed.append(i)

        await asyncio.wait_for(
            asyncio.gather(*[worker(i) for i in range(N)]),
            timeout=10.0,
        )
        assert len(completed) == N


class TestAsyncAcquireWorkerIntegration:
    """Test async_acquire() through actual concurry workers across all execution modes.

    All modes support async def methods:
    - sync/thread/process: via asyncio.run() (one coroutine per call, no shared loop)
    - asyncio: via shared persistent event loop (multiple coroutines, deadlock-prone)
    - ray: via Ray actor async support

    async_acquire() works in all modes because await asyncio.sleep() is valid in
    any event loop. The deadlock fix specifically matters for asyncio mode where
    multiple coroutines share one loop, but the API should work everywhere.
    """

    def test_worker_async_method_with_async_acquire(self, worker_mode):
        """Test async def worker method using async_acquire across all modes.

        Steps:
        1. Define worker with async method that calls async_acquire
        2. Create worker in each mode with ResourceLimit
        3. Call the async method
        4. Verify result is correct (limits acquired and released properly)
        """
        from concurry import Worker

        limits = [ResourceLimit(key="slots", capacity=5)]

        class AsyncLimitWorker(Worker):
            def __init__(self):
                pass

            async def do_work(self, value: int) -> int:
                import asyncio

                async with await self.limits.async_acquire(requested={"slots": 1}):
                    await asyncio.sleep(0.01)
                    return value * 2

        if worker_mode == "thread":
            w = AsyncLimitWorker.options(mode=worker_mode, max_workers=30, limits=limits).init()
        elif worker_mode == "process":
            w = AsyncLimitWorker.options(mode=worker_mode, max_workers=4, limits=limits).init()
        elif worker_mode == "ray":
            w = AsyncLimitWorker.options(mode=worker_mode, max_workers=0, limits=limits).init()
        else:
            w = AsyncLimitWorker.options(mode=worker_mode, limits=limits).init()

        result = w.do_work(21).result(timeout=15)
        assert result == 42
        w.stop()

    def test_worker_async_method_with_async_acquire_multiple_calls(self, worker_mode):
        """Test multiple sequential calls to async methods using async_acquire.

        Steps:
        1. Define worker with async method using async_acquire
        2. Make 3 sequential calls
        3. Verify all return correct results (limits properly released between calls)
        """
        from concurry import Worker

        limits = [ResourceLimit(key="slots", capacity=2)]

        class AsyncLimitWorker(Worker):
            def __init__(self):
                pass

            async def compute(self, x: int) -> int:
                import asyncio

                async with await self.limits.async_acquire(requested={"slots": 1}):
                    await asyncio.sleep(0.01)
                    return x ** 2

        if worker_mode == "thread":
            w = AsyncLimitWorker.options(mode=worker_mode, max_workers=30, limits=limits).init()
        elif worker_mode == "process":
            w = AsyncLimitWorker.options(mode=worker_mode, max_workers=4, limits=limits).init()
        elif worker_mode == "ray":
            w = AsyncLimitWorker.options(mode=worker_mode, max_workers=0, limits=limits).init()
        else:
            w = AsyncLimitWorker.options(mode=worker_mode, limits=limits).init()

        results = []
        for x in [3, 5, 7]:
            results.append(w.compute(x).result(timeout=15))

        assert results == [9, 25, 49]
        w.stop()


class TestAsyncAcquireSharedLimitsAcrossWorkers:
    """Test async_acquire() with shared LimitSets across multiple workers.

    Mirrors TestSharedLimitSets and TestSharedLimitAcquisitionTracking from
    test_shared_limits.py but using async_acquire inside async worker methods.
    """

    def test_shared_limitset_async_acquire_inmemory(self, worker_mode):
        """Shared InMemorySharedLimitSet with async_acquire across 2 workers.

        Steps:
        1. Create shared InMemorySharedLimitSet with ResourceLimit(capacity=2)
        2. Create 2 workers sharing the same LimitSet
        3. Both call async methods that use async_acquire
        4. Verify all calls succeed and limits are properly shared
        """
        if worker_mode in ("process", "ray"):
            pytest.skip("InMemorySharedLimitSet is only for sync/thread/asyncio modes")

        from concurry import Worker

        shared_limits = LimitSet(
            limits=[ResourceLimit(key="slots", capacity=5)],
            shared=True,
            mode=worker_mode,
        )

        class AsyncCounter(Worker):
            def __init__(self):
                pass

            async def increment(self) -> int:
                import asyncio

                async with await self.limits.async_acquire(requested={"slots": 1}):
                    await asyncio.sleep(0.01)
                    return 1

        if worker_mode == "thread":
            w1 = AsyncCounter.options(mode=worker_mode, max_workers=30, limits=shared_limits).init()
            w2 = AsyncCounter.options(mode=worker_mode, max_workers=30, limits=shared_limits).init()
        else:
            w1 = AsyncCounter.options(mode=worker_mode, limits=shared_limits).init()
            w2 = AsyncCounter.options(mode=worker_mode, limits=shared_limits).init()

        futures = []
        for _ in range(3):
            futures.append(w1.increment())
            futures.append(w2.increment())

        results = [f.result(timeout=15) for f in futures]
        assert all(r == 1 for r in results)
        assert len(results) == 6

        w1.stop()
        w2.stop()

    def test_shared_limitset_async_acquire_process(self):
        """Shared MultiprocessSharedLimitSet with async_acquire across 2 process workers.

        Steps:
        1. Create shared MultiprocessSharedLimitSet with ResourceLimit(capacity=3)
        2. Create 2 process workers sharing the same LimitSet
        3. Submit 6 tasks (3 from each) that use async_acquire and hold for 1s
        4. With capacity=3, tasks form 2 waves (~2s total)
        5. Verify total elapsed >= 1.7s (proves sharing)
        """
        from concurry import Worker

        class AsyncResourceWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id

            def ping(self) -> bool:
                return True

            async def hold_resource(self, task_id: int) -> dict:
                import asyncio
                import time as _time

                start = _time.time()
                async with await self.limits.async_acquire(requested={"resource": 1}):
                    acquire_time = _time.time() - start
                    await asyncio.sleep(1.0)
                    return {
                        "worker_id": self.worker_id,
                        "task_id": task_id,
                        "acquire_time": acquire_time,
                    }

        shared_limits = LimitSet(
            limits=[ResourceLimit(key="resource", capacity=3)],
            shared=True,
            mode="process",
        )

        w1 = AsyncResourceWorker.options(mode="process", max_workers=4, limits=shared_limits).init(worker_id=1)
        w2 = AsyncResourceWorker.options(mode="process", max_workers=4, limits=shared_limits).init(worker_id=2)

        start_time = time.time()
        futures = []
        for i in range(3):
            futures.append(w1.hold_resource(i))
            futures.append(w2.hold_resource(i + 3))

        results = [f.result(timeout=30) for f in futures]
        elapsed = time.time() - start_time

        assert len(results) == 6
        assert elapsed >= 1.7, (
            f"Total time {elapsed:.2f}s too fast. Expected >= 1.7s. "
            f"If < 1.5s, limits are NOT shared across processes."
        )
        assert elapsed <= 10.0, f"Total time {elapsed:.2f}s too slow, possible deadlock."

        w1.stop()
        w2.stop()

    def test_shared_limitset_async_acquire_ray(self, requires_ray_mode):
        """Shared RaySharedLimitSet with async_acquire across 6 Ray actors.

        Steps:
        1. Create shared RaySharedLimitSet with ResourceLimit(capacity=3)
        2. Create 6 Ray actors sharing the same LimitSet
        3. Each actor calls async method using async_acquire, holds for 1s
        4. With capacity=3, tasks form 2 waves (~2s total)
        5. Verify total elapsed >= 1.7s (proves sharing across Ray actors)
        """
        pytest.importorskip("ray")
        from concurry import Worker

        class AsyncResourceWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id

            def ping(self) -> bool:
                return True

            async def hold_resource(self, task_id: int) -> dict:
                import asyncio
                import time as _time

                start = _time.time()
                async with await self.limits.async_acquire(requested={"resource": 1}):
                    acquire_time = _time.time() - start
                    await asyncio.sleep(1.0)
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
            w = AsyncResourceWorker.options(mode="ray", max_workers=0, limits=shared_limits).init(worker_id=i)
            workers.append(w)

        for w in workers:
            assert w.ping().result(timeout=30) is True

        start_time = time.time()
        futures = [w.hold_resource(i) for i, w in enumerate(workers)]
        results = [f.result(timeout=30) for f in futures]
        elapsed = time.time() - start_time

        assert len(results) == 6
        assert elapsed >= 1.7, (
            f"Total time {elapsed:.2f}s too fast. Expected >= 1.7s. "
            f"If < 1.5s, limits are NOT shared across Ray actors."
        )
        assert elapsed <= 10.0, f"Total time {elapsed:.2f}s too slow, possible deadlock."

        for w in workers:
            w.stop()


class TestAsyncAcquireSharedCapacityEnforcement:
    """Test that shared async_acquire respects capacity across worker pools.

    Mirrors TestSharedLimitAcquisitionTracking from test_shared_limits.py.
    Uses pool_mode fixture to test thread, process, and ray pools.
    """

    def test_shared_resource_limit_sequential_waves_async(self, pool_mode):
        """4 workers share ResourceLimit(capacity=2) via async_acquire.
        4 tasks of 1s each should form 2 waves => total ~2s.

        Steps:
        1. Create shared LimitSet with ResourceLimit(capacity=2)
        2. Create pool of 4 workers
        3. Each worker runs async method using async_acquire, holds 1s
        4. Validate total elapsed >= 1.5s (2 sequential waves)
        5. Validate capacity never exceeded via timeline analysis
        """
        from concurry import Worker

        class AsyncHoldWorker(Worker):
            def __init__(self):
                pass

            async def hold_resource(self, hold_time: float) -> dict:
                import asyncio
                import time as _time

                request_time = _time.time()
                async with await self.limits.async_acquire(requested={"resource": 1}):
                    grant_time = _time.time()
                    wait_time = grant_time - request_time
                    await asyncio.sleep(hold_time)
                    release_time = _time.time()
                    return {
                        "request_time": request_time,
                        "grant_time": grant_time,
                        "release_time": release_time,
                        "wait_time": wait_time,
                    }

        shared_limits = LimitSet(
            limits=[ResourceLimit(key="resource", capacity=2)],
            shared=True,
            mode=pool_mode,
        )

        if pool_mode == "thread":
            w = AsyncHoldWorker.options(mode=pool_mode, max_workers=4, limits=shared_limits).init()
        elif pool_mode == "process":
            w = AsyncHoldWorker.options(mode=pool_mode, max_workers=4, limits=shared_limits).init()
        elif pool_mode == "ray":
            w = AsyncHoldWorker.options(mode=pool_mode, max_workers=0, limits=shared_limits).init()
        else:
            w = AsyncHoldWorker.options(mode=pool_mode, limits=shared_limits).init()

        start_time = time.time()
        futures = [w.hold_resource(1.0) for _ in range(4)]
        results = [f.result(timeout=30) for f in futures]
        total_elapsed = time.time() - start_time

        assert total_elapsed >= 1.5, (
            f"Total {total_elapsed:.2f}s too fast. Expected >= 1.5s for 2 waves."
        )
        assert total_elapsed <= 8.0, (
            f"Total {total_elapsed:.2f}s too slow, possible deadlock."
        )

        events = []
        for i, r in enumerate(results):
            events.append(("grant", r["grant_time"], i))
            events.append(("release", r["release_time"], i))
        events.sort(key=lambda e: e[1])

        current_holdings = set()
        max_concurrent = 0
        for event_type, _, worker_id in events:
            if event_type == "grant":
                current_holdings.add(worker_id)
                max_concurrent = max(max_concurrent, len(current_holdings))
            else:
                current_holdings.discard(worker_id)

        assert max_concurrent <= 2, (
            f"Max concurrent {max_concurrent} exceeded capacity 2. "
            f"Shared limit enforcement failed."
        )

        w.stop()


class TestAsyncAcquireDirectImplementations:
    """Direct unit tests for async_acquire on each LimitSet implementation."""

    @pytest.mark.asyncio
    async def test_multiprocess_shared_limitset_async_acquire(self):
        """Direct test of MultiprocessSharedLimitSet.async_acquire().

        Steps:
        1. Create MultiprocessSharedLimitSet with ResourceLimit(capacity=3)
        2. async_acquire 3 times (fills capacity)
        3. Verify all succeed
        4. Release all
        5. async_acquire again to confirm capacity restored
        """
        from concurry.core.limit.limit_set import MultiprocessSharedLimitSet

        limit_set = MultiprocessSharedLimitSet(
            limits=[ResourceLimit(key="slots", capacity=3)],
            shared=True,
        )

        acquisitions = []
        for _ in range(3):
            acq = await limit_set.async_acquire(requested={"slots": 1})
            assert acq.successful is True
            acquisitions.append(acq)

        for acq in acquisitions:
            acq.release()

        acq_after = await limit_set.async_acquire(requested={"slots": 1}, timeout=2.0)
        assert acq_after.successful is True
        acq_after.release()

    @pytest.mark.asyncio
    async def test_multiprocess_shared_limitset_async_acquire_timeout(self):
        """MultiprocessSharedLimitSet.async_acquire() raises TimeoutError when exhausted.

        Steps:
        1. Create MultiprocessSharedLimitSet with ResourceLimit(capacity=1)
        2. async_acquire to fill capacity
        3. Second async_acquire with timeout should raise TimeoutError
        """
        from concurry.core.limit.limit_set import MultiprocessSharedLimitSet

        limit_set = MultiprocessSharedLimitSet(
            limits=[ResourceLimit(key="slots", capacity=1)],
            shared=True,
        )

        acq1 = await limit_set.async_acquire(requested={"slots": 1})

        with pytest.raises(TimeoutError):
            await limit_set.async_acquire(requested={"slots": 1}, timeout=0.5)

        acq1.release()

    @pytest.mark.asyncio
    async def test_ray_shared_limitset_async_acquire(self, requires_ray_mode):
        """Direct test of RaySharedLimitSet.async_acquire().

        Steps:
        1. Create RaySharedLimitSet with ResourceLimit(capacity=3)
        2. async_acquire 3 times (fills capacity)
        3. Verify all succeed
        4. Release all
        5. async_acquire again to confirm capacity restored
        """
        pytest.importorskip("ray")
        from concurry.core.limit.limit_set import RaySharedLimitSet

        limit_set = RaySharedLimitSet(
            limits=[ResourceLimit(key="slots", capacity=3)],
            shared=True,
        )

        acquisitions = []
        for _ in range(3):
            acq = await limit_set.async_acquire(requested={"slots": 1})
            assert acq.successful is True
            acquisitions.append(acq)

        for acq in acquisitions:
            acq.release()

        acq_after = await limit_set.async_acquire(requested={"slots": 1}, timeout=2.0)
        assert acq_after.successful is True
        acq_after.release()

    @pytest.mark.asyncio
    async def test_ray_shared_limitset_async_acquire_timeout(self, requires_ray_mode):
        """RaySharedLimitSet.async_acquire() raises TimeoutError when exhausted.

        Steps:
        1. Create RaySharedLimitSet with ResourceLimit(capacity=1)
        2. async_acquire to fill capacity
        3. Second async_acquire with timeout should raise TimeoutError
        """
        pytest.importorskip("ray")
        from concurry.core.limit.limit_set import RaySharedLimitSet

        limit_set = RaySharedLimitSet(
            limits=[ResourceLimit(key="slots", capacity=1)],
            shared=True,
        )

        acq1 = await limit_set.async_acquire(requested={"slots": 1})

        with pytest.raises(TimeoutError):
            await limit_set.async_acquire(requested={"slots": 1}, timeout=0.5)

        acq1.release()
