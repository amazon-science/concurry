"""Tests for async_wait() and async_gather() functions with coroutines."""

import asyncio
import time

import pytest

from concurry import ReturnWhen, async_gather, async_wait


class TestCoroutineWithoutEventLoop:
    """Test coroutine handling when no event loop exists."""

    def test_wrap_future_coroutine_without_event_loop(self):
        """Test that wrap_future raises clear error for coroutines without event loop."""
        import asyncio

        from concurry.core.future import wrap_future

        async def simple_coro():
            await asyncio.sleep(0.01)
            return 42

        # Create coroutine without event loop
        coro = simple_coro()

        # wrap_future should raise RuntimeError with clear message
        with pytest.raises(RuntimeError, match="Cannot schedule coroutine.*no running event loop"):
            wrap_future(coro)

        # Clean up the coroutine
        coro.close()


class TestAsyncWaitBasics:
    """Basic tests for async_wait() function."""

    @pytest.mark.asyncio
    async def test_wait_single_coroutine(self):
        """Test async_wait with single coroutine."""

        async def simple_task():
            await asyncio.sleep(0.01)
            return 42

        coro = simple_task()
        done, not_done = await async_wait(coro, timeout=5.0)

        assert len(done) == 1
        assert len(not_done) == 0

        # Get result
        task = list(done)[0]
        result = await task
        assert result == 42

    @pytest.mark.asyncio
    async def test_wait_list_of_coroutines(self):
        """Test async_wait with list of coroutines."""

        async def compute(x):
            await asyncio.sleep(0.01)
            return x * 2

        coros = [compute(i) for i in range(5)]
        done, not_done = await async_wait(coros, timeout=5.0)

        assert len(done) == 5
        assert len(not_done) == 0

        # Get results
        results = [await task for task in done]
        assert set(results) == {0, 2, 4, 6, 8}

    @pytest.mark.asyncio
    async def test_wait_empty_list(self):
        """Test async_wait with empty list."""
        done, not_done = await async_wait([])

        assert len(done) == 0
        assert len(not_done) == 0

    @pytest.mark.asyncio
    async def test_wait_all_completed(self):
        """Test async_wait with ALL_COMPLETED (default)."""

        async def task(x):
            await asyncio.sleep(0.01)
            return x

        coros = [task(i) for i in range(3)]
        done, not_done = await async_wait(coros, return_when=ReturnWhen.ALL_COMPLETED, timeout=5.0)

        assert len(done) == 3
        assert len(not_done) == 0

    @pytest.mark.asyncio
    async def test_wait_first_completed(self):
        """Test async_wait with FIRST_COMPLETED."""

        async def fast_task():
            await asyncio.sleep(0.01)
            return "fast"

        async def slow_task():
            await asyncio.sleep(1.0)
            return "slow"

        coros = [slow_task(), fast_task(), slow_task()]
        done, not_done = await async_wait(coros, return_when=ReturnWhen.FIRST_COMPLETED, timeout=5.0)

        # At least one should be done
        assert len(done) >= 1
        assert len(done) + len(not_done) == 3

        # Cancel remaining tasks
        for task in not_done:
            task.cancel()

    @pytest.mark.asyncio
    async def test_wait_first_exception(self):
        """Test async_wait with FIRST_EXCEPTION."""

        async def normal_task():
            await asyncio.sleep(0.5)
            return "success"

        async def failing_task():
            await asyncio.sleep(0.01)
            raise ValueError("Test error")

        coros = [normal_task(), failing_task(), normal_task()]
        done, not_done = await async_wait(coros, return_when=ReturnWhen.FIRST_EXCEPTION, timeout=5.0)

        # Should return when exception occurs
        assert len(done) >= 1

        # Check that at least one has an exception
        has_exception = False
        for task in done:
            try:
                task.exception()
                has_exception = True
                break
            except:
                pass

        assert has_exception or len(done) > 1

        # Cancel remaining tasks
        for task in not_done:
            task.cancel()

    @pytest.mark.asyncio
    async def test_wait_timeout(self):
        """Test async_wait timeout behavior."""

        async def slow_task():
            await asyncio.sleep(5.0)
            return "slow"

        coros = [slow_task() for _ in range(3)]

        with pytest.raises(TimeoutError, match="async_wait.*timed out"):
            await async_wait(coros, timeout=0.1)

    @pytest.mark.asyncio
    async def test_wait_with_string_return_when(self):
        """Test async_wait with return_when as string."""

        async def task(x):
            await asyncio.sleep(0.01)
            return x

        coros = [task(i) for i in range(3)]
        done, not_done = await async_wait(coros, return_when="all_completed", timeout=5.0)

        assert len(done) == 3
        assert len(not_done) == 0


class TestAsyncWaitProgress:
    """Tests for async_wait() with progress tracking."""

    @pytest.mark.asyncio
    async def test_wait_progress_bool_true(self):
        """Test async_wait with progress=True."""

        async def task(x):
            await asyncio.sleep(0.01)
            return x

        coros = [task(i) for i in range(10)]
        done, not_done = await async_wait(coros, progress=True, timeout=5.0)

        assert len(done) == 10
        assert len(not_done) == 0

    @pytest.mark.asyncio
    async def test_wait_progress_dict(self):
        """Test async_wait with progress as dict configuration."""

        async def task(x):
            await asyncio.sleep(0.01)
            return x

        coros = [task(i) for i in range(5)]
        done, not_done = await async_wait(coros, progress={"desc": "Testing", "unit": "task"}, timeout=5.0)

        assert len(done) == 5
        assert len(not_done) == 0

    @pytest.mark.asyncio
    async def test_wait_progress_callback(self):
        """Test async_wait with progress callback."""
        callback_calls = []

        def progress_callback(completed, total, elapsed):
            callback_calls.append((completed, total, elapsed))

        async def task(x):
            await asyncio.sleep(0.01)
            return x

        coros = [task(i) for i in range(5)]
        done, not_done = await async_wait(coros, progress=progress_callback, timeout=5.0)

        assert len(done) == 5
        assert len(not_done) == 0
        # Callback should have been called
        assert len(callback_calls) > 0


class TestAsyncWaitDict:
    """Tests for async_wait() with dict of coroutines."""

    @pytest.mark.asyncio
    async def test_wait_dict_basic(self):
        """Test async_wait with dict of coroutines."""

        async def compute(x):
            await asyncio.sleep(0.01)
            return x * 10

        coros_dict = {
            "task1": compute(1),
            "task2": compute(2),
            "task3": compute(3),
        }

        done, not_done = await async_wait(coros_dict, timeout=5.0)

        assert len(done) == 3
        assert len(not_done) == 0

    @pytest.mark.asyncio
    async def test_wait_dict_empty(self):
        """Test async_wait with empty dict."""
        done, not_done = await async_wait({})

        assert len(done) == 0
        assert len(not_done) == 0

    @pytest.mark.asyncio
    async def test_wait_dict_first_completed(self):
        """Test async_wait dict with FIRST_COMPLETED."""

        async def fast_task():
            await asyncio.sleep(0.01)
            return "fast"

        async def slow_task():
            await asyncio.sleep(1.0)
            return "slow"

        coros_dict = {
            "fast": fast_task(),
            "slow": slow_task(),
        }

        done, not_done = await async_wait(coros_dict, return_when=ReturnWhen.FIRST_COMPLETED, timeout=5.0)

        # At least one should be done
        assert len(done) >= 1
        assert len(done) + len(not_done) == 2

        # Cancel remaining
        for task in not_done:
            task.cancel()


class TestAsyncGatherBasics:
    """Basic tests for async_gather() function."""

    @pytest.mark.asyncio
    async def test_gather_single_coroutine(self):
        """Test async_gather with single coroutine."""

        async def simple_task():
            await asyncio.sleep(0.01)
            return 42

        coro = simple_task()
        results = await async_gather(coro, timeout=5.0)

        assert results == [42]

    @pytest.mark.asyncio
    async def test_gather_multiple_coroutines(self):
        """Test async_gather with multiple coroutines."""

        async def compute(x):
            await asyncio.sleep(0.01)
            return x * 2

        coros = [compute(i) for i in range(5)]
        results = await async_gather(coros, timeout=5.0)

        assert results == [0, 2, 4, 6, 8]

    @pytest.mark.asyncio
    async def test_gather_empty(self):
        """Test async_gather with empty list."""
        results = await async_gather([])

        assert results == []

    @pytest.mark.asyncio
    async def test_gather_preserves_order(self):
        """Test that async_gather preserves input order."""

        async def task(x, delay):
            await asyncio.sleep(delay)
            return x

        # Submit in order with varying delays - fast task in middle
        coros = [
            task(0, 0.1),
            task(1, 0.01),  # Fastest
            task(2, 0.05),
        ]
        results = await async_gather(coros, timeout=5.0)

        # Order should be preserved despite different completion times
        assert results == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_gather_with_exceptions_raised(self):
        """Test async_gather raises exceptions by default."""

        async def failing_task():
            await asyncio.sleep(0.01)
            raise ValueError("Test error")

        async def normal_task():
            await asyncio.sleep(0.01)
            return 1

        coros = [normal_task(), failing_task(), normal_task()]

        with pytest.raises(ValueError, match="Test error"):
            await async_gather(coros, timeout=5.0)

    @pytest.mark.asyncio
    async def test_gather_return_exceptions(self):
        """Test async_gather with return_exceptions=True."""

        async def failing_task():
            await asyncio.sleep(0.01)
            raise ValueError("Test error")

        async def normal_task(x):
            await asyncio.sleep(0.01)
            return x

        coros = [normal_task(1), failing_task(), normal_task(3)]
        results = await async_gather(coros, return_exceptions=True, timeout=5.0)

        assert results[0] == 1
        assert isinstance(results[1], ValueError)
        assert results[2] == 3

    @pytest.mark.asyncio
    async def test_gather_timeout(self):
        """Test async_gather timeout behavior."""

        async def slow_task():
            await asyncio.sleep(5.0)
            return "slow"

        coros = [slow_task() for _ in range(3)]

        with pytest.raises(TimeoutError, match="async_gather.*timed out"):
            await async_gather(coros, timeout=0.1)


class TestAsyncGatherProgress:
    """Tests for async_gather() with progress tracking."""

    @pytest.mark.asyncio
    async def test_gather_progress_bool(self):
        """Test async_gather with progress=True."""

        async def task(x):
            await asyncio.sleep(0.01)
            return x

        coros = [task(i) for i in range(10)]
        results = await async_gather(coros, progress=True, timeout=5.0)

        assert len(results) == 10

    @pytest.mark.asyncio
    async def test_gather_progress_dict(self):
        """Test async_gather with progress dict configuration."""

        async def task(x):
            await asyncio.sleep(0.01)
            return x

        coros = [task(i) for i in range(5)]
        results = await async_gather(coros, progress={"desc": "Gathering", "unit": "result"}, timeout=5.0)

        assert len(results) == 5

    @pytest.mark.asyncio
    async def test_gather_progress_callback(self):
        """Test async_gather with progress callback."""
        callback_calls = []

        def progress_callback(completed, total, elapsed):
            callback_calls.append((completed, total, elapsed))

        async def task(x):
            await asyncio.sleep(0.01)
            return x

        coros = [task(i) for i in range(5)]
        results = await async_gather(coros, progress=progress_callback, timeout=5.0)

        assert len(results) == 5
        # Callback should have been called
        assert len(callback_calls) > 0


class TestAsyncGatherDict:
    """Tests for async_gather() with dict of coroutines."""

    @pytest.mark.asyncio
    async def test_gather_dict_basic(self):
        """Test async_gather with dict of coroutines returns dict with same keys."""

        async def compute(x):
            await asyncio.sleep(0.01)
            return x * 10

        coros_dict = {
            "task1": compute(1),
            "task2": compute(2),
            "task3": compute(3),
        }

        results = await async_gather(coros_dict, timeout=5.0)

        assert isinstance(results, dict)
        assert results == {"task1": 10, "task2": 20, "task3": 30}

    @pytest.mark.asyncio
    async def test_gather_dict_empty(self):
        """Test async_gather with empty dict."""
        results = await async_gather({})

        assert isinstance(results, dict)
        assert results == {}

    @pytest.mark.asyncio
    async def test_gather_dict_preserves_key_order(self):
        """Test that async_gather preserves dict key order."""

        async def compute(x):
            await asyncio.sleep(0.01)
            return x

        coros_dict = {
            "first": compute(10),
            "second": compute(20),
            "third": compute(30),
        }

        results = await async_gather(coros_dict, timeout=5.0)

        assert list(results.keys()) == ["first", "second", "third"]
        assert list(results.values()) == [10, 20, 30]

    @pytest.mark.asyncio
    async def test_gather_dict_with_exceptions(self):
        """Test async_gather dict with exceptions raised."""

        async def failing_task():
            await asyncio.sleep(0.01)
            raise ValueError("Failed")

        async def normal_task(x):
            await asyncio.sleep(0.01)
            return x

        coros_dict = {
            "good": normal_task(42),
            "bad": failing_task(),
        }

        with pytest.raises(ValueError, match="Failed"):
            await async_gather(coros_dict, timeout=5.0)

    @pytest.mark.asyncio
    async def test_gather_dict_return_exceptions(self):
        """Test async_gather dict with return_exceptions=True."""

        async def failing_task():
            await asyncio.sleep(0.01)
            raise ValueError("Failed")

        async def normal_task(x):
            await asyncio.sleep(0.01)
            return x

        coros_dict = {
            "good": normal_task(42),
            "bad": failing_task(),
            "also_good": normal_task(100),
        }

        results = await async_gather(coros_dict, return_exceptions=True, timeout=5.0)

        assert isinstance(results, dict)
        assert results["good"] == 42
        assert isinstance(results["bad"], ValueError)
        assert results["also_good"] == 100


class TestMixedTypes:
    """Tests with mixed future types in async context."""

    @pytest.mark.asyncio
    async def test_async_gather_mixed_coroutines_and_values(self):
        """Test async_gather with mix of coroutines and regular values."""

        async def compute(x):
            await asyncio.sleep(0.01)
            return x * 2

        mixed = [compute(5), 42, compute(10)]
        results = await async_gather(mixed, timeout=5.0)

        assert results == [10, 42, 20]

    @pytest.mark.asyncio
    async def test_async_wait_mixed_types(self):
        """Test async_wait with mix of coroutines and values."""

        async def compute(x):
            await asyncio.sleep(0.01)
            return x * 2

        mixed = [compute(5), 42, compute(10)]
        done, not_done = await async_wait(mixed, timeout=5.0)

        assert len(done) == 3
        assert len(not_done) == 0


class TestVariadicSignature:
    """Tests for variadic signature."""

    @pytest.mark.asyncio
    async def test_wait_multiple_coroutines(self):
        """Test async_wait with multiple individual coroutines."""

        async def task(x):
            await asyncio.sleep(0.01)
            return x

        c1 = task(1)
        c2 = task(2)
        c3 = task(3)

        done, not_done = await async_wait(c1, c2, c3, timeout=5.0)

        assert len(done) == 3
        assert len(not_done) == 0

    @pytest.mark.asyncio
    async def test_gather_multiple_coroutines(self):
        """Test async_gather with multiple individual coroutines."""

        async def task(x):
            await asyncio.sleep(0.01)
            return x * 10

        c1 = task(10)
        c2 = task(20)
        c3 = task(30)

        results = await async_gather(c1, c2, c3, timeout=5.0)

        assert results == [100, 200, 300]

    @pytest.mark.asyncio
    async def test_wait_error_on_mixed_usage(self):
        """Test that mixing list and variadic args raises error."""

        async def task(x):
            return x

        coros = [task(i) for i in range(3)]
        extra = task(99)

        with pytest.raises(ValueError, match="Cannot provide both a structure"):
            await async_wait(coros, extra)

    @pytest.mark.asyncio
    async def test_gather_error_on_mixed_usage(self):
        """Test that mixing list and variadic args raises error."""

        async def task(x):
            return x

        coros = [task(i) for i in range(3)]
        extra = task(99)

        with pytest.raises(ValueError, match="Cannot provide both a structure"):
            await async_gather(coros, extra)


class TestEdgeCases:
    """Edge case tests for async_wait() and async_gather()."""

    @pytest.mark.asyncio
    async def test_gather_large_batch(self):
        """Test async_gather with large batch (100 coroutines)."""

        async def compute(x):
            await asyncio.sleep(0.001)
            return x

        coros = [compute(i) for i in range(100)]
        results = await async_gather(coros, timeout=10.0)

        assert len(results) == 100
        assert results == list(range(100))

    @pytest.mark.asyncio
    async def test_wait_large_batch(self):
        """Test async_wait with large batch (100 coroutines)."""

        async def compute(x):
            await asyncio.sleep(0.001)
            return x

        coros = [compute(i) for i in range(100)]
        done, not_done = await async_wait(coros, timeout=10.0)

        assert len(done) == 100
        assert len(not_done) == 0

    @pytest.mark.asyncio
    async def test_gather_dict_with_non_string_keys(self):
        """Test async_gather dict with non-string keys."""

        async def compute(x):
            await asyncio.sleep(0.01)
            return x * 10

        coros_dict = {
            1: compute(10),
            2: compute(20),
            3: compute(30),
        }

        results = await async_gather(coros_dict, timeout=5.0)

        assert results == {1: 100, 2: 200, 3: 300}

    @pytest.mark.asyncio
    async def test_gather_dict_with_tuple_keys(self):
        """Test async_gather dict with tuple keys."""

        async def compute(x):
            await asyncio.sleep(0.01)
            return x * 10

        coros_dict = {
            ("a", 1): compute(10),
            ("b", 2): compute(20),
        }

        results = await async_gather(coros_dict, timeout=5.0)

        assert results == {("a", 1): 100, ("b", 2): 200}

    @pytest.mark.asyncio
    async def test_wait_with_already_completed_tasks(self):
        """Test async_wait with some already completed tasks."""

        async def fast_task():
            return "fast"

        async def slow_task():
            await asyncio.sleep(0.1)
            return "slow"

        # Create tasks
        task1 = asyncio.create_task(fast_task())
        task2 = asyncio.create_task(slow_task())

        # Wait for first to complete
        await asyncio.sleep(0.05)

        # Now wait on both (one already done)
        done, not_done = await async_wait([task1, task2], timeout=5.0)

        assert len(done) == 2
        assert len(not_done) == 0

    @pytest.mark.asyncio
    async def test_performance_comparison(self):
        """Test that async_gather is reasonably fast."""

        async def quick_task(x):
            await asyncio.sleep(0.001)
            return x

        coros = [quick_task(i) for i in range(100)]

        start = time.time()
        results = await async_gather(coros, timeout=10.0)
        elapsed = time.time() - start

        assert len(results) == 100
        # Should complete in reasonable time (not serialized)
        assert elapsed < 5.0  # Much less than 100 * 0.001 = 0.1s if parallel
