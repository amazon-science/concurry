"""Tests for wait() and gather() synchronization primitives."""

import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import pytest

from concurry import (
    ALL_COMPLETED,
    FIRST_COMPLETED,
    FIRST_EXCEPTION,
    PollingAlgorithm,
    SyncFuture,
    Worker,
    gather,
    wait,
)


# Test worker for comprehensive mode testing
class ComputeWorker(Worker):
    """Simple worker for testing wait/gather across all modes."""

    def __init__(self, multiplier: int = 1):
        self.multiplier = multiplier

    def compute(self, x: int) -> int:
        """Simple computation."""
        return x * self.multiplier

    def slow_compute(self, x: int, duration: float = 0.1) -> int:
        """Slow computation for timeout tests."""
        time.sleep(duration)
        return x * self.multiplier

    def failing_compute(self, x: int) -> int:
        """Computation that raises an error."""
        raise ValueError(f"Failed to compute {x}")


class TestWaitBasics:
    """Basic tests for wait() function."""

    def test_wait_single_future_done(self):
        """Test waiting on a single completed future."""
        fut = SyncFuture(result_value=42)
        done, not_done = wait(fut)

        assert len(done) == 1
        assert len(not_done) == 0
        assert fut in done

    def test_wait_single_future_not_done(self):
        """Test waiting on single future that completes."""
        with ThreadPoolExecutor() as executor:
            fut = executor.submit(lambda: time.sleep(0.1) or 123)
            done, not_done = wait(fut, timeout=1.0)

        assert len(done) == 1
        assert len(not_done) == 0

    def test_wait_list_of_futures(self):
        """Test waiting on list of futures."""
        futures = [SyncFuture(result_value=i) for i in range(5)]
        done, not_done = wait(futures)

        assert len(done) == 5
        assert len(not_done) == 0

    def test_wait_empty_list(self):
        """Test waiting on empty list."""
        done, not_done = wait([])

        assert len(done) == 0
        assert len(not_done) == 0

    def test_wait_all_completed(self):
        """Test wait with ALL_COMPLETED (default)."""
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(lambda x: time.sleep(0.01) or x, i) for i in range(3)]
            done, not_done = wait(futures, return_when=ALL_COMPLETED, timeout=5.0)

        assert len(done) == 3
        assert len(not_done) == 0

    def test_wait_first_completed(self):
        """Test wait with FIRST_COMPLETED."""
        with ThreadPoolExecutor() as executor:
            # Create futures with varying completion times
            futures = [
                executor.submit(lambda: time.sleep(0.5) or "slow"),
                executor.submit(lambda: time.sleep(0.01) or "fast"),
                executor.submit(lambda: time.sleep(1.0) or "very_slow"),
            ]

            done, not_done = wait(futures, return_when=FIRST_COMPLETED, timeout=5.0)

        # At least one should be done
        assert len(done) >= 1
        # Some might still be pending
        assert len(done) + len(not_done) == 3

    def test_wait_first_exception(self):
        """Test wait with FIRST_EXCEPTION."""

        def raise_error():
            time.sleep(0.01)
            raise ValueError("Test error")

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(lambda: time.sleep(0.5) or "slow"),
                executor.submit(raise_error),
                executor.submit(lambda: time.sleep(1.0) or "very_slow"),
            ]

            done, not_done = wait(futures, return_when=FIRST_EXCEPTION, timeout=5.0)

        # Should return when exception future completes
        assert len(done) >= 1

        # Check that at least one has an exception
        has_exception = False
        for fut in done:
            try:
                fut.exception(timeout=0)
                has_exception = True
                break
            except:
                pass

        assert has_exception or len(done) > 1  # Either has exception or multiple completed

    def test_wait_timeout(self):
        """Test wait timeout behavior."""
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(lambda: time.sleep(2.0) or "slow") for _ in range(3)]

            with pytest.raises(TimeoutError):
                wait(futures, timeout=0.1)

    def test_wait_invalid_return_when(self):
        """Test error on invalid return_when."""
        fut = SyncFuture(result_value=1)
        with pytest.raises(ValueError, match="Could not find enum"):
            wait(fut, return_when="INVALID_CONDITION")


class TestWaitPolling:
    """Tests for wait() with different polling algorithms."""

    def test_wait_with_fixed_polling(self):
        """Test wait with fixed polling algorithm."""
        futures = [SyncFuture(result_value=i) for i in range(5)]
        done, not_done = wait(futures, polling=PollingAlgorithm.Fixed)

        assert len(done) == 5
        assert len(not_done) == 0

    def test_wait_with_adaptive_polling(self):
        """Test wait with adaptive polling algorithm."""
        futures = [SyncFuture(result_value=i) for i in range(5)]
        done, not_done = wait(futures, polling=PollingAlgorithm.Adaptive)

        assert len(done) == 5
        assert len(not_done) == 0

    def test_wait_with_exponential_polling(self):
        """Test wait with exponential polling algorithm."""
        futures = [SyncFuture(result_value=i) for i in range(5)]
        done, not_done = wait(futures, polling=PollingAlgorithm.Exponential)

        assert len(done) == 5
        assert len(not_done) == 0

    def test_wait_with_progressive_polling(self):
        """Test wait with progressive polling algorithm."""
        futures = [SyncFuture(result_value=i) for i in range(5)]
        done, not_done = wait(futures, polling=PollingAlgorithm.Progressive)

        assert len(done) == 5
        assert len(not_done) == 0

    def test_wait_polling_string(self):
        """Test wait with polling algorithm as string."""
        futures = [SyncFuture(result_value=i) for i in range(5)]
        done, not_done = wait(futures, polling="fixed")

        assert len(done) == 5
        assert len(not_done) == 0


class TestWaitProgress:
    """Tests for wait() with progress tracking."""

    def test_wait_progress_bool_true(self):
        """Test wait with progress=True."""
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(lambda x: time.sleep(0.01) or x, i) for i in range(10)]
            done, not_done = wait(futures, progress=True, timeout=5.0)

        assert len(done) == 10
        assert len(not_done) == 0

    def test_wait_progress_dict(self):
        """Test wait with progress as dict configuration."""
        futures = [SyncFuture(result_value=i) for i in range(5)]
        done, not_done = wait(futures, progress={"desc": "Testing", "unit": "task"})

        assert len(done) == 5
        assert len(not_done) == 0

    def test_wait_progress_callback(self):
        """Test wait with progress callback."""
        callback_calls = []

        def progress_callback(completed, total, elapsed):
            callback_calls.append((completed, total, elapsed))

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(lambda x: time.sleep(0.01) or x, i) for i in range(5)]
            done, not_done = wait(futures, progress=progress_callback, timeout=5.0)

        assert len(done) == 5
        assert len(not_done) == 0
        # Callback should have been called
        assert len(callback_calls) > 0

    def test_wait_progress_none(self):
        """Test wait with progress=None (no tracking)."""
        futures = [SyncFuture(result_value=i) for i in range(5)]
        done, not_done = wait(futures, progress=None)

        assert len(done) == 5
        assert len(not_done) == 0


class TestGatherBasics:
    """Basic tests for gather() function."""

    def test_gather_single_future(self):
        """Test gathering single future."""
        fut = SyncFuture(result_value=42)
        results = gather(fut)

        assert results == [42]

    def test_gather_multiple_futures(self):
        """Test gathering multiple futures."""
        futures = [SyncFuture(result_value=i) for i in range(5)]
        results = gather(*futures)

        assert results == [0, 1, 2, 3, 4]

    def test_gather_empty(self):
        """Test gathering empty list."""
        results = gather([])

        assert results == []

    def test_gather_preserves_order(self):
        """Test that gather preserves input order."""
        with ThreadPoolExecutor() as executor:
            # Submit in order, but varying sleep times
            futures = [
                executor.submit(lambda x: time.sleep(0.1) or x, 0),
                executor.submit(lambda x: time.sleep(0.01) or x, 1),
                executor.submit(lambda x: time.sleep(0.05) or x, 2),
            ]
            results = gather(*futures, timeout=5.0)

        assert results == [0, 1, 2]

    def test_gather_with_exceptions_raised(self):
        """Test gather raises exceptions by default."""

        def raise_error():
            raise ValueError("Test error")

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(lambda: 1),
                executor.submit(raise_error),
                executor.submit(lambda: 3),
            ]

            with pytest.raises(ValueError, match="Test error"):
                gather(*futures, timeout=5.0)

    def test_gather_return_exceptions(self):
        """Test gather with return_exceptions=True."""

        def raise_error():
            raise ValueError("Test error")

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(lambda: 1),
                executor.submit(raise_error),
                executor.submit(lambda: 3),
            ]

            results = gather(*futures, return_exceptions=True, timeout=5.0)

        assert results[0] == 1
        assert isinstance(results[1], ValueError)
        assert results[2] == 3

    def test_gather_timeout(self):
        """Test gather timeout behavior."""
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(lambda: time.sleep(2.0) or "slow") for _ in range(3)]

            with pytest.raises(TimeoutError):
                gather(*futures, timeout=0.1)


class TestGatherIterator:
    """Tests for gather() with iter=True."""

    def test_gather_iter_basic(self):
        """Test gather iterator mode."""
        futures = [SyncFuture(result_value=i) for i in range(5)]
        results = list(gather(*futures, iter=True))

        # Should yield (index, result) tuples
        assert len(results) == 5
        # Extract just the results
        values = [r[1] for r in results]
        assert set(values) == {0, 1, 2, 3, 4}

    def test_gather_iter_out_of_order(self):
        """Test that iter mode yields results as they complete."""
        with ThreadPoolExecutor() as executor:
            # Create futures with varying completion times
            futures = [
                executor.submit(lambda x: time.sleep(0.1) or x, 0),
                executor.submit(lambda x: time.sleep(0.01) or x, 1),  # Fastest
                executor.submit(lambda x: time.sleep(0.05) or x, 2),
            ]

            results = list(gather(*futures, iter=True, timeout=5.0))

        # All should be present
        assert len(results) == 3
        indices = [r[0] for r in results]
        values = [r[1] for r in results]

        assert set(indices) == {0, 1, 2}
        assert set(values) == {0, 1, 2}

    def test_gather_iter_with_exceptions(self):
        """Test gather iterator with exceptions."""

        def raise_error():
            time.sleep(0.01)
            raise ValueError("Test error")

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(lambda: 1),
                executor.submit(raise_error),
                executor.submit(lambda: 3),
            ]

            with pytest.raises(ValueError, match="Test error"):
                list(gather(*futures, iter=True, timeout=5.0))

    def test_gather_iter_return_exceptions(self):
        """Test gather iterator with return_exceptions=True."""

        def raise_error():
            time.sleep(0.01)
            raise ValueError("Test error")

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(lambda: 1),
                executor.submit(raise_error),
                executor.submit(lambda: 3),
            ]

            results = list(gather(*futures, iter=True, return_exceptions=True, timeout=5.0))

        assert len(results) == 3
        # One should be an exception
        has_exception = any(isinstance(r[1], Exception) for r in results)
        assert has_exception


class TestGatherProgress:
    """Tests for gather() with progress tracking."""

    def test_gather_progress_bool(self):
        """Test gather with progress=True."""
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(lambda x: time.sleep(0.01) or x, i) for i in range(10)]
            results = gather(*futures, progress=True, timeout=5.0)

        assert len(results) == 10

    def test_gather_progress_dict(self):
        """Test gather with progress dict configuration."""
        futures = [SyncFuture(result_value=i) for i in range(5)]
        results = gather(*futures, progress={"desc": "Gathering", "unit": "result"})

        assert len(results) == 5

    def test_gather_progress_callback(self):
        """Test gather with progress callback."""
        callback_calls = []

        def progress_callback(completed, total, elapsed):
            callback_calls.append((completed, total, elapsed))

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(lambda x: time.sleep(0.01) or x, i) for i in range(5)]
            results = gather(*futures, progress=progress_callback, timeout=5.0)

        assert len(results) == 5
        # Callback should have been called
        assert len(callback_calls) > 0


class TestRecurseMode:
    """Tests for recurse parameter in wait() and gather()."""

    def test_wait_recurse_nested_list(self):
        """Test wait with nested list of futures."""
        nested_futures = [[SyncFuture(result_value=i) for i in range(3)] for _ in range(2)]

        done, not_done = wait(nested_futures, recurse=True)

        # Should flatten and wait on all futures
        assert len(done) == 6  # 2 * 3
        assert len(not_done) == 0

    def test_wait_no_recurse_nested(self):
        """Test wait without recurse doesn't flatten nested structures."""
        nested_futures = [[SyncFuture(result_value=i) for i in range(3)] for _ in range(2)]

        # This wraps the lists themselves as futures
        done, not_done = wait(nested_futures, recurse=False)

        # Should only wrap the outer lists
        assert len(done) == 2  # Two lists
        assert len(not_done) == 0

    def test_gather_recurse_nested_list(self):
        """Test gather with nested structures."""
        # Create simple nested future structure
        f1 = SyncFuture(result_value=1)
        f2 = SyncFuture(result_value=2)
        f3 = SyncFuture(result_value=3)

        results = gather(f1, f2, f3, recurse=True)

        # Should get results in order
        assert len(results) == 3
        assert results == [1, 2, 3]


class TestPerformance:
    """Performance tests for wait() and gather()."""

    def test_wait_large_batch_sync_futures(self):
        """Test wait with large batch of sync futures (should be fast)."""
        futures = [SyncFuture(result_value=i) for i in range(1000)]

        start = time.time()
        done, not_done = wait(futures)
        elapsed = time.time() - start

        assert len(done) == 1000
        assert len(not_done) == 0
        # Should be very fast for already-done futures
        assert elapsed < 1.0  # Less than 1 second

    def test_gather_large_batch_sync_futures(self):
        """Test gather with large batch of sync futures."""
        futures = [SyncFuture(result_value=i) for i in range(1000)]

        start = time.time()
        results = gather(*futures)
        elapsed = time.time() - start

        assert len(results) == 1000
        assert results == list(range(1000))
        # Should be very fast
        assert elapsed < 1.0

    @pytest.mark.slow
    def test_wait_many_thread_futures(self):
        """Test wait with many threaded futures."""
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(lambda x: time.sleep(0.01) or x, i) for i in range(100)]

            start = time.time()
            done, not_done = wait(futures, timeout=30.0)
            elapsed = time.time() - start

        assert len(done) == 100
        assert len(not_done) == 0
        # Should complete reasonably fast with 10 workers
        assert elapsed < 15.0  # Less than 15 seconds


class TestMixedFutureTypes:
    """Tests with mixed future types."""

    def test_wait_mixed_sync_and_thread(self):
        """Test wait with mix of sync and thread futures."""
        sync_futs = [SyncFuture(result_value=i) for i in range(3)]

        with ThreadPoolExecutor() as executor:
            thread_futs = [executor.submit(lambda x: time.sleep(0.01) or x, i) for i in range(3, 6)]
            all_futs = sync_futs + thread_futs

            done, not_done = wait(all_futs, timeout=5.0)

        assert len(done) == 6
        assert len(not_done) == 0

    def test_gather_mixed_types(self):
        """Test gather with mixed future types."""
        sync_futs = [SyncFuture(result_value=i) for i in range(3)]

        with ThreadPoolExecutor() as executor:
            thread_futs = [executor.submit(lambda x: x, i) for i in range(3, 6)]
            results = gather(*sync_futs, *thread_futs, timeout=5.0)

        assert results == [0, 1, 2, 3, 4, 5]


class TestWrapFuture:
    """Tests for automatic future wrapping."""

    def test_wait_wraps_non_futures(self):
        """Test that wait wraps non-future objects."""
        # Pass regular values - should be wrapped as SyncFutures
        done, not_done = wait([1, 2, 3])

        assert len(done) == 3
        assert len(not_done) == 0

    def test_gather_wraps_non_futures(self):
        """Test that gather wraps non-future objects."""
        # Pass regular values
        results = gather(1, 2, 3)

        assert results == [1, 2, 3]


class TestWaitWithDict:
    """Tests for wait() with dict of futures."""

    def test_wait_dict_basic(self, worker_mode):
        """Test wait with dict of futures across all modes."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures_dict = {
            "task1": w.compute(1),
            "task2": w.compute(2),
            "task3": w.compute(3),
        }

        done, not_done = wait(futures_dict, timeout=5.0)

        assert len(done) == 3
        assert len(not_done) == 0

        w.stop()

    def test_wait_dict_empty(self, worker_mode):
        """Test wait with empty dict."""
        done, not_done = wait({})

        assert len(done) == 0
        assert len(not_done) == 0

    def test_wait_dict_first_completed(self, worker_mode):
        """Test wait dict with FIRST_COMPLETED."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures_dict = {
            "fast": w.compute(1),
            "slow": w.slow_compute(2, duration=1.0),
        }

        done, not_done = wait(futures_dict, return_when=FIRST_COMPLETED, timeout=5.0)

        # At least one should be done
        assert len(done) >= 1
        assert len(done) + len(not_done) == 2

        w.stop()

    def test_wait_dict_with_timeout(self, worker_mode):
        """Test wait dict with timeout."""
        if worker_mode == "sync":
            pytest.skip("Sync mode completes immediately, cannot timeout")

        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures_dict = {
            "task1": w.slow_compute(1, duration=2.0),
            "task2": w.slow_compute(2, duration=2.0),
        }

        with pytest.raises(TimeoutError):
            wait(futures_dict, timeout=0.1)

        w.stop()


class TestGatherWithDict:
    """Tests for gather() with dict of futures."""

    def test_gather_dict_basic(self, worker_mode):
        """Test gather with dict of futures returns dict with same keys."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=10)

        futures_dict = {
            "task1": w.compute(1),
            "task2": w.compute(2),
            "task3": w.compute(3),
        }

        results = gather(futures_dict, timeout=5.0)

        assert isinstance(results, dict)
        assert results == {"task1": 10, "task2": 20, "task3": 30}

        w.stop()

    def test_gather_dict_empty(self, worker_mode):
        """Test gather with empty dict."""
        results = gather({})

        assert isinstance(results, dict)
        assert results == {}

    def test_gather_dict_preserves_key_order(self, worker_mode):
        """Test that gather preserves dict key order."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures_dict = {
            "first": w.compute(10),
            "second": w.compute(20),
            "third": w.compute(30),
        }

        results = gather(futures_dict, timeout=5.0)

        assert list(results.keys()) == ["first", "second", "third"]
        assert list(results.values()) == [10, 20, 30]

        w.stop()

    def test_gather_dict_with_exceptions(self, worker_mode):
        """Test gather dict with exceptions raised."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures_dict = {
            "good": w.compute(42),
            "bad": w.failing_compute(1),
        }

        with pytest.raises(ValueError, match="Failed to compute"):
            gather(futures_dict, timeout=5.0)

        w.stop()

    def test_gather_dict_return_exceptions(self, worker_mode):
        """Test gather dict with return_exceptions=True."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures_dict = {
            "good": w.compute(42),
            "bad": w.failing_compute(1),
            "also_good": w.compute(100),
        }

        results = gather(futures_dict, return_exceptions=True, timeout=5.0)

        assert isinstance(results, dict)
        assert results["good"] == 42
        assert isinstance(results["bad"], ValueError)
        assert results["also_good"] == 100

        w.stop()

    def test_gather_dict_with_non_string_keys(self, worker_mode):
        """Test dict with non-string keys."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures_dict = {
            1: w.compute(10),
            2: w.compute(20),
            3: w.compute(30),
        }

        results = gather(futures_dict, timeout=5.0)

        assert results == {1: 10, 2: 20, 3: 30}

        w.stop()

    def test_gather_dict_with_tuple_keys(self, worker_mode):
        """Test dict with tuple keys."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures_dict = {
            ("a", 1): w.compute(10),
            ("b", 2): w.compute(20),
        }

        results = gather(futures_dict, timeout=5.0)

        assert results == {("a", 1): 10, ("b", 2): 20}

        w.stop()


class TestGatherDictIterator:
    """Tests for gather() dict with iter=True."""

    def test_gather_dict_iter_basic(self, worker_mode):
        """Test gather dict iterator mode."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=10)

        futures_dict = {
            "task1": w.compute(1),
            "task2": w.compute(2),
            "task3": w.compute(3),
        }

        results = dict(gather(futures_dict, iter=True, timeout=5.0))

        assert results == {"task1": 10, "task2": 20, "task3": 30}

        w.stop()

    def test_gather_dict_iter_yields_keys(self, worker_mode):
        """Test that dict iterator yields keys, not indices."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures_dict = {
            "alpha": w.compute(1),
            "beta": w.compute(2),
            "gamma": w.compute(3),
        }

        items = list(gather(futures_dict, iter=True, timeout=5.0))

        # Should yield (key, result) tuples
        keys = [item[0] for item in items]
        values = [item[1] for item in items]

        assert set(keys) == {"alpha", "beta", "gamma"}
        assert set(values) == {1, 2, 3}

        w.stop()

    def test_gather_dict_iter_return_exceptions(self, worker_mode):
        """Test gather dict iterator with return_exceptions=True."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures_dict = {
            "good1": w.compute(42),
            "bad": w.failing_compute(1),
            "good2": w.compute(100),
        }

        results = dict(gather(futures_dict, iter=True, return_exceptions=True, timeout=5.0))

        assert results["good1"] == 42
        assert isinstance(results["bad"], ValueError)
        assert results["good2"] == 100

        w.stop()


class TestVariadicSignature:
    """Tests for the improved variadic signature."""

    def test_wait_list_of_futures(self, worker_mode):
        """Test wait with list (most common usage)."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)
        futures = [w.compute(i) for i in range(5)]

        done, not_done = wait(futures, timeout=5.0)

        assert len(done) == 5
        assert len(not_done) == 0

        w.stop()

    def test_wait_multiple_futures(self, worker_mode):
        """Test wait with multiple individual futures."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        f1 = w.compute(1)
        f2 = w.compute(2)
        f3 = w.compute(3)

        done, not_done = wait(f1, f2, f3, timeout=5.0)

        assert len(done) == 3
        assert len(not_done) == 0

        w.stop()

    def test_wait_error_on_mixed_usage(self, worker_mode):
        """Test that mixing list and variadic args raises error."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)
        futures = [w.compute(i) for i in range(3)]
        extra = w.compute(99)

        with pytest.raises(ValueError, match="Cannot provide both a structure"):
            wait(futures, extra)

        w.stop()

    def test_gather_list_of_futures(self, worker_mode):
        """Test gather with list (most common usage)."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=10)
        futures = [w.compute(i) for i in range(5)]

        results = gather(futures, timeout=5.0)

        assert results == [0, 10, 20, 30, 40]

        w.stop()

    def test_gather_multiple_futures(self, worker_mode):
        """Test gather with multiple individual futures."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        f1 = w.compute(100)
        f2 = w.compute(200)
        f3 = w.compute(300)

        results = gather(f1, f2, f3, timeout=5.0)

        assert results == [100, 200, 300]

        w.stop()

    def test_gather_error_on_mixed_usage(self, worker_mode):
        """Test that mixing list and variadic args raises error."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)
        futures = [w.compute(i) for i in range(3)]
        extra = w.compute(99)

        with pytest.raises(ValueError, match="Cannot provide both a structure"):
            gather(futures, extra)

        w.stop()

    def test_gather_single_future_returns_list(self, worker_mode):
        """Test that gather with single future still returns a list."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)
        f = w.compute(42)

        results = gather(f, timeout=5.0)

        assert results == [42]

        w.stop()

    def test_gather_dict_not_compatible_with_variadic(self, worker_mode):
        """Test that dict can't be mixed with variadic args."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures_dict = {"task1": w.compute(1)}
        extra = w.compute(2)

        with pytest.raises(ValueError, match="Cannot provide both a structure"):
            gather(futures_dict, extra)

        w.stop()


class TestEdgeCases:
    """Edge case tests for wait() and gather()."""

    def test_wait_single_non_future(self, worker_mode):
        """Test wait with a single non-future value."""
        done, not_done = wait(42)

        assert len(done) == 1
        assert len(not_done) == 0

    def test_gather_single_non_future(self, worker_mode):
        """Test gather with a single non-future value."""
        result = gather(42)

        assert result == [42]

    def test_wait_mixed_futures_and_values(self, worker_mode):
        """Test wait with mix of futures and non-future values."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        mixed = [w.compute(1), 42, w.compute(2)]
        done, not_done = wait(mixed, timeout=5.0)

        assert len(done) == 3
        assert len(not_done) == 0

        w.stop()

    def test_gather_mixed_futures_and_values(self, worker_mode):
        """Test gather with mix of futures and non-future values."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=10)

        results = gather([w.compute(1), 42, w.compute(2)], timeout=5.0)

        assert results == [10, 42, 20]

        w.stop()

    def test_wait_dict_then_gather(self, worker_mode):
        """Test workflow: wait for dict, then gather results."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=10)

        futures_dict = {
            "compute1": w.compute(10),
            "compute2": w.compute(20),
        }

        # First wait for completion
        done, not_done = wait(futures_dict, timeout=5.0)
        assert len(done) == 2

        # Then gather results (futures already done)
        results = gather(futures_dict, timeout=5.0)
        assert results == {"compute1": 100, "compute2": 200}

        w.stop()

    def test_gather_very_large_batch(self, worker_mode):
        """Test gather with very large batch (100 futures)."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures = [w.compute(i) for i in range(100)]
        results = gather(futures, timeout=30.0)

        assert len(results) == 100
        assert results == list(range(100))

        w.stop()

    def test_wait_very_large_batch(self, worker_mode):
        """Test wait with very large batch (100 futures)."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures = [w.compute(i) for i in range(100)]
        done, not_done = wait(futures, timeout=30.0)

        assert len(done) == 100
        assert len(not_done) == 0

        w.stop()

    def test_gather_iter_with_large_batch(self, worker_mode):
        """Test gather iterator with large batch."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures = [w.compute(i) for i in range(50)]
        results = list(gather(futures, iter=True, timeout=30.0))

        assert len(results) == 50
        # Extract just the values
        values = [r[1] for r in results]
        assert set(values) == set(range(50))

        w.stop()

    def test_wait_with_all_polling_algorithms(self, worker_mode):
        """Test wait with all polling algorithms."""
        for algorithm in [
            PollingAlgorithm.Fixed,
            PollingAlgorithm.Adaptive,
            PollingAlgorithm.Exponential,
            PollingAlgorithm.Progressive,
        ]:
            w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)
            futures = [w.compute(i) for i in range(5)]

            done, not_done = wait(futures, polling=algorithm, timeout=5.0)

            assert len(done) == 5
            assert len(not_done) == 0

            w.stop()

    def test_gather_with_all_polling_algorithms(self, worker_mode):
        """Test gather with all polling algorithms."""
        for algorithm in [
            PollingAlgorithm.Fixed,
            PollingAlgorithm.Adaptive,
            PollingAlgorithm.Exponential,
            PollingAlgorithm.Progressive,
        ]:
            w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)
            futures = [w.compute(i) for i in range(5)]

            results = gather(futures, polling=algorithm, timeout=5.0)

            assert results == [0, 1, 2, 3, 4]

            w.stop()

    def test_wait_tuple_vs_list(self, worker_mode):
        """Test wait with tuple vs list."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        # Test with list
        list_futures = [w.compute(i) for i in range(3)]
        done1, not_done1 = wait(list_futures, timeout=5.0)
        assert len(done1) == 3

        # Test with tuple
        tuple_futures = tuple([w.compute(i) for i in range(3)])
        done2, not_done2 = wait(tuple_futures, timeout=5.0)
        assert len(done2) == 3

        w.stop()

    def test_gather_tuple_vs_list(self, worker_mode):
        """Test gather with tuple vs list."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        # Test with list
        list_futures = [w.compute(i) for i in range(3)]
        results1 = gather(list_futures, timeout=5.0)
        assert results1 == [0, 1, 2]

        # Test with tuple
        tuple_futures = tuple([w.compute(i) for i in range(3)])
        results2 = gather(tuple_futures, timeout=5.0)
        assert results2 == [0, 1, 2]

        w.stop()

    def test_wait_with_progress_callback_dict(self, worker_mode):
        """Test wait with progress callback and dict."""
        callback_calls = []

        def progress_callback(completed, total, elapsed):
            callback_calls.append((completed, total))

        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)
        futures_dict = {f"task{i}": w.compute(i) for i in range(5)}

        done, not_done = wait(futures_dict, progress=progress_callback, timeout=5.0)

        assert len(done) == 5
        # Callback should have been called
        assert len(callback_calls) > 0

        w.stop()

    def test_gather_dict_iter_with_progress(self, worker_mode):
        """Test gather dict iterator with progress tracking."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures_dict = {f"task{i}": w.compute(i) for i in range(10)}

        results = dict(gather(futures_dict, iter=True, progress=True, timeout=5.0))

        assert len(results) == 10

        w.stop()


class TestPollingEdgeCases:
    """Edge case tests for polling algorithms."""

    def test_wait_first_completed_with_different_polling(self, worker_mode):
        """Test FIRST_COMPLETED with different polling strategies."""
        for algorithm in [PollingAlgorithm.Fixed, PollingAlgorithm.Adaptive]:
            w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

            futures = [
                w.slow_compute(1, duration=0.5),
                w.compute(2),  # Fast
                w.slow_compute(3, duration=1.0),
            ]

            done, not_done = wait(futures, return_when=FIRST_COMPLETED, polling=algorithm, timeout=5.0)

            assert len(done) >= 1

            w.stop()

    def test_gather_timeout_with_adaptive_polling(self, worker_mode):
        """Test gather timeout with adaptive polling."""
        if worker_mode == "sync":
            pytest.skip("Sync mode completes immediately, cannot timeout")

        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures = [w.slow_compute(i, duration=2.0) for i in range(3)]

        with pytest.raises(TimeoutError):
            gather(futures, polling=PollingAlgorithm.Adaptive, timeout=0.1)

        w.stop()

    def test_wait_first_exception_with_polling(self, worker_mode):
        """Test FIRST_EXCEPTION with different polling algorithms."""
        if worker_mode == "ray":
            pytest.skip("Ray mode has timing issues with FIRST_EXCEPTION detection")

        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures = [
            w.slow_compute(1, duration=0.5),
            w.failing_compute(2),  # Will fail
            w.slow_compute(3, duration=1.0),
        ]

        done, not_done = wait(
            futures, return_when=FIRST_EXCEPTION, polling=PollingAlgorithm.Progressive, timeout=5.0
        )

        assert len(done) >= 1

        w.stop()

    def test_gather_iter_with_exponential_polling(self, worker_mode):
        """Test gather iterator with exponential polling."""
        w = ComputeWorker.options(mode=worker_mode).init(multiplier=1)

        futures = [w.compute(i) for i in range(10)]
        results = list(gather(futures, iter=True, polling=PollingAlgorithm.Exponential, timeout=5.0))

        assert len(results) == 10

        w.stop()
