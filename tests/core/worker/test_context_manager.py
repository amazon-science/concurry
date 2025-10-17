"""Tests for Worker and WorkerPool context manager behavior."""

import pytest

from concurry import Worker


class SimpleWorker(Worker):
    """Simple worker for testing."""

    def __init__(self):
        self.count = 0

    def process(self, x: int) -> int:
        """Process a value."""
        self.count += 1
        return x * 2

    def get_count(self) -> int:
        """Get count."""
        return self.count


class TestWorkerContextManager:
    """Test Worker context manager behavior."""

    def test_worker_context_manager_sync(self):
        """Test context manager with sync worker."""
        with SimpleWorker.options(mode="sync", blocking=True).init() as worker:
            # Worker should be active inside context
            assert worker._stopped is False
            result = worker.process(10)
            assert result == 20

        # Worker should be stopped after context exits
        assert worker._stopped is True

    def test_worker_context_manager_thread(self):
        """Test context manager with thread worker."""
        with SimpleWorker.options(mode="thread", blocking=True).init() as worker:
            assert worker._stopped is False
            result = worker.process(10)
            assert result == 20

        assert worker._stopped is True

    def test_worker_context_manager_process(self):
        """Test context manager with process worker."""
        with SimpleWorker.options(mode="process", blocking=True).init() as worker:
            assert worker._stopped is False
            result = worker.process(10)
            assert result == 20

        assert worker._stopped is True

    def test_worker_context_manager_asyncio(self):
        """Test context manager with asyncio worker."""
        with SimpleWorker.options(mode="asyncio", blocking=True).init() as worker:
            assert worker._stopped is False
            result = worker.process(10)
            assert result == 20

        assert worker._stopped is True

    def test_worker_context_manager_with_exception(self):
        """Test that worker is stopped even when exception occurs."""

        class ErrorWorker(Worker):
            def error_method(self):
                raise ValueError("Test error")

        with pytest.raises(ValueError, match="Test error"):
            with ErrorWorker.options(mode="sync", blocking=True).init() as worker:
                assert worker._stopped is False
                worker.error_method()

        # Worker should still be stopped after exception
        assert worker._stopped is True

    def test_worker_context_manager_non_blocking(self):
        """Test context manager with non-blocking worker."""
        with SimpleWorker.options(mode="thread").init() as worker:
            assert worker._stopped is False
            future = worker.process(10)
            result = future.result()
            assert result == 20

        assert worker._stopped is True

    def test_worker_context_manager_multiple_calls(self):
        """Test context manager with multiple method calls."""
        with SimpleWorker.options(mode="sync", blocking=True).init() as worker:
            results = []
            for i in range(5):
                results.append(worker.process(i))

            assert results == [0, 2, 4, 6, 8]
            count = worker.get_count()
            assert count == 5

        assert worker._stopped is True


class TestWorkerPoolContextManager:
    """Test WorkerPool context manager behavior."""

    def test_pool_context_manager_thread(self):
        """Test context manager with thread pool."""
        with SimpleWorker.options(mode="thread", max_workers=3, blocking=True).init() as pool:
            assert pool._stopped is False
            result = pool.process(10)
            assert result == 20

        assert pool._stopped is True

    def test_pool_context_manager_process(self):
        """Test context manager with process pool."""
        with SimpleWorker.options(mode="process", max_workers=2, blocking=True).init() as pool:
            assert pool._stopped is False
            result = pool.process(10)
            assert result == 20

        assert pool._stopped is True

    def test_pool_context_manager_ray(self):
        """Test context manager with Ray pool."""
        with SimpleWorker.options(mode="ray", max_workers=2, blocking=True).init() as pool:
            assert pool._stopped is False
            result = pool.process(10)
            assert result == 20

        assert pool._stopped is True

    def test_pool_context_manager_with_exception(self):
        """Test that pool is stopped even when exception occurs."""

        class ErrorWorker(Worker):
            def error_method(self):
                raise ValueError("Test error")

        with pytest.raises(ValueError, match="Test error"):
            with ErrorWorker.options(mode="thread", max_workers=2, blocking=True).init() as pool:
                assert pool._stopped is False
                pool.error_method()

        # Pool should still be stopped after exception
        assert pool._stopped is True

    def test_pool_context_manager_non_blocking(self):
        """Test context manager with non-blocking pool."""
        with SimpleWorker.options(mode="thread", max_workers=3).init() as pool:
            assert pool._stopped is False
            future = pool.process(10)
            result = future.result()
            assert result == 20

        assert pool._stopped is True

    def test_pool_context_manager_multiple_calls(self):
        """Test context manager with multiple method calls to pool."""
        with SimpleWorker.options(mode="thread", max_workers=3, blocking=True).init() as pool:
            results = []
            for i in range(10):
                results.append(pool.process(i))

            assert results == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

        assert pool._stopped is True

    def test_pool_context_manager_on_demand(self):
        """Test context manager with on-demand pool."""
        with SimpleWorker.options(mode="thread", max_workers=5, on_demand=True).init() as pool:
            assert pool._stopped is False
            future = pool.process(10)
            result = future.result()
            assert result == 20

        assert pool._stopped is True


class TestContextManagerComparison:
    """Test comparing manual stop vs context manager."""

    def test_manual_stop_vs_context_manager(self):
        """Compare manual stop with context manager."""
        # Manual stop
        worker1 = SimpleWorker.options(mode="sync", blocking=True).init()
        worker1.process(10)
        assert worker1._stopped is False
        worker1.stop()
        assert worker1._stopped is True

        # Context manager (cleaner)
        with SimpleWorker.options(mode="sync", blocking=True).init() as worker2:
            worker2.process(10)
            assert worker2._stopped is False
        assert worker2._stopped is True

    def test_nested_context_managers(self):
        """Test nested context managers with multiple workers."""
        with SimpleWorker.options(mode="thread", blocking=True).init() as worker1:
            with SimpleWorker.options(mode="thread", blocking=True).init() as worker2:
                result1 = worker1.process(5)
                result2 = worker2.process(10)
                assert result1 == 10
                assert result2 == 20
                assert worker1._stopped is False
                assert worker2._stopped is False

            # worker2 should be stopped
            assert worker2._stopped is True
            assert worker1._stopped is False

        # worker1 should be stopped
        assert worker1._stopped is True
