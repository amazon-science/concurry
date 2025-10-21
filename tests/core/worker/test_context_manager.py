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
        """Test context manager with sync worker.

        This test:
        1. Creates a sync worker using context manager (blocking mode)
        2. Verifies worker is active (not stopped) inside context
        3. Calls process method and verifies result (20)
        4. Exits context
        5. Verifies worker is automatically stopped after context exit
        """
        with SimpleWorker.options(mode="sync", blocking=True).init() as worker:
            # Worker should be active inside context
            assert worker._stopped is False
            result = worker.process(10)
            assert result == 20

        # Worker should be stopped after context exits
        assert worker._stopped is True

    def test_worker_context_manager_thread(self):
        """Test context manager with thread worker.

        This test:
        1. Creates a thread worker using context manager (blocking mode)
        2. Verifies worker is active (not stopped) inside context
        3. Calls process method and verifies result (20)
        4. Exits context
        5. Verifies worker is automatically stopped after context exit
        """
        with SimpleWorker.options(mode="thread", blocking=True).init() as worker:
            assert worker._stopped is False
            result = worker.process(10)
            assert result == 20

        assert worker._stopped is True

    def test_worker_context_manager_process(self):
        """Test context manager with process worker.

        This test:
        1. Creates a process worker using context manager (blocking mode)
        2. Verifies worker is active (not stopped) inside context
        3. Calls process method and verifies result (20)
        4. Exits context
        5. Verifies worker is automatically stopped after context exit
        """
        with SimpleWorker.options(mode="process", blocking=True).init() as worker:
            assert worker._stopped is False
            result = worker.process(10)
            assert result == 20

        assert worker._stopped is True

    def test_worker_context_manager_asyncio(self):
        """Test context manager with asyncio worker.

        This test:
        1. Creates an asyncio worker using context manager (blocking mode)
        2. Verifies worker is active (not stopped) inside context
        3. Calls process method and verifies result (20)
        4. Exits context
        5. Verifies worker is automatically stopped after context exit
        """
        with SimpleWorker.options(mode="asyncio", blocking=True).init() as worker:
            assert worker._stopped is False
            result = worker.process(10)
            assert result == 20

        assert worker._stopped is True

    def test_worker_context_manager_with_exception(self):
        """Test that worker is stopped even when exception occurs.

        This test:
        1. Defines an ErrorWorker that raises an exception
        2. Creates worker using context manager (blocking mode)
        3. Verifies worker is active inside context
        4. Calls method that raises ValueError
        5. Catches the exception (context exits with exception)
        6. Verifies worker is still automatically stopped despite the exception
        """

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
        """Test context manager with non-blocking worker.

        This test:
        1. Creates a thread worker using context manager (non-blocking mode)
        2. Verifies worker is active inside context
        3. Calls process method which returns a Future
        4. Waits for and verifies the result (20)
        5. Exits context
        6. Verifies worker is automatically stopped after context exit
        """
        with SimpleWorker.options(mode="thread").init() as worker:
            assert worker._stopped is False
            future = worker.process(10)
            result = future.result()
            assert result == 20

        assert worker._stopped is True

    def test_worker_context_manager_multiple_calls(self):
        """Test context manager with multiple method calls.

        This test:
        1. Creates a sync worker using context manager (blocking mode)
        2. Makes 5 method calls to process different values
        3. Verifies all results are correct [0, 2, 4, 6, 8]
        4. Verifies worker maintained state (count=5)
        5. Exits context
        6. Verifies worker is automatically stopped after context exit
        """
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
        """Test context manager with thread pool.

        This test:
        1. Creates a thread pool (3 workers) using context manager (blocking mode)
        2. Verifies pool is active (not stopped) inside context
        3. Calls process method and verifies result (20)
        4. Exits context
        5. Verifies pool is automatically stopped after context exit
        """
        with SimpleWorker.options(mode="thread", max_workers=3, blocking=True).init() as pool:
            assert pool._stopped is False
            result = pool.process(10)
            assert result == 20

        assert pool._stopped is True

    def test_pool_context_manager_process(self):
        """Test context manager with process pool.

        This test:
        1. Creates a process pool (2 workers) using context manager (blocking mode)
        2. Verifies pool is active (not stopped) inside context
        3. Calls process method and verifies result (20)
        4. Exits context
        5. Verifies pool is automatically stopped after context exit
        """
        with SimpleWorker.options(mode="process", max_workers=2, blocking=True).init() as pool:
            assert pool._stopped is False
            result = pool.process(10)
            assert result == 20

        assert pool._stopped is True

    def test_pool_context_manager_ray(self):
        """Test context manager with Ray pool.

        This test:
        1. Creates a Ray pool (2 workers) using context manager (blocking mode)
        2. Verifies pool is active (not stopped) inside context
        3. Calls process method and verifies result (20)
        4. Exits context
        5. Verifies pool is automatically stopped after context exit
        """
        with SimpleWorker.options(mode="ray", max_workers=2, blocking=True).init() as pool:
            assert pool._stopped is False
            result = pool.process(10)
            assert result == 20

        assert pool._stopped is True

    def test_pool_context_manager_with_exception(self):
        """Test that pool is stopped even when exception occurs.

        This test:
        1. Defines an ErrorWorker that raises an exception
        2. Creates a thread pool using context manager (blocking mode)
        3. Verifies pool is active inside context
        4. Calls method that raises ValueError
        5. Catches the exception (context exits with exception)
        6. Verifies pool is still automatically stopped despite the exception
        """

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
        """Test context manager with non-blocking pool.

        This test:
        1. Creates a thread pool (3 workers) using context manager (non-blocking mode)
        2. Verifies pool is active inside context
        3. Calls process method which returns a Future
        4. Waits for and verifies the result (20)
        5. Exits context
        6. Verifies pool is automatically stopped after context exit
        """
        with SimpleWorker.options(mode="thread", max_workers=3).init() as pool:
            assert pool._stopped is False
            future = pool.process(10)
            result = future.result()
            assert result == 20

        assert pool._stopped is True

    def test_pool_context_manager_multiple_calls(self):
        """Test context manager with multiple method calls to pool.

        This test:
        1. Creates a thread pool (3 workers) using context manager (blocking mode)
        2. Makes 10 method calls to process different values
        3. Verifies all results are correct [0, 2, 4, 6, ..., 18]
        4. Exits context
        5. Verifies pool is automatically stopped after context exit
        """
        with SimpleWorker.options(mode="thread", max_workers=3, blocking=True).init() as pool:
            results = []
            for i in range(10):
                results.append(pool.process(i))

            assert results == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

        assert pool._stopped is True

    def test_pool_context_manager_on_demand(self):
        """Test context manager with on-demand pool.

        This test:
        1. Creates a thread pool with on-demand workers using context manager
        2. Verifies pool is active inside context
        3. Calls process method which creates a worker and returns a Future
        4. Waits for and verifies the result (20)
        5. Exits context
        6. Verifies pool is automatically stopped after context exit
        """
        with SimpleWorker.options(mode="thread", max_workers=5, on_demand=True).init() as pool:
            assert pool._stopped is False
            future = pool.process(10)
            result = future.result()
            assert result == 20

        assert pool._stopped is True


class TestContextManagerComparison:
    """Test comparing manual stop vs context manager."""

    def test_manual_stop_vs_context_manager(self):
        """Compare manual stop with context manager.

        This test:
        1. Creates a worker with manual stop pattern
        2. Verifies worker is active, calls method, then manually stops
        3. Creates another worker using context manager pattern
        4. Verifies worker is active, calls method, exits context
        5. Compares both approaches - context manager is cleaner and automatic
        """
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
        """Test nested context managers with multiple workers.

        This test:
        1. Creates first worker (worker1) using outer context manager
        2. Creates second worker (worker2) using nested context manager
        3. Calls methods on both workers and verifies results
        4. Verifies both workers are active while both contexts are active
        5. Exits inner context - verifies worker2 is stopped, worker1 still active
        6. Exits outer context - verifies worker1 is now also stopped
        """
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
