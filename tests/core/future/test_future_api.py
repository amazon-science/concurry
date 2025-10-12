"""Tests for BaseFuture API compatibility with concurrent.futures.Future."""

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from concurry.core.future import (
    AsyncioFuture,
    ConcurrentFuture,
    SyncFuture,
)


class TestRunningMethod:
    """Test the running() method across all future types."""

    def test_sync_future_running(self):
        """SyncFuture is never running (always completed at creation)."""
        future = SyncFuture(result_value=42)
        assert future.running() is False
        assert future.done() is True

    def test_concurrent_future_running(self):
        """Test ConcurrentFuture running() method."""
        with ThreadPoolExecutor(max_workers=1) as executor:
            # Create a future that takes time to execute
            def slow_task():
                time.sleep(0.2)
                return 42

            py_future = executor.submit(slow_task)
            future = ConcurrentFuture(future=py_future)

            # Check if running (might be running or done depending on timing)
            running_status = future.running()
            assert isinstance(running_status, bool)

            # Wait for completion
            result = future.result(timeout=1)
            assert result == 42
            assert future.running() is False
            assert future.done() is True

    def test_asyncio_future_running(self):
        """Test AsyncioFuture running() method."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async_future = loop.create_future()
            future = AsyncioFuture(future=async_future)

            # Future is not done yet
            assert future.done() is False
            # running() returns True if not done and not cancelled
            running_status = future.running()
            assert isinstance(running_status, bool)

            # Complete the future
            async_future.set_result(42)

            assert future.result(timeout=1) == 42
            assert future.running() is False
            assert future.done() is True
        finally:
            loop.close()


class TestImmutableMethods:
    """Test that set_* methods raise NotImplementedError for immutable futures."""

    def test_sync_future_set_result_raises(self):
        """SyncFuture.set_result() should raise NotImplementedError."""
        future = SyncFuture(result_value=42)

        with pytest.raises(NotImplementedError, match="immutable"):
            future.set_result(100)

    def test_sync_future_set_exception_raises(self):
        """SyncFuture.set_exception() should raise NotImplementedError."""
        future = SyncFuture(result_value=42)

        with pytest.raises(NotImplementedError, match="immutable"):
            future.set_exception(ValueError("test"))

    def test_sync_future_set_running_or_notify_cancel_raises(self):
        """SyncFuture.set_running_or_notify_cancel() should raise NotImplementedError."""
        future = SyncFuture(result_value=42)

        with pytest.raises(NotImplementedError, match="manages state internally"):
            future.set_running_or_notify_cancel()

    def test_concurrent_future_set_result_raises(self):
        """ConcurrentFuture.set_result() should raise NotImplementedError."""
        with ThreadPoolExecutor() as executor:
            py_future = executor.submit(lambda: 42)
            future = ConcurrentFuture(future=py_future)

            with pytest.raises(NotImplementedError, match="immutable"):
                future.set_result(100)

    def test_concurrent_future_set_exception_raises(self):
        """ConcurrentFuture.set_exception() should raise NotImplementedError."""
        with ThreadPoolExecutor() as executor:
            py_future = executor.submit(lambda: 42)
            future = ConcurrentFuture(future=py_future)

            with pytest.raises(NotImplementedError, match="immutable"):
                future.set_exception(ValueError("test"))

    def test_asyncio_future_set_result_raises(self):
        """AsyncioFuture.set_result() should raise NotImplementedError."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async_future = loop.create_future()
            future = AsyncioFuture(future=async_future)

            with pytest.raises(NotImplementedError, match="immutable"):
                future.set_result(100)
        finally:
            loop.close()

    def test_asyncio_future_set_exception_raises(self):
        """AsyncioFuture.set_exception() should raise NotImplementedError."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async_future = loop.create_future()
            future = AsyncioFuture(future=async_future)

            with pytest.raises(NotImplementedError, match="immutable"):
                future.set_exception(ValueError("test"))
        finally:
            loop.close()


class TestThreadSafety:
    """Test thread-safety of future operations."""

    def test_concurrent_future_thread_safety(self):
        """Test that ConcurrentFuture operations are thread-safe."""
        results = []
        errors = []

        def access_future(future, index):
            try:
                # Try to access the future from multiple threads
                done_status = future.done()
                running_status = future.running()
                if done_status:
                    result = future.result(timeout=1)
                    results.append((index, result))
            except Exception as e:
                errors.append((index, e))

        with ThreadPoolExecutor(max_workers=1) as executor:
            py_future = executor.submit(lambda: 42)
            future = ConcurrentFuture(future=py_future)

            # Access from multiple threads
            threads = []
            for i in range(10):
                t = threading.Thread(target=access_future, args=(future, i))
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            # Should have no errors
            assert len(errors) == 0
            # Should have multiple successful accesses
            assert len(results) > 0

    def test_asyncio_future_thread_safety(self):
        """Test that AsyncioFuture operations are thread-safe."""
        results = []
        errors = []

        def access_future(future, index):
            try:
                # Try to access the future from multiple threads
                done_status = future.done()
                running_status = future.running()
                cancelled_status = future.cancelled()
                results.append((index, done_status, running_status, cancelled_status))
            except Exception as e:
                errors.append((index, e))

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async_future = loop.create_future()
            async_future.set_result(42)
            future = AsyncioFuture(future=async_future)

            # Access from multiple threads
            threads = []
            for i in range(10):
                t = threading.Thread(target=access_future, args=(future, i))
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            # Should have no errors
            assert len(errors) == 0
            # Should have 10 successful accesses
            assert len(results) == 10
        finally:
            loop.close()

    def test_sync_future_thread_safety(self):
        """Test that SyncFuture is thread-safe (due to immutability)."""
        results = []
        errors = []

        def access_future(future, index):
            try:
                result = future.result()
                done = future.done()
                running = future.running()
                results.append((index, result, done, running))
            except Exception as e:
                errors.append((index, e))

        future = SyncFuture(result_value=42)

        # Access from multiple threads
        threads = []
        for i in range(10):
            t = threading.Thread(target=access_future, args=(future, i))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        # Should have no errors
        assert len(errors) == 0
        # Should have 10 successful accesses
        assert len(results) == 10
        # All results should be identical (42, True, False)
        for index, result, done, running in results:
            assert result == 42
            assert done is True
            assert running is False


class TestAPICompatibility:
    """Test that BaseFuture API matches concurrent.futures.Future."""

    def test_api_methods_exist(self):
        """Verify all concurrent.futures.Future methods exist on BaseFuture."""
        future = SyncFuture(result_value=42)

        # Methods from concurrent.futures.Future
        assert hasattr(future, "cancel")
        assert hasattr(future, "cancelled")
        assert hasattr(future, "running")
        assert hasattr(future, "done")
        assert hasattr(future, "result")
        assert hasattr(future, "exception")
        assert hasattr(future, "add_done_callback")
        assert hasattr(future, "set_result")
        assert hasattr(future, "set_exception")
        assert hasattr(future, "set_running_or_notify_cancel")

        # All should be callable
        assert callable(future.cancel)
        assert callable(future.cancelled)
        assert callable(future.running)
        assert callable(future.done)
        assert callable(future.result)
        assert callable(future.exception)
        assert callable(future.add_done_callback)
        assert callable(future.set_result)
        assert callable(future.set_exception)
        assert callable(future.set_running_or_notify_cancel)

    def test_api_signatures_match(self):
        """Verify method signatures match concurrent.futures.Future where applicable."""
        with ThreadPoolExecutor() as executor:
            py_future = executor.submit(lambda: 42)
            our_future = ConcurrentFuture(future=py_future)

            # Test that methods can be called with same arguments
            our_future.cancel()
            our_future.cancelled()
            our_future.running()
            our_future.done()
            our_future.result(timeout=None)
            our_future.exception(timeout=None)
            our_future.add_done_callback(lambda f: None)

            # These should raise NotImplementedError but accept the same arguments
            with pytest.raises(NotImplementedError):
                our_future.set_result(42)

            with pytest.raises(NotImplementedError):
                our_future.set_exception(ValueError("test"))

            with pytest.raises(NotImplementedError):
                our_future.set_running_or_notify_cancel()
