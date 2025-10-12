"""Tests for concurry.core.future module."""

import asyncio
import concurrent.futures
import time
from unittest.mock import Mock

import pytest

from concurry.core.future import (
    AsyncioFuture,
    ConcurrentFuture,
    SyncFuture,
    wrap_future,
)


class TestSyncFuture:
    """Test SyncFuture class."""

    def test_sync_future_with_result(self):
        """Test SyncFuture with a successful result."""
        future = SyncFuture(result_value=42)

        assert future.result() == 42
        assert future.done()
        assert not future.cancelled()
        assert future.exception() is None
        assert not future.cancel()  # Already done, cannot cancel

    def test_sync_future_with_exception(self):
        """Test SyncFuture with an exception."""
        exc = ValueError("test error")
        future = SyncFuture(exception_value=exc)

        with pytest.raises(ValueError, match="test error"):
            future.result()

        assert future.done()
        assert not future.cancelled()
        assert future.exception() is exc
        assert not future.cancel()  # Already done, cannot cancel

    def test_sync_future_callbacks(self):
        """Test SyncFuture callback functionality."""
        future = SyncFuture(result_value="test")
        callback_called = []

        def callback(fut):
            callback_called.append(fut.result())

        future.add_done_callback(callback)
        assert callback_called == ["test"]  # Called immediately since already done

    def test_sync_future_awaitable(self):
        """Test SyncFuture can be awaited."""

        async def test_await():
            future = SyncFuture(result_value="awaited")
            result = await future
            return result

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(test_await())
            assert result == "awaited"
        finally:
            loop.close()

    def test_sync_future_initialization_performance(self):
        """Test that SyncFuture initialization is fast."""
        import time

        # Warm up (to avoid any JIT compilation overhead)
        for _ in range(10):
            SyncFuture(result_value=42)

        # Measure time for multiple initializations
        repeats: int = 10_000
        start_time = time.perf_counter()
        for _ in range(repeats):
            SyncFuture(result_value=42)
        end_time = time.perf_counter()

        # Calculate average time per initialization
        total_time = end_time - start_time
        avg_time_per_init = total_time / repeats

        assert avg_time_per_init < 3e-6, (
            f"SyncFuture initialization too slow: {avg_time_per_init:.2e} seconds per init"
        )

        # Verify the future still works correctly
        future = SyncFuture(result_value=42)
        assert future.result() == 42
        assert future.done() is True
        assert future.cancelled() is False


class TestConcurrentFuture:
    """Test ConcurrentFuture class."""

    def test_concurrent_future_with_result(self):
        """Test ConcurrentFuture wrapping concurrent.futures.Future."""
        cf_future = concurrent.futures.Future()
        cf_future.set_result(123)

        future = ConcurrentFuture(future=cf_future)

        assert future.result() == 123
        assert future.done()
        assert not future.cancelled()
        assert future.exception() is None

    def test_concurrent_future_with_exception(self):
        """Test ConcurrentFuture with exception."""
        cf_future = concurrent.futures.Future()
        exc = RuntimeError("concurrent error")
        cf_future.set_exception(exc)

        future = ConcurrentFuture(future=cf_future)

        with pytest.raises(RuntimeError, match="concurrent error"):
            future.result()

        assert future.done()
        assert not future.cancelled()
        assert future.exception() is exc

    def test_concurrent_future_timeout(self):
        """Test ConcurrentFuture with timeout."""
        cf_future = concurrent.futures.Future()
        future = ConcurrentFuture(future=cf_future)

        with pytest.raises(concurrent.futures.TimeoutError):
            future.result(timeout=0.1)

    def test_concurrent_future_cancel(self):
        """Test ConcurrentFuture cancellation."""
        cf_future = concurrent.futures.Future()
        future = ConcurrentFuture(future=cf_future)

        assert future.cancel()
        assert future.cancelled()

    def test_concurrent_future_callbacks(self):
        """Test ConcurrentFuture callback functionality."""
        cf_future = concurrent.futures.Future()
        future = ConcurrentFuture(future=cf_future)
        callback_results = []

        def callback(fut):
            callback_results.append("called")

        future.add_done_callback(callback)
        cf_future.set_result("done")

        # Give a moment for callback to be called
        time.sleep(0.01)
        assert callback_results == ["called"]


class TestAsyncioFuture:
    """Test AsyncioFuture class."""

    def test_asyncio_future_with_result(self):
        """Test AsyncioFuture wrapping asyncio.Future."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            asyncio_future = loop.create_future()
            asyncio_future.set_result(99)

            future = AsyncioFuture(future=asyncio_future)

            assert future.result() == 99
            assert future.done()
            assert not future.cancelled()
            assert future.exception() is None
        finally:
            loop.close()

    def test_asyncio_future_with_exception(self):
        """Test AsyncioFuture with exception."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            asyncio_future = loop.create_future()
            exc = ValueError("asyncio error")
            asyncio_future.set_exception(exc)

            future = AsyncioFuture(future=asyncio_future)

            with pytest.raises(ValueError, match="asyncio error"):
                future.result()

            assert future.done()
            assert not future.cancelled()
            assert future.exception() is exc
        finally:
            loop.close()

    def test_asyncio_future_timeout(self):
        """Test AsyncioFuture with timeout."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            asyncio_future = loop.create_future()
            future = AsyncioFuture(future=asyncio_future)

            with pytest.raises(TimeoutError, match="Future did not complete within timeout"):
                future.result(timeout=0.1)
        finally:
            loop.close()

    def test_asyncio_future_cancel(self):
        """Test AsyncioFuture cancellation."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            asyncio_future = loop.create_future()
            future = AsyncioFuture(future=asyncio_future)

            assert future.cancel()
            assert future.cancelled()
        finally:
            loop.close()

    def test_asyncio_future_callbacks(self):
        """Test AsyncioFuture callback functionality."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            asyncio_future = loop.create_future()
            future = AsyncioFuture(future=asyncio_future)
            callback_results = []

            def callback(fut):
                callback_results.append(fut.result())

            future.add_done_callback(callback)
            asyncio_future.set_result("asyncio_done")

            # Give the event loop a chance to run the callback
            loop.run_until_complete(asyncio.sleep(0))

            # The callback should be called
            assert callback_results == ["asyncio_done"]
        finally:
            loop.close()


class TestWrapFuture:
    """Test wrap_future function."""

    def test_wrap_future_with_sync_future(self):
        """Test wrap_future with SyncFuture returns the same instance."""
        original = SyncFuture(result_value=42)
        wrapped = wrap_future(original)

        assert wrapped is original
        assert wrapped.result() == 42

    def test_wrap_future_with_concurrent_future(self):
        """Test wrap_future with concurrent.futures.Future."""
        cf_future = concurrent.futures.Future()
        cf_future.set_result(123)

        wrapped = wrap_future(cf_future)

        assert isinstance(wrapped, ConcurrentFuture)
        assert wrapped.result() == 123

    def test_wrap_future_with_asyncio_future(self):
        """Test wrap_future with asyncio.Future."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            asyncio_future = loop.create_future()
            asyncio_future.set_result(99)

            wrapped = wrap_future(asyncio_future)

            assert isinstance(wrapped, AsyncioFuture)
            assert wrapped.result() == 99
        finally:
            loop.close()

    def test_wrap_future_with_asyncio_task(self):
        """Test wrap_future with asyncio.Task (which is also an asyncio future)."""

        async def dummy_coroutine():
            return "task_result"

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            task = loop.create_task(dummy_coroutine())
            loop.run_until_complete(task)  # Complete the task

            wrapped = wrap_future(task)

            assert isinstance(wrapped, AsyncioFuture)
            assert wrapped.result() == "task_result"
        finally:
            loop.close()

    def test_wrap_future_with_plain_value(self):
        """Test wrap_future with a plain value (fallback case)."""
        wrapped = wrap_future("hello")

        assert isinstance(wrapped, SyncFuture)
        assert wrapped.result() == "hello"

    def test_wrap_future_with_none(self):
        """Test wrap_future with None value."""
        wrapped = wrap_future(None)

        assert isinstance(wrapped, SyncFuture)
        assert wrapped.result() is None

    def test_wrap_future_with_complex_object(self):
        """Test wrap_future with a complex object."""
        obj = {"key": "value", "list": [1, 2, 3]}
        wrapped = wrap_future(obj)

        assert isinstance(wrapped, SyncFuture)
        assert wrapped.result() == obj

    def test_wrap_future_rejects_duck_typed_objects(self):
        """Test that wrap_future doesn't incorrectly identify objects with result/done methods."""
        # Create a mock object that has result and done methods but isn't an asyncio future
        mock_obj = Mock()
        mock_obj.result = Mock(return_value="fake_result")
        mock_obj.done = Mock(return_value=True)

        # This should NOT be treated as an asyncio future
        wrapped = wrap_future(mock_obj)

        # Should fall back to SyncFuture with the mock object as the result
        assert isinstance(wrapped, SyncFuture)
        assert wrapped.result() is mock_obj


class TestAsyncAwait:
    """Test async/await functionality of futures."""

    def test_sync_future_await(self):
        """Test awaiting SyncFuture."""

        async def test_await():
            future = SyncFuture(result_value="awaited_sync")
            result = await future
            return result

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(test_await())
            assert result == "awaited_sync"
        finally:
            loop.close()

    def test_concurrent_future_await(self):
        """Test awaiting ConcurrentFuture."""

        async def test_await():
            cf_future = concurrent.futures.Future()
            cf_future.set_result("awaited_concurrent")
            future = ConcurrentFuture(future=cf_future)
            result = await future
            return result

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(test_await())
            assert result == "awaited_concurrent"
        finally:
            loop.close()

    def test_asyncio_future_await(self):
        """Test awaiting AsyncioFuture."""

        async def test_await():
            loop = asyncio.get_event_loop()
            asyncio_future = loop.create_future()
            asyncio_future.set_result("awaited_asyncio")
            future = AsyncioFuture(future=asyncio_future)
            result = await future
            return result

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(test_await())
            assert result == "awaited_asyncio"
        finally:
            loop.close()


class TestIntegration:
    """Integration tests combining multiple features."""

    def test_wrap_future_comprehensive(self):
        """Comprehensive test of wrap_future with all supported types."""
        # Test all the cases from the notebook

        # SyncFuture
        sync_future = SyncFuture(result_value=42)
        wrapped_sync = wrap_future(sync_future)
        assert wrapped_sync is sync_future
        assert wrapped_sync.result() == 42

        # concurrent.futures.Future
        cf_future = concurrent.futures.Future()
        cf_future.set_result(123)
        wrapped_concurrent = wrap_future(cf_future)
        assert isinstance(wrapped_concurrent, ConcurrentFuture)
        assert wrapped_concurrent.result() == 123

        # asyncio.Future
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            asyncio_future = loop.create_future()
            asyncio_future.set_result(99)
            wrapped_asyncio = wrap_future(asyncio_future)
            assert isinstance(wrapped_asyncio, AsyncioFuture)
            assert wrapped_asyncio.result() == 99
        finally:
            loop.close()

        # Plain value
        wrapped_value = wrap_future("hello")
        assert isinstance(wrapped_value, SyncFuture)
        assert wrapped_value.result() == "hello"

    def test_error_handling_across_types(self):
        """Test error handling across different future types."""
        # SyncFuture with exception
        sync_exc = ValueError("sync error")
        sync_future = SyncFuture(exception_value=sync_exc)
        with pytest.raises(ValueError, match="sync error"):
            sync_future.result()

        # ConcurrentFuture with exception
        cf_future = concurrent.futures.Future()
        cf_exc = RuntimeError("concurrent error")
        cf_future.set_exception(cf_exc)
        concurrent_future = ConcurrentFuture(future=cf_future)
        with pytest.raises(RuntimeError, match="concurrent error"):
            concurrent_future.result()

        # AsyncioFuture with exception
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            asyncio_future = loop.create_future()
            asyncio_exc = ValueError("asyncio error")
            asyncio_future.set_exception(asyncio_exc)
            asyncio_wrapper = AsyncioFuture(future=asyncio_future)
            with pytest.raises(ValueError, match="asyncio error"):
                asyncio_wrapper.result()
        finally:
            loop.close()
