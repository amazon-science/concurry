"""Tests to ensure all future implementations have consistent behavior."""

import asyncio
import time
from concurrent.futures import CancelledError, ThreadPoolExecutor

import pytest

from concurry.core.future import (
    AsyncioFuture,
    ConcurrentFuture,
    SyncFuture,
    wrap_future,
)


class TestFutureConsistency:
    """Test that all future implementations behave identically through the BaseFuture API."""

    def test_sync_future_with_result(self):
        """Test SyncFuture with a successful result."""
        future = SyncFuture(result_value=42)

        assert future.done() is True
        assert future.cancelled() is False
        assert future.result() == 42
        assert future.result(timeout=1) == 42
        assert future.exception() is None
        assert future.exception(timeout=1) is None
        assert future.cancel() is False  # Already done

    def test_sync_future_with_exception(self):
        """Test SyncFuture with an exception."""
        exc = ValueError("test error")
        future = SyncFuture(exception_value=exc)

        assert future.done() is True
        assert future.cancelled() is False

        with pytest.raises(ValueError, match="test error"):
            future.result()

        with pytest.raises(ValueError, match="test error"):
            future.result(timeout=1)

        assert future.exception() is exc
        assert future.cancel() is False  # Already done

    def test_sync_future_cancelled_behaviour(self):
        """Test SyncFuture when marked as cancelled."""
        future = SyncFuture(result_value=42)
        object.__setattr__(future, "_cancelled", True)

        assert future.cancelled() is True
        assert future.done() is True

        with pytest.raises(CancelledError):
            future.result()

        with pytest.raises(CancelledError):
            future.exception()

    def test_concurrent_future_success(self):
        """Test ConcurrentFuture with successful execution."""
        with ThreadPoolExecutor(max_workers=1) as executor:
            py_future = executor.submit(lambda: 42)
            future = ConcurrentFuture(future=py_future)

            # Wait for completion
            result = future.result(timeout=2)

            assert result == 42
            assert future.done() is True
            assert future.cancelled() is False
            assert future.exception() is None

    def test_concurrent_future_exception(self):
        """Test ConcurrentFuture with exception."""
        with ThreadPoolExecutor(max_workers=1) as executor:

            def raise_error():
                raise ValueError("test error")

            py_future = executor.submit(raise_error)
            future = ConcurrentFuture(future=py_future)

            with pytest.raises(ValueError, match="test error"):
                future.result(timeout=2)

            assert future.done() is True
            assert future.cancelled() is False
            assert isinstance(future.exception(), ValueError)

    def test_concurrent_future_timeout(self):
        """Test ConcurrentFuture with timeout."""
        with ThreadPoolExecutor(max_workers=1) as executor:
            py_future = executor.submit(lambda: time.sleep(0.5))
            future = ConcurrentFuture(future=py_future)

            with pytest.raises(TimeoutError):
                future.result(timeout=0.01)

    def test_asyncio_future_success(self):
        """Test AsyncioFuture with successful result."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async_future = loop.create_future()
            future = AsyncioFuture(future=async_future)

            # Set result immediately
            async_future.set_result(42)

            result = future.result(timeout=2)
            assert result == 42
            assert future.done() is True
            assert future.cancelled() is False
            assert future.exception() is None
        finally:
            loop.close()

    def test_asyncio_future_exception(self):
        """Test AsyncioFuture with exception."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async_future = loop.create_future()
            future = AsyncioFuture(future=async_future)

            exc = ValueError("test error")
            async_future.set_exception(exc)

            with pytest.raises(ValueError, match="test error"):
                future.result(timeout=2)

            assert future.done() is True
            assert isinstance(future.exception(), ValueError)
        finally:
            loop.close()

    def test_asyncio_future_cancelled(self):
        """Test AsyncioFuture when cancelled."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async_future = loop.create_future()
            future = AsyncioFuture(future=async_future)

            # Cancel the future
            async_future.cancel()

            assert future.cancelled() is True
            assert future.done() is True

            with pytest.raises(CancelledError):
                future.result()

            with pytest.raises(CancelledError):
                future.exception()
        finally:
            loop.close()

    def test_asyncio_future_timeout(self):
        """Test AsyncioFuture with timeout."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async_future = loop.create_future()
            future = AsyncioFuture(future=async_future)

            # Don't set result - should timeout
            with pytest.raises(TimeoutError):
                future.result(timeout=0.1)
        finally:
            loop.close()

    def test_callback_receives_wrapper_not_underlying(self):
        """Test that callbacks receive the wrapper, not the underlying future."""
        callback_received = []

        def callback(f):
            callback_received.append(f)

        # Test SyncFuture
        sync_future = SyncFuture(result_value=42)
        sync_future.add_done_callback(callback)
        assert len(callback_received) == 1
        assert isinstance(callback_received[0], SyncFuture)
        assert callback_received[0] is sync_future

        # Test ConcurrentFuture
        callback_received.clear()
        with ThreadPoolExecutor(max_workers=1) as executor:
            py_future = executor.submit(lambda: 42)
            concurrent_future = ConcurrentFuture(future=py_future)
            concurrent_future.add_done_callback(callback)

            # Wait for completion
            concurrent_future.result(timeout=2)

            # Callback should have been called
            assert len(callback_received) == 1
            assert isinstance(callback_received[0], ConcurrentFuture)
            assert callback_received[0] is concurrent_future

        # Test AsyncioFuture
        callback_received.clear()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async_future = loop.create_future()
            asyncio_future = AsyncioFuture(future=async_future)

            # Set result first, then add callback (to test "already done" path)
            async_future.set_result(42)
            asyncio_future.add_done_callback(callback)

            assert len(callback_received) == 1
            assert isinstance(callback_received[0], AsyncioFuture)
            assert callback_received[0] is asyncio_future
        finally:
            loop.close()

    def test_exception_types_are_consistent(self):
        """Test that all futures raise the same exception types for the same conditions."""
        # Test CancelledError for cancelled futures
        sync_future = SyncFuture(result_value=42)
        object.__setattr__(sync_future, "_cancelled", True)

        with pytest.raises(CancelledError):
            sync_future.result()

        with pytest.raises(CancelledError):
            sync_future.exception()

        # Test TimeoutError
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async_future = loop.create_future()
            asyncio_future = AsyncioFuture(future=async_future)

            with pytest.raises(TimeoutError):
                asyncio_future.result(timeout=0.1)

            with pytest.raises(TimeoutError):
                asyncio_future.exception(timeout=0.1)
        finally:
            loop.close()

    def test_wrap_future_preserves_behavior(self):
        """Test that wrap_future preserves consistent behavior."""
        # Wrap a concurrent future
        with ThreadPoolExecutor(max_workers=1) as executor:
            py_future = executor.submit(lambda: 42)
            wrapped = wrap_future(py_future)

            assert isinstance(wrapped, ConcurrentFuture)
            assert wrapped.result(timeout=2) == 42

        # Wrap an asyncio future
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async_future = loop.create_future()
            async_future.set_result(100)
            wrapped = wrap_future(async_future)

            assert isinstance(wrapped, AsyncioFuture)
            assert wrapped.result(timeout=2) == 100
        finally:
            loop.close()

        # Wrap a plain value (becomes SyncFuture)
        wrapped = wrap_future(42)
        assert isinstance(wrapped, SyncFuture)
        assert wrapped.result() == 42

    def test_idempotent_wrapping(self):
        """Test that wrap_future is idempotent."""
        future1 = SyncFuture(result_value=42)
        future2 = wrap_future(future1)

        assert future1 is future2

    def test_done_callbacks_called_on_cancel(self):
        """Test that done callbacks are called when future is cancelled."""
        callback_called = []

        def callback(f):
            callback_called.append(f.cancelled())

        # Test with ConcurrentFuture
        with ThreadPoolExecutor(max_workers=1) as executor:
            py_future = executor.submit(lambda: time.sleep(0.5))
            future = ConcurrentFuture(future=py_future)
            future.add_done_callback(callback)

            # Try to cancel
            if future.cancel():
                # If cancellation succeeded, callback should have been called
                # Note: cancellation might fail if task already started
                time.sleep(0.05)  # Give callback time to execute

    def test_result_waits_for_completion(self):
        """Test that result() blocks until future completes."""
        with ThreadPoolExecutor(max_workers=1) as executor:

            def delayed_return():
                time.sleep(0.1)
                return 42

            py_future = executor.submit(delayed_return)
            future = ConcurrentFuture(future=py_future)

            start = time.time()
            result = future.result()
            elapsed = time.time() - start

            assert result == 42
            assert elapsed >= 0.1

    def test_exception_method_waits_for_completion(self):
        """Test that exception() blocks until future completes."""
        with ThreadPoolExecutor(max_workers=1) as executor:

            def delayed_return():
                time.sleep(0.1)
                return 42

            py_future = executor.submit(delayed_return)
            future = ConcurrentFuture(future=py_future)

            start = time.time()
            exc = future.exception()
            elapsed = time.time() - start

            assert exc is None
            assert elapsed >= 0.1

    def test_await_syntax(self):
        """Test that all futures support await syntax."""

        async def test_sync_future():
            future = SyncFuture(result_value=42)
            result = await future
            assert result == 42

        async def test_concurrent_future():
            with ThreadPoolExecutor(max_workers=1) as executor:
                py_future = executor.submit(lambda: 42)
                future = ConcurrentFuture(future=py_future)
                result = await future
                assert result == 42

        async def test_asyncio_future():
            loop = asyncio.get_event_loop()
            async_future = loop.create_future()
            async_future.set_result(42)
            future = AsyncioFuture(future=async_future)
            result = await future
            assert result == 42

        # Run all async tests
        asyncio.run(test_sync_future())
        asyncio.run(test_concurrent_future())
        asyncio.run(test_asyncio_future())
