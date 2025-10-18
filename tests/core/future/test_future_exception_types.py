"""Tests to verify that BaseFuture raises the correct exception types matching concurrent.futures.Future."""

import asyncio
import concurrent.futures
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from concurry.core.future import (
    AsyncioFuture,
    ConcurrentFuture,
    SyncFuture,
)
from concurry.core.future import CancelledError as ImportedCancelledError


class TestFutureExceptionTypes:
    """Verify that all futures raise concurrent.futures exception types."""

    def test_sync_future_raises_concurrent_futures_cancelled_error(self):
        """SyncFuture should raise concurrent.futures.CancelledError."""
        future = SyncFuture(result_value=42)
        # Manually mark as cancelled for testing
        object.__setattr__(future, "_cancelled", True)

        with pytest.raises(concurrent.futures.CancelledError):
            future.result()

        with pytest.raises(concurrent.futures.CancelledError):
            future.exception()

    def test_concurrent_future_raises_concurrent_futures_exceptions(self):
        """ConcurrentFuture should raise concurrent.futures exceptions."""
        # Test CancelledError: Create a future and cancel it before it runs
        py_future = concurrent.futures.Future()
        py_future.cancel()
        future = ConcurrentFuture(future=py_future)

        with pytest.raises(concurrent.futures.CancelledError):
            future.result()

        # Test TimeoutError: Use a task that takes longer than the timeout
        with ThreadPoolExecutor(max_workers=1) as executor:
            py_future2 = executor.submit(lambda: time.sleep(0.5))
            future2 = ConcurrentFuture(future=py_future2)

            with pytest.raises(concurrent.futures.TimeoutError):
                future2.result(timeout=0.01)  # Timeout after 10ms

    def test_asyncio_future_raises_concurrent_futures_cancelled_error(self):
        """AsyncioFuture should raise concurrent.futures.CancelledError, NOT asyncio.CancelledError."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async_future = loop.create_future()
            future = AsyncioFuture(future=async_future)

            # Cancel the underlying asyncio future
            async_future.cancel()

            # Should raise concurrent.futures.CancelledError, not asyncio.CancelledError
            with pytest.raises(concurrent.futures.CancelledError) as exc_info:
                future.result()

            # Verify it's NOT asyncio.CancelledError
            assert not isinstance(exc_info.value, asyncio.CancelledError)
            assert isinstance(exc_info.value, concurrent.futures.CancelledError)

            # Same for exception()
            with pytest.raises(concurrent.futures.CancelledError) as exc_info:
                future.exception()

            assert not isinstance(exc_info.value, asyncio.CancelledError)
            assert isinstance(exc_info.value, concurrent.futures.CancelledError)

        finally:
            loop.close()

    def test_asyncio_future_raises_timeout_error(self):
        """AsyncioFuture should raise TimeoutError."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async_future = loop.create_future()
            future = AsyncioFuture(future=async_future)

            # Don't set result - should timeout
            with pytest.raises(TimeoutError) as exc_info:
                future.result(timeout=0.1)

            # Verify it's the built-in TimeoutError (same as concurrent.futures.TimeoutError)
            assert exc_info.value.__class__ is TimeoutError

            # Same for exception()
            with pytest.raises(TimeoutError):
                future.exception(timeout=0.1)

        finally:
            loop.close()

    def test_exception_types_are_from_concurrent_futures(self):
        """Verify we're using the concurrent.futures exception types."""
        # Verify CancelledError is from concurrent.futures
        assert ImportedCancelledError is concurrent.futures.CancelledError

        # TimeoutError should be the built-in (which concurrent.futures.TimeoutError aliases)
        assert concurrent.futures.TimeoutError is TimeoutError

    def test_asyncio_vs_concurrent_futures_cancelled_error_difference(self):
        """Document that asyncio.CancelledError and concurrent.futures.CancelledError are different."""
        # This test documents the fact that these are different exception types in Python 3.8+
        assert asyncio.CancelledError is not concurrent.futures.CancelledError

        # Our API should always raise concurrent.futures.CancelledError
        future = SyncFuture(result_value=42)
        object.__setattr__(future, "_cancelled", True)

        try:
            future.result()
            assert False, "Should have raised CancelledError"
        except asyncio.CancelledError:
            assert False, "Should NOT raise asyncio.CancelledError"
        except concurrent.futures.CancelledError:
            pass  # Correct!
