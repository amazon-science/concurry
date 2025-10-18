"""Tests for type validation in Future classes."""

import asyncio
import concurrent.futures

import pytest

from concurry.core.future import AsyncioFuture, ConcurrentFuture, SyncFuture, wrap_future


class TestSyncFutureValidation:
    """Test type validation for SyncFuture."""

    def test_sync_future_accepts_correct_types(self):
        """Test that SyncFuture accepts correct types."""
        # Should work with any result value
        future = SyncFuture(result_value=42)
        assert future.result() == 42

        future = SyncFuture(result_value="string")
        assert future.result() == "string"

        future = SyncFuture(result_value=None)
        assert future.result() is None

        # Should work with exception
        exc = ValueError("test")
        future = SyncFuture(exception_value=exc)
        assert future.exception() is exc

    def test_sync_future_with_non_exception_in_exception_value(self):
        """Test that SyncFuture with non-Exception in exception_value fails at construction."""
        # Should raise TypeError during __post_init__
        with pytest.raises(TypeError, match="exception_value must be an Exception or None"):
            SyncFuture(exception_value="not an exception")  # type: ignore


class TestConcurrentFutureValidation:
    """Test type validation for ConcurrentFuture."""

    def test_concurrent_future_accepts_correct_type(self):
        """Test that ConcurrentFuture accepts concurrent.futures.Future."""
        cf_future = concurrent.futures.Future()
        cf_future.set_result(42)

        future = ConcurrentFuture(future=cf_future)
        assert future.result() == 42

    def test_concurrent_future_with_wrong_type(self):
        """Test that ConcurrentFuture with wrong type fails at construction."""
        # Should raise TypeError during __post_init__
        with pytest.raises(TypeError, match="future must be a concurrent.futures.Future"):
            ConcurrentFuture(future="not a future")  # type: ignore

    def test_concurrent_future_with_none(self):
        """Test that ConcurrentFuture with None fails at construction."""
        # Should raise TypeError during __post_init__
        with pytest.raises(TypeError, match="future must be a concurrent.futures.Future"):
            ConcurrentFuture(future=None)  # type: ignore


class TestAsyncioFutureValidation:
    """Test type validation for AsyncioFuture."""

    def test_asyncio_future_accepts_correct_type(self):
        """Test that AsyncioFuture accepts asyncio.Future."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async_future = loop.create_future()
            async_future.set_result(42)

            future = AsyncioFuture(future=async_future)
            assert future.result() == 42
        finally:
            loop.close()

    def test_asyncio_future_with_wrong_type(self):
        """Test that AsyncioFuture with wrong type fails at construction."""
        # Should raise TypeError during __post_init__
        with pytest.raises(TypeError, match="future must be an asyncio.Future"):
            AsyncioFuture(future="not a future")  # type: ignore

    def test_asyncio_future_with_none(self):
        """Test that AsyncioFuture with None fails at construction."""
        # Should raise TypeError during __post_init__
        with pytest.raises(TypeError, match="future must be an asyncio.Future"):
            AsyncioFuture(future=None)  # type: ignore

    def test_asyncio_future_with_concurrent_future(self):
        """Test that AsyncioFuture with concurrent.futures.Future fails at construction."""
        cf_future = concurrent.futures.Future()
        cf_future.set_result(42)

        # Should raise TypeError during __post_init__ (concurrent.futures.Future is not an asyncio.Future)
        with pytest.raises(TypeError, match="future must be an asyncio.Future"):
            AsyncioFuture(future=cf_future)  # type: ignore


class TestMissingRequiredArguments:
    """Test behavior with missing required arguments."""

    def test_concurrent_future_without_future_argument(self):
        """Test that ConcurrentFuture requires the 'future' argument."""
        with pytest.raises(TypeError, match="missing 1 required positional argument: 'future'"):
            ConcurrentFuture()  # type: ignore

    def test_asyncio_future_without_future_argument(self):
        """Test that AsyncioFuture requires the 'future' argument."""
        with pytest.raises(TypeError, match="missing 1 required positional argument: 'future'"):
            AsyncioFuture()  # type: ignore

    def test_sync_future_without_arguments_works(self):
        """Test that SyncFuture works without arguments (has defaults)."""
        future = SyncFuture()
        assert future.result() is None
        assert future.done() is True


class TestTypeHintsWithMypy:
    """Document expected behavior with static type checkers.

    Note: These tests document what mypy SHOULD catch.
    To verify static type checking, run: mypy tests/core/test_future_validation.py

    With runtime validation in __post_init__, wrong types now raise TypeError at construction.
    """

    def test_wrong_type_caught_at_runtime_with_validation(self):
        """With validation in __post_init__, wrong types now raise TypeError at construction."""
        # These now raise TypeError during construction (in __post_init__)
        with pytest.raises(TypeError):
            ConcurrentFuture(future="wrong")  # type: ignore

        with pytest.raises(TypeError):
            AsyncioFuture(future=123)  # type: ignore

        with pytest.raises(TypeError):
            SyncFuture(exception_value="not an exception")  # type: ignore


class TestWrapFutureValidation:
    """Test type validation in wrap_future function."""

    def test_wrap_future_with_invalid_types(self):
        """Test that wrap_future handles various input types."""
        # Should wrap any non-future object as SyncFuture with that object as result
        wrapped = wrap_future("string")
        assert isinstance(wrapped, SyncFuture)
        assert wrapped.result() == "string"

        wrapped = wrap_future(123)
        assert isinstance(wrapped, SyncFuture)
        assert wrapped.result() == 123

        wrapped = wrap_future(None)
        assert isinstance(wrapped, SyncFuture)
        assert wrapped.result() is None
