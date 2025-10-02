"""Unified Future interface for concurry."""

import asyncio
import time
from abc import ABC, abstractmethod
from concurrent.futures import Future as PyFuture
from typing import Any, Callable, Optional


class ConcurryFuture(ABC):
    """
    Abstract base class providing a unified future interface.

    This class serves as an abstraction layer that unifies different types of futures from various frameworks:
    - Python's standard `concurrent.futures.Future`
    - `asyncio.Future`
    - Ray's `ObjectRef`
    - Custom synchronous futures

    Key benefits:
    1. **Framework Agnostic**: Code can work with futures without needing to know their specific framework. The `wrap_future()` function automatically converts any future-like object into this unified interface.

    2. **Consistent API**: Provides a common interface (Adapter pattern) across all future types with:
        - `__await__` support for async/await syntax
        - Consistent timeout and error handling
        - Uniform callback mechanisms

    3. **Extensible**: New future types can be easily added by implementing this interface, allowing support for additional frameworks.

    4. **Control**: Gives precise control over future behavior, especially for edge cases and error conditions. For example, custom timeout handling can be implemented differently from framework defaults.
    """

    @abstractmethod
    def result(self, timeout: Optional[float] = None) -> Any:
        """Get the result of the future, blocking if necessary.

        Args:
            timeout: Maximum time to wait for result in seconds

        Returns:
            The result of the computation

        Raises:
            TimeoutError: If timeout is exceeded
            Exception: Any exception raised by the computation
        """
        pass

    @abstractmethod
    def cancel(self) -> bool:
        """Attempt to cancel the future.

        Returns:
            True if cancellation was successful, False otherwise
        """
        pass

    @abstractmethod
    def cancelled(self) -> bool:
        """Check if the future was cancelled.

        Returns:
            True if the future was cancelled
        """
        pass

    @abstractmethod
    def done(self) -> bool:
        """Check if the future is done (completed, cancelled, or failed).

        Returns:
            True if the future is done
        """
        pass

    @abstractmethod
    def exception(self, timeout: Optional[float] = None) -> Optional[Exception]:
        """Get the exception raised by the computation, if any.

        Args:
            timeout: Maximum time to wait for completion in seconds

        Returns:
            The exception raised, or None if computation succeeded
        """
        pass

    @abstractmethod
    def add_done_callback(self, fn: Callable) -> None:
        """Add a callback to be called when the future completes.

        Args:
            fn: Callback function that takes the future as argument
        """
        pass

    def __await__(self):
        """Make Future awaitable for async/await syntax."""
        # Simple implementation that yields until done
        while not self.done():
            yield
        return self.result()


class SyncFuture(ConcurryFuture):
    """Future implementation for synchronous execution."""

    def __init__(self, result: Any = None, exception: Optional[Exception] = None):
        self._result = result
        self._exception = exception
        self._done = True
        self._cancelled = False
        self._callbacks = []

    def result(self, timeout: Optional[float] = None) -> Any:
        if self._exception:
            raise self._exception
        return self._result

    def cancel(self) -> bool:
        return False  # Already done, cannot cancel

    def cancelled(self) -> bool:
        return self._cancelled

    def done(self) -> bool:
        return self._done

    def exception(self, timeout: Optional[float] = None) -> Optional[Exception]:
        return self._exception

    def add_done_callback(self, fn: Callable) -> None:
        # Already done, call immediately
        fn(self)


class ConcurrentFuture(ConcurryFuture):
    """Wrapper for concurrent.futures.Future to provide unified interface."""

    def __init__(self, future: PyFuture):
        self._future = future

    def result(self, timeout: Optional[float] = None) -> Any:
        return self._future.result(timeout)

    def cancel(self) -> bool:
        return self._future.cancel()

    def cancelled(self) -> bool:
        return self._future.cancelled()

    def done(self) -> bool:
        return self._future.done()

    def exception(self, timeout: Optional[float] = None) -> Optional[Exception]:
        return self._future.exception(timeout)

    def add_done_callback(self, fn: Callable) -> None:
        self._future.add_done_callback(fn)


class AsyncioFuture(ConcurryFuture):
    """Wrapper for asyncio Future to provide unified interface."""

    def __init__(self, future):
        self._future = future
        self._loop = None
        try:
            import asyncio

            self._loop = asyncio.get_event_loop()
        except:
            pass

    def result(self, timeout: Optional[float] = None) -> Any:
        if not self.done():
            if timeout is not None:
                start_time = time.time()
                while not self.done() and (time.time() - start_time) < timeout:
                    time.sleep(0.01)
                if not self.done():
                    raise TimeoutError("Future did not complete within timeout")
            else:
                while not self.done():
                    time.sleep(0.01)

        if self._future.cancelled():
            raise RuntimeError("Future was cancelled")

        exception = self._future.exception()
        if exception:
            raise exception

        return self._future.result()

    def cancel(self) -> bool:
        return self._future.cancel()

    def cancelled(self) -> bool:
        return self._future.cancelled()

    def done(self) -> bool:
        return self._future.done()

    def exception(self, timeout: Optional[float] = None) -> Optional[Exception]:
        if not self.done():
            if timeout is not None:
                start_time = time.time()
                while not self.done() and (time.time() - start_time) < timeout:
                    time.sleep(0.01)
                if not self.done():
                    raise TimeoutError("Future did not complete within timeout")
            else:
                while not self.done():
                    time.sleep(0.01)

        return self._future.exception()

    def add_done_callback(self, fn: Callable) -> None:
        self._future.add_done_callback(lambda fut: fn(self))


def wrap_future(future: Any) -> ConcurryFuture:
    """Wrap any future-like object in the unified Future interface.

    Args:
        future: A future-like object from any execution framework

    Returns:
        A Future instance providing the unified interface
    """
    if isinstance(future, ConcurryFuture):
        return future
    elif isinstance(future, PyFuture):
        return ConcurrentFuture(future)
    elif asyncio.isfuture(future):
        return AsyncioFuture(future)

    # Fallback - wrap as completed future
    return SyncFuture(result=future)
