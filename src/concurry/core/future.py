"""Unified Future interface for concurry."""

import asyncio
import threading
import time
import uuid
from abc import ABC, abstractmethod
from concurrent.futures import Future as PyFuture
from typing import Any, Callable, ClassVar, Dict, NoReturn, Optional

from morphic import Typed

from ..utils.frameworks import _IS_RAY_INSTALLED


class BaseFuture(Typed, ABC):
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

    FUTURE_UUID_PREFIX: ClassVar[str]

    uuid: str

    @classmethod
    def pre_initialize(cls, data: Dict) -> NoReturn:
        """Pre-initialize the future."""
        data["uuid"] = f"{cls.FUTURE_UUID_PREFIX}{str(uuid.uuid4())}"

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


class SyncFuture(BaseFuture):
    """Future implementation for synchronous execution."""

    FUTURE_UUID_PREFIX: ClassVar[str] = "sync-future-"

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


class ConcurrentFuture(BaseFuture):
    """Wrapper for concurrent.futures.Future to provide unified interface."""

    FUTURE_UUID_PREFIX: ClassVar[str] = "concurrent-future-"

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


class AsyncioFuture(BaseFuture):
    """Wrapper for asyncio Future to provide unified interface."""

    FUTURE_UUID_PREFIX: ClassVar[str] = "asyncio-future-"

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


if _IS_RAY_INSTALLED:
    import ray

    class RayFutureWrapper(BaseFuture):
        """Wrapper for Ray ObjectRef to provide unified interface."""

        FUTURE_UUID_PREFIX: ClassVar[str] = "ray-future-"

        def __init__(self, object_ref):
            self._object_ref = object_ref
            self._done = False
            self._result = None
            self._exception = None
            self._cancelled = False
            self._callbacks = []
            self._lock = threading.Lock()

        def result(self, timeout: Optional[float] = None) -> Any:
            try:
                if timeout is not None:
                    result = ray.get(self._object_ref, timeout=timeout)
                else:
                    result = ray.get(self._object_ref)

                with self._lock:
                    self._result = result
                    self._done = True
                    # Call callbacks
                    for callback in self._callbacks:
                        try:
                            callback(self)
                        except:
                            pass  # Ignore callback errors
                    self._callbacks.clear()

                return result
            except Exception as e:
                with self._lock:
                    self._exception = e
                    self._done = True
                    # Call callbacks
                    for callback in self._callbacks:
                        try:
                            callback(self)
                        except:
                            pass  # Ignore callback errors
                    self._callbacks.clear()
                raise

        def cancel(self) -> bool:
            try:
                ray.cancel(self._object_ref)
                with self._lock:
                    self._cancelled = True
                    self._done = True
                return True
            except:
                return False

        def cancelled(self) -> bool:
            return self._cancelled

        def done(self) -> bool:
            if self._done:
                return True

            try:
                ready, not_ready = ray.wait([self._object_ref], timeout=0)
                done = len(ready) > 0
                if done:
                    with self._lock:
                        self._done = True
                return done
            except:
                return False

        def exception(self, timeout: Optional[float] = None) -> Optional[Exception]:
            if not self.done():
                try:
                    self.result(timeout)
                except Exception as e:
                    return e
            return self._exception

        def add_done_callback(self, fn: Callable) -> None:
            with self._lock:
                if self._done:
                    fn(self)
                else:
                    self._callbacks.append(fn)


def wrap_future(future: Any) -> BaseFuture:
    """Wrap any future-like object in the unified Future interface.

    Args:
        future: A future-like object from any execution framework

    Returns:
        A Future instance providing the unified interface
    """
    if isinstance(future, BaseFuture):
        return future
    elif isinstance(future, PyFuture):
        return ConcurrentFuture(future)
    elif asyncio.isfuture(future):
        return AsyncioFuture(future)
    elif _IS_RAY_INSTALLED:
        import ray

        if isinstance(future, ray.ObjectRef):
            return RayFutureWrapper(future)

    # Fallback - wrap as completed future
    return SyncFuture(result=future)
