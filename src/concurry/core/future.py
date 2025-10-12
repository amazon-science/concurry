"""Unified Future interface for concurry."""

import asyncio
import os
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import CancelledError
from concurrent.futures import Future as PyFuture
from dataclasses import dataclass, field
from typing import Any, Callable, ClassVar, Optional

from ..utils.frameworks import _IS_RAY_INSTALLED


@dataclass(frozen=True)
class BaseFuture(ABC):
    """
    Abstract base class providing a unified future interface.

    This class serves as an abstraction layer that unifies different types of futures from various frameworks:
    - Python's standard `concurrent.futures.Future`
    - `asyncio.Future`
    - Ray's `ObjectRef`
    - Synchronous results (wrapped for API compatibility)

    The API closely mirrors Python's `concurrent.futures.Future` to ensure familiarity and compatibility.

    Key benefits:
    1. **Framework Agnostic**: Code can work with futures without needing to know their specific framework. The `wrap_future()` function automatically converts any future-like object into this unified interface.

    2. **Consistent API**: Provides a common interface (Adapter pattern) across all future types with:
        - `__await__` support for async/await syntax
        - Consistent timeout and error handling
        - Uniform callback mechanisms

    3. **Thread-Safe**: All operations are thread-safe when a lock is provided. Implementations use locks
       to ensure thread-safety except for immutable futures like SyncFuture.

    4. **Extensible**: New future types can be easily added by implementing this interface, allowing support for additional frameworks.

    5. **Control**: Gives precise control over future behavior, especially for edge cases and error conditions. For example, custom timeout handling can be implemented differently from framework defaults.

    Behavioral Guarantees:
    ---------------------
    All implementations of BaseFuture provide identical behavior through the public API:

    1. **Exception Types**: All futures raise the same exception types for the same conditions:
        - `CancelledError` when accessing a cancelled future
        - `TimeoutError` when operations exceed the specified timeout
        - Original exception from the computation when it fails

    2. **Callbacks**: All `add_done_callback()` implementations pass the wrapper future (not the underlying
       framework future) to the callback. Callbacks are called exactly once when the future completes.

    3. **Cancellation**: `cancel()` returns False if the future is already done, True if cancellation succeeded.
       Once cancelled, `result()` and `exception()` raise `CancelledError`.

    4. **Blocking Behavior**: `result()` and `exception()` block until the future completes (unless a timeout
       is specified). Both methods respect the timeout parameter consistently.

    5. **Await Support**: All futures support async/await syntax through `__await__`, making them usable
       in async contexts regardless of the underlying framework.

    Thread Safety:
    --------------
    All future operations are thread-safe. Each future maintains a private lock (`_lock`) used to
    synchronize access to internal state. SyncFuture sets this to None as it's immutable and doesn't
    require locking.

    Immutability:
    ------------
    BaseFuture is immutable (frozen via dataclass). The `set_result()` and `set_exception()` methods
    are provided for API compatibility with `concurrent.futures.Future` but raise `NotImplementedError`
    since the immutable design prevents modification after creation.
    """

    FUTURE_UUID_PREFIX: ClassVar[str] = ""

    # UUID is generated in __post_init__, but needs init=False to avoid conflicts with subclass fields
    uuid: str = field(default="", init=False)

    # Private members common to all futures (matching concurrent.futures.Future)
    _result: Any = field(default=None, init=False, repr=False)
    _exception: Optional[Exception] = field(default=None, init=False, repr=False)
    _done: bool = field(default=False, init=False, repr=False)
    _cancelled: bool = field(default=False, init=False, repr=False)
    _callbacks: list = field(default_factory=list, init=False, repr=False)
    _lock: Optional[threading.Lock] = field(default=None, init=False, repr=False)

    @abstractmethod
    def result(self, timeout: Optional[float] = None) -> Any:
        """Get the result of the future, blocking if necessary.

        This method blocks until the future completes or the timeout expires.
        Behavior is guaranteed to be identical across all future implementations.

        Args:
            timeout: Maximum time to wait for result in seconds. None means wait indefinitely.

        Returns:
            The result of the computation

        Raises:
            CancelledError: If the future was cancelled
            TimeoutError: If timeout is exceeded before completion
            Exception: Any exception raised by the underlying computation
        """
        pass

    @abstractmethod
    def cancel(self) -> bool:
        """Attempt to cancel the future.

        If the call is currently being executed or finished running and cannot be cancelled,
        the method will return False. Otherwise, the call will be cancelled and the method
        will return True.

        Returns:
            True if cancellation was successful, False otherwise
        """
        pass

    @abstractmethod
    def cancelled(self) -> bool:
        """Check if the future was cancelled.

        Returns:
            True if the future was successfully cancelled
        """
        pass

    @abstractmethod
    def running(self) -> bool:
        """Check if the future is currently being executed.

        Returns:
            True if the future is currently being executed and cannot be cancelled
        """
        pass

    @abstractmethod
    def done(self) -> bool:
        """Check if the future is done (completed, cancelled, or failed).

        Returns:
            True if the future is done (finished or was cancelled)
        """
        pass

    @abstractmethod
    def exception(self, timeout: Optional[float] = None) -> Optional[Exception]:
        """Get the exception raised by the computation, if any.

        This method blocks until the future completes or the timeout expires.
        Behavior is guaranteed to be identical across all future implementations.

        Args:
            timeout: Maximum time to wait for completion in seconds. None means wait indefinitely.

        Returns:
            The exception raised by the computation, or None if it succeeded

        Raises:
            CancelledError: If the future was cancelled
            TimeoutError: If timeout is exceeded before completion
        """
        pass

    @abstractmethod
    def add_done_callback(self, fn: Callable) -> None:
        """Add a callback to be called when the future completes.

        The callback receives the wrapper future (this BaseFuture instance), not the
        underlying framework future. This ensures consistent behavior across all implementations.

        If the future is already done, the callback is called immediately (in the same thread).
        Otherwise, it's called when the future completes.

        Args:
            fn: Callback function that takes the future (BaseFuture) as its single argument
        """
        pass

    def set_result(self, result: Any) -> None:
        """Set the result of the future.

        This method is provided for API compatibility with concurrent.futures.Future but is
        not implemented since BaseFuture is immutable.

        Args:
            result: The result to set

        Raises:
            NotImplementedError: Always raised as BaseFuture is immutable
        """
        raise NotImplementedError(
            "BaseFuture is immutable. Results are set during initialization, not after creation."
        )

    def set_exception(self, exception: Exception) -> None:
        """Set the exception of the future.

        This method is provided for API compatibility with concurrent.futures.Future but is
        not implemented since BaseFuture is immutable.

        Args:
            exception: The exception to set

        Raises:
            NotImplementedError: Always raised as BaseFuture is immutable
        """
        raise NotImplementedError(
            "BaseFuture is immutable. Exceptions are set during initialization, not after creation."
        )

    def set_running_or_notify_cancel(self) -> bool:
        """Mark the future as running or cancel it if already cancelled.

        This method is provided for API compatibility with concurrent.futures.Future but is
        not implemented since BaseFuture's state is managed internally.

        Returns:
            bool: Would return False if cancelled, True if set to running

        Raises:
            NotImplementedError: Always raised as state is managed internally
        """
        raise NotImplementedError("BaseFuture manages state internally. This method is not supported.")

    def __await__(self):
        """Make Future awaitable for async/await syntax."""
        # Simple implementation that yields until done
        while not self.done():
            yield
        return self.result()


@dataclass(frozen=True)
class SyncFuture(BaseFuture):
    """Future implementation for synchronous execution.

    This future type represents a computation that has already completed.
    It's useful for wrapping immediate results in the unified future interface.

    Args:
        result_value: The result value (default: None)
        exception_value: An exception that was raised (default: None)

    Example:
        ```python
        # Create a future with a result
        future = SyncFuture(result_value=42)
        print(future.result())  # 42

        # Create a future with an exception
        future = SyncFuture(exception_value=ValueError("Error"))
        try:
            future.result()
        except ValueError as e:
            print(f"Got error: {e}")
        ```
    """

    FUTURE_UUID_PREFIX: ClassVar[str] = "sync-future-"

    result_value: Any = None
    exception_value: Optional[Exception] = None

    # SyncFuture doesn't need any framework-specific private members

    def __post_init__(self) -> None:
        """Initialize private state after instance creation.

        SyncFuture is immutable and doesn't require locking since all state
        is set at initialization and never changes.

        Raises:
            TypeError: If exception_value is not None and not an Exception instance
        """
        # Validate exception_value if provided
        if self.exception_value is not None and not isinstance(self.exception_value, BaseException):
            raise TypeError(
                f"exception_value must be an Exception or None, got {type(self.exception_value).__name__}"
            )

        # Generate ID using os.urandom (fast and thread-safe)
        object.__setattr__(self, "uuid", f"{self.FUTURE_UUID_PREFIX}{os.urandom(16).hex()}")

        # Set private members that differ from BaseFuture defaults
        object.__setattr__(self, "_result", self.result_value)
        object.__setattr__(self, "_exception", self.exception_value)
        object.__setattr__(self, "_done", True)  # Default is False, set to True for sync
        # _cancelled, _callbacks, _lock already have correct defaults (False, [], None)

    def result(self, timeout: Optional[float] = None) -> Any:
        if self._cancelled:
            raise CancelledError("Future was cancelled")
        if self._exception:
            raise self._exception
        return self._result

    def cancel(self) -> bool:
        return False  # Already done, cannot cancel

    def cancelled(self) -> bool:
        return self._cancelled

    def running(self) -> bool:
        """Return False as SyncFuture is never in a running state.

        Returns:
            False: SyncFuture is always completed at creation
        """
        return False  # Never running, always completed

    def done(self) -> bool:
        return self._done

    def exception(self, timeout: Optional[float] = None) -> Optional[Exception]:
        if self._cancelled:
            raise CancelledError("Future was cancelled")
        return self._exception

    def add_done_callback(self, fn: Callable) -> None:
        # Already done, call immediately
        fn(self)


@dataclass(frozen=True)
class ConcurrentFuture(BaseFuture):
    """Wrapper for concurrent.futures.Future to provide unified interface.

    This wrapper provides a consistent API for futures from Python's standard
    `concurrent.futures` module (ThreadPoolExecutor, ProcessPoolExecutor).

    Args:
        future: A `concurrent.futures.Future` instance

    Example:
        ```python
        from concurrent.futures import ThreadPoolExecutor
        from concurry.core.future import ConcurrentFuture

        with ThreadPoolExecutor() as executor:
            py_future = executor.submit(lambda: 42)
            future = ConcurrentFuture(future=py_future)
            result = future.result(timeout=5)
        ```
    """

    FUTURE_UUID_PREFIX: ClassVar[str] = "concurrent-future-"

    future: PyFuture

    # Framework-specific private member for the underlying future
    _future: PyFuture = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialize private state after instance creation.

        ConcurrentFuture wraps a concurrent.futures.Future which is already thread-safe,
        but we maintain a lock for consistency with the BaseFuture interface.

        Raises:
            TypeError: If future is not a concurrent.futures.Future instance
        """
        # Validate future type
        if not isinstance(self.future, PyFuture):
            raise TypeError(f"future must be a concurrent.futures.Future, got {type(self.future).__name__}")

        # Generate ID using os.urandom (fast and thread-safe)
        object.__setattr__(self, "uuid", f"{self.FUTURE_UUID_PREFIX}{os.urandom(16).hex()}")

        # Set members that differ from BaseFuture defaults
        object.__setattr__(self, "_lock", threading.Lock())  # Default is None, but we need a Lock
        object.__setattr__(self, "_future", self.future)  # Framework-specific member
        # _result, _exception, _done, _cancelled, _callbacks already have correct defaults

    def result(self, timeout: Optional[float] = None) -> Any:
        # PyFuture is already thread-safe, so we delegate directly
        return self._future.result(timeout)

    def cancel(self) -> bool:
        # PyFuture.cancel() is thread-safe
        return self._future.cancel()

    def cancelled(self) -> bool:
        # PyFuture.cancelled() is thread-safe
        return self._future.cancelled()

    def running(self) -> bool:
        """Check if the future is currently being executed.

        Returns:
            True if the future is currently being executed
        """
        # PyFuture.running() is thread-safe
        return self._future.running()

    def done(self) -> bool:
        # PyFuture.done() is thread-safe
        return self._future.done()

    def exception(self, timeout: Optional[float] = None) -> Optional[Exception]:
        # PyFuture.exception() is thread-safe
        return self._future.exception(timeout)

    def add_done_callback(self, fn: Callable) -> None:
        # Wrap callback to pass the wrapper instead of underlying future
        # PyFuture.add_done_callback() is thread-safe
        self._future.add_done_callback(lambda _: fn(self))


@dataclass(frozen=True)
class AsyncioFuture(BaseFuture):
    """Wrapper for asyncio Future to provide unified interface.

    This wrapper provides a consistent API for asyncio futures, including
    support for timeout parameters that aren't available in the native asyncio API.

    Args:
        future: An `asyncio.Future` instance

    Example:
        ```python
        import asyncio
        from concurry.core.future import AsyncioFuture

        async def example():
            loop = asyncio.get_event_loop()
            async_future = loop.create_future()
            future = AsyncioFuture(future=async_future)

            # Set result
            async_future.set_result(42)

            # Get result with timeout (not available in native asyncio!)
            result = future.result(timeout=5)
            return result
        ```
    """

    FUTURE_UUID_PREFIX: ClassVar[str] = "asyncio-future-"

    future: Any

    # Framework-specific private members for asyncio
    _future: Any = field(default=None, init=False, repr=False)
    _loop: Any = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialize private state after instance creation.

        AsyncioFuture adds thread-safety via a lock since asyncio futures
        are not inherently thread-safe when accessed from multiple threads.

        Raises:
            TypeError: If future is not an asyncio.Future instance
        """
        # Validate future type
        if not asyncio.isfuture(self.future):
            raise TypeError(f"future must be an asyncio.Future, got {type(self.future).__name__}")

        # Generate ID using os.urandom (fast and thread-safe)
        object.__setattr__(self, "uuid", f"{self.FUTURE_UUID_PREFIX}{os.urandom(16).hex()}")

        # Set members that differ from BaseFuture defaults
        object.__setattr__(self, "_lock", threading.Lock())  # Default is None, but we need a Lock
        object.__setattr__(self, "_future", self.future)  # Framework-specific member

        # Set _loop (try to get event loop, otherwise leave as None default)
        try:
            object.__setattr__(self, "_loop", asyncio.get_event_loop())
        except:
            pass  # _loop remains None (the default)
        # _result, _exception, _done, _cancelled, _callbacks already have correct defaults

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
            # Raise concurrent.futures.CancelledError, not asyncio.CancelledError
            raise CancelledError("Future was cancelled")

        try:
            exception = self._future.exception()
        except asyncio.CancelledError:
            # Convert asyncio.CancelledError to concurrent.futures.CancelledError
            raise CancelledError("Future was cancelled") from None

        if exception:
            raise exception

        try:
            return self._future.result()
        except asyncio.CancelledError:
            # Convert asyncio.CancelledError to concurrent.futures.CancelledError
            raise CancelledError("Future was cancelled") from None

    def cancel(self) -> bool:
        with self._lock:
            return self._future.cancel()

    def cancelled(self) -> bool:
        with self._lock:
            return self._future.cancelled()

    def running(self) -> bool:
        """Check if the future is currently being executed.

        Note: asyncio.Future doesn't have a running() method, so we consider
        it running if it's neither done nor cancelled.

        Returns:
            True if the future is currently being executed
        """
        with self._lock:
            return not self._future.done() and not self._future.cancelled()

    def done(self) -> bool:
        with self._lock:
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

        if self._future.cancelled():
            # Raise concurrent.futures.CancelledError, not asyncio.CancelledError
            raise CancelledError("Future was cancelled")

        try:
            return self._future.exception()
        except asyncio.CancelledError:
            # Convert asyncio.CancelledError to concurrent.futures.CancelledError
            raise CancelledError("Future was cancelled") from None

    def add_done_callback(self, fn: Callable) -> None:
        # Wrap callback to pass the wrapper instead of underlying future
        def wrapped_callback(fut):
            fn(self)

        with self._lock:
            self._future.add_done_callback(wrapped_callback)

            # asyncio.Future.add_done_callback should call immediately if done,
            # but to be safe, check and call if needed
            if self._future.done():
                try:
                    wrapped_callback(self._future)
                except:
                    pass  # Callback may have already been called


if _IS_RAY_INSTALLED:
    import ray

    @dataclass(frozen=True)
    class RayFuture(BaseFuture):
        """Wrapper for Ray ObjectRef to provide unified interface.

        This wrapper provides a consistent API for Ray's ObjectRef, which is returned
        when submitting tasks to Ray. Requires Ray to be installed.

        Args:
            object_ref: A Ray `ObjectRef` instance

        Example:
            ```python
            import ray
            from concurry.core.future import RayFuture

            ray.init()

            @ray.remote
            def compute(x):
                return x ** 2

            # Ray returns an ObjectRef
            object_ref = compute.remote(42)

            # Wrap in unified interface
            future = RayFuture(object_ref=object_ref)
            result = future.result(timeout=10)

            ray.shutdown()
            ```

        Note:
            This class is only available when Ray is installed.
            Install with: `pip install concurry[ray]`
        """

        FUTURE_UUID_PREFIX: ClassVar[str] = "ray-future-"

        object_ref: Any

        # Framework-specific private member for Ray ObjectRef
        _object_ref: Any = field(default=None, init=False, repr=False)

        def __post_init__(self) -> None:
            """Initialize private state after instance creation.

            RayFuture uses a lock to ensure thread-safety when accessing and
            modifying internal state across multiple threads.

            Raises:
                TypeError: If object_ref is not a Ray ObjectRef instance
            """
            # Validate object_ref type
            if not isinstance(self.object_ref, ray.ObjectRef):
                raise TypeError(f"object_ref must be a Ray ObjectRef, got {type(self.object_ref).__name__}")

            # Generate ID using os.urandom (fast and thread-safe)
            object.__setattr__(self, "uuid", f"{self.FUTURE_UUID_PREFIX}{os.urandom(16).hex()}")

            # Set members that differ from BaseFuture defaults
            object.__setattr__(self, "_lock", threading.Lock())  # Default is None, but we need a Lock
            object.__setattr__(self, "_object_ref", self.object_ref)  # Framework-specific member
            # _result, _exception, _done, _cancelled, _callbacks already have correct defaults

        def result(self, timeout: Optional[float] = None) -> Any:
            if self._cancelled:
                raise CancelledError("Future was cancelled")

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
                # Convert Ray's GetTimeoutError to standard TimeoutError
                if e.__class__.__name__ == "GetTimeoutError":
                    with self._lock:
                        self._done = False  # Not actually done, just timed out
                    raise TimeoutError("Future did not complete within timeout") from e

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
            with self._lock:
                # Can't cancel if already done
                if self._done:
                    return False

                try:
                    ray.cancel(self._object_ref)
                    self._cancelled = True
                    self._done = True
                    # Call callbacks
                    for callback in self._callbacks:
                        try:
                            callback(self)
                        except:
                            pass  # Ignore callback errors
                    self._callbacks.clear()
                    return True
                except:
                    return False

        def cancelled(self) -> bool:
            return self._cancelled

        def running(self) -> bool:
            """Check if the future is currently being executed.

            Returns:
                True if the future is currently being executed (not done and not cancelled)
            """
            with self._lock:
                return not self._done and not self._cancelled

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
            if self._cancelled:
                raise CancelledError("Future was cancelled")

            if not self.done():
                try:
                    self.result(timeout)
                except CancelledError:
                    # Re-raise CancelledError
                    raise
                except Exception as e:
                    # Store exception for future calls
                    with self._lock:
                        self._exception = e
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

    This function automatically detects the type of future and wraps it in the
    appropriate BaseFuture subclass. It's the main entry point for using the
    unified future interface.

    Args:
        future: A future-like object from any execution framework. Supported types:
            - `BaseFuture` (returned as-is)
            - `concurrent.futures.Future`
            - `asyncio.Future`
            - Ray's `ObjectRef` (if Ray is installed)
            - Any other object (wrapped as `SyncFuture` with the object as result)

    Returns:
        A BaseFuture instance providing the unified interface

    Example:
        ```python
        from concurry.core.future import wrap_future
        from concurrent.futures import ThreadPoolExecutor
        import asyncio

        # Works with threading futures
        with ThreadPoolExecutor() as executor:
            thread_future = executor.submit(lambda: 42)
            unified = wrap_future(thread_future)
            result = unified.result(timeout=5)

        # Works with asyncio futures
        async def async_example():
            loop = asyncio.get_event_loop()
            async_future = loop.create_future()
            async_future.set_result(100)
            unified = wrap_future(async_future)
            result = unified.result(timeout=5)
            return result

        # Works with Ray (if installed)
        import ray
        ray.init()

        @ray.remote
        def remote_task(x):
            return x ** 2

        object_ref = remote_task.remote(42)
        unified = wrap_future(object_ref)
        result = unified.result(timeout=10)

        ray.shutdown()
        ```

    Note:
        If the input is already a BaseFuture, it's returned as-is without wrapping.
        This makes the function idempotent.
    """
    if isinstance(future, BaseFuture):
        return future
    elif isinstance(future, PyFuture):
        return ConcurrentFuture(future=future)
    elif asyncio.isfuture(future):
        return AsyncioFuture(future=future)
    elif _IS_RAY_INSTALLED:
        import ray

        if isinstance(future, ray.ObjectRef):
            return RayFuture(object_ref=future)

    # Fallback - wrap as completed future
    return SyncFuture(result_value=future)
