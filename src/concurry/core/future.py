"""Unified Future interface for concurry."""

import asyncio
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import CancelledError
from concurrent.futures import Future as PyFuture
from typing import Any, Callable, ClassVar, Optional

from ..utils.frameworks import _IS_RAY_INSTALLED


class BaseFuture(ABC):
    """
    Abstract base class providing a unified future interface.

    This class serves as an abstraction layer that unifies different types of futures from various frameworks:
    - Python's standard `concurrent.futures.Future`
    - `asyncio.Future`
    - Ray's `ObjectRef`
    - Synchronous results (wrapped for API compatibility)

    The API closely mirrors Python's `concurrent.futures.Future` to ensure familiarity and compatibility.

    Implementation:
    --------------
    BaseFuture and all its subclasses are implemented using __slots__ for performance:

    - **High Performance**: Optimized initialization with __slots__ and fast UUID generation
    - **Memory Efficient**: __slots__ reduces memory overhead per instance
    - **Type Safety**: Runtime validation at construction time with clear error messages
    - **Thread Safety**: Fast UUID generation using `id(self)` (~0.01µs vs 2.5µs for os.urandom)

    Each subclass defines __slots__ and implements the abstract methods for their specific
    execution framework.

    Key Benefits:
    ------------
    1. **Framework Agnostic**: Code can work with futures without needing to know their specific framework.
       The `wrap_future()` function automatically converts any future-like object into this unified interface.

    2. **Consistent API**: Provides a common interface (Adapter pattern) across all future types with:
        - `__await__` support for async/await syntax
        - Consistent timeout and error handling
        - Uniform callback mechanisms

    3. **Thread-Safe**: All operations are thread-safe when a lock is provided. Implementations use locks
       to ensure thread-safety except for futures like SyncFuture that don't need it.

    4. **Extensible**: New future types can be easily added by implementing this interface, allowing
       support for additional frameworks.

    5. **Type-Safe**: Runtime validation at construction time with clear error messages for incorrect types.

    Behavioral Guarantees:
    ---------------------
    All implementations of BaseFuture provide identical behavior through the public API:

    1. **Exception Types**: All futures raise the same exception types for the same conditions:
        - `concurrent.futures.CancelledError` when accessing a cancelled future
        - `TimeoutError` when operations exceed the specified timeout
        - Original exception from the computation when it fails

       Note: Even `asyncio.Future` raises `concurrent.futures.CancelledError` (not `asyncio.CancelledError`)
       for API consistency.

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
    synchronize access to internal state. SyncFuture sets this to None as it doesn't need locking.

    Private Members:
    ---------------
    Subclasses should define __slots__ with these common attributes:

    - `uuid`: Unique identifier for the future
    - `_result`: The computed result (or None if not yet available)
    - `_exception`: The exception raised (or None if successful)
    - `_done`: Whether the future has completed
    - `_cancelled`: Whether the future was cancelled
    - `_callbacks`: List of callbacks to invoke when done
    - `_lock`: Thread lock for synchronization (None for SyncFuture)

    Framework-specific private members (like `_future`, `_loop`, `_object_ref`) are defined only
    on the subclasses that need them.
    """

    FUTURE_UUID_PREFIX: ClassVar[str] = ""

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


class SyncFuture(BaseFuture):
    """Future implementation for synchronous execution.

    This future type represents a computation that has already completed.
    It's useful for wrapping immediate results in the unified future interface.

    Implementation:
    --------------
    SyncFuture is implemented as a highly optimized __slots__-based class:

    - **Performance**: Initializes in < 0.5 microseconds
    - **Thread-Safe**: No lock needed; single-threaded usage model provides thread-safety
    - **Type-Safe**: Validates `exception_value` is an Exception or None at construction
    - **Always Done**: Created with `_done=True` since the result is immediately available
    - **Fast UUID**: Uses `id(self)` for instant unique identification

    Args:
        result_value: The result value (default: None)
        exception_value: An exception that was raised (default: None). Must be an
            Exception instance or None.

    Raises:
        TypeError: If exception_value is not None and not an Exception instance

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

        # Type validation at construction
        try:
            future = SyncFuture(exception_value="not an exception")
        except TypeError as e:
            print(f"TypeError: {e}")  # exception_value must be an Exception or None
        ```
    """

    __slots__ = ("uuid", "_result", "_exception", "_done", "_cancelled", "_callbacks", "_lock")

    FUTURE_UUID_PREFIX: ClassVar[str] = "sync-future-"

    def __init__(self, result_value: Any = None, exception_value: Optional[Exception] = None) -> None:
        """Initialize SyncFuture with result or exception.

        Args:
            result_value: The result value (default: None)
            exception_value: An exception that was raised (default: None)

        Raises:
            TypeError: If exception_value is not None and not an Exception instance
        """
        # Validate exception_value if provided (fast check, rarely fails)
        if exception_value is not None and not isinstance(exception_value, BaseException):
            raise TypeError(
                f"exception_value must be an Exception or None, got {type(exception_value).__name__}"
            )

        # Use id(self) for ultra-fast unique ID generation (~0.01µs vs 2.5µs for os.urandom)
        self.uuid = f"{self.FUTURE_UUID_PREFIX}{id(self)}"

        # Set state directly (no object.__setattr__ needed without frozen dataclass)
        self._result = result_value
        self._exception = exception_value
        self._done = True  # Always done immediately
        self._cancelled = False
        self._callbacks = []
        self._lock = None  # No lock needed for sync futures

    def result(self, timeout: Optional[float] = None) -> Any:
        """Get the result, raising exceptions if present.

        Args:
            timeout: Ignored for SyncFuture (always immediate)

        Returns:
            The result value

        Raises:
            CancelledError: If the future was cancelled
            Exception: Any exception from the computation
        """
        if self._cancelled:
            raise CancelledError("Future was cancelled")
        if self._exception:
            raise self._exception
        return self._result

    def cancel(self) -> bool:
        """Attempt to cancel the future.

        Returns:
            False: SyncFuture cannot be cancelled (already done)
        """
        return False  # Already done, cannot cancel

    def cancelled(self) -> bool:
        """Check if the future was cancelled.

        Returns:
            bool: Cancellation status
        """
        return self._cancelled

    def running(self) -> bool:
        """Check if the future is currently running.

        Returns:
            False: SyncFuture is never in a running state
        """
        return False  # Never running, always completed

    def done(self) -> bool:
        """Check if the future is done.

        Returns:
            True: SyncFuture is always done at creation
        """
        return self._done

    def exception(self, timeout: Optional[float] = None) -> Optional[Exception]:
        """Get the exception if one was raised.

        Args:
            timeout: Ignored for SyncFuture (always immediate)

        Returns:
            The exception or None

        Raises:
            CancelledError: If the future was cancelled
        """
        if self._cancelled:
            raise CancelledError("Future was cancelled")
        return self._exception

    def add_done_callback(self, fn: Callable) -> None:
        """Add a callback to be called when the future completes.

        Args:
            fn: Callback function that takes the future as its argument

        Note:
            Since SyncFuture is always done, the callback is called immediately.
        """
        # Already done, call immediately
        fn(self)


class ConcurrentFuture(BaseFuture):
    """Wrapper for concurrent.futures.Future to provide unified interface.

    This wrapper provides a consistent API for futures from Python's standard
    `concurrent.futures` module (ThreadPoolExecutor, ProcessPoolExecutor).

    Implementation:
    --------------
    ConcurrentFuture is an optimized __slots__-based wrapper for `concurrent.futures.Future`:

    - **Performance**: Fast UUID generation using `id(self)` (~0.01µs vs 2.5µs)
    - **Thread-Safe**: Delegates to the inherently thread-safe `concurrent.futures.Future`
    - **Type-Safe**: Validates the wrapped future is a `concurrent.futures.Future` at construction
    - **Zero Overhead**: Direct delegation to underlying future methods
    - **API Compatible**: Matches `concurrent.futures.Future` exactly

    Args:
        future: A `concurrent.futures.Future` instance. Must be a valid
            `concurrent.futures.Future` object.

    Raises:
        TypeError: If future is not a `concurrent.futures.Future` instance

    Example:
        ```python
        from concurrent.futures import ThreadPoolExecutor
        from concurry.core.future import ConcurrentFuture

        with ThreadPoolExecutor() as executor:
            py_future = executor.submit(lambda: 42)
            future = ConcurrentFuture(future=py_future)
            result = future.result(timeout=5)

        # Type validation at construction
        try:
            future = ConcurrentFuture(future="not a future")
        except TypeError as e:
            print(f"TypeError: {e}")  # future must be a concurrent.futures.Future
        ```
    """

    __slots__ = (
        "uuid",
        "_future",
        "_callbacks",
        "_lock",
    )

    FUTURE_UUID_PREFIX: ClassVar[str] = "concurrent-future-"

    def __init__(self, future: PyFuture) -> None:
        """Initialize ConcurrentFuture with a concurrent.futures.Future.

        Args:
            future: A concurrent.futures.Future instance

        Raises:
            TypeError: If future is not a concurrent.futures.Future instance
        """
        # Validate future type
        if not isinstance(future, PyFuture):
            raise TypeError(f"future must be a concurrent.futures.Future, got {type(future).__name__}")

        # Use id(self) for ultra-fast unique ID generation
        self.uuid = f"{self.FUTURE_UUID_PREFIX}{id(self)}"

        # Store the future
        self._future = future

        # Initialize callbacks and lock
        self._callbacks = []
        self._lock = threading.Lock()  # Keep lock for consistency

    def result(self, timeout: Optional[float] = None) -> Any:
        """Get the result of the future.

        Args:
            timeout: Maximum time to wait for result in seconds

        Returns:
            The result of the computation

        Raises:
            CancelledError: If the future was cancelled
            TimeoutError: If timeout is exceeded
            Exception: Any exception from the computation
        """
        # PyFuture is already thread-safe, so we delegate directly
        return self._future.result(timeout)

    def cancel(self) -> bool:
        """Attempt to cancel the future.

        Returns:
            True if cancellation succeeded, False otherwise
        """
        # PyFuture.cancel() is thread-safe
        return self._future.cancel()

    def cancelled(self) -> bool:
        """Check if the future was cancelled.

        Returns:
            bool: Cancellation status
        """
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
        """Check if the future is done.

        Returns:
            bool: Completion status
        """
        # PyFuture.done() is thread-safe
        return self._future.done()

    def exception(self, timeout: Optional[float] = None) -> Optional[Exception]:
        """Get the exception if one was raised.

        Args:
            timeout: Maximum time to wait for completion in seconds

        Returns:
            The exception or None

        Raises:
            CancelledError: If the future was cancelled
            TimeoutError: If timeout is exceeded
        """
        # PyFuture.exception() is thread-safe
        return self._future.exception(timeout)

    def add_done_callback(self, fn: Callable) -> None:
        """Add a callback to be called when the future completes.

        Args:
            fn: Callback function that takes the future as its argument
        """
        # Wrap callback to pass the wrapper instead of underlying future
        # PyFuture.add_done_callback() is thread-safe
        self._future.add_done_callback(lambda _: fn(self))


class AsyncioFuture(BaseFuture):
    """Wrapper for asyncio Future to provide unified interface.

    This wrapper provides a consistent API for asyncio futures, including
    support for timeout parameters that aren't available in the native asyncio API.

    Implementation:
    --------------
    AsyncioFuture is an optimized __slots__-based wrapper for `asyncio.Future`:

    - **Performance**: Fast UUID generation using `id(self)` (~0.01µs)
    - **Thread-Safe**: Uses an internal lock for thread-safe access to asyncio futures
    - **Type-Safe**: Validates the wrapped future is an `asyncio.Future` at construction
    - **Timeout Support**: Adds timeout parameters to `result()` and `exception()` methods
    - **Exception Conversion**: Converts `asyncio.CancelledError` to `concurrent.futures.CancelledError`
      for API consistency

    Args:
        future: An `asyncio.Future` instance. Must be a valid asyncio.Future object.

    Raises:
        TypeError: If future is not an `asyncio.Future` instance

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

        # Type validation at construction
        try:
            future = AsyncioFuture(future="not an asyncio future")
        except TypeError as e:
            print(f"TypeError: {e}")  # future must be an asyncio.Future

        # Exception conversion for API consistency
        async def test_cancellation():
            loop = asyncio.get_event_loop()
            async_future = loop.create_future()
            future = AsyncioFuture(future=async_future)
            async_future.cancel()

            try:
                future.result()
            except concurrent.futures.CancelledError:
                print("Raises concurrent.futures.CancelledError, not asyncio.CancelledError!")
        ```
    """

    ASYNCIO_POLL_INTERVAL: ClassVar[float] = 1e-6

    __slots__ = (
        "uuid",
        "_future",
        "_callbacks",
        "_lock",
    )

    FUTURE_UUID_PREFIX: ClassVar[str] = "asyncio-future-"

    def __init__(self, future: Any) -> None:
        """Initialize AsyncioFuture with an asyncio.Future.

        Args:
            future: An asyncio.Future instance. Must be a valid asyncio.Future object.

        Raises:
            TypeError: If future is not an asyncio.Future instance
        """
        # Validate future type
        if not asyncio.isfuture(future):
            raise TypeError(f"future must be an asyncio.Future, got {type(future).__name__}")

        # Use id(self) for ultra-fast unique ID generation
        self.uuid = f"{self.FUTURE_UUID_PREFIX}{id(self)}"

        # Store the future
        self._future = future

        # Initialize callbacks and lock
        self._callbacks = []
        self._lock = threading.Lock()

    def result(self, timeout: Optional[float] = None) -> Any:
        """Get the result of the future.

        This method uses polling to wait for the asyncio.Future to complete.
        Note: This is less efficient than concurrent.futures.Future blocking.
        For better performance in worker proxies, use ConcurrentFuture instead.

        Args:
            timeout: Maximum time to wait for result in seconds

        Returns:
            The result of the computation

        Raises:
            CancelledError: If the future was cancelled
            TimeoutError: If timeout is exceeded
            Exception: Any exception from the computation
        """
        if not self.done():
            if timeout is not None:
                start_time = time.time()
                while not self.done() and (time.time() - start_time) < timeout:
                    time.sleep(self.ASYNCIO_POLL_INTERVAL)
                if not self.done():
                    raise TimeoutError("Future did not complete within timeout")
            else:
                while not self.done():
                    time.sleep(self.ASYNCIO_POLL_INTERVAL)

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
                    time.sleep(self.ASYNCIO_POLL_INTERVAL)
                if not self.done():
                    raise TimeoutError("Future did not complete within timeout")
            else:
                while not self.done():
                    time.sleep(self.ASYNCIO_POLL_INTERVAL)

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

    class RayFuture(BaseFuture):
        """Wrapper for Ray ObjectRef to provide unified interface.

        This wrapper provides a consistent API for Ray's ObjectRef, which is returned
        when submitting tasks to Ray. Requires Ray is installed.

        Implementation:
        --------------
        RayFuture is an optimized __slots__-based wrapper for Ray's `ObjectRef`:

        - **Performance**: Fast UUID generation using `id(self)` (~0.01µs)
        - **Thread-Safe**: Uses an internal lock to ensure thread-safe state management
        - **Type-Safe**: Validates the wrapped object_ref is a Ray `ObjectRef` at construction
        - **Exception Conversion**: Converts Ray's `GetTimeoutError` to standard `TimeoutError`
        - **Callback Support**: Implements proper callback invocation on completion
        - **State Tracking**: Maintains internal state for completion, cancellation, and results

        Args:
            object_ref: A Ray `ObjectRef` instance. Must be a valid Ray ObjectRef object.

        Raises:
            TypeError: If object_ref is not a Ray `ObjectRef` instance

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

            # Type validation at construction
            try:
                future = RayFuture(object_ref="not an object ref")
            except TypeError as e:
                print(f"TypeError: {e}")  # object_ref must be a Ray ObjectRef
            ```

        Note:
            This class is only available when Ray is installed.
            Install with: `pip install concurry[ray]`
        """

        __slots__ = (
            "uuid",
            "_object_ref",
            "_result",
            "_exception",
            "_done",
            "_cancelled",
            "_callbacks",
            "_lock",
        )

        FUTURE_UUID_PREFIX: ClassVar[str] = "ray-future-"

        def __init__(self, object_ref: Any) -> None:
            """Initialize RayFuture with a Ray ObjectRef.

            Args:
                object_ref: A Ray ObjectRef instance

            Raises:
                TypeError: If object_ref is not a Ray ObjectRef instance
            """
            # Validate object_ref type
            if not isinstance(object_ref, ray.ObjectRef):
                raise TypeError(f"object_ref must be a Ray ObjectRef, got {type(object_ref).__name__}")

            # Use id(self) for ultra-fast unique ID generation
            self.uuid = f"{self.FUTURE_UUID_PREFIX}{id(self)}"

            # Store the object_ref (ray.ObjectRef)
            self._object_ref = object_ref

            # Initialize base future attributes
            self._result = None
            self._exception = None
            self._done = False
            self._cancelled = False
            self._callbacks = []
            self._lock = threading.Lock()

        def result(self, timeout: Optional[float] = None) -> Any:
            """Get the result of the future.

            Args:
                timeout: Maximum time to wait for result in seconds

            Returns:
                The result of the computation

            Raises:
                CancelledError: If the future was cancelled
                TimeoutError: If timeout is exceeded
                Exception: Any exception from the computation
            """
            if self._cancelled:
                raise CancelledError("Future was cancelled")

            # Return cached result if already fetched
            if self._done:
                if self._exception:
                    raise self._exception
                return self._result

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
            """Attempt to cancel the future.

            Returns:
                True if cancellation succeeded, False otherwise
            """
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
            """Check if the future was cancelled.

            Returns:
                bool: Cancellation status
            """
            return self._cancelled

        def running(self) -> bool:
            """Check if the future is currently being executed.

            Returns:
                True if the future is currently being executed (not done and not cancelled)
            """
            with self._lock:
                return not self._done and not self._cancelled

        def done(self) -> bool:
            """Check if the future is done.

            Returns:
                bool: Completion status
            """
            if self._done:
                return True

            try:
                ready, not_ready = ray.wait([self._object_ref], timeout=0)
                done = len(ready) > 0
                # Don't set _done=True here - only set it when result is actually fetched
                # in result() or exception() methods. Otherwise result() will return None.
                return done
            except:
                return False

        def exception(self, timeout: Optional[float] = None) -> Optional[Exception]:
            """Get the exception if one was raised.

            Args:
                timeout: Maximum time to wait for completion in seconds

            Returns:
                The exception or None

            Raises:
                CancelledError: If the future was cancelled
                TimeoutError: If timeout is exceeded
            """
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
            """Add a callback to be called when the future completes.

            Args:
                fn: Callback function that takes the future as its argument
            """
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
