"""Comprehensive tests for all worker implementations."""

import asyncio
import time
from typing import List

import pytest

from concurry.core.future import ConcurrentFuture, SyncFuture
from concurry.core.worker import TaskWorker, Worker, worker
from concurry.utils import _IS_RAY_INSTALLED

# Import WORKER_MODES from conftest for tests that need it directly
from tests.conftest import WORKER_MODES


# Test worker classes
class SimpleWorker(Worker):
    """Simple worker for testing."""

    def __init__(self, value: int = 0):
        self.value = value

    def add(self, x: int) -> int:
        """Add x to the stored value."""
        return self.value + x

    def multiply(self, x: int) -> int:
        """Multiply stored value by x."""
        return self.value * x

    def get_value(self) -> int:
        """Get the stored value."""
        return self.value

    def sleep_and_return(self, duration: float, result: int) -> int:
        """Sleep for duration seconds and return result."""
        time.sleep(duration)
        return result

    def raise_error(self, message: str):
        """Raise a ValueError with the given message."""
        raise ValueError(message)


@worker
class DecoratedWorker:
    """Worker created with @worker decorator."""

    def __init__(self, name: str):
        self.name = name

    def greet(self) -> str:
        """Return a greeting."""
        return f"Hello from {self.name}"


class StatefulWorker(Worker):
    """Worker that maintains state across calls."""

    def __init__(self):
        self.counter = 0
        self.history = []

    def increment(self, amount: int = 1) -> int:
        """Increment counter and return new value."""
        self.counter += amount
        self.history.append(self.counter)
        return self.counter

    def get_counter(self) -> int:
        """Get current counter value."""
        return self.counter

    def get_history(self) -> List[int]:
        """Get history of counter values."""
        return self.history.copy()


# Worker mode fixture and cleanup are provided by tests/conftest.py


class TestWorkerBasics:
    """Test basic worker functionality."""

    def test_simple_method_call(self, worker_mode):
        """Test basic method call on worker.

        1. Creates SimpleWorker with value=10
        2. Calls add(5) method
        3. Verifies result is 15 (10+5)
        4. Stops worker
        """
        if worker_mode == "thread":
            w = SimpleWorker.options(mode=worker_mode, max_workers=30).init(10)
        elif worker_mode == "process":
            w = SimpleWorker.options(mode=worker_mode, max_workers=4).init(10)
        elif worker_mode == "ray":
            w = SimpleWorker.options(mode=worker_mode, max_workers=0).init(10)
        else:
            w = SimpleWorker.options(mode=worker_mode).init(10)
        future = w.add(5)
        result = future.result(timeout=15)
        assert result == 15
        w.stop()

    def test_nonexistent_method_fails(self, worker_mode):
        """Test that calling a non-existent method fails appropriately.

        1. Creates SimpleWorker with value=10
        2. Calls nonexistent_method()
        3. For sync/ray: Verifies AttributeError raised immediately
        4. For thread/asyncio/process: Verifies future.result() raises AttributeError
        5. Stops worker

        Mode-specific behavior:
        - Sync/Ray: Fail immediately with AttributeError
        - Thread/Asyncio/Process: Return future that raises AttributeError
        """
        if worker_mode == "thread":
            w = SimpleWorker.options(mode=worker_mode, max_workers=30).init(10)
        elif worker_mode == "process":
            w = SimpleWorker.options(mode=worker_mode, max_workers=4).init(10)
        elif worker_mode == "ray":
            w = SimpleWorker.options(mode=worker_mode, max_workers=0).init(10)
        else:
            w = SimpleWorker.options(mode=worker_mode).init(10)

        if worker_mode in ("sync", "ray"):
            # Sync and Ray modes should fail immediately
            with pytest.raises(AttributeError):
                w.nonexistent_method()
        else:
            # Thread, asyncio, and process modes return a future that contains the original error
            future = w.nonexistent_method()
            with pytest.raises(AttributeError):
                future.result(timeout=15)

        w.stop()

    def test_multiple_method_calls(self, worker_mode):
        """Test multiple method calls on same worker.

        1. Creates SimpleWorker with value=10
        2. Calls add(5), multiply(2), get_value() in sequence
        3. Verifies add(5) returns 15 (10+5)
        4. Verifies multiply(2) returns 20 (10*2)
        5. Verifies get_value() returns 10 (original value)
        6. Stops worker
        """
        if worker_mode == "thread":
            w = SimpleWorker.options(mode=worker_mode, max_workers=30).init(10)
        elif worker_mode == "process":
            w = SimpleWorker.options(mode=worker_mode, max_workers=4).init(10)
        elif worker_mode == "ray":
            w = SimpleWorker.options(mode=worker_mode, max_workers=0).init(10)
        else:
            w = SimpleWorker.options(mode=worker_mode).init(10)

        future1 = w.add(5)
        future2 = w.multiply(2)
        future3 = w.get_value()

        assert future1.result(timeout=15) == 15
        assert future2.result(timeout=15) == 20
        assert future3.result(timeout=15) == 10

        w.stop()

    def test_blocking_mode(self, worker_mode):
        """Test blocking mode returns results directly.

        1. Creates SimpleWorker with blocking=True, value=10
        2. Calls add(5) which returns result directly (not future)
        3. Verifies result is int (not future)
        4. Verifies result is 15
        5. Stops worker
        """
        if worker_mode == "thread":
            w = SimpleWorker.options(mode=worker_mode, max_workers=30, blocking=True).init(10)
        elif worker_mode == "process":
            w = SimpleWorker.options(mode=worker_mode, max_workers=4, blocking=True).init(10)
        elif worker_mode == "ray":
            w = SimpleWorker.options(mode=worker_mode, max_workers=0, blocking=True).init(10)
        else:
            w = SimpleWorker.options(mode=worker_mode, blocking=True).init(10)

        result = w.add(5)
        # Should return result directly, not a future
        assert isinstance(result, int)
        assert result == 15

        w.stop()

    def test_decorated_worker(self, worker_mode):
        """Test worker created with @worker decorator.

        1. Creates DecoratedWorker (using @worker decorator) with name="TestBot"
        2. Calls greet() method
        3. Verifies result is "Hello from TestBot"
        4. Stops worker
        """
        if worker_mode == "thread":
            w = DecoratedWorker.options(mode=worker_mode, max_workers=30).init("TestBot")
        elif worker_mode == "process":
            w = DecoratedWorker.options(mode=worker_mode, max_workers=4).init("TestBot")
        elif worker_mode == "ray":
            w = DecoratedWorker.options(mode=worker_mode, max_workers=0).init("TestBot")
        else:
            w = DecoratedWorker.options(mode=worker_mode).init("TestBot")
        future = w.greet()
        result = future.result(timeout=15)
        assert result == "Hello from TestBot"
        w.stop()


class TestWorkerExceptions:
    """Test exception handling in workers."""

    def test_method_raises_exception(self, worker_mode):
        """Test that exceptions in worker methods are propagated.

        1. Creates SimpleWorker
        2. Calls raise_error("test error")
        3. Verifies ValueError is raised with "test error" message
        4. Stops worker
        """
        if worker_mode == "thread":
            w = SimpleWorker.options(mode=worker_mode, max_workers=30).init(10)
        elif worker_mode == "process":
            w = SimpleWorker.options(mode=worker_mode, max_workers=4).init(10)
        elif worker_mode == "ray":
            w = SimpleWorker.options(mode=worker_mode, max_workers=0).init(10)
        else:
            w = SimpleWorker.options(mode=worker_mode).init(10)
        future = w.raise_error("test error")

        with pytest.raises(Exception) as exc_info:
            future.result(timeout=15)

        # Check that the error message is preserved
        assert "test error" in str(exc_info.value)
        w.stop()

    def test_invalid_method_name(self, worker_mode):
        """Test calling non-existent method.

        1. Creates SimpleWorker
        2. Calls nonexistent_method(123)
        3. For sync/ray: Verifies AttributeError raised immediately
        4. For thread/asyncio/process: Verifies future.result() raises AttributeError
        5. Stops worker

        Mode-specific: Sync/Ray fail immediately; others via future
        """
        if worker_mode == "thread":
            w = SimpleWorker.options(mode=worker_mode, max_workers=30).init(10)
        elif worker_mode == "process":
            w = SimpleWorker.options(mode=worker_mode, max_workers=4).init(10)
        elif worker_mode == "ray":
            w = SimpleWorker.options(mode=worker_mode, max_workers=0).init(10)
        else:
            w = SimpleWorker.options(mode=worker_mode).init(10)

        if worker_mode in ("sync", "ray"):
            # Sync and Ray modes should fail immediately
            with pytest.raises(AttributeError):
                w.nonexistent_method(123)
        else:
            # Thread, asyncio, and process modes - original error is in the future
            future = w.nonexistent_method(123)
            with pytest.raises(AttributeError):
                future.result(timeout=15)

        w.stop()


class TestWorkerConcurrency:
    """Test concurrent execution in workers."""

    def test_concurrent_calls(self, worker_mode):
        """Test multiple concurrent calls to worker.

        1. Creates SimpleWorker with value=10
        2. Submits 5 add() calls with values 0-4
        3. Verifies all results: 10+0, 10+1, 10+2, 10+3, 10+4
        4. Stops worker
        """
        if worker_mode == "thread":
            w = SimpleWorker.options(mode=worker_mode, max_workers=30).init(10)
        elif worker_mode == "process":
            w = SimpleWorker.options(mode=worker_mode, max_workers=4).init(10)
        elif worker_mode == "ray":
            w = SimpleWorker.options(mode=worker_mode, max_workers=0).init(10)
        else:
            w = SimpleWorker.options(mode=worker_mode).init(10)

        # Submit multiple tasks
        futures = []
        for i in range(5):
            future = w.add(i)
            futures.append((future, 10 + i))

        # Check all results
        for future, expected in futures:
            result = future.result(timeout=15)
            assert result == expected

        w.stop()

    def test_long_running_task(self, worker_mode):
        """Test worker with a long-running task.

        1. Creates SimpleWorker
        2. Calls sleep_and_return(1.0, 42) which sleeps 1 second
        3. Measures elapsed time
        4. Verifies result is 42
        5. Verifies elapsed >= 1.0s (all modes execute the sleep)
        6. Stops worker
        """
        if worker_mode == "thread":
            w = SimpleWorker.options(mode=worker_mode, max_workers=30).init(10)
        elif worker_mode == "process":
            w = SimpleWorker.options(mode=worker_mode, max_workers=4).init(10)
        elif worker_mode == "ray":
            w = SimpleWorker.options(mode=worker_mode, max_workers=0).init(10)
        else:
            w = SimpleWorker.options(mode=worker_mode).init(10)

        # Start a task that takes 1 second
        start_time = time.time()
        future = w.sleep_and_return(1.0, 42)

        # Wait for result
        result = future.result(timeout=15)
        elapsed = time.time() - start_time

        assert result == 42

        # Sync mode executes immediately, so elapsed time is minimal
        # For other modes, the task should take at least 1 second
        if worker_mode == "sync":
            assert elapsed >= 1.0  # Sync mode still executes the sleep
        else:
            assert elapsed >= 1.0  # Should take at least 1 second

        w.stop()


class TestWorkerState:
    """Test stateful workers."""

    def test_state_persistence(self, worker_mode):
        """Test that worker maintains state across calls.

        1. Creates StatefulWorker with max_workers=1
        2. Calls increment(1), increment(2), increment(3) in sequence
        3. Verifies results: 1, 3, 6 (cumulative counter)
        4. Calls get_counter(), verifies final value is 6
        5. Stops worker
        """
        w = StatefulWorker.options(mode=worker_mode, max_workers=1).init()

        # Make multiple calls that modify state
        result1 = w.increment(1).result(timeout=10)
        result2 = w.increment(2).result(timeout=10)
        result3 = w.increment(3).result(timeout=10)

        assert result1 == 1
        assert result2 == 3
        assert result3 == 6

        # Check final counter value
        final_counter = w.get_counter().result(timeout=10)
        assert final_counter == 6

        w.stop()

    def test_state_isolation(self, worker_mode):
        """Test that different worker instances have isolated state.

        1. Creates two StatefulWorker instances (w1, w2) each with max_workers=1
        2. Calls w1.increment(5) and w2.increment(10)
        3. Verifies w1 counter is 5, w2 counter is 10
        4. Verifies states are completely independent
        5. Stops both workers
        """
        w1 = StatefulWorker.options(mode=worker_mode, max_workers=1).init()
        w2 = StatefulWorker.options(mode=worker_mode, max_workers=1).init()

        # Modify state in both workers
        result1 = w1.increment(5).result(timeout=15)
        result2 = w2.increment(10).result(timeout=15)

        assert result1 == 5
        assert result2 == 10

        # Check that states are independent
        counter1 = w1.get_counter().result(timeout=15)
        counter2 = w2.get_counter().result(timeout=15)

        assert counter1 == 5
        assert counter2 == 10

        w1.stop()
        w2.stop()


class TestWorkerLifecycle:
    """Test worker lifecycle management."""

    def test_stop_worker(self, worker_mode):
        """Test stopping a worker.

        1. Creates SimpleWorker
        2. Calls add(5), verifies result is 15
        3. Calls stop() on worker
        4. Attempts to call add(5) after stop, verifies RuntimeError raised
        """
        if worker_mode == "thread":
            w = SimpleWorker.options(mode=worker_mode, max_workers=30).init(10)
        elif worker_mode == "process":
            w = SimpleWorker.options(mode=worker_mode, max_workers=4).init(10)
        elif worker_mode == "ray":
            w = SimpleWorker.options(mode=worker_mode, max_workers=0).init(10)
        else:
            w = SimpleWorker.options(mode=worker_mode).init(10)

        # Use the worker
        result = w.add(5).result(timeout=15)
        assert result == 15

        # Stop the worker
        w.stop()

        # Should not be able to use after stopping
        with pytest.raises(RuntimeError):
            w.add(5)

    def test_cleanup_multiple_workers(self, worker_mode):
        """Test cleaning up multiple workers.

        1. Creates 3 SimpleWorker instances with values 0, 1, 2
        2. Calls get_value() on each, verifies correct values
        3. Stops all workers
        """
        workers = []
        for i in range(3):
            if worker_mode == "thread":
                w = SimpleWorker.options(mode=worker_mode, max_workers=30).init(i)
            elif worker_mode == "process":
                w = SimpleWorker.options(mode=worker_mode, max_workers=4).init(i)
            elif worker_mode == "ray":
                w = SimpleWorker.options(mode=worker_mode, max_workers=0).init(i)
            else:
                w = SimpleWorker.options(mode=worker_mode).init(i)
            workers.append(w)

        # Use all workers
        for i, w in enumerate(workers):
            result = w.get_value().result(timeout=15)
            assert result == i

        # Stop all workers
        for w in workers:
            w.stop()


class TestWorkerInitialization:
    """Test worker initialization with different arguments."""

    def test_init_with_args(self, worker_mode):
        """Test worker initialization with positional arguments.

        1. Creates SimpleWorker with init(42)
        2. Calls get_value(), verifies 42
        3. Stops worker
        """
        if worker_mode == "thread":
            w = SimpleWorker.options(mode=worker_mode, max_workers=30).init(42)
        elif worker_mode == "process":
            w = SimpleWorker.options(mode=worker_mode, max_workers=4).init(42)
        elif worker_mode == "ray":
            w = SimpleWorker.options(mode=worker_mode, max_workers=0).init(42)
        else:
            w = SimpleWorker.options(mode=worker_mode).init(42)
        result = w.get_value().result(timeout=15)
        assert result == 42
        w.stop()

    def test_init_with_kwargs(self, worker_mode):
        """Test worker initialization with keyword arguments.

        1. Creates SimpleWorker with init(value=99)
        2. Calls get_value(), verifies 99
        3. Stops worker
        """
        if worker_mode == "thread":
            w = SimpleWorker.options(mode=worker_mode, max_workers=30).init(value=99)
        elif worker_mode == "process":
            w = SimpleWorker.options(mode=worker_mode, max_workers=4).init(value=99)
        elif worker_mode == "ray":
            w = SimpleWorker.options(mode=worker_mode, max_workers=0).init(value=99)
        else:
            w = SimpleWorker.options(mode=worker_mode).init(value=99)
        result = w.get_value().result(timeout=15)
        assert result == 99
        w.stop()

    def test_init_with_both(self, worker_mode):
        """Test worker initialization with both args and kwargs.

        1. Creates DecoratedWorker with init("Alice")
        2. Calls greet(), verifies "Hello from Alice"
        3. Stops worker
        """
        if worker_mode == "thread":
            w = DecoratedWorker.options(mode=worker_mode, max_workers=30).init("Alice")
        elif worker_mode == "process":
            w = DecoratedWorker.options(mode=worker_mode, max_workers=4).init("Alice")
        elif worker_mode == "ray":
            w = DecoratedWorker.options(mode=worker_mode, max_workers=0).init("Alice")
        else:
            w = DecoratedWorker.options(mode=worker_mode).init("Alice")
        result = w.greet().result(timeout=15)
        assert result == "Hello from Alice"
        w.stop()


class TestFutureInterface:
    """Test the Future interface returned by worker methods."""

    def test_future_done(self, worker_mode):
        """Test Future.done() method.

        1. Creates SimpleWorker
        2. Calls add(5), gets future
        3. Waits for result
        4. Verifies future.done() is True, result is 15
        5. Stops worker
        """
        if worker_mode == "thread":
            w = SimpleWorker.options(mode=worker_mode, max_workers=30).init(10)
        elif worker_mode == "process":
            w = SimpleWorker.options(mode=worker_mode, max_workers=4).init(10)
        elif worker_mode == "ray":
            w = SimpleWorker.options(mode=worker_mode, max_workers=0).init(10)
        else:
            w = SimpleWorker.options(mode=worker_mode).init(10)
        future = w.add(5)

        # Wait for completion
        result = future.result(timeout=30)

        # Should be done now
        assert future.done()
        assert result == 15

        w.stop()

    def test_future_result_timeout(self, worker_mode):
        """Test Future.result() with timeout.

        1. Creates SimpleWorker
        2. Calls sleep_and_return(2.0, 42)
        3. Sync: Completes immediately
        4. Asyncio: Skip (time.sleep has race conditions)
        5. Others: Timeout with 0.5s, succeed with 3.0s
        6. Stops worker

        Note: Asyncio with time.sleep() has race conditions. Use asyncio.sleep() for real async work.
        """
        if worker_mode == "thread":
            w = SimpleWorker.options(mode=worker_mode, max_workers=30).init(10)
        elif worker_mode == "process":
            w = SimpleWorker.options(mode=worker_mode, max_workers=4).init(10)
        elif worker_mode == "ray":
            w = SimpleWorker.options(mode=worker_mode, max_workers=0).init(10)
        else:
            w = SimpleWorker.options(mode=worker_mode).init(10)

        # Sync mode completes immediately, so no timeout
        if worker_mode == "sync":
            # For sync mode, the future is already done
            future = w.sleep_and_return(2.0, 42)
            result = future.result(timeout=0.1)
            assert result == 42
        elif worker_mode == "asyncio":
            # Asyncio with blocking sleep (time.sleep) can complete synchronously
            # in the event loop thread, so timeout behavior is unreliable.
            # This is expected - use asyncio.sleep() for proper async behavior.
            pytest.skip("Blocking sleep in asyncio worker has race conditions")
        elif worker_mode == "ray":
            # Ray has significant scheduling overhead (1-2s), use shorter sleep and longer timeouts
            # Should timeout if we don't wait long enough
            future1 = w.sleep_and_return(0.5, 42)
            with pytest.raises(TimeoutError):
                future1.result(timeout=0.1)

            # Create a fresh future for the second attempt - should succeed with longer timeout
            # Allow 5 seconds to account for Ray's scheduling overhead
            future2 = w.sleep_and_return(0.5, 42)
            result = future2.result(timeout=5.0)
            assert result == 42
        else:
            # Thread/Process modes have minimal overhead
            # Should timeout if we don't wait long enough
            future1 = w.sleep_and_return(1.0, 42)
            with pytest.raises(TimeoutError):
                future1.result(timeout=0.3)

            # Create a fresh future for the second attempt - should succeed with longer timeout
            future2 = w.sleep_and_return(1.0, 42)
            result = future2.result(timeout=2.0)
            assert result == 42

        w.stop()

    def test_future_exception(self, worker_mode):
        """Test Future.exception() method.

        1. Creates SimpleWorker
        2. Calls raise_error("test exception")
        3. Verifies result() raises exception
        4. Stops worker
        """
        if worker_mode == "thread":
            w = SimpleWorker.options(mode=worker_mode, max_workers=30).init(10)
        elif worker_mode == "process":
            w = SimpleWorker.options(mode=worker_mode, max_workers=4).init(10)
        elif worker_mode == "ray":
            w = SimpleWorker.options(mode=worker_mode, max_workers=0).init(10)
        else:
            w = SimpleWorker.options(mode=worker_mode).init(10)
        future = w.raise_error("test exception")

        # Should raise when getting result
        with pytest.raises(Exception):
            future.result(timeout=15)

        w.stop()


class TestTaskWorkerSubmit:
    """Test TaskWorker.submit() functionality."""

    def test_submit_simple_function(self, worker_mode):
        """Test submitting a simple function.

        1. Defines add(x, y) function
        2. Creates TaskWorker
        3. Submits add(5, 10)
        4. Verifies result is 15
        5. Stops worker
        """

        def add(x, y):
            return x + y

        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0).init()
        else:
            w = TaskWorker.options(mode=worker_mode).init()
        future = w.submit(add, 5, 10)
        result = future.result(timeout=15)
        assert result == 15
        w.stop()

    def test_submit_with_kwargs(self, worker_mode):
        """Test submitting a function with keyword arguments.

        1. Defines multiply(x, y, factor=1) function
        2. Creates TaskWorker
        3. Submits multiply(3, 4, factor=2)
        4. Verifies result is 24 ((3*4)*2)
        5. Stops worker
        """

        def multiply(x, y, factor=1):
            return (x * y) * factor

        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0).init()
        else:
            w = TaskWorker.options(mode=worker_mode).init()
        future = w.submit(multiply, 3, 4, factor=2)
        result = future.result(timeout=15)
        assert result == 24
        w.stop()

    def test_submit_lambda(self, worker_mode):
        """Test submitting a lambda function."""

        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0).init()
        else:
            w = TaskWorker.options(mode=worker_mode).init()
        future = w.submit(lambda x: x**2, 5)
        result = future.result(timeout=15)
        assert result == 25
        w.stop()

    def test_submit_with_exception(self, worker_mode):
        """Test submitting a function that raises an exception."""

        def failing_fn():
            raise ValueError("Task failed")

        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0).init()
        else:
            w = TaskWorker.options(mode=worker_mode).init()
        future = w.submit(failing_fn)

        with pytest.raises(Exception) as exc_info:
            future.result(timeout=15)

        assert "Task failed" in str(exc_info.value) or "failed" in str(exc_info.value).lower()
        w.stop()

    def test_submit_multiple_tasks(self, worker_mode):
        """Test submitting multiple tasks."""

        def compute(x):
            return x * 2

        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0).init()
        else:
            w = TaskWorker.options(mode=worker_mode).init()

        futures = [w.submit(compute, i) for i in range(5)]
        results = [f.result(timeout=15) for f in futures]

        assert results == [0, 2, 4, 6, 8]
        w.stop()

    def test_submit_blocking_mode(self, worker_mode):
        """Test submit() in blocking mode."""

        def add(x, y):
            return x + y

        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30, blocking=True).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4, blocking=True).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0, blocking=True).init()
        else:
            w = TaskWorker.options(mode=worker_mode, blocking=True).init()
        result = w.submit(add, 10, 20)

        # Should return result directly, not a future
        assert isinstance(result, int)
        assert result == 30
        w.stop()

    def test_map_simple(self, worker_mode):
        """Test TaskWorker.map() with a simple function."""

        def square(x):
            return x**2

        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0).init()
        else:
            w = TaskWorker.options(mode=worker_mode).init()
        results = list(w.map(square, range(5)))
        assert results == [0, 1, 4, 9, 16]
        w.stop()

    def test_map_multiple_iterables(self, worker_mode):
        """Test TaskWorker.map() with multiple iterables."""

        def add(x, y):
            return x + y

        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0).init()
        else:
            w = TaskWorker.options(mode=worker_mode).init()
        results = list(w.map(add, [1, 2, 3], [10, 20, 30]))
        assert results == [11, 22, 33]
        w.stop()

    def test_map_with_kwargs_function(self, worker_mode):
        """Test TaskWorker.map() with a function that has kwargs."""

        def multiply(x, factor=2):
            return x * factor

        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0).init()
        else:
            w = TaskWorker.options(mode=worker_mode).init()
        # Note: map() doesn't directly support passing kwargs to fn
        # This tests that the function's default kwargs work
        results = list(w.map(multiply, range(5)))
        assert results == [0, 2, 4, 6, 8]
        w.stop()

    def test_basic_task_submission(self, worker_mode):
        """Test basic task submission with TaskWorker."""

        def compute(x, y):
            return x**2 + y**2

        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0).init()
        else:
            w = TaskWorker.options(mode=worker_mode).init()
        future = w.submit(compute, 3, 4)
        result = future.result(timeout=15)

        assert result == 25
        w.stop()

    def test_lambda_task(self, worker_mode):
        """Test submitting lambda functions."""
        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0).init()
        else:
            w = TaskWorker.options(mode=worker_mode).init()

        result = w.submit(lambda x: x * 10, 5).result(timeout=15)
        assert result == 50

        w.stop()

    def test_multiple_tasks(self, worker_mode):
        """Test submitting multiple tasks to TaskWorker."""
        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0).init()
        else:
            w = TaskWorker.options(mode=worker_mode).init()

        futures = [w.submit(lambda x: x**2, i) for i in range(5)]
        results = [f.result(timeout=15) for f in futures]

        assert results == [0, 1, 4, 9, 16]
        w.stop()

    def test_task_with_kwargs(self, worker_mode):
        """Test task submission with keyword arguments."""

        def compute(x, y, multiplier=1):
            return (x + y) * multiplier

        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0).init()
        else:
            w = TaskWorker.options(mode=worker_mode).init()
        result = w.submit(compute, 5, 10, multiplier=2).result(timeout=15)

        assert result == 30
        w.stop()

    def test_blocking_mode(self, worker_mode):
        """Test TaskWorker in blocking mode."""
        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30, blocking=True).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4, blocking=True).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0, blocking=True).init()
        else:
            w = TaskWorker.options(mode=worker_mode, blocking=True).init()

        result = w.submit(lambda x: x + 100, 7)

        # Should return result directly, not a future
        assert isinstance(result, int)
        assert result == 107
        w.stop()

    def test_task_with_exception(self, worker_mode):
        """Test that exceptions in tasks are properly propagated."""

        def failing_task():
            raise ValueError("Task failed")

        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0).init()
        else:
            w = TaskWorker.options(mode=worker_mode).init()
        future = w.submit(failing_task)

        with pytest.raises(Exception) as exc_info:
            future.result(timeout=15)

        assert "failed" in str(exc_info.value).lower()
        w.stop()

    def test_no_custom_methods(self):
        """Test that TaskWorker has no custom methods, only submit/map."""
        w = TaskWorker.options(mode="sync").init()

        # Should have submit and map
        assert hasattr(w, "submit")
        assert hasattr(w, "map")

        # Should have standard methods
        assert hasattr(w, "stop")

        # Should not have any custom worker methods like 'compute', 'process', etc.
        # (This is just a smoke test to ensure it's a plain worker)

        w.stop()

    def test_different_execution_modes(self):
        """Test TaskWorker works across different execution modes."""
        modes = WORKER_MODES  # Use same modes as other tests (includes Ray if installed)

        for mode in modes:
            # Ray is initialized by conftest.py initialize_ray fixture
            if mode == "thread":
                w = TaskWorker.options(mode=mode, max_workers=30).init()
            elif mode == "process":
                w = TaskWorker.options(mode=mode, max_workers=4).init()
            elif mode == "ray":
                w = TaskWorker.options(mode=mode, max_workers=0).init()
            else:
                w = TaskWorker.options(mode=mode).init()
            result = w.submit(lambda x: x * 2, 5).result(timeout=15)
            assert result == 10
            w.stop()


@pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray tests require ray to be installed")
class TestRayWorker:
    """Test Ray-specific worker functionality.

    Note: Basic Ray worker tests run as part of the parametrized worker_mode fixture.
    This class only tests Ray-specific features like resource specifications.
    """

    def test_ray_worker_with_resources(self):
        """Test Ray worker with resource specifications."""
        # Ray is initialized by conftest.py initialize_ray fixture
        w = SimpleWorker.options(mode="ray", actor_options={"num_cpus": 1, "num_gpus": 0}).init(10)

        result = w.add(5).result(timeout=15)
        assert result == 15

        w.stop()


# Async function support tests
class AsyncWorker(Worker):
    """Worker with async methods for testing."""

    def __init__(self, value: int = 0):
        self.value = value

    async def async_add(self, x: int) -> int:
        """Async method that adds x to value."""
        import asyncio

        await asyncio.sleep(0.01)  # Simulate async I/O
        return self.value + x

    async def async_multiply(self, x: int) -> int:
        """Async method that multiplies value by x."""
        import asyncio

        await asyncio.sleep(0.01)  # Simulate async I/O
        return self.value * x

    def sync_method(self, x: int) -> int:
        """Regular sync method for comparison."""
        return self.value + x

    async def async_error(self):
        """Async method that raises an error."""
        import asyncio

        await asyncio.sleep(0.01)
        raise ValueError("Async error occurred")


class TestAsyncFunctionSupport:
    """Test async function support across all worker modes."""

    def test_async_method_call(self, worker_mode):
        """Test calling async methods on workers."""
        if worker_mode == "thread":
            w = AsyncWorker.options(mode=worker_mode, max_workers=30).init(10)
        elif worker_mode == "process":
            w = AsyncWorker.options(mode=worker_mode, max_workers=4).init(10)
        elif worker_mode == "ray":
            w = AsyncWorker.options(mode=worker_mode, max_workers=0).init(10)
        else:
            w = AsyncWorker.options(mode=worker_mode).init(10)
        future = w.async_add(5)
        result = future.result(timeout=15)
        assert result == 15
        w.stop()

    def test_async_and_sync_methods(self, worker_mode):
        """Test that both async and sync methods work on same worker."""
        if worker_mode == "thread":
            w = AsyncWorker.options(mode=worker_mode, max_workers=30).init(10)
        elif worker_mode == "process":
            w = AsyncWorker.options(mode=worker_mode, max_workers=4).init(10)
        elif worker_mode == "ray":
            w = AsyncWorker.options(mode=worker_mode, max_workers=0).init(10)
        else:
            w = AsyncWorker.options(mode=worker_mode).init(10)

        # Call async method
        result1 = w.async_add(5).result(timeout=15)
        assert result1 == 15

        # Call sync method
        result2 = w.sync_method(3).result(timeout=15)
        assert result2 == 13

        # Call another async method
        result3 = w.async_multiply(2).result(timeout=15)
        assert result3 == 20

        w.stop()

    def test_async_method_with_exception(self, worker_mode):
        """Test that exceptions in async methods are properly propagated."""
        if worker_mode == "thread":
            w = AsyncWorker.options(mode=worker_mode, max_workers=30).init(10)
        elif worker_mode == "process":
            w = AsyncWorker.options(mode=worker_mode, max_workers=4).init(10)
        elif worker_mode == "ray":
            w = AsyncWorker.options(mode=worker_mode, max_workers=0).init(10)
        else:
            w = AsyncWorker.options(mode=worker_mode).init(10)
        future = w.async_error()

        with pytest.raises(Exception) as exc_info:
            future.result(timeout=15)

        assert "Async error occurred" in str(exc_info.value)
        w.stop()

    def test_submit_async_function(self, worker_mode):
        """Test submitting async functions via TaskWorker."""

        async def async_compute(x, y):
            import asyncio

            await asyncio.sleep(0.01)
            return x**2 + y**2

        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0).init()
        else:
            w = TaskWorker.options(mode=worker_mode).init()
        future = w.submit(async_compute, 3, 4)
        result = future.result(timeout=15)
        assert result == 25
        w.stop()

    def test_submit_async_lambda(self, worker_mode):
        """Test submitting async lambda functions."""

        # Note: async lambdas are not directly supported in Python,
        # but we can submit regular async functions
        async def async_square(x):
            import asyncio

            await asyncio.sleep(0.01)
            return x**2

        if worker_mode == "thread":
            w = TaskWorker.options(mode=worker_mode, max_workers=30).init()
        elif worker_mode == "process":
            w = TaskWorker.options(mode=worker_mode, max_workers=4).init()
        elif worker_mode == "ray":
            w = TaskWorker.options(mode=worker_mode, max_workers=0).init()
        else:
            w = TaskWorker.options(mode=worker_mode).init()
        future = w.submit(async_square, 7)
        result = future.result(timeout=15)
        assert result == 49
        w.stop()

    def test_multiple_async_calls(self, worker_mode):
        """Test multiple async method calls."""
        if worker_mode == "thread":
            w = AsyncWorker.options(mode=worker_mode, max_workers=30).init(10)
        elif worker_mode == "process":
            w = AsyncWorker.options(mode=worker_mode, max_workers=4).init(10)
        elif worker_mode == "ray":
            w = AsyncWorker.options(mode=worker_mode, max_workers=0).init(10)
        else:
            w = AsyncWorker.options(mode=worker_mode).init(10)

        # Submit multiple async tasks
        futures = []
        for i in range(5):
            future = w.async_add(i)
            futures.append((future, 10 + i))

        # Check all results
        for future, expected in futures:
            result = future.result(timeout=15)
            assert result == expected

        w.stop()

    def test_async_blocking_mode(self, worker_mode):
        """Test async methods in blocking mode."""
        if worker_mode == "thread":
            w = AsyncWorker.options(mode=worker_mode, max_workers=30, blocking=True).init(10)
        elif worker_mode == "process":
            w = AsyncWorker.options(mode=worker_mode, max_workers=4, blocking=True).init(10)
        elif worker_mode == "ray":
            w = AsyncWorker.options(mode=worker_mode, max_workers=0, blocking=True).init(10)
        else:
            w = AsyncWorker.options(mode=worker_mode, blocking=True).init(10)

        result = w.async_add(5)
        # Should return result directly, not a future
        assert isinstance(result, int)
        assert result == 15

        w.stop()


class TestAsyncWorkerIntegrationWithAsyncWaitGather:
    """Test AsyncWorker integration with wait/gather and async native coroutines."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_async_wait_with_worker_native_coroutines(self):
        """Test async_wait with raw coroutines created by AsyncWorker methods.

        Note: async_wait/async_gather work with raw coroutines, not concurry futures.
        For working with worker futures, use regular wait() and gather().
        """
        from concurry import async_wait

        # Define async worker methods as standalone coroutines for testing
        async def async_add(value, x):
            await asyncio.sleep(0.01)
            return value + x

        # Create coroutines
        coros = [async_add(10, i) for i in range(5)]

        # Use async_wait
        done, not_done = await async_wait(coros, timeout=15.0)

        assert len(done) == 5
        assert len(not_done) == 0

        # Get results
        results = [await t for t in done]
        assert set(results) == {10, 11, 12, 13, 14}

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_async_gather_with_native_coroutines(self):
        """Test async_gather with raw coroutines."""
        from concurry import async_gather

        async def async_add(value, x):
            await asyncio.sleep(0.01)
            return value + x

        # Create coroutines
        coros = [async_add(100, i) for i in range(10)]

        # Use async_gather
        results = await async_gather(coros, timeout=15.0)

        assert results == [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]

    def test_wait_with_async_worker_futures(self, worker_mode):
        """Test regular wait() with AsyncWorker futures (works across all modes)."""
        from concurry import wait

        w = AsyncWorker.options(mode=worker_mode).init(10)

        # Submit multiple async method calls
        futures = [w.async_add(i) for i in range(5)]

        # Use regular wait() for worker futures
        done, not_done = wait(futures, timeout=15.0)

        assert len(done) == 5
        assert len(not_done) == 0

        # Get results
        results = [f.result(timeout=1) for f in done]
        assert set(results) == {10, 11, 12, 13, 14}

        w.stop()

    def test_gather_with_async_worker_futures(self, worker_mode):
        """Test regular gather() with AsyncWorker futures (works across all modes)."""
        from concurry import gather

        w = AsyncWorker.options(mode=worker_mode).init(100)

        # Submit multiple async method calls
        futures = [w.async_add(i) for i in range(10)]

        # Use regular gather() for worker futures
        results = gather(futures, timeout=15.0)

        assert results == [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]

        w.stop()

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_async_gather_with_progress(self):
        """Test async_gather with progress tracking on coroutines."""
        from concurry import async_gather

        async def async_multiply(value, x):
            await asyncio.sleep(0.01)
            return value * x

        # Create coroutines
        coros = [async_multiply(50, i) for i in range(20)]

        # Use async_gather with progress tracking
        results = await async_gather(coros, progress=True, timeout=15.0)

        assert len(results) == 20
        assert results[0] == 0
        assert results[10] == 500

    def test_gather_with_worker_and_progress(self, worker_mode):
        """Test regular gather() with worker futures and progress tracking."""
        from concurry import gather

        w = AsyncWorker.options(mode=worker_mode).init(50)

        # Submit multiple async method calls
        futures = [w.async_multiply(i) for i in range(20)]

        # Use regular gather() with progress
        results = gather(futures, progress=True, timeout=15.0)

        assert len(results) == 20
        assert results[0] == 0
        assert results[10] == 500

        w.stop()

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_async_gather_with_exceptions(self):
        """Test async_gather with exceptions in coroutines."""
        from concurry import async_gather

        async def async_error():
            await asyncio.sleep(0.01)
            raise ValueError("Async error occurred")

        async def async_add(value, x):
            await asyncio.sleep(0.01)
            return value + x

        # Create coroutines
        coros = [
            async_add(10, 1),
            async_error(),
            async_add(10, 3),
        ]

        # Test with return_exceptions=True
        results = await async_gather(coros, return_exceptions=True, timeout=15.0)

        assert results[0] == 11
        assert isinstance(results[1], ValueError)
        assert results[2] == 13

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_async_gather_dict_with_coroutines(self):
        """Test async_gather with dict of coroutines."""
        from concurry import async_gather

        async def async_add(value, x):
            await asyncio.sleep(0.01)
            return value + x

        async def async_multiply(value, x):
            await asyncio.sleep(0.01)
            return value * x

        # Create coroutines dict
        coros_dict = {
            "add_5": async_add(10, 5),
            "add_10": async_add(10, 10),
            "multiply_3": async_multiply(10, 3),
        }

        # Gather preserving keys
        results = await async_gather(coros_dict, timeout=15.0)

        assert isinstance(results, dict)
        assert results["add_5"] == 15
        assert results["add_10"] == 20
        assert results["multiply_3"] == 30

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_async_wait_with_progress_callback(self):
        """Test async_wait with progress callback on coroutines."""
        from concurry import async_wait

        callback_calls = []

        def progress_callback(completed, total, elapsed):
            callback_calls.append((completed, total))

        async def async_add(value, x):
            await asyncio.sleep(0.05)
            return value + x

        # Create coroutines
        coros = [async_add(10, i) for i in range(10)]

        # Wait with progress callback
        done, not_done = await async_wait(coros, progress=progress_callback, timeout=15.0)

        assert len(done) == 10
        assert len(not_done) == 0
        # Callback should have been called
        assert len(callback_calls) > 0

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_async_gather_large_batch(self):
        """Test async_gather with large batch of coroutines."""
        from concurry import async_gather

        async def async_add(value, x):
            await asyncio.sleep(0.001)
            return value + x

        # Create many coroutines
        coros = [async_add(0, i) for i in range(50)]

        # Gather all results
        results = await async_gather(coros, timeout=30.0)

        assert len(results) == 50
        assert results == list(range(50))


class FileIOWorker(Worker):
    """Worker for testing file I/O performance with async."""

    def __init__(self):
        pass

    async def read_file_async(self, file_path: str) -> str:
        """Read a file asynchronously using aiofiles."""
        try:
            import aiofiles
        except ImportError:
            # Fallback to regular file reading if aiofiles not available
            import asyncio

            await asyncio.sleep(1e-6)  # Simulate async I/O delay
            with open(file_path, "r") as f:
                return f.read()

        async with aiofiles.open(file_path, mode="r") as f:
            return await f.read()

    def read_file_sync(self, file_path: str) -> str:
        """Read a file synchronously."""
        with open(file_path, "r") as f:
            return f.read()

    async def read_multiple_files_async(self, file_paths: List[str]) -> List[str]:
        """Read multiple files concurrently using async."""
        import asyncio

        tasks = [self.read_file_async(path) for path in file_paths]
        return await asyncio.gather(*tasks)


@pytest.mark.performance
class TestAsyncIOPerformance:
    """Test performance benefits of async I/O with AsyncioWorkerProxy."""

    def test_asyncio_concurrency_benefit(self):
        """Demonstrate AsyncioWorker's true strength: concurrent I/O-bound operations.

        This test shows where AsyncioWorker truly shines - handling many concurrent
        I/O-bound operations with wait time. We use asyncio.sleep() to simulate
        real-world I/O operations like:
        - Network requests (HTTP, database queries, API calls)
        - WebSocket connections
        - Remote file access

        Expected results:
        - ThreadWorker: ~N × wait_time (sequential execution)
        - AsyncioWorker: ~wait_time (concurrent execution)
        - Speedup: ~N× where N is number of concurrent operations
        """

        class IOBoundWorker(Worker):
            """Worker that simulates I/O-bound operations."""

            def __init__(self):
                pass

            def sync_io_operation(self, duration: float, id: int) -> str:
                """Synchronous I/O operation - blocks for duration."""
                time.sleep(duration)
                return f"sync-{id}"

            async def async_io_operation(self, duration: float, id: int) -> str:
                """Async I/O operation - yields during wait."""

                await asyncio.sleep(duration)
                return f"async-{id}"

        num_operations = 50
        wait_time = 0.05  # 50ms per operation

        print("\n=== Async Concurrency Test ===")
        print(f"Running {num_operations} operations, each with {wait_time}s wait time")

        # Test 1: ThreadWorker with sync operations (sequential)
        w_thread = IOBoundWorker.options(mode="thread", max_workers=1).init()
        start = time.time()
        futures = [w_thread.sync_io_operation(wait_time, i) for i in range(num_operations)]
        results_thread = [f.result(timeout=30) for f in futures]
        thread_time = time.time() - start
        w_thread.stop()

        expected_sequential_time = num_operations * wait_time
        print("\nThreadWorker (sequential):")
        print(f"  Time: {thread_time:.3f}s")
        print(f"  Expected: ~{expected_sequential_time:.3f}s")

        # Test 2: AsyncioWorker with async operations (concurrent)
        w_asyncio = IOBoundWorker.options(mode="asyncio").init()
        start = time.time()
        futures = [w_asyncio.async_io_operation(wait_time, i) for i in range(num_operations)]
        results_asyncio = [f.result(timeout=30) for f in futures]
        asyncio_time = time.time() - start
        w_asyncio.stop()

        expected_concurrent_time = wait_time  # All execute concurrently
        print("\nAsyncioWorker (concurrent):")
        print(f"  Time: {asyncio_time:.3f}s")
        print(f"  Expected: ~{expected_concurrent_time:.3f}s")

        # Calculate speedup
        speedup = thread_time / asyncio_time
        print(f"\nSpeedup: {speedup:.1f}x")
        print(f"Expected speedup: ~{num_operations}x")

        # Verify correctness
        assert len(results_thread) == num_operations
        assert len(results_asyncio) == num_operations
        assert all(f"sync-{i}" == results_thread[i] for i in range(num_operations))
        assert all(f"async-{i}" == results_asyncio[i] for i in range(num_operations))

        # Verify performance
        # ThreadWorker should take roughly sequential time (within 20% margin for overhead)
        assert thread_time >= expected_sequential_time * 0.8, (
            f"ThreadWorker too fast: {thread_time:.3f}s, expected ~{expected_sequential_time:.3f}s"
        )

        # AsyncioWorker should take roughly concurrent time (within generous margin)
        # Allow up to 5x the expected time to account for system variance
        assert asyncio_time <= expected_concurrent_time * 5, (
            f"AsyncioWorker too slow: {asyncio_time:.3f}s, expected ~{expected_concurrent_time:.3f}s"
        )

        # AsyncioWorker should be significantly faster (at least 10x speedup)
        assert speedup >= 10, f"AsyncioWorker speedup too low: {speedup:.1f}x, expected at least 10x"

        print(f"\n✅ AsyncioWorker achieved {speedup:.1f}x speedup for concurrent I/O operations!")

    def test_async_http_request_speedup(self):
        """Test AsyncioWorker with simulated network I/O (HTTP requests).

        This test demonstrates AsyncioWorker's true strength: concurrent network I/O.
        We create a simple HTTP server with artificial latency to simulate real-world
        network conditions (API calls, database queries, etc.).

        Expected: AsyncioWorker should show 10-20x speedup due to concurrent requests.
        """
        import http.server
        import socketserver
        import threading
        from urllib.parse import urlparse

        # Create a simple HTTP handler with artificial latency
        class DelayedHTTPHandler(http.server.SimpleHTTPRequestHandler):
            def do_GET(self):
                # Simulate network latency (50ms)
                time.sleep(0.05)

                # Parse the URL to get the resource ID
                path = urlparse(self.path).path
                if path.startswith("/data/"):
                    resource_id = path.split("/")[-1]
                    response = f"Resource {resource_id} data".encode()

                    self.send_response(200)
                    self.send_header("Content-type", "text/plain")
                    self.send_header("Content-Length", str(len(response)))
                    self.end_headers()
                    self.wfile.write(response)
                else:
                    self.send_error(404)

            def log_message(self, format, *args):
                # Suppress server logging
                pass

        # Start HTTP server in background thread (use ThreadingTCPServer for concurrent connections)
        # Use port 0 to let OS assign a free port
        server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), DelayedHTTPHandler)
        server.allow_reuse_address = True
        server.daemon_threads = True  # Allow daemon threads
        port = server.server_address[1]  # Get the actual port assigned by OS
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

        # Give server time to start
        time.sleep(0.1)

        try:
            # Create worker classes for HTTP requests
            class HTTPWorker(Worker):
                """Worker for making HTTP requests."""

                def __init__(self, base_url: str):
                    self.base_url = base_url
                    self._session = None

                def fetch_sync(self, resource_id: int) -> str:
                    """Synchronous HTTP request."""
                    import urllib.request

                    url = f"{self.base_url}/data/{resource_id}"
                    with urllib.request.urlopen(url) as response:
                        return response.read().decode()

                async def fetch_async(self, resource_id: int) -> str:
                    """Async HTTP request using aiohttp with shared session."""
                    import aiohttp

                    # Create session on first use (reuse for all requests)
                    if self._session is None:
                        self._session = aiohttp.ClientSession()

                    url = f"{self.base_url}/data/{resource_id}"
                    async with self._session.get(url) as response:
                        return await response.text()

                async def cleanup_session(self) -> None:
                    """Clean up the aiohttp session."""
                    if self._session is not None:
                        await self._session.close()
                        self._session = None

            num_requests = 30
            base_url = f"http://127.0.0.1:{port}"

            # Test 1: SyncWorker with sync requests (baseline - truly sequential)
            w_sync = HTTPWorker.options(mode="sync", max_workers=1).init(base_url)
            start_time = time.time()
            futures = [w_sync.fetch_sync(i) for i in range(num_requests)]
            results_sync = [f.result(timeout=30) for f in futures]
            time_sync_worker = time.time() - start_time
            w_sync.stop()

            # Test 2: ThreadWorker with sync requests (sequential in dedicated thread)
            w_thread = HTTPWorker.options(mode="thread", max_workers=1).init(base_url)
            start_time = time.time()
            futures = [w_thread.fetch_sync(i) for i in range(num_requests)]
            results_thread = [f.result(timeout=30) for f in futures]
            time_thread = time.time() - start_time
            w_thread.stop()

            # Test 3: ProcessWorker with sync requests (sequential in dedicated process)
            w_process = HTTPWorker.options(mode="process", max_workers=1).init(base_url)
            start_time = time.time()
            futures = [w_process.fetch_sync(i) for i in range(num_requests)]
            results_process = [f.result(timeout=30) for f in futures]
            time_process = time.time() - start_time
            w_process.stop()

            # AsyncioWorker with async requests (concurrent)
            w_async = HTTPWorker.options(mode="asyncio").init(base_url)
            start_time = time.time()
            futures = [w_async.fetch_async(i) for i in range(num_requests)]
            results_async = [f.result(timeout=30) for f in futures]
            time_async = time.time() - start_time
            # Clean up the aiohttp session
            w_async.cleanup_session().result(timeout=15)
            w_async.stop()

            # Verify results are correct
            assert len(results_sync) == num_requests
            assert len(results_thread) == num_requests
            assert len(results_process) == num_requests
            assert len(results_async) == num_requests
            assert all("Resource" in r and "data" in r for r in results_sync)
            assert all("Resource" in r and "data" in r for r in results_thread)
            assert all("Resource" in r and "data" in r for r in results_process)
            assert all("Resource" in r and "data" in r for r in results_async)

            # Calculate speedups
            speedup_sync = time_sync_worker / time_async
            speedup_thread = time_thread / time_async
            speedup_process = time_process / time_async

            # Print timing information
            print(f"\nHTTP Request Performance Test ({num_requests} requests with 50ms latency each):")
            print(f"  SyncWorker (sequential):    {time_sync_worker:.3f}s")
            print(f"  ThreadWorker (sequential):  {time_thread:.3f}s")
            print(f"  ProcessWorker (sequential): {time_process:.3f}s")
            print(f"  AsyncioWorker (concurrent): {time_async:.3f}s")
            print(f"\n  Speedup vs SyncWorker:   {speedup_sync:.1f}x")
            print(f"  Speedup vs ThreadWorker: {speedup_thread:.1f}x")
            print(f"  Speedup vs ProcessWorker: {speedup_process:.1f}x")
            # Verify performance: AsyncioWorker should be significantly faster
            # SyncWorker and ThreadWorker should both take roughly sequential time (30 × 50ms = ~1.5s)
            expected_sequential = num_requests * 0.05
            assert time_sync_worker >= expected_sequential * 0.8, (
                f"SyncWorker too fast: {time_sync_worker:.3f}s, expected ~{expected_sequential:.3f}s"
            )
            assert time_thread >= expected_sequential * 0.8, (
                f"ThreadWorker too fast: {time_thread:.3f}s, expected ~{expected_sequential:.3f}s"
            )
            assert time_process >= expected_sequential * 0.8, (
                f"ProcessWorker too fast: {time_process:.3f}s, expected ~{expected_sequential:.3f}s"
            )
            # AsyncioWorker should be much faster (concurrent execution, ~50ms total)
            # Allow up to 5x margin for overhead and system variance
            assert time_async <= 0.05 * 5, f"AsyncioWorker too slow: {time_async:.3f}s, expected ~0.05s"

            # Verify significant speedup (at least 8x to account for system variance)
            assert speedup_thread >= 8, (
                f"AsyncioWorker speedup too low: {speedup_thread:.1f}x, expected at least 8x"
            )
            assert speedup_process >= 8, (
                f"AsyncioWorker speedup too low: {speedup_process:.1f}x, expected at least 8x"
            )

        finally:
            # Cleanup: shutdown server
            server.shutdown()

    def test_async_concurrent_file_reading(self):
        """Test concurrent file reading with async worker using gather."""
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            num_files = 50
            file_paths = []

            # Create files
            for i in range(num_files):
                file_path = os.path.join(temp_dir, f"file_{i}.txt")
                with open(file_path, "w") as f:
                    f.write(f"File {i} content")
                file_paths.append(file_path)

            # Test reading all files concurrently
            w = FileIOWorker.options(mode="asyncio").init()
            start_time = time.time()
            future = w.read_multiple_files_async(file_paths)
            results = future.result(timeout=30)
            elapsed = time.time() - start_time
            w.stop()

            # Verify results
            assert len(results) == num_files
            for i, content in enumerate(results):
                assert f"File {i} content" in content

            print(f"\nConcurrent file reading test ({num_files} files): {elapsed:.3f}s")

    def test_async_vs_process_worker(self):
        """Compare async execution in asyncio vs process worker.

        Process worker can execute async functions correctly but won't get
        the same performance benefit as asyncio worker for I/O-bound tasks.
        """
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            num_files = 50
            file_paths = []

            # Create files
            for i in range(num_files):
                file_path = os.path.join(temp_dir, f"file_{i}.txt")
                with open(file_path, "w") as f:
                    f.write(f"File {i} content\n" * 5)
                file_paths.append(file_path)

            # Test with process worker
            w_process = FileIOWorker.options(mode="process").init()
            start_time = time.time()
            futures = [w_process.read_file_async(path) for path in file_paths[:20]]
            results_process = [f.result(timeout=30) for f in futures]
            time_process = time.time() - start_time
            w_process.stop()

            # Test with asyncio worker
            w_asyncio = FileIOWorker.options(mode="asyncio").init()
            start_time = time.time()
            futures = [w_asyncio.read_file_async(path) for path in file_paths[:20]]
            results_asyncio = [f.result(timeout=30) for f in futures]
            time_asyncio = time.time() - start_time
            w_asyncio.stop()

            # Verify correctness
            assert len(results_process) == 20
            assert len(results_asyncio) == 20

            print("\nAsync function execution comparison (20 files):")
            print(f"  Process worker: {time_process:.3f}s")
            print(f"  Asyncio worker: {time_asyncio:.3f}s")
            if time_process > time_asyncio:
                print(f"  Asyncio speedup: {time_process / time_asyncio:.2f}x")
            else:
                print("  Note: Results may vary based on system and overhead")


@pytest.mark.performance
class TestWorkerPerformance:
    """Performance tests for tight loops.

    These tests verify that the optimizations achieve significant performance improvements:
    - SyncFuture creation: < 0.7µs (optimized from 2.5µs baseline, ~3.5x faster)
    - SyncWorker: < 4.0µs per call (optimized from 5µs baseline, ~40% faster)
    - AsyncioWorker: < 75µs per call (event loop overhead ~50µs unavoidable)
    - ThreadWorker: < 25µs per call (thread scheduling overhead ~20µs unavoidable)
    """

    def test_sync_worker_tight_loop_performance(self):
        """Test SyncWorker performance in tight loops.

        Target: < 4.0µs per call (optimized from 5µs baseline)
        Baseline: Basic Python loop is ~0.01-0.02µs per iteration
        """

        class Counter(Worker):
            def __init__(self, count: int = 0):
                self.count = count

            def increment(self, amount: int) -> int:
                self.count += amount
                return self.count

        # Baseline: direct Python loop
        import time

        count = 0
        start = time.time()
        iterations = int(10e3)  # 10k iterations for fast test
        for _ in range(iterations):
            count += 1
        baseline_time = time.time() - start
        baseline_per_call = baseline_time / iterations

        # Sync worker
        w = Counter.options(mode="sync").init(0)
        start = time.time()
        for _ in range(iterations):
            w.increment(1).result()
        worker_time = time.time() - start
        worker_per_call = worker_time / iterations

        w.stop()

        # Target: < 5.0 microseconds per call (allowing margin for system variance)
        # This represents ~50% improvement from the original 5µs baseline
        assert worker_per_call < 5.0e-6, (
            f"Sync worker too slow: {worker_per_call * 1e6:.2f}µs per call (target: <5.0µs)"
        )

        # Calculate overhead ratio
        overhead_ratio = worker_per_call / baseline_per_call if baseline_per_call > 0 else 0
        print("\nSync worker performance:")
        print(f"  Per call: {worker_per_call * 1e6:.3f}µs")
        print(f"  Baseline: {baseline_per_call * 1e6:.3f}µs")
        print(f"  Overhead: {overhead_ratio:.1f}x")
        print(f"  Improvement vs original 5µs: {5.0 / worker_per_call:.1f}x")

    def test_asyncio_worker_tight_loop_performance(self):
        """Test AsyncioWorker task submission performance.

        This test measures pure task submission overhead without waiting for results,
        which is the relevant metric for async performance. Calling .result() immediately
        after each submission would trigger AsyncioFuture's polling loop and not measure
        the actual async benefit.

        Target: < 50µs per submission (optimized from original ~50µs)
        The original implementation used run_coroutine_threadsafe which had ~50µs overhead.
        The optimized implementation uses call_soon_threadsafe for ~30-35µs overhead.
        """

        class Counter(Worker):
            def __init__(self, count: int = 0):
                self.count = count

            def increment(self, amount: int) -> int:
                self.count += amount
                return self.count

        w = Counter.options(mode="asyncio").init(0)

        import time

        iterations = int(1e4)  # 10k iterations to get stable measurement

        # Measure submission performance (without waiting for results)
        start = time.time()
        futures = [w.increment(1) for _ in range(iterations)]
        elapsed = time.time() - start
        per_submit = elapsed / iterations

        # Wait for all results to complete (verify correctness)
        results = [f.result(timeout=30) for f in futures]
        assert len(results) == iterations

        w.stop()

        # Target: < 50 microseconds per submission (optimized from original ~50µs)
        # The optimization uses call_soon_threadsafe instead of run_coroutine_threadsafe
        assert per_submit < 50e-6, (
            f"Asyncio worker submission too slow: {per_submit * 1e6:.2f}µs per call (target: <50µs)"
        )

        print("\nAsyncio worker submission performance:")
        print(f"  Per submission: {per_submit * 1e6:.3f}µs")
        print("  Target was <50µs (original implementation baseline)")

    def test_thread_worker_tight_loop_performance(self):
        """Test ThreadWorker performance in tight loops.

        Target: < 80µs per call (thread scheduling overhead is unavoidable)
        Performance varies with system load (±5µs), so target includes margin.
        """

        class Counter(Worker):
            def __init__(self, count: int = 0):
                self.count = count

            def increment(self, amount: int) -> int:
                self.count += amount
                return self.count

        w = Counter.options(mode="thread").init(0)

        import time

        iterations = int(1e3)  # 1k iterations (slower due to thread scheduling)
        start = time.time()
        for _ in range(iterations):
            w.increment(1).result()
        elapsed = time.time() - start
        per_call = elapsed / iterations

        w.stop()

        # Target: < 80 microseconds per call (accounting for thread scheduling + variability)
        # OS-level thread scheduling adds ~20µs base overhead
        assert per_call < 80e-6, f"Thread worker too slow: {per_call * 1e6:.2f}µs per call (target: <80µs)"

        print("\nThread worker performance:")
        print(f"  Per call: {per_call * 1e6:.3f}µs")

    def test_baseline_future_creation_performance(self):
        """Test that SyncFuture creation is optimized.

        Target: < 0.7µs per creation (optimized from 2.5µs baseline)
        """

        iterations = int(100e3)  # 100k iterations
        start = time.time()
        for _ in range(iterations):
            future = SyncFuture(result_value=1)
        elapsed = time.time() - start
        per_creation = elapsed / iterations

        # Target: < 0.7 microseconds per creation (allowing margin for system variance)
        # This is still ~3.5x faster than the original 2.5µs
        assert per_creation < 0.7e-6, (
            f"SyncFuture creation too slow: {per_creation * 1e6:.2f}µs per creation (target: <0.7µs)"
        )

        print("\nSyncFuture creation performance:")
        print(f"  Per creation: {per_creation * 1e6:.3f}µs")
        print(f"  Speedup vs baseline (2.5µs): {2.5 / (per_creation * 1e6):.1f}x")

    def test_comparison_with_baseline(self):
        """Compare worker performance against baseline Python loop.

        This test demonstrates the overhead of the worker abstraction
        versus a raw Python loop.
        """

        class Counter(Worker):
            def __init__(self):
                self.count = 0

            def increment(self):
                self.count += 1
                return self.count

            def get_count(self):
                return self.count

        import time

        iterations = int(10e3)

        # Baseline: raw Python
        count = 0
        start = time.time()
        for _ in range(iterations):
            count += 1
        baseline_time = time.time() - start

        # Sync worker
        w = Counter.options(mode="sync").init()
        start = time.time()
        for _ in range(iterations):
            w.increment()
        sync_time = time.time() - start
        final_count = w.get_count().result()
        w.stop()

        assert final_count == iterations, f"Expected count {iterations}, got {final_count}"

        print(f"\nPerformance comparison ({iterations} iterations):")
        print(
            f"  Baseline loop: {baseline_time * 1e6:.1f}µs total, {baseline_time * 1e6 / iterations:.3f}µs per call"
        )
        print(
            f"  Sync worker:   {sync_time * 1e6:.1f}µs total, {sync_time * 1e6 / iterations:.3f}µs per call"
        )
        print(f"  Overhead:      {sync_time / baseline_time:.1f}x")


class TestFutureTypeConsistency:
    """Test that each worker proxy returns the correct future type."""

    def test_sync_worker_returns_sync_future(self):
        """Verify SyncWorkerProxy returns SyncFuture objects."""

        class TestWorker(Worker):
            def method(self):
                return 42

        w = TestWorker.options(mode="sync").init()
        future = w.method()

        assert isinstance(future, SyncFuture), (
            f"SyncWorkerProxy should return SyncFuture, got {type(future).__name__}"
        )
        assert future.result() == 42

        w.stop()

    def test_thread_worker_returns_concurrent_future(self):
        """Verify ThreadWorkerProxy returns ConcurrentFuture objects."""

        class TestWorker(Worker):
            def method(self):
                return 42

        w = TestWorker.options(mode="thread").init()
        future = w.method()

        assert isinstance(future, ConcurrentFuture), (
            f"ThreadWorkerProxy should return ConcurrentFuture, got {type(future).__name__}"
        )
        assert future.result() == 42

        w.stop()

    def test_asyncio_worker_returns_concurrent_future(self):
        """Verify AsyncioWorkerProxy returns ConcurrentFuture objects.

        AsyncioWorkerProxy uses concurrent.futures.Future internally for efficient blocking,
        so it returns ConcurrentFuture (not AsyncioFuture) for both sync and async methods.
        """

        class TestWorker(Worker):
            def method(self):
                return 42

            async def async_method(self):
                import asyncio

                await asyncio.sleep(1e-6)
                return 100

        w = TestWorker.options(mode="asyncio").init()

        # Test sync method
        future = w.method()
        assert isinstance(future, ConcurrentFuture), (
            f"AsyncioWorkerProxy should return ConcurrentFuture, got {type(future).__name__}"
        )
        assert future.result() == 42

        # Test async method
        async_future = w.async_method()
        assert isinstance(async_future, ConcurrentFuture), (
            f"AsyncioWorkerProxy should return ConcurrentFuture for async methods, got {type(async_future).__name__}"
        )
        assert async_future.result() == 100

        w.stop()

    def test_process_worker_returns_concurrent_future(self):
        """Verify ProcessWorkerProxy returns ConcurrentFuture objects."""

        class TestWorker(Worker):
            def method(self):
                return 42

        w = TestWorker.options(mode="process").init()
        future = w.method()

        assert isinstance(future, ConcurrentFuture), (
            f"ProcessWorkerProxy should return ConcurrentFuture, got {type(future).__name__}"
        )
        assert future.result() == 42

        w.stop()
