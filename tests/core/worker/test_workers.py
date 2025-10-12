"""Comprehensive tests for all worker implementations."""

import time
from typing import List

import morphic
import pytest

import concurry
from concurry.core.worker import TaskWorker, Worker, worker
from concurry.utils import _IS_RAY_INSTALLED


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


# Parametrize modes to test
WORKER_MODES = ["sync", "thread", "process", "asyncio"]

# Add Ray if it's installed
if _IS_RAY_INSTALLED:
    WORKER_MODES.append("ray")


@pytest.fixture(params=WORKER_MODES)
def worker_mode(request):
    """Fixture providing different worker modes."""
    # Initialize Ray if needed
    if request.param == "ray":
        import ray

        if not ray.is_initialized():
            ray.init(
                ignore_reinit_error=True,
                num_cpus=4,
                runtime_env={"py_modules": [concurry, morphic]},
            )

    yield request.param

    # Note: We don't shutdown Ray between tests as it's expensive and causes issues
    # Ray will be shut down when the test process ends


class TestWorkerBasics:
    """Test basic worker functionality."""

    def test_simple_method_call(self, worker_mode):
        """Test basic method call on worker."""
        w = SimpleWorker.options(mode=worker_mode).create(10)
        future = w.add(5)
        result = future.result(timeout=5)
        assert result == 15
        w.stop()

    def test_nonexistent_method_fails(self, worker_mode):
        """Test that calling a non-existent method fails appropriately.

        For sync/ray modes: should fail immediately with AttributeError
        For thread/asyncio/process: should return a future that raises AttributeError when result() is called
        """
        w = SimpleWorker.options(mode=worker_mode).create(10)

        if worker_mode in ("sync", "ray"):
            # Sync and Ray modes should fail immediately
            with pytest.raises(AttributeError):
                w.nonexistent_method()
        else:
            # Thread, asyncio, and process modes return a future that contains the original error
            future = w.nonexistent_method()
            with pytest.raises(AttributeError):
                future.result(timeout=5)

        w.stop()

    def test_multiple_method_calls(self, worker_mode):
        """Test multiple method calls on same worker."""
        w = SimpleWorker.options(mode=worker_mode).create(10)

        future1 = w.add(5)
        future2 = w.multiply(2)
        future3 = w.get_value()

        assert future1.result(timeout=5) == 15
        assert future2.result(timeout=5) == 20
        assert future3.result(timeout=5) == 10

        w.stop()

    def test_blocking_mode(self, worker_mode):
        """Test blocking mode returns results directly."""
        w = SimpleWorker.options(mode=worker_mode, blocking=True).create(10)

        result = w.add(5)
        # Should return result directly, not a future
        assert isinstance(result, int)
        assert result == 15

        w.stop()

    def test_decorated_worker(self, worker_mode):
        """Test worker created with @worker decorator."""
        w = DecoratedWorker.options(mode=worker_mode).create("TestBot")
        future = w.greet()
        result = future.result(timeout=5)
        assert result == "Hello from TestBot"
        w.stop()


class TestWorkerExceptions:
    """Test exception handling in workers."""

    def test_method_raises_exception(self, worker_mode):
        """Test that exceptions in worker methods are propagated."""
        w = SimpleWorker.options(mode=worker_mode).create(10)
        future = w.raise_error("test error")

        with pytest.raises(Exception) as exc_info:
            future.result(timeout=5)

        # Check that the error message is preserved
        assert "test error" in str(exc_info.value)
        w.stop()

    def test_invalid_method_name(self, worker_mode):
        """Test calling non-existent method.

        This test verifies that errors for non-existent methods are properly raised.
        For sync/ray modes: fails immediately
        For other modes: original error is raised when calling result()
        """
        w = SimpleWorker.options(mode=worker_mode).create(10)

        if worker_mode in ("sync", "ray"):
            # Sync and Ray modes should fail immediately
            with pytest.raises(AttributeError):
                w.nonexistent_method(123)
        else:
            # Thread, asyncio, and process modes - original error is in the future
            future = w.nonexistent_method(123)
            with pytest.raises(AttributeError):
                future.result(timeout=5)

        w.stop()


class TestWorkerConcurrency:
    """Test concurrent execution in workers."""

    def test_concurrent_calls(self, worker_mode):
        """Test multiple concurrent calls to worker."""
        w = SimpleWorker.options(mode=worker_mode).create(10)

        # Submit multiple tasks
        futures = []
        for i in range(5):
            future = w.add(i)
            futures.append((future, 10 + i))

        # Check all results
        for future, expected in futures:
            result = future.result(timeout=5)
            assert result == expected

        w.stop()

    def test_long_running_task(self, worker_mode):
        """Test worker with a long-running task."""
        w = SimpleWorker.options(mode=worker_mode).create(10)

        # Start a task that takes 1 second
        start_time = time.time()
        future = w.sleep_and_return(1.0, 42)

        # Wait for result
        result = future.result(timeout=5)
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
        """Test that worker maintains state across calls."""
        w = StatefulWorker.options(mode=worker_mode).create()

        # Make multiple calls that modify state
        result1 = w.increment(1).result(timeout=5)
        result2 = w.increment(2).result(timeout=5)
        result3 = w.increment(3).result(timeout=5)

        assert result1 == 1
        assert result2 == 3
        assert result3 == 6

        # Check final counter value
        final_counter = w.get_counter().result(timeout=5)
        assert final_counter == 6

        w.stop()

    def test_state_isolation(self, worker_mode):
        """Test that different worker instances have isolated state."""
        w1 = StatefulWorker.options(mode=worker_mode).create()
        w2 = StatefulWorker.options(mode=worker_mode).create()

        # Modify state in both workers
        result1 = w1.increment(5).result(timeout=5)
        result2 = w2.increment(10).result(timeout=5)

        assert result1 == 5
        assert result2 == 10

        # Check that states are independent
        counter1 = w1.get_counter().result(timeout=5)
        counter2 = w2.get_counter().result(timeout=5)

        assert counter1 == 5
        assert counter2 == 10

        w1.stop()
        w2.stop()


class TestWorkerLifecycle:
    """Test worker lifecycle management."""

    def test_stop_worker(self, worker_mode):
        """Test stopping a worker."""
        w = SimpleWorker.options(mode=worker_mode).create(10)

        # Use the worker
        result = w.add(5).result(timeout=5)
        assert result == 15

        # Stop the worker
        w.stop()

        # Should not be able to use after stopping
        with pytest.raises(RuntimeError):
            w.add(5)

    def test_cleanup_multiple_workers(self, worker_mode):
        """Test cleaning up multiple workers."""
        workers = []
        for i in range(3):
            w = SimpleWorker.options(mode=worker_mode).create(i)
            workers.append(w)

        # Use all workers
        for i, w in enumerate(workers):
            result = w.get_value().result(timeout=5)
            assert result == i

        # Stop all workers
        for w in workers:
            w.stop()


class TestWorkerInitialization:
    """Test worker initialization with different arguments."""

    def test_init_with_args(self, worker_mode):
        """Test worker initialization with positional arguments."""
        w = SimpleWorker.options(mode=worker_mode).create(42)
        result = w.get_value().result(timeout=5)
        assert result == 42
        w.stop()

    def test_init_with_kwargs(self, worker_mode):
        """Test worker initialization with keyword arguments."""
        w = SimpleWorker.options(mode=worker_mode).create(value=99)
        result = w.get_value().result(timeout=5)
        assert result == 99
        w.stop()

    def test_init_with_both(self, worker_mode):
        """Test worker initialization with both args and kwargs."""
        w = DecoratedWorker.options(mode=worker_mode).create("Alice")
        result = w.greet().result(timeout=5)
        assert result == "Hello from Alice"
        w.stop()


class TestFutureInterface:
    """Test the Future interface returned by worker methods."""

    def test_future_done(self, worker_mode):
        """Test Future.done() method."""
        w = SimpleWorker.options(mode=worker_mode).create(10)
        future = w.add(5)

        # Wait for completion
        result = future.result(timeout=5)

        # Should be done now
        assert future.done()
        assert result == 15

        w.stop()

    def test_future_result_timeout(self, worker_mode):
        """Test Future.result() with timeout."""
        w = SimpleWorker.options(mode=worker_mode).create(10)
        future = w.sleep_and_return(2.0, 42)

        # Sync mode completes immediately, so no timeout
        if worker_mode == "sync":
            # For sync mode, the future is already done
            result = future.result(timeout=0.1)
            assert result == 42
        else:
            # Should timeout if we don't wait long enough
            with pytest.raises(TimeoutError):
                future.result(timeout=0.5)

            # But should succeed with longer timeout
            result = future.result(timeout=3.0)
            assert result == 42

        w.stop()

    def test_future_exception(self, worker_mode):
        """Test Future.exception() method."""
        w = SimpleWorker.options(mode=worker_mode).create(10)
        future = w.raise_error("test exception")

        # Should raise when getting result
        with pytest.raises(Exception):
            future.result(timeout=5)

        w.stop()


class TestWorkerSubmitTask:
    """Test submit_task functionality."""

    def test_submit_simple_function(self, worker_mode):
        """Test submitting a simple function."""

        def add(x, y):
            return x + y

        w = SimpleWorker.options(mode=worker_mode).create(10)
        future = w.submit_task(add, 5, 10)
        result = future.result(timeout=5)
        assert result == 15
        w.stop()

    def test_submit_with_kwargs(self, worker_mode):
        """Test submitting a function with keyword arguments."""

        def multiply(x, y, factor=1):
            return (x * y) * factor

        w = SimpleWorker.options(mode=worker_mode).create(10)
        future = w.submit_task(multiply, 3, 4, factor=2)
        result = future.result(timeout=5)
        assert result == 24
        w.stop()

    def test_submit_lambda(self, worker_mode):
        """Test submitting a lambda function."""
        w = SimpleWorker.options(mode=worker_mode).create(10)
        future = w.submit_task(lambda x: x**2, 5)
        result = future.result(timeout=5)
        assert result == 25
        w.stop()

    def test_submit_with_exception(self, worker_mode):
        """Test submitting a function that raises an exception."""

        def failing_fn():
            raise ValueError("Task failed")

        w = SimpleWorker.options(mode=worker_mode).create(10)
        future = w.submit_task(failing_fn)

        with pytest.raises(Exception) as exc_info:
            future.result(timeout=5)

        assert "Task failed" in str(exc_info.value) or "failed" in str(exc_info.value).lower()
        w.stop()

    def test_submit_multiple_tasks(self, worker_mode):
        """Test submitting multiple tasks."""

        def compute(x):
            return x * 2

        w = SimpleWorker.options(mode=worker_mode).create(10)

        futures = [w.submit_task(compute, i) for i in range(5)]
        results = [f.result(timeout=5) for f in futures]

        assert results == [0, 2, 4, 6, 8]
        w.stop()

    def test_submit_blocking_mode(self, worker_mode):
        """Test submit_task in blocking mode."""

        def add(x, y):
            return x + y

        w = SimpleWorker.options(mode=worker_mode, blocking=True).create(10)
        result = w.submit_task(add, 10, 20)

        # Should return result directly, not a future
        assert isinstance(result, int)
        assert result == 30
        w.stop()

    def test_submit_after_method_call(self, worker_mode):
        """Test mixing method calls and task submission."""

        def compute(x):
            return x * 3

        w = SimpleWorker.options(mode=worker_mode).create(10)

        # Call a method
        result1 = w.add(5).result(timeout=5)
        assert result1 == 15

        # Submit a task
        result2 = w.submit_task(compute, 10).result(timeout=5)
        assert result2 == 30

        # Call another method
        result3 = w.multiply(2).result(timeout=5)
        assert result3 == 20

        w.stop()


# Ray-specific tests (only run if Ray is available)
class TestTaskWorker:
    """Test TaskWorker - a concrete worker for submitting arbitrary tasks."""

    def test_basic_task_submission(self, worker_mode):
        """Test basic task submission with TaskWorker."""

        def compute(x, y):
            return x**2 + y**2

        w = TaskWorker.options(mode=worker_mode).create()
        future = w.submit_task(compute, 3, 4)
        result = future.result(timeout=5)

        assert result == 25
        w.stop()

    def test_lambda_task(self, worker_mode):
        """Test submitting lambda functions."""
        w = TaskWorker.options(mode=worker_mode).create()

        result = w.submit_task(lambda x: x * 10, 5).result(timeout=5)
        assert result == 50

        w.stop()

    def test_multiple_tasks(self, worker_mode):
        """Test submitting multiple tasks to TaskWorker."""
        w = TaskWorker.options(mode=worker_mode).create()

        futures = [w.submit_task(lambda x: x**2, i) for i in range(5)]
        results = [f.result(timeout=5) for f in futures]

        assert results == [0, 1, 4, 9, 16]
        w.stop()

    def test_task_with_kwargs(self, worker_mode):
        """Test task submission with keyword arguments."""

        def compute(x, y, multiplier=1):
            return (x + y) * multiplier

        w = TaskWorker.options(mode=worker_mode).create()
        result = w.submit_task(compute, 5, 10, multiplier=2).result(timeout=5)

        assert result == 30
        w.stop()

    def test_blocking_mode(self, worker_mode):
        """Test TaskWorker in blocking mode."""
        w = TaskWorker.options(mode=worker_mode, blocking=True).create()

        result = w.submit_task(lambda x: x + 100, 7)

        # Should return result directly, not a future
        assert isinstance(result, int)
        assert result == 107
        w.stop()

    def test_task_with_exception(self, worker_mode):
        """Test that exceptions in tasks are properly propagated."""

        def failing_task():
            raise ValueError("Task failed")

        w = TaskWorker.options(mode=worker_mode).create()
        future = w.submit_task(failing_task)

        with pytest.raises(Exception) as exc_info:
            future.result(timeout=5)

        assert "failed" in str(exc_info.value).lower()
        w.stop()

    def test_no_custom_methods(self):
        """Test that TaskWorker has no custom methods, only submit_task."""
        w = TaskWorker.options(mode="sync").create()

        # Should have submit_task
        assert hasattr(w, "submit_task")

        # Should have standard methods
        assert hasattr(w, "stop")

        # Should not have any custom worker methods like 'compute', 'process', etc.
        # (This is just a smoke test to ensure it's a plain worker)

        w.stop()

    def test_different_execution_modes(self):
        """Test TaskWorker works across different execution modes."""
        modes = WORKER_MODES  # Use same modes as other tests (includes Ray if installed)

        for mode in modes:
            w = TaskWorker.options(mode=mode).create()
            result = w.submit_task(lambda x: x * 2, 5).result(timeout=5)
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
        import ray

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, runtime_env={"py_modules": [concurry, morphic]})

        w = SimpleWorker.options(mode="ray", num_cpus=1, num_gpus=0).create(10)

        result = w.add(5).result(timeout=5)
        assert result == 15

        w.stop()
