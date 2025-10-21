"""Tests for Executor function."""

from concurry import Executor


def test_executor_creates_task_worker():
    """Test that Executor creates a TaskWorker.

    This test:
    1. Creates an Executor with thread mode and 3 workers
    2. Verifies the executor has TaskWorker interface (submit and map methods)
    3. Submits a simple lambda task (multiply by 2)
    4. Verifies the result is correct (10)
    5. Stops the executor
    """
    executor = Executor(mode="thread", max_workers=3)

    # Should have submit and map methods (TaskWorker interface)
    assert hasattr(executor, "submit")
    assert hasattr(executor, "map")

    # Test basic functionality
    future = executor.submit(lambda x: x * 2, 5)
    result = future.result()
    assert result == 10

    executor.stop()


def test_executor_with_blocking_mode():
    """Test Executor in blocking mode.

    This test:
    1. Creates an Executor with blocking=True (returns results directly)
    2. Submits a task (add 10)
    3. Verifies the result is returned directly as an int (not a Future)
    4. Verifies the result value is correct (15)
    5. Stops the executor
    """
    executor = Executor(mode="thread", max_workers=2, blocking=True)

    # Should return result directly
    result = executor.submit(lambda x: x + 10, 5)
    assert isinstance(result, int)
    assert result == 15

    executor.stop()


def test_executor_map():
    """Test Executor with map method.

    This test:
    1. Creates an Executor with thread mode and 3 workers
    2. Uses map() to apply a function (square) to a range of values
    3. Verifies all results are correct [0, 1, 4, 9, 16]
    4. Stops the executor
    """
    executor = Executor(mode="thread", max_workers=3)

    results = list(executor.map(lambda x: x**2, range(5)))
    assert results == [0, 1, 4, 9, 16]

    executor.stop()


def test_executor_on_demand():
    """Test Executor with on-demand workers.

    This test:
    1. Creates an Executor with on_demand=True (creates workers per task)
    2. Submits a single task (multiply by 3)
    3. Verifies the result is correct (21)
    4. Stops the executor
    """
    executor = Executor(mode="thread", on_demand=True)

    future = executor.submit(lambda x: x * 3, 7)
    result = future.result()
    assert result == 21

    executor.stop()


def test_executor_with_load_balancing():
    """Test Executor with load balancing.

    This test:
    1. Creates an Executor with 4 workers and round-robin load balancing
    2. Submits 10 tasks (increment by 1) that will be distributed across workers
    3. Collects all results
    4. Verifies all results are correct [1, 2, 3, ..., 11]
    5. Stops the executor
    """
    executor = Executor(mode="thread", max_workers=4, load_balancing="rr")

    futures = [executor.submit(lambda x: x + 1, i) for i in range(10)]
    results = [f.result() for f in futures]
    assert results == list(range(1, 11))

    executor.stop()
