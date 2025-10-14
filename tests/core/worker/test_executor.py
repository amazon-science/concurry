"""Tests for Executor function."""

from concurry import Executor


def test_executor_creates_task_worker():
    """Test that Executor creates a TaskWorker."""
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
    """Test Executor in blocking mode."""
    executor = Executor(mode="thread", max_workers=2, blocking=True)

    # Should return result directly
    result = executor.submit(lambda x: x + 10, 5)
    assert isinstance(result, int)
    assert result == 15

    executor.stop()


def test_executor_map():
    """Test Executor with map method."""
    executor = Executor(mode="thread", max_workers=3)

    results = list(executor.map(lambda x: x**2, range(5)))
    assert results == [0, 1, 4, 9, 16]

    executor.stop()


def test_executor_on_demand():
    """Test Executor with on-demand workers."""
    executor = Executor(mode="thread", on_demand=True)

    future = executor.submit(lambda x: x * 3, 7)
    result = future.result()
    assert result == 21

    executor.stop()


def test_executor_with_load_balancing():
    """Test Executor with load balancing."""
    executor = Executor(mode="thread", max_workers=4, load_balancing="rr")

    futures = [executor.submit(lambda x: x + 1, i) for i in range(10)]
    results = [f.result() for f in futures]
    assert results == list(range(1, 11))

    executor.stop()
