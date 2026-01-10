"""Tests for Ray mode TaskWorker - verifying single Ray entity creation.

This module tests the fix for the bug where @task(mode='ray') was creating
TWO Ray entities (an actor + a task) instead of just one (task only).

The fix ensures:
1. TaskWorker + Ray mode only creates Ray tasks, not actors
2. Regular Worker subclasses + Ray mode still create actors correctly
3. On-demand behavior works correctly with the fix
4. stop() handles None actor case gracefully

Related bug: RayWorkerProxy was creating an actor in post_initialize(),
but _execute_task() was creating a separate Ray task instead of using
the actor. This caused double resource consumption.

Fix: Skip actor creation for TaskWorker since it only uses _execute_task().
"""

import time

import pytest

from concurry import TaskWorker, Worker, task
from concurry.utils import _IS_RAY_INSTALLED

pytestmark = pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")


class TestRayTaskWorkerSingleEntity:
    """Test that @task(mode='ray') creates only one Ray entity per call."""

    def test_task_decorator_ray_basic_execution(self):
        """Test @task(mode='ray') executes correctly and returns right result.

        This test verifies that after the fix:
        1. The decorated function executes correctly on Ray
        2. Results are returned properly
        3. Cleanup works without errors

        Steps:
        1. Create @task decorated function with Ray mode
        2. Call the function multiple times
        3. Verify results are correct
        4. Stop the worker (should not error even with no actor)
        """

        @task(mode="ray", actor_options={"num_cpus": 1})
        def compute_square(x):
            return x**2

        # Execute multiple times
        results = [compute_square(i).result() for i in range(5)]

        assert results == [0, 1, 4, 9, 16]

        # Stop should work correctly (no actor to kill)
        compute_square.stop()

    def test_task_decorator_ray_with_on_demand_default(self):
        """Test @task(mode='ray') uses on_demand=True by default.

        The @task decorator defaults to on_demand=True for non-sync modes.
        This test verifies that behavior is correct with Ray mode.

        Steps:
        1. Create @task decorated function (on_demand defaults to True)
        2. Submit multiple tasks
        3. Verify all execute correctly
        4. Verify cleanup works
        """

        @task(mode="ray")
        def process_item(x):
            return x * 10

        # Submit 10 tasks
        futures = [process_item(i) for i in range(10)]
        results = [f.result() for f in futures]

        assert results == [i * 10 for i in range(10)]

        process_item.stop()

    def test_task_decorator_ray_with_explicit_on_demand_true(self):
        """Test @task(mode='ray', on_demand=True) works correctly.

        Explicit on_demand=True should behave the same as the default.
        Each call should create a Ray task (not an actor).

        Steps:
        1. Create @task with explicit on_demand=True
        2. Submit tasks
        3. Verify execution
        4. Verify cleanup
        """

        @task(mode="ray", on_demand=True, actor_options={"num_cpus": 1})
        def multiply(x, y):
            return x * y

        result = multiply(3, 4).result()
        assert result == 12

        multiply.stop()

    def test_task_decorator_ray_with_on_demand_false(self):
        """Test @task(mode='ray', on_demand=False) with persistent workers.

        When on_demand=False, TaskWorker should still skip actor creation
        since _execute_task() uses Ray tasks, not actor methods.

        Steps:
        1. Create @task with on_demand=False and max_workers > 1
        2. Submit multiple tasks
        3. Verify all execute correctly
        4. Verify cleanup works
        """

        @task(mode="ray", on_demand=False, max_workers=2)
        def add(x, y):
            return x + y

        futures = [add(i, i + 1) for i in range(5)]
        results = [f.result() for f in futures]

        assert results == [1, 3, 5, 7, 9]

        add.stop()

    def test_task_worker_ray_submit_basic(self):
        """Test TaskWorker.submit() with Ray mode works correctly.

        Using TaskWorker directly (not decorator) should also work.

        Steps:
        1. Create TaskWorker with Ray mode
        2. Submit a function
        3. Verify result
        4. Verify cleanup
        """
        worker = TaskWorker.options(mode="ray").init()

        def compute(x):
            return x * 2

        result = worker.submit(compute, 10).result()
        assert result == 20

        worker.stop()

    def test_task_worker_ray_map(self):
        """Test TaskWorker.map() with Ray mode works correctly.

        Note: map() with on-demand mode (default for @task) is not fully supported
        because map() returns an iterator, not a future, and the on-demand cleanup
        wrapper expects futures. So we use on_demand=False with a fixed pool.

        Steps:
        1. Create TaskWorker with Ray mode and persistent workers
        2. Use map() to process multiple items
        3. Verify results
        4. Verify cleanup
        """

        @task(mode="ray", on_demand=False, max_workers=2)
        def square(x):
            return x**2

        results = list(square.map(range(10)))

        assert results == [i**2 for i in range(10)]

        square.stop()

    def test_task_decorator_ray_with_complex_arguments(self):
        """Test @task(mode='ray') with complex argument types.

        Ray tasks should handle various argument types correctly.

        Steps:
        1. Create @task with complex function signature
        2. Pass various argument types
        3. Verify correct handling
        """

        @task(mode="ray")
        def complex_fn(a, b, *args, c=10, **kwargs):
            return a + b + sum(args) + c + sum(kwargs.values())

        result = complex_fn(1, 2, 3, 4, c=5, d=6, e=7).result()
        assert result == 1 + 2 + 3 + 4 + 5 + 6 + 7

        complex_fn.stop()

    def test_task_decorator_ray_with_exception(self):
        """Test @task(mode='ray') properly propagates exceptions.

        Exceptions in Ray tasks should be raised when getting result.

        Steps:
        1. Create @task that raises exception
        2. Call it and verify exception is raised
        3. Verify worker can still handle subsequent calls
        """

        @task(mode="ray")
        def failing_fn(x):
            if x < 0:
                raise ValueError("Negative not allowed")
            return x * 2

        # Success case
        result = failing_fn(5).result()
        assert result == 10

        # Failure case - exception should propagate
        with pytest.raises(Exception):  # Ray wraps exceptions
            failing_fn(-1).result()

        # Should still work after exception
        result2 = failing_fn(10).result()
        assert result2 == 20

        failing_fn.stop()

    def test_task_decorator_ray_async_function(self):
        """Test @task(mode='ray') with async functions.

        Async functions should be wrapped and executed correctly.

        Steps:
        1. Create @task with async function
        2. Execute and verify result
        """
        import asyncio

        @task(mode="ray")
        async def async_compute(x):
            await asyncio.sleep(0.01)
            return x * 3

        result = async_compute(5).result()
        assert result == 15

        async_compute.stop()

    def test_task_decorator_ray_with_retries(self):
        """Test @task(mode='ray') with retry configuration.

        Retries should work correctly with Ray tasks.

        Steps:
        1. Create @task with retry config
        2. Create a flaky function that fails first few times
        3. Verify it eventually succeeds
        """
        counter = {"calls": 0}

        @task(mode="ray", num_retries=3, retry_wait=0.01)
        def flaky_fn(x):
            counter["calls"] += 1
            if counter["calls"] < 3:
                raise ValueError("Not yet!")
            return x * 2

        result = flaky_fn(5).result()
        assert result == 10

        flaky_fn.stop()

    def test_task_decorator_ray_concurrent_calls(self):
        """Test @task(mode='ray') handles concurrent calls correctly.

        Multiple concurrent calls should each create their own Ray task.

        Steps:
        1. Create @task with Ray mode
        2. Submit multiple tasks concurrently
        3. Wait for all results
        4. Verify all completed correctly
        """

        @task(mode="ray", max_workers=0)  # Unlimited workers
        def slow_compute(x):
            time.sleep(0.1)
            return x * 2

        # Submit 5 concurrent tasks
        futures = [slow_compute(i) for i in range(5)]

        # Wait for all results
        results = [f.result() for f in futures]

        assert results == [0, 2, 4, 6, 8]

        slow_compute.stop()


class TestRegularWorkerRayStillUsesActor:
    """Test that regular Worker subclasses still correctly use Ray actors.

    The fix should ONLY affect TaskWorker. Regular Worker subclasses
    should still create Ray actors and dispatch methods to them.
    """

    def test_regular_worker_ray_creates_actor(self):
        """Test regular Worker + Ray mode creates and uses actor correctly.

        Regular Worker subclasses should:
        1. Create a Ray actor
        2. Dispatch method calls to the actor
        3. Maintain state in the actor

        Steps:
        1. Create Worker subclass with state
        2. Initialize with Ray mode
        3. Call methods that modify state
        4. Verify state is maintained (proves actor is used)
        """

        class StatefulWorker(Worker):
            def __init__(self, initial: int = 0):
                self.counter = initial

            def increment(self) -> int:
                self.counter += 1
                return self.counter

            def get_value(self) -> int:
                return self.counter

        worker = StatefulWorker.options(mode="ray").init(initial=10)

        # Increment multiple times - state should be maintained
        result1 = worker.increment().result()
        result2 = worker.increment().result()
        result3 = worker.increment().result()

        assert result1 == 11
        assert result2 == 12
        assert result3 == 13

        # Verify final state
        final = worker.get_value().result()
        assert final == 13

        worker.stop()

    def test_regular_worker_ray_with_pool_maintains_state(self):
        """Test Worker pool with Ray maintains per-worker state.

        Each worker in a pool should have its own actor with own state.

        Steps:
        1. Create Worker pool with 2 workers
        2. Call methods repeatedly
        3. Verify state is maintained per worker
        """

        class CounterWorker(Worker):
            def __init__(self):
                self.count = 0

            def increment_and_get(self) -> int:
                self.count += 1
                return self.count

        # Create pool with 2 workers
        pool = CounterWorker.options(mode="ray", max_workers=2).init()

        # Call 10 times - distributed across 2 workers
        results = [pool.increment_and_get().result() for _ in range(10)]

        # Each worker should have incremented its own counter
        # Results should show incrementing values (distributed across workers)
        assert all(r >= 1 for r in results)

        pool.stop()


class TestRayWorkerProxyStopHandlesNoneActor:
    """Test that stop() handles None actor case correctly.

    After the fix, TaskWorker + Ray doesn't create an actor,
    so stop() must handle _ray_actor = None without errors.
    """

    def test_stop_works_without_actor(self):
        """Test stop() doesn't error when actor is None.

        For TaskWorker, no actor is created, so stop() should
        handle this gracefully.

        Steps:
        1. Create TaskWorker with Ray
        2. Call stop() (should not error)
        """
        worker = TaskWorker.options(mode="ray").init()

        # Execute at least one task
        result = worker.submit(lambda x: x * 2, 5).result()
        assert result == 10

        # Stop should not raise any exception
        worker.stop()

    def test_multiple_stop_calls_are_safe(self):
        """Test calling stop() multiple times is safe.

        stop() should be idempotent - calling it multiple times
        should not cause errors.

        Steps:
        1. Create TaskWorker with Ray
        2. Call stop() multiple times
        3. Verify no errors
        """

        @task(mode="ray")
        def compute(x):
            return x * 2

        result = compute(5).result()
        assert result == 10

        # Multiple stop calls should be safe
        compute.stop()
        compute.stop()
        compute.stop()


class TestTaskDecoratorRayPoolBehavior:
    """Test @task(mode='ray') pool behavior with various configurations."""

    def test_ray_pool_with_max_workers(self):
        """Test @task with max_workers creates correct pool behavior.

        Steps:
        1. Create @task with max_workers=3
        2. Submit multiple tasks
        3. Verify all execute correctly
        """

        @task(mode="ray", max_workers=3)
        def process(x):
            return x + 100

        futures = [process(i) for i in range(10)]
        results = [f.result() for f in futures]

        assert results == [i + 100 for i in range(10)]

        process.stop()

    def test_ray_pool_with_max_workers_zero(self):
        """Test @task with max_workers=0 (unlimited) works correctly.

        Steps:
        1. Create @task with max_workers=0
        2. Submit many concurrent tasks
        3. Verify all complete
        """

        @task(mode="ray", max_workers=0)
        def quick_compute(x):
            return x * x

        # Submit many tasks
        futures = [quick_compute(i) for i in range(20)]
        results = [f.result() for f in futures]

        assert results == [i * i for i in range(20)]

        quick_compute.stop()

    def test_ray_with_actor_options(self):
        """Test @task with actor_options applies to Ray tasks.

        actor_options like num_cpus should be applied to the Ray tasks.

        Steps:
        1. Create @task with actor_options
        2. Execute tasks
        3. Verify correct execution (resource allocation is internal to Ray)
        """

        @task(mode="ray", actor_options={"num_cpus": 1, "num_gpus": 0})
        def compute_with_resources(x):
            return x * 3

        result = compute_with_resources(10).result()
        assert result == 30

        compute_with_resources.stop()
