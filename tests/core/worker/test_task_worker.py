"""Tests for TaskWorker bound function functionality."""

import pytest

from concurry import ExecutionMode, TaskWorker


class TestTaskWorkerBoundFunction:
    """Test TaskWorker with bound functions."""

    def test_bound_function_submit(self, worker_mode):
        """Test submit() with bound function across all modes.

        This test:
        1. Defines a compute function (x²+y²)
        2. Creates a TaskWorker with the function bound during init
        3. Calls submit() without passing the function (uses bound one)
        4. Verifies result is correct (3²+4² = 25)
        5. Stops the worker
        """

        def compute(x, y):
            return x**2 + y**2

        worker = TaskWorker.options(mode=worker_mode).init(fn=compute)

        # Submit without passing function
        result = worker.submit(3, 4).result()
        assert result == 25

        worker.stop()

    def test_bound_function_map(self, worker_mode):
        """Test map() with bound function across all modes.

        This test:
        1. Defines a square function (x²)
        2. Creates a TaskWorker with the function bound during init
        3. Calls map() on range(5) without passing the function
        4. Verifies results are correct [0, 1, 4, 9, 16]
        5. Stops the worker
        """

        def square(x):
            return x**2

        worker = TaskWorker.options(mode=worker_mode).init(fn=square)

        # Map without passing function
        results = list(worker.map(range(5)))
        assert results == [0, 1, 4, 9, 16]

        worker.stop()

    def test_bound_function_call(self, worker_mode):
        """Test __call__() with bound function across all modes.

        This test:
        1. Defines a multiply function (x*y)
        2. Creates a TaskWorker with the function bound during init
        3. Calls the worker directly using __call__ syntax (no submit/map)
        4. Verifies result is correct (3*4 = 12)
        5. Stops the worker
        """

        def multiply(x, y):
            return x * y

        worker = TaskWorker.options(mode=worker_mode).init(fn=multiply)

        # Call directly
        result = worker(3, 4).result()
        assert result == 12

        worker.stop()

    def test_submit_without_bound_function_raises_error(self):
        """Test that submit() without bound function raises TypeError.

        This test:
        1. Creates a TaskWorker without binding a function
        2. Attempts to submit a non-callable (int 5)
        3. Verifies TypeError is raised with appropriate message
        4. Attempts to submit None
        5. Verifies TypeError is raised for None
        6. Stops the worker
        """
        worker = TaskWorker.options(mode=ExecutionMode.Sync).init()

        # Passing non-callable should fail
        with pytest.raises(TypeError, match="submit\\(\\) requires a callable function"):
            worker.submit(5)

        # Passing None should also fail (None is not callable)
        with pytest.raises(TypeError, match="submit\\(\\) requires a callable function, got NoneType"):
            worker.submit(None)

        worker.stop()

    def test_map_without_bound_function_raises_error(self):
        """Test that map() without bound function raises TypeError.

        This test:
        1. Creates a TaskWorker without binding a function
        2. Attempts to call map() with a non-callable (int 5)
        3. Verifies TypeError is raised with appropriate message
        4. Attempts to call map() with None
        5. Verifies TypeError is raised for None
        6. Stops the worker
        """
        worker = TaskWorker.options(mode=ExecutionMode.Sync).init()

        # Passing non-callable should fail
        with pytest.raises(TypeError, match="map\\(\\) requires a callable function"):
            list(worker.map(5))

        # Passing None should also fail (None is not callable)
        with pytest.raises(TypeError, match="map\\(\\) requires a callable function, got NoneType"):
            list(worker.map(None))

        worker.stop()

    def test_call_without_bound_function_raises_error(self):
        """Test that __call__() without bound function raises TypeError.

        This test:
        1. Creates a TaskWorker without binding a function
        2. Attempts to call worker directly with a non-callable (int 5)
        3. Verifies TypeError is raised with appropriate message
        4. Attempts to call worker with None
        5. Verifies TypeError is raised for None
        6. Stops the worker
        """
        worker = TaskWorker.options(mode=ExecutionMode.Sync).init()

        # Passing non-callable should fail
        with pytest.raises(TypeError, match="submit\\(\\) requires a callable function"):
            worker(5)

        # Passing None should also fail (None is not callable)
        with pytest.raises(TypeError, match="submit\\(\\) requires a callable function, got NoneType"):
            worker(None)

        worker.stop()


class TestTaskWorkerProgressBar:
    """Test ProgressBar integration in TaskWorker.map()."""

    def test_map_with_progress_true(self, worker_mode):
        """Test map() with progress=True across all modes.

        This test:
        1. Defines a process function (x+1)
        2. Creates a TaskWorker with the function bound
        3. Calls map() with progress=True (shows progress bar)
        4. Verifies results are correct [1, 2, 3, ..., 10]
        5. Stops the worker
        """

        def process(x):
            return x + 1

        worker = TaskWorker.options(mode=worker_mode).init(fn=process)

        results = list(worker.map(range(10), progress=True))
        assert results == list(range(1, 11))

        worker.stop()

    def test_map_with_progress_dict(self, worker_mode):
        """Test map() with progress as dict across all modes.

        This test:
        1. Defines a double function (x*2)
        2. Creates a TaskWorker with the function bound
        3. Calls map() with progress dict (custom description, disabled)
        4. Verifies results are correct [0, 2, 4, 6, 8]
        5. Stops the worker
        """

        def double(x):
            return x * 2

        worker = TaskWorker.options(mode=worker_mode).init(fn=double)

        results = list(worker.map(range(5), progress={"desc": "Doubling", "disable": True}))
        assert results == [0, 2, 4, 6, 8]

        worker.stop()

    def test_map_with_progress_false(self, worker_mode):
        """Test map() with progress=False (default) across all modes.

        This test:
        1. Defines a triple function (x*3)
        2. Creates a TaskWorker with the function bound
        3. Calls map() with progress=False (no progress bar)
        4. Verifies results are correct [0, 3, 6]
        5. Stops the worker
        """

        def triple(x):
            return x * 3

        worker = TaskWorker.options(mode=worker_mode).init(fn=triple)

        results = list(worker.map(range(3), progress=False))
        assert results == [0, 3, 6]

        worker.stop()


class TestTaskWorkerMixedUsage:
    """Test TaskWorker with both bound and explicit functions."""

    def test_bound_vs_unbound_workers(self):
        """Test bound workers vs unbound workers.

        This test:
        1. Defines two functions: bound_func (x*2) and explicit_func (x*3)
        2. Creates a bound worker with fn=bound_func (submit takes data directly)
        3. Submits data (5) to bound worker, verifies result is 10
        4. Creates an unbound worker without fn parameter
        5. Submits (function, data) to unbound worker, verifies result is 15
        6. Stops both workers
        """

        def bound_func(x):
            return x * 2

        def explicit_func(x):
            return x * 3

        # Bound worker - first argument is data
        bound_worker = TaskWorker.options(mode=ExecutionMode.Sync).init(fn=bound_func)
        result1 = bound_worker.submit(5).result()
        assert result1 == 10
        bound_worker.stop()

        # Unbound worker - first argument must be callable
        unbound_worker = TaskWorker.options(mode=ExecutionMode.Sync).init()
        result2 = unbound_worker.submit(explicit_func, 5).result()
        assert result2 == 15
        unbound_worker.stop()


class TestTaskWorkerDifferentModes:
    """Test TaskWorker bound functions with different execution modes."""

    def test_bound_function_different_modes(self, worker_mode):
        """Test bound functions work across execution modes.

        This test:
        1. Defines a compute function (x+y)
        2. Creates a TaskWorker with fn=compute
        3. Calls worker(10, 20) using __call__ syntax
        4. Verifies result is 30
        5. Stops the worker
        """

        def compute(x, y):
            return x + y

        worker = TaskWorker.options(mode=worker_mode).init(fn=compute)

        result = worker(10, 20).result()
        assert result == 30

        worker.stop()


class TestTaskWorkerAsyncFunctions:
    """Test TaskWorker with async bound functions."""

    def test_async_bound_function(self):
        """Test TaskWorker with async bound function.

        This test:
        1. Defines an async function async_compute (x+y with sleep)
        2. Creates an asyncio TaskWorker with fn=async_compute
        3. Calls worker(3, 4) to execute async function
        4. Verifies result is 7
        5. Stops the worker
        """
        import asyncio

        async def async_compute(x, y):
            await asyncio.sleep(0.001)
            return x + y

        worker = TaskWorker.options(mode=ExecutionMode.Asyncio).init(fn=async_compute)

        result = worker(3, 4).result()
        assert result == 7

        worker.stop()


class TestTaskWorkerEdgeCases:
    """Test TaskWorker edge cases and error conditions."""

    def test_bound_function_with_no_args(self, worker_mode):
        """Test bound function that takes no arguments across all modes.

        This test:
        1. Defines a get_constant function that takes no arguments
        2. Creates a TaskWorker with fn=get_constant
        3. Calls worker() with no arguments, verifies result is 42
        4. Also calls worker.submit() with no arguments, verifies result is 42
        5. Stops the worker
        """

        def get_constant():
            return 42

        worker = TaskWorker.options(mode=worker_mode).init(fn=get_constant)

        # Should work with empty call
        result = worker().result()
        assert result == 42

        # Should also work with submit
        result2 = worker.submit().result()
        assert result2 == 42

        worker.stop()

    def test_bound_function_with_varargs(self, worker_mode):
        """Test bound function with *args across all modes.

        This test:
        1. Defines a sum_all function that accepts *args
        2. Creates a TaskWorker with fn=sum_all
        3. Calls worker(1, 2, 3, 4, 5) with varargs
        4. Verifies result is 15 (sum of all args)
        5. Stops the worker
        """

        def sum_all(*args):
            return sum(args)

        worker = TaskWorker.options(mode=worker_mode).init(fn=sum_all)

        result = worker(1, 2, 3, 4, 5).result()
        assert result == 15

        worker.stop()

    def test_bound_function_with_kwargs(self, worker_mode):
        """Test bound function with **kwargs across all modes.

        This test:
        1. Defines a build_dict function that accepts **kwargs
        2. Creates a TaskWorker with fn=build_dict
        3. Calls worker(a=1, b=2, c=3) with keyword arguments
        4. Verifies result is {'a': 1, 'b': 2, 'c': 3}
        5. Stops the worker
        """

        def build_dict(**kwargs):
            return kwargs

        worker = TaskWorker.options(mode=worker_mode).init(fn=build_dict)

        result = worker(a=1, b=2, c=3).result()
        assert result == {"a": 1, "b": 2, "c": 3}

        worker.stop()

    def test_bound_function_with_default_args(self, worker_mode):
        """Test bound function with default arguments across all modes.

        This test:
        1. Defines a compute function with defaults y=10, z=20
        2. Creates a TaskWorker with fn=compute
        3. Calls with all defaults: worker(5), verifies result is 35
        4. Calls with some overrides: worker(5, y=15), verifies result is 40
        5. Calls with all overrides: worker(5, 15, 25), verifies result is 45
        6. Stops the worker
        """

        def compute(x, y=10, z=20):
            return x + y + z

        worker = TaskWorker.options(mode=worker_mode).init(fn=compute)

        # Call with all defaults
        result1 = worker(5).result()
        assert result1 == 35

        # Override some defaults
        result2 = worker(5, y=15).result()
        assert result2 == 40

        # Override all
        result3 = worker(5, 15, 25).result()
        assert result3 == 45

        worker.stop()

    def test_map_with_empty_iterable(self, worker_mode):
        """Test map() with empty iterable across all modes.

        This test:
        1. Defines a square function (x**2)
        2. Creates a TaskWorker with fn=square
        3. Calls map([]) with empty list
        4. Verifies results is empty list []
        5. Stops the worker
        """

        def square(x):
            return x**2

        worker = TaskWorker.options(mode=worker_mode).init(fn=square)

        results = list(worker.map([]))
        assert results == []

        worker.stop()

    def test_map_with_single_item(self, worker_mode):
        """Test map() with single item iterable across all modes.

        This test:
        1. Defines a double function (x*2)
        2. Creates a TaskWorker with fn=double
        3. Calls map([5]) with single-item list
        4. Verifies results is [10]
        5. Stops the worker
        """

        def double(x):
            return x * 2

        worker = TaskWorker.options(mode=worker_mode).init(fn=double)

        results = list(worker.map([5]))
        assert results == [10]

        worker.stop()

    def test_map_with_uneven_iterables(self, worker_mode):
        """Test map() with multiple iterables of different lengths across all modes.

        This test:
        1. Defines an add function (x+y)
        2. Creates a TaskWorker with fn=add
        3. Calls map with two iterables of different lengths [1,2,3,4] and [10,20]
        4. Verifies results is [11, 22] (zip stops at shortest iterable)
        5. Stops the worker
        """

        def add(x, y):
            return x + y

        worker = TaskWorker.options(mode=worker_mode).init(fn=add)

        # Python's zip stops at shortest iterable
        results = list(worker.map([1, 2, 3, 4], [10, 20]))
        assert results == [11, 22]

        worker.stop()

    def test_bound_function_raises_exception(self, worker_mode):
        """Test bound function that raises exception across all modes.

        This test:
        1. Defines a failing_fn that raises ValueError for negative inputs
        2. Creates a TaskWorker with fn=failing_fn
        3. Calls worker(5) for success case, verifies result is 10
        4. Calls worker(-1) for failure case, verifies ValueError is raised
        5. Stops the worker
        """

        def failing_fn(x):
            if x < 0:
                raise ValueError("Negative value not allowed")
            return x * 2

        worker = TaskWorker.options(mode=worker_mode).init(fn=failing_fn)

        # Success case
        result1 = worker(5).result()
        assert result1 == 10

        # Failure case
        with pytest.raises(ValueError, match="Negative value"):
            worker(-1).result()

        worker.stop()

    def test_map_with_exception_in_one_item(self, worker_mode):
        """Test map() when one item raises exception across all modes.

        This test:
        1. Defines a process function that raises ValueError for x==3
        2. Creates a TaskWorker with fn=process
        3. Calls map([1, 2, 3, 4]) which should fail at x==3
        4. Verifies ValueError is raised with "Cannot process 3"
        5. Stops the worker
        """

        def process(x):
            if x == 3:
                raise ValueError(f"Cannot process {x}")
            return x * 2

        worker = TaskWorker.options(mode=worker_mode).init(fn=process)

        # Should raise on the failing item
        with pytest.raises(ValueError, match="Cannot process 3"):
            list(worker.map([1, 2, 3, 4]))

        worker.stop()

    def test_bound_lambda_function(self, worker_mode):
        """Test binding a lambda function across all modes.

        This test:
        1. Creates a TaskWorker with fn=lambda x, y: x**y
        2. Calls worker(2, 3) to compute 2**3
        3. Verifies result is 8
        4. Stops the worker
        """

        worker = TaskWorker.options(mode=worker_mode).init(fn=lambda x, y: x**y)

        result = worker(2, 3).result()
        assert result == 8

        worker.stop()

    def test_progress_with_custom_progressbar_instance(self, worker_mode):
        """Test map() with custom ProgressBar instance across all modes.

        This test:
        1. Defines a process function (x+1)
        2. Creates a TaskWorker with fn=process
        3. Creates a custom ProgressBar instance (disabled for tests)
        4. Calls map with custom progress bar
        5. Verifies results are [1, 2, 3, 4, 5]
        6. Stops the worker
        """
        from concurry.utils.progress import ProgressBar

        def process(x):
            return x + 1

        worker = TaskWorker.options(mode=worker_mode).init(fn=process)

        # Create custom progress bar
        pbar = ProgressBar(total=5, desc="Custom Progress", disable=True)

        results = list(worker.map(range(5), progress=pbar))
        assert results == [1, 2, 3, 4, 5]

        worker.stop()

    def test_map_timeout_behavior(self, pool_mode):
        """Test map() with timeout parameter across pool modes.

        This test:
        1. Defines a slow_fn that sleeps 0.01s then returns x*2
        2. Creates a TaskWorker pool with fn=slow_fn
        3. Calls map(range(3), timeout=5.0) with generous timeout
        4. Verifies results are [0, 2, 4] (all complete within timeout)
        5. Stops the worker
        """
        import time

        def slow_fn(x):
            time.sleep(0.01)
            return x * 2

        worker = TaskWorker.options(mode=pool_mode).init(fn=slow_fn)

        # Should complete within timeout
        results = list(worker.map(range(3), timeout=5.0))
        assert results == [0, 2, 4]

        worker.stop()

    def test_submit_with_none_as_argument(self, worker_mode):
        """Test submit() when None is passed as argument value across all modes.

        This test:
        1. Defines a process function that returns tuple of (x is None, y is None)
        2. Creates a TaskWorker with fn=process
        3. Calls worker(None, 5), verifies result is (True, False)
        4. Calls worker(5, None), verifies result is (False, True)
        5. Calls worker(None, None), verifies result is (True, True)
        6. Stops the worker

        With bound functions, None can now be passed as any argument including the first.
        """

        def process(x, y):
            return (x is None, y is None)

        worker = TaskWorker.options(mode=worker_mode).init(fn=process)

        # None as first argument works
        result = worker(None, 5).result()
        assert result == (True, False)

        # None as second argument works
        result2 = worker(5, None).result()
        assert result2 == (False, True)

        # Both None works
        result3 = worker(None, None).result()
        assert result3 == (True, True)

        worker.stop()

    def test_worker_with_pool_and_bound_function(self, pool_mode):
        """Test TaskWorker pool with bound function across pool modes.

        This test:
        1. Defines a compute function (x**2)
        2. Creates a TaskWorker pool with 3 workers and fn=compute
        3. Submits 10 tasks [0..9] to the pool
        4. Collects all results
        5. Verifies results are [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
        6. Stops the worker pool
        """

        def compute(x):
            return x**2

        # Create pool with multiple workers
        worker = TaskWorker.options(mode=pool_mode, max_workers=3).init(fn=compute)

        # Submit multiple tasks
        futures = [worker(i) for i in range(10)]
        results = [f.result() for f in futures]

        assert results == [i**2 for i in range(10)]

        worker.stop()

    def test_bound_function_with_nested_calls(self, worker_mode):
        """Test bound function that calls other bound functions across all modes.

        This test:
        1. Defines outer_fn (x*2) and inner_fn (x+10)
        2. Creates two separate TaskWorkers, each with different bound function
        3. Calls worker1(5), gets result1=10
        4. Uses result1 as input to worker2(result1), gets result2=20
        5. Verifies both results are correct
        6. Stops both workers
        """

        def outer_fn(x):
            return x * 2

        def inner_fn(x):
            return x + 10

        worker1 = TaskWorker.options(mode=worker_mode).init(fn=outer_fn)
        worker2 = TaskWorker.options(mode=worker_mode).init(fn=inner_fn)

        # Use result from one as input to another
        result1 = worker1(5).result()  # 10
        result2 = worker2(result1).result()  # 20

        assert result1 == 10
        assert result2 == 20

        worker1.stop()
        worker2.stop()

    def test_map_preserves_order(self, pool_mode):
        """Test that map() preserves input order in results across pool modes.

        This test:
        1. Defines variable_delay function with inverse delays (small x = long delay)
        2. Creates a TaskWorker pool with 3 workers and fn=variable_delay
        3. Calls map(range(10)) - tasks complete in different order due to delays
        4. Verifies results preserve input order [0, 2, 4, 6, ..., 18]
        5. Stops the worker pool
        """
        import time

        def variable_delay(x):
            # Longer delay for smaller numbers (inverse order)
            time.sleep(0.001 * (10 - x))
            return x * 2

        worker = TaskWorker.options(mode=pool_mode, max_workers=3).init(fn=variable_delay)

        results = list(worker.map(range(10)))
        # Results should still be in order despite variable delays
        assert results == [i * 2 for i in range(10)]

        worker.stop()

    def test_reusing_worker_after_exception(self, worker_mode):
        """Test that worker can be reused after exception across all modes.

        This test:
        1. Defines conditional_fail that raises ValueError for negative values
        2. Creates a TaskWorker with fn=conditional_fail
        3. First call worker(5) succeeds, verifies result is 10
        4. Second call worker(-10) fails, verifies ValueError is raised
        5. Third call worker(15) succeeds, verifies result is 30 (worker is reusable)
        6. Stops the worker
        """

        # Use a function that doesn't require shared state across process boundaries
        def conditional_fail(x):
            # Fail on negative values
            if x < 0:
                raise ValueError("Negative value fails")
            return x * 2

        worker = TaskWorker.options(mode=worker_mode).init(fn=conditional_fail)

        # First call succeeds
        result1 = worker(5).result()
        assert result1 == 10

        # Second call fails
        with pytest.raises(ValueError, match="Negative value fails"):
            worker(-10).result()

        # Third call should still work (worker is reusable)
        result3 = worker(15).result()
        assert result3 == 30

        worker.stop()
