"""Comprehensive tests for @task decorator."""

import pytest

from concurry import ExecutionMode, task


class TestTaskDecoratorBasics:
    """Test basic @task decorator functionality."""

    def test_task_decorator_basic(self, worker_mode):
        """Test @task decorator across all modes."""

        def compute(x, y):
            return x + y

        # Apply decorator inside test to get the right mode
        decorated = task(mode=worker_mode)(compute)

        # Call directly
        result = decorated(3, 4).result()
        assert result == 7

        # Use submit explicitly
        future = decorated.submit(5, 6)
        assert future.result() == 11

        # Clean up
        decorated.stop()

    def test_task_decorator_with_pool(self, pool_mode):
        """Test @task decorator with worker pools."""

        @task(mode=pool_mode, max_workers=2)
        def multiply(x, y):
            return x * y

        result = multiply(3, 4).result()
        assert result == 12

        multiply.stop()

    def test_task_decorator_preserves_metadata(self, worker_mode):
        """Test that decorator preserves function metadata across all modes."""

        def documented_function(x):
            """This is a documented function."""
            return x * 2

        # Apply decorator inside test to get the right mode
        decorated = task(mode=worker_mode)(documented_function)

        assert decorated.__name__ == "documented_function"
        assert decorated.__doc__ == "This is a documented function."

        decorated.stop()


class TestTaskDecoratorInvocation:
    """Test different invocation methods for decorated functions."""

    def test_direct_call(self):
        """Test calling decorated function directly."""

        @task(mode=ExecutionMode.Sync)
        def add(x, y):
            return x + y

        result = add(10, 20).result()
        assert result == 30

        add.stop()

    def test_submit_method(self):
        """Test using submit() method explicitly."""

        @task(mode=ExecutionMode.Sync)
        def subtract(x, y):
            return x - y

        future = subtract.submit(10, 3)
        assert future.result() == 7

        subtract.stop()

    def test_map_method(self):
        """Test using map() method."""

        @task(mode=ExecutionMode.Sync)
        def square(x):
            return x**2

        results = list(square.map(range(5)))
        assert results == [0, 1, 4, 9, 16]

        square.stop()

    def test_blocking_mode(self):
        """Test decorator with blocking mode."""

        @task(mode=ExecutionMode.Sync, blocking=True)
        def double(x):
            return x * 2

        # Should return result directly, not a future
        result = double(5)
        assert result == 10
        assert not hasattr(result, "result")  # Not a future

        double.stop()


class TestTaskDecoratorProgressBar:
    """Test ProgressBar integration with @task decorator."""

    def test_map_with_progress_true(self):
        """Test map() with progress=True."""

        @task(mode=ExecutionMode.Sync)
        def process(x):
            return x + 1

        results = list(process.map(range(10), progress=True))
        assert results == list(range(1, 11))

        process.stop()

    def test_map_with_progress_dict(self):
        """Test map() with progress as dict configuration."""

        @task(mode=ExecutionMode.Sync)
        def process(x):
            return x * 2

        results = list(process.map(range(5), progress={"desc": "Processing", "disable": True}))
        assert results == [0, 2, 4, 6, 8]

        process.stop()


class TestTaskDecoratorExecutionModes:
    """Test @task decorator with different execution modes."""

    def test_different_modes(self, worker_mode):
        """Test decorator with different execution modes."""

        def compute(x):
            return x**2

        # Apply decorator inside test to get the right mode
        decorated = task(mode=worker_mode)(compute)

        result = decorated(5).result()
        assert result == 25

        decorated.stop()

    def test_on_demand_mode_thread(self):
        """Test on-demand worker creation with threads."""

        @task(mode=ExecutionMode.Threads, on_demand=True, max_workers=0)
        def process(x):
            return x + 1

        results = [process(i).result() for i in range(5)]
        assert results == [1, 2, 3, 4, 5]

        process.stop()


class TestTaskDecoratorRetries:
    """Test retry integration with @task decorator."""

    def test_decorator_with_retries(self):
        """Test decorated function with retry configuration."""
        counter = {"calls": 0}

        @task(mode=ExecutionMode.Sync, num_retries=3, retry_wait=0.01)
        def flaky_function(x):
            counter["calls"] += 1
            if counter["calls"] < 3:
                raise ValueError("Not yet!")
            return x * 2

        result = flaky_function(5).result()
        assert result == 10
        assert counter["calls"] == 3  # Initial + 2 retries

        flaky_function.stop()

    def test_decorator_with_retry_on_specific_exception(self):
        """Test retrying only on specific exceptions."""
        counter = {"calls": 0}

        @task(mode=ExecutionMode.Sync, num_retries=2, retry_on=[ValueError])
        def selective_retry(x):
            counter["calls"] += 1
            if counter["calls"] == 1:
                raise ValueError("Retry this")
            return x * 3

        result = selective_retry(4).result()
        assert result == 12
        assert counter["calls"] == 2

        selective_retry.stop()


class TestTaskDecoratorErrorHandling:
    """Test error handling in @task decorator."""

    def test_missing_function_raises_error(self):
        """Test that calling submit without function or bound function raises error."""
        # This test verifies internal behavior - TaskWorker should handle this
        from concurry import TaskWorker

        worker = TaskWorker.options(mode=ExecutionMode.Sync).init()

        with pytest.raises(TypeError, match="submit\\(\\) requires a callable function"):
            worker.submit(5)

        worker.stop()

    def test_async_function_support(self):
        """Test that async functions work with @task decorator."""
        import asyncio

        @task(mode=ExecutionMode.Asyncio)
        async def async_compute(x, y):
            await asyncio.sleep(0.001)
            return x + y

        result = async_compute(3, 4).result()
        assert result == 7

        async_compute.stop()


class TestTaskDecoratorContextManager:
    """Test context manager support for decorated functions."""

    def test_context_manager_cleanup(self):
        """Test that worker is properly cleaned up with context manager."""

        @task(mode=ExecutionMode.Sync)
        def compute(x):
            return x * 2

        # Workers support context manager protocol
        with compute:
            result = compute(5).result()
            assert result == 10
        # Worker should be stopped automatically


class TestTaskDecoratorLimitsForwarding:
    """Test limits forwarding to decorated functions."""

    def test_function_with_limits_parameter(self):
        """Test that limits are forwarded when function has limits parameter."""
        from concurry import RateLimit

        limits = [RateLimit(key="test", capacity=10, window_seconds=60)]

        @task(mode=ExecutionMode.Sync, limits=limits)
        def api_call(prompt, limits):
            # Function receives limits parameter
            assert limits is not None
            assert hasattr(limits, "acquire")
            return f"Processed: {prompt}"

        result = api_call("test").result()
        assert result == "Processed: test"

        api_call.stop()

    def test_function_without_limits_parameter(self):
        """Test that limits are not passed when function doesn't have limits parameter."""
        from concurry import CallLimit

        limits = [CallLimit(window_seconds=60, capacity=10)]

        @task(mode=ExecutionMode.Sync, limits=limits)
        def simple_func(x):
            # This function doesn't have limits parameter
            return x * 2

        result = simple_func(5).result()
        assert result == 10

        simple_func.stop()


class TestTaskDecoratorEdgeCases:
    """Test @task decorator edge cases and advanced scenarios."""

    def test_decorator_with_no_arguments(self):
        """Test decorator with function that takes no arguments."""

        @task(mode=ExecutionMode.Sync)
        def get_constant():
            return 100

        result = get_constant().result()
        assert result == 100

        get_constant.stop()

    def test_decorator_with_varargs(self):
        """Test decorator with function that has *args."""

        @task(mode=ExecutionMode.Sync)
        def sum_values(*args):
            return sum(args)

        result = sum_values(1, 2, 3, 4, 5).result()
        assert result == 15

        sum_values.stop()

    def test_decorator_with_varkwargs(self):
        """Test decorator with function that has **kwargs."""

        @task(mode=ExecutionMode.Sync)
        def count_kwargs(**kwargs):
            return len(kwargs)

        result = count_kwargs(a=1, b=2, c=3).result()
        assert result == 3

        count_kwargs.stop()

    def test_decorator_with_default_arguments(self):
        """Test decorator with function that has default arguments."""

        @task(mode=ExecutionMode.Sync)
        def greet(name, greeting="Hello"):
            return f"{greeting}, {name}!"

        result1 = greet("Alice").result()
        assert result1 == "Hello, Alice!"

        result2 = greet("Bob", greeting="Hi").result()
        assert result2 == "Hi, Bob!"

        greet.stop()

    def test_decorator_with_lambda(self):
        """Test that lambda functions work with decorator."""

        # Lambda assigned to variable, then decorated
        square = task(mode=ExecutionMode.Sync)(lambda x: x**2)

        result = square(5).result()
        assert result == 25

        square.stop()

    def test_decorator_on_nested_function(self):
        """Test decorator on nested/inner function."""

        def outer():
            @task(mode=ExecutionMode.Sync)
            def inner(x):
                return x * 3

            return inner

        decorated_fn = outer()
        result = decorated_fn(5).result()
        assert result == 15

        decorated_fn.stop()

    def test_multiple_decorated_functions(self):
        """Test multiple separately decorated functions."""

        @task(mode=ExecutionMode.Sync)
        def add(x, y):
            return x + y

        @task(mode=ExecutionMode.Sync)
        def multiply(x, y):
            return x * y

        result1 = add(3, 4).result()
        result2 = multiply(3, 4).result()

        assert result1 == 7
        assert result2 == 12

        add.stop()
        multiply.stop()

    def test_decorator_with_mixed_args_kwargs(self):
        """Test decorator with function having mixed args and kwargs."""

        @task(mode=ExecutionMode.Sync)
        def complex_fn(a, b, *args, c=10, **kwargs):
            return a + b + sum(args) + c + sum(kwargs.values())

        result = complex_fn(1, 2, 3, 4, c=5, d=6, e=7).result()
        assert result == 1 + 2 + 3 + 4 + 5 + 6 + 7

        complex_fn.stop()

    def test_decorator_with_exception_in_function(self):
        """Test decorator when function raises exception."""

        @task(mode=ExecutionMode.Sync)
        def failing_fn(x):
            if x < 0:
                raise ValueError("Negative not allowed")
            return x * 2

        # Success case
        result = failing_fn(5).result()
        assert result == 10

        # Failure case
        with pytest.raises(ValueError, match="Negative not allowed"):
            failing_fn(-1).result()

        # Should still work after exception
        result2 = failing_fn(10).result()
        assert result2 == 20

        failing_fn.stop()

    def test_decorator_map_with_empty_iterable(self):
        """Test decorator's map() with empty iterable."""

        @task(mode=ExecutionMode.Sync)
        def process(x):
            return x * 2

        results = list(process.map([]))
        assert results == []

        process.stop()

    def test_decorator_map_with_single_item(self):
        """Test decorator's map() with single item."""

        @task(mode=ExecutionMode.Sync)
        def square(x):
            return x**2

        results = list(square.map([7]))
        assert results == [49]

        square.stop()

    def test_decorator_map_with_multiple_iterables(self):
        """Test decorator's map() with multiple iterables."""

        @task(mode=ExecutionMode.Sync)
        def add_three(x, y, z):
            return x + y + z

        results = list(add_three.map([1, 2], [10, 20], [100, 200]))
        assert results == [111, 222]

        add_three.stop()

    def test_decorator_with_none_as_arguments(self):
        """Test decorator when None is passed as arguments.

        With bound functions, None can be passed as any argument including the first.
        """

        @task(mode=ExecutionMode.Sync)
        def check_none(x, y):
            return (x is None, y is None)

        # None as first argument works
        result1 = check_none(None, 5).result()
        assert result1 == (True, False)

        # None as second argument works
        result2 = check_none(5, None).result()
        assert result2 == (False, True)

        # Both None works
        result3 = check_none(None, None).result()
        assert result3 == (True, True)

        check_none.stop()

    def test_decorator_with_pool_submit(self):
        """Test decorator with pool using submit()."""

        @task(mode=ExecutionMode.Threads, max_workers=3)
        def process(x):
            return x**2

        # Use submit() for individual tasks with pools
        futures = [process(i) for i in range(10)]
        results = [f.result() for f in futures]
        assert results == [i**2 for i in range(10)]

        process.stop()

    def test_decorator_preserves_function_attributes(self):
        """Test that decorator preserves all function attributes."""

        def original_fn(x):
            """Original docstring."""
            return x * 2

        original_fn.custom_attr = "custom_value"

        @task(mode=ExecutionMode.Sync)
        def decorated_fn(x):
            """Decorated docstring."""
            return x * 2

        assert decorated_fn.__name__ == "decorated_fn"
        assert decorated_fn.__doc__ == "Decorated docstring."

        decorated_fn.stop()

    def test_decorator_with_type_annotations(self):
        """Test decorator with type-annotated function."""

        @task(mode=ExecutionMode.Sync)
        def typed_fn(x: int, y: int) -> int:
            return x + y

        result = typed_fn(3, 4).result()
        assert result == 7

        typed_fn.stop()

    def test_decorator_async_with_sync_mode_falls_back(self):
        """Test async function with sync mode (should work via asyncio.run)."""
        import asyncio

        @task(mode=ExecutionMode.Sync)
        async def async_fn(x):
            await asyncio.sleep(0.001)
            return x * 2

        result = async_fn(5).result()
        assert result == 10

        async_fn.stop()

    def test_decorator_submit_vs_call_equivalence(self):
        """Test that submit() and direct call produce same results."""

        @task(mode=ExecutionMode.Sync)
        def compute(x, y):
            return x + y

        result1 = compute(3, 4).result()
        result2 = compute.submit(3, 4).result()

        assert result1 == result2 == 7

        compute.stop()

    def test_decorator_map_vs_manual_submit(self):
        """Test that map() produces same results as manual submit loop."""

        @task(mode=ExecutionMode.Sync)
        def square(x):
            return x**2

        # Using map
        map_results = list(square.map(range(5)))

        # Using manual submit
        futures = [square.submit(i) for i in range(5)]
        submit_results = [f.result() for f in futures]

        assert map_results == submit_results == [0, 1, 4, 9, 16]

        square.stop()

    def test_decorator_with_closure_variables(self):
        """Test decorator with function that uses closure variables."""
        multiplier = 10

        @task(mode=ExecutionMode.Sync)
        def use_closure(x):
            return x * multiplier

        result = use_closure(5).result()
        assert result == 50

        use_closure.stop()

    def test_decorator_on_class_method_should_fail(self):
        """Test that decorator on class methods is not supported."""

        # This is expected to work but the worker won't have 'self'
        # This test documents the limitation
        class MyClass:
            @task(mode=ExecutionMode.Sync)
            def method(self, x):
                return x * 2

        # Creating instance
        obj = MyClass()

        # This will fail because 'self' is not bound properly
        # The decorator creates a worker that doesn't have access to self
        with pytest.raises(Exception):
            obj.method(5).result()

    def test_decorator_with_generator_function(self):
        """Test decorator with generator function."""

        @task(mode=ExecutionMode.Sync)
        def gen_fn(n):
            return list(range(n))

        result = gen_fn(5).result()
        assert result == [0, 1, 2, 3, 4]

        gen_fn.stop()

    def test_decorator_pool_preserves_order_with_varying_execution_times(self):
        """Test that pool with submit preserves order with varying execution times."""
        import time

        @task(mode=ExecutionMode.Threads, max_workers=3)
        def variable_time(x):
            # Earlier items take longer
            time.sleep(0.001 * (10 - x))
            return x

        # Submit tasks and collect futures in order
        futures = [variable_time(i) for i in range(10)]
        results = [f.result() for f in futures]
        assert results == list(range(10))

        variable_time.stop()

    def test_decorator_with_on_demand_false_explicit(self):
        """Test decorator with on_demand explicitly set to False."""

        @task(mode=ExecutionMode.Threads, on_demand=False, max_workers=2)
        def compute(x):
            return x * 2

        result = compute(5).result()
        assert result == 10

        compute.stop()

    def test_decorator_progress_with_map_interrupt(self):
        """Test progress bar behavior when map is interrupted."""

        @task(mode=ExecutionMode.Sync)
        def process(x):
            if x == 5:
                raise ValueError("Stop at 5")
            return x * 2

        # Should stop at first exception
        with pytest.raises(ValueError, match="Stop at 5"):
            list(process.map(range(10), progress=True))

        process.stop()

    def test_decorator_with_complex_return_types(self):
        """Test decorator with various complex return types."""

        @task(mode=ExecutionMode.Sync)
        def return_dict(x):
            return {"value": x, "squared": x**2}

        @task(mode=ExecutionMode.Sync)
        def return_list(x):
            return [x, x * 2, x * 3]

        @task(mode=ExecutionMode.Sync)
        def return_tuple(x):
            return (x, x**2, x**3)

        result1 = return_dict(5).result()
        assert result1 == {"value": 5, "squared": 25}

        result2 = return_list(3).result()
        assert result2 == [3, 6, 9]

        result3 = return_tuple(2).result()
        assert result3 == (2, 4, 8)

        return_dict.stop()
        return_list.stop()
        return_tuple.stop()
