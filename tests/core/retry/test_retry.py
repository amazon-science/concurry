"""Unit tests for retry utilities."""

import pytest
from functools import partial
from typing import Dict, Any, List

from pydantic import BaseModel
from concurry import Worker, worker, async_gather, global_config
from concurry.core.retry import (
    RetryAlgorithm,
    RetryConfig,
    RetryValidationError,
    calculate_retry_wait,
    execute_with_retry,
)


class TestRetryConfig:
    """Test RetryConfig validation and creation."""

    def test_default_config(self):
        """Test default RetryConfig uses global config defaults."""
        config = RetryConfig()
        # Compare against global config defaults
        defaults = global_config.defaults
        assert config.num_retries == defaults.num_retries
        assert config.retry_on == [Exception]
        assert config.retry_algorithm == defaults.retry_algorithm
        assert config.retry_wait == defaults.retry_wait
        assert config.retry_jitter == defaults.retry_jitter
        assert config.retry_until is None

    def test_custom_config(self):
        """Test custom RetryConfig."""
        config = RetryConfig(
            num_retries=3,
            retry_on=[ValueError, ConnectionError],
            retry_algorithm=RetryAlgorithm.Linear,
            retry_wait=2.0,
            retry_jitter=0.5,
        )
        assert config.num_retries == 3
        assert len(config.retry_on) == 2
        assert config.retry_algorithm == RetryAlgorithm.Linear
        assert config.retry_wait == 2.0
        assert config.retry_jitter == 0.5

    def test_retry_on_single_exception(self):
        """Test retry_on with single exception class."""
        config = RetryConfig(retry_on=ValueError)
        assert len(config.retry_on) == 1
        assert config.retry_on[0] == ValueError

    def test_retry_on_callable(self):
        """Test retry_on with callable filter."""

        def filter_func(exception, **ctx):
            return isinstance(exception, ValueError) and "retry" in str(exception)

        config = RetryConfig(retry_on=filter_func)
        assert len(config.retry_on) == 1
        assert callable(config.retry_on[0])

    def test_retry_on_invalid_type(self):
        """Test retry_on with invalid type raises error."""
        with pytest.raises(ValueError):  # Pydantic type validation
            RetryConfig(retry_on=["not_valid"])

    def test_retry_on_non_exception_class(self):
        """Test retry_on with non-exception class raises error."""
        with pytest.raises(ValueError, match="must be subclasses of BaseException"):
            RetryConfig(retry_on=str)  # str is not a BaseException subclass

    def test_retry_until_single_validator(self):
        """Test retry_until with single validator."""
        validator = lambda result, **ctx: result > 0
        config = RetryConfig(retry_until=validator)
        assert len(config.retry_until) == 1
        assert callable(config.retry_until[0])

    def test_retry_until_multiple_validators(self):
        """Test retry_until with multiple validators."""
        validators = [
            lambda result, **ctx: isinstance(result, dict),
            lambda result, **ctx: "data" in result,
        ]
        config = RetryConfig(retry_until=validators)
        assert len(config.retry_until) == 2

    def test_retry_until_invalid_type(self):
        """Test retry_until with invalid type raises error."""
        with pytest.raises(ValueError):  # Pydantic type validation
            RetryConfig(retry_until="not_callable")

    def test_validation_bounds(self):
        """Test that field validation enforces bounds."""
        # num_retries must be >= 0
        with pytest.raises(ValueError):
            RetryConfig(num_retries=-1)

        # retry_wait must be > 0
        with pytest.raises(ValueError):
            RetryConfig(retry_wait=0)

        # retry_jitter must be 0 <= jitter <= 1
        with pytest.raises(ValueError):
            RetryConfig(retry_jitter=-0.1)

        with pytest.raises(ValueError):
            RetryConfig(retry_jitter=1.5)


class TestCalculateRetryWait:
    """Test retry wait time calculation."""

    def test_linear_strategy(self):
        """Test linear backoff strategy."""
        config = RetryConfig(
            retry_algorithm=RetryAlgorithm.Linear,
            retry_wait=1.0,
            retry_jitter=0,  # Disable jitter for predictable results
        )

        assert calculate_retry_wait(1, config) == 1.0
        assert calculate_retry_wait(2, config) == 2.0
        assert calculate_retry_wait(3, config) == 3.0

    def test_exponential_strategy(self):
        """Test exponential backoff strategy."""
        config = RetryConfig(
            retry_algorithm=RetryAlgorithm.Exponential,
            retry_wait=1.0,
            retry_jitter=0,
        )

        assert calculate_retry_wait(1, config) == 1.0  # 1 * 2^0
        assert calculate_retry_wait(2, config) == 2.0  # 1 * 2^1
        assert calculate_retry_wait(3, config) == 4.0  # 1 * 2^2
        assert calculate_retry_wait(4, config) == 8.0  # 1 * 2^3

    def test_fibonacci_strategy(self):
        """Test Fibonacci backoff strategy."""
        config = RetryConfig(
            retry_algorithm=RetryAlgorithm.Fibonacci,
            retry_wait=1.0,
            retry_jitter=0,
        )

        # Fibonacci sequence: 1, 1, 2, 3, 5, 8, 13, ...
        assert calculate_retry_wait(1, config) == 1.0
        assert calculate_retry_wait(2, config) == 1.0
        assert calculate_retry_wait(3, config) == 2.0
        assert calculate_retry_wait(4, config) == 3.0
        assert calculate_retry_wait(5, config) == 5.0

    def test_jitter_applied(self):
        """Test that jitter randomizes wait time."""
        config = RetryConfig(
            retry_algorithm=RetryAlgorithm.Exponential,
            retry_wait=1.0,
            retry_jitter=0.5,  # Enable jitter
        )

        # With jitter, result should be between 0 and base_wait
        wait_times = [calculate_retry_wait(3, config) for _ in range(100)]

        # All should be >= 0 and <= 4.0 (base wait for attempt 3)
        assert all(0 <= w <= 4.0 for w in wait_times)

        # With 100 samples, we should see some variation
        assert len(set(wait_times)) > 10  # At least 10 different values

    def test_jitter_zero(self):
        """Test that jitter=0 produces deterministic results."""
        config = RetryConfig(
            retry_algorithm=RetryAlgorithm.Linear,
            retry_wait=2.0,
            retry_jitter=0,
        )

        # Should always return same value for same attempt
        results = [calculate_retry_wait(2, config) for _ in range(10)]
        assert len(set(results)) == 1  # All same
        assert results[0] == 4.0


class TestExecuteWithRetry:
    """Test execute_with_retry function."""

    def test_success_no_retry(self):
        """Test successful execution without retries."""
        call_count = [0]

        def succeeds():
            call_count[0] += 1
            return "success"

        config = RetryConfig(num_retries=3)
        context = {"method_name": "succeeds"}

        result = execute_with_retry(succeeds, (), {}, config, context)

        assert result == "success"
        assert call_count[0] == 1  # Only called once

    def test_retry_on_exception(self):
        """Test retry on exception."""
        call_count = [0]

        def fails_twice():
            call_count[0] += 1
            if call_count[0] < 3:
                raise ValueError("Temporary failure")
            return "success"

        config = RetryConfig(
            num_retries=3,
            retry_on=[ValueError],
            retry_wait=0.01,  # Fast retries for testing
            retry_jitter=0,
        )
        context = {"method_name": "fails_twice"}

        result = execute_with_retry(fails_twice, (), {}, config, context)

        assert result == "success"
        assert call_count[0] == 3  # Called 3 times (failed twice, succeeded third time)

    def test_retry_exhaustion(self):
        """Test that retries are exhausted and exception is raised."""
        call_count = [0]

        def always_fails():
            call_count[0] += 1
            raise ValueError("Always fails")

        config = RetryConfig(
            num_retries=2,
            retry_on=[ValueError],
            retry_wait=0.01,
            retry_jitter=0,
        )
        context = {"method_name": "always_fails"}

        with pytest.raises(ValueError, match="Always fails"):
            execute_with_retry(always_fails, (), {}, config, context)

        # Should try initial + 2 retries = 3 times total
        assert call_count[0] == 3

    def test_retry_on_specific_exception_only(self):
        """Test that only specific exceptions trigger retry."""
        call_count = [0]

        def fails_with_runtime_error():
            call_count[0] += 1
            raise RuntimeError("Wrong exception type")

        config = RetryConfig(
            num_retries=3,
            retry_on=[ValueError],  # Only retry on ValueError
            retry_wait=0.01,
            retry_jitter=0,
        )
        context = {"method_name": "fails_with_runtime_error"}

        with pytest.raises(RuntimeError, match="Wrong exception type"):
            execute_with_retry(fails_with_runtime_error, (), {}, config, context)

        # Should only be called once (no retry for RuntimeError)
        assert call_count[0] == 1

    def test_retry_with_callable_filter(self):
        """Test retry with callable exception filter."""
        call_count = [0]

        def fails_conditionally():
            call_count[0] += 1
            if call_count[0] == 1:
                raise ValueError("retry me")
            elif call_count[0] == 2:
                raise ValueError("don't retry")
            return "success"

        def filter_func(exception, **ctx):
            return "retry me" in str(exception)

        config = RetryConfig(
            num_retries=3,
            retry_on=filter_func,
            retry_wait=0.01,
            retry_jitter=0,
        )
        context = {"method_name": "fails_conditionally"}

        # First call raises "retry me" -> retry
        # Second call raises "don't retry" -> no retry, raises immediately
        with pytest.raises(ValueError, match="don't retry"):
            execute_with_retry(fails_conditionally, (), {}, config, context)

        assert call_count[0] == 2

    def test_retry_until_validation(self):
        """Test retry with output validation."""
        call_count = [0]

        def returns_invalid_then_valid():
            call_count[0] += 1
            if call_count[0] < 3:
                return {"status": "pending"}
            return {"status": "success", "data": "result"}

        config = RetryConfig(
            num_retries=5,
            retry_until=lambda result, **ctx: result.get("status") == "success",
            retry_wait=0.01,
            retry_jitter=0,
        )
        context = {"method_name": "returns_invalid_then_valid"}

        result = execute_with_retry(returns_invalid_then_valid, (), {}, config, context)

        assert result == {"status": "success", "data": "result"}
        assert call_count[0] == 3

    def test_retry_until_exhaustion(self):
        """Test that validation failures raise RetryValidationError."""
        call_count = [0]

        def returns_invalid():
            call_count[0] += 1
            return {"status": "pending", "attempt": call_count[0]}

        config = RetryConfig(
            num_retries=2,
            retry_until=lambda result, **ctx: result.get("status") == "success",
            retry_wait=0.01,
            retry_jitter=0,
        )
        context = {"method_name": "returns_invalid"}

        with pytest.raises(RetryValidationError) as exc_info:
            execute_with_retry(returns_invalid, (), {}, config, context)

        error = exc_info.value
        assert error.attempts == 3  # Initial + 2 retries
        assert len(error.all_results) == 3
        assert len(error.validation_errors) == 3
        assert error.method_name == "returns_invalid"
        # Verify all results are in all_results
        assert error.all_results[0] == {"status": "pending", "attempt": 1}
        assert error.all_results[1] == {"status": "pending", "attempt": 2}
        assert error.all_results[2] == {"status": "pending", "attempt": 3}

    def test_retry_until_multiple_validators(self):
        """Test retry with multiple validators (all must pass)."""
        call_count = [0]

        def returns_gradually_valid():
            call_count[0] += 1
            if call_count[0] == 1:
                return "invalid"
            elif call_count[0] == 2:
                return {"incomplete": True}
            else:
                return {"status": "success", "data": "result"}

        validators = [
            lambda result, **ctx: isinstance(result, dict),
            lambda result, **ctx: result.get("status") == "success",
        ]

        config = RetryConfig(
            num_retries=5,
            retry_until=validators,
            retry_wait=0.01,
            retry_jitter=0,
        )
        context = {"method_name": "returns_gradually_valid"}

        result = execute_with_retry(returns_gradually_valid, (), {}, config, context)

        assert result == {"status": "success", "data": "result"}
        assert call_count[0] == 3

    def test_context_passed_to_filters(self):
        """Test that context is passed to exception filters and validators."""
        contexts_seen = []

        def track_context(exception=None, result=None, **ctx):
            contexts_seen.append(ctx.copy())
            if exception:
                return True  # Always retry
            if result:
                return result.get("done", False)

        call_count = [0]

        def func_with_context(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 2:
                raise ValueError("retry")
            return {"done": True}

        config = RetryConfig(
            num_retries=3,
            retry_on=track_context,
            retry_until=track_context,
            retry_wait=0.01,
            retry_jitter=0,
        )
        context = {
            "method_name": "func_with_context",
            "worker_class": "TestWorker",
            "args": (1, 2),
            "kwargs": {"key": "value"},
        }

        result = execute_with_retry(func_with_context, (1, 2), {"key": "value"}, config, context)

        assert result == {"done": True}
        assert len(contexts_seen) >= 2  # At least 2 calls (exception filter + validator)

        # Verify context includes expected keys
        for ctx in contexts_seen:
            assert "attempt" in ctx
            assert "elapsed_time" in ctx
            assert "method_name" in ctx
            assert ctx["method_name"] == "func_with_context"


class TestRetryValidationError:
    """Test RetryValidationError exception."""

    def test_error_attributes(self):
        """Test RetryValidationError attributes."""
        results = [1, 2, 3]
        errors = ["error1", "error2", "error3"]

        error = RetryValidationError(
            attempts=3,
            all_results=results,
            validation_errors=errors,
            method_name="test_method",
        )

        assert error.attempts == 3
        assert error.all_results == results
        assert error.validation_errors == errors
        assert error.method_name == "test_method"
        assert "test_method" in str(error)
        assert "3 attempts" in str(error)


class TestInternalAsyncCallsWithValidation:
    """Test internal async method calls with retry_until validation.

    This tests the scenario where one worker method calls another worker method
    internally, and the inner method has validation that can fail.
    This mimics the user's scenario: call_batch() -> self.call_llm()
    """

    def test_internal_call_validation_failure_num_retries_zero(self):
        """Test that validation failure in internal call raises exception (num_retries=0).

        Scenario:
        - Worker has two async methods: inner() and outer()
        - outer() calls self.inner() internally
        - inner() has num_retries=0 and retry_until validation
        - When validation fails, RetryValidationError should propagate through outer()

        This tests the exact scenario the user reported.
        """

        class TestWorker(Worker):
            async def inner(self, value: int) -> dict:
                """Inner method that returns a dict."""
                return {"value": value, "status": "pending"}

            async def outer(self, value: int) -> dict:
                """Outer method that calls inner internally."""
                # This mimics call_batch calling self.call_llm
                result = await self.inner(value)
                return result

        def validate_status(result, **ctx):
            """Validator that requires status='success'."""
            return result.get("status") == "success"

        worker = TestWorker.options(
            mode="asyncio",
            num_retries={"*": 0, "inner": 0},
            retry_until={"*": None, "inner": validate_status},
        ).init()

        try:
            # Call outer, which internally calls inner
            # inner's validation will fail (status='pending' not 'success')
            with pytest.raises(RetryValidationError) as exc_info:
                worker.outer(42).result()

            # Verify exception details
            error = exc_info.value
            assert error.attempts == 1  # num_retries=0 means 1 attempt
            assert error.method_name == "inner"
            assert len(error.all_results) == 1
            assert error.all_results[0] == {"value": 42, "status": "pending"}
        finally:
            worker.stop()

    def test_internal_call_validation_failure_with_retries(self):
        """Test that validation failure in internal call retries correctly.

        Same as above but with num_retries>0 to verify retries work.
        """

        call_count = [0]

        class TestWorker(Worker):
            async def inner(self, value: int) -> dict:
                """Inner method that succeeds on 3rd call."""
                call_count[0] += 1
                if call_count[0] < 3:
                    return {"value": value, "status": "pending"}
                return {"value": value, "status": "success"}

            async def outer(self, value: int) -> dict:
                """Outer method that calls inner internally."""
                result = await self.inner(value)
                return result

        def validate_status(result, **ctx):
            """Validator that requires status='success'."""
            return result.get("status") == "success"

        worker = TestWorker.options(
            mode="asyncio",
            num_retries={"*": 0, "inner": 5},  # inner can retry
            retry_until={"*": None, "inner": validate_status},
            retry_wait={"*": 1.0, "inner": 0.01},  # Fast retries for testing
            retry_algorithm="linear",
            retry_jitter=0,
        ).init()

        try:
            # Call outer, which internally calls inner
            # inner will fail validation twice, succeed on 3rd attempt
            result = worker.outer(42).result()

            # Should get successful result after retries
            assert result == {"value": 42, "status": "success"}
            assert call_count[0] == 3
        finally:
            worker.stop()

    def test_async_gather_multiple_internal_calls_one_fails(self):
        """Test async_gather with multiple internal calls where one fails validation.

        This is the EXACT scenario the user reported:
        - call_batch() uses async_gather([self.call_llm(p) for p in prompts])
        - One of the call_llm() calls fails validation
        - Exception should propagate through async_gather
        """

        class TestWorker(Worker):
            async def inner(self, value: int) -> dict:
                """Inner method that returns a dict."""
                # Value 42 will fail validation, others succeed
                if value == 42:
                    return {"value": value, "status": "pending"}
                return {"value": value, "status": "success"}

            async def batch_process(self, values: list) -> list:
                """Batch method that calls inner multiple times via async_gather."""
                # This mimics call_batch calling self.call_llm multiple times
                tasks = [self.inner(v) for v in values]
                results = await async_gather(tasks)
                return results

        def validate_status(result, **ctx):
            """Validator that requires status='success'."""
            return result.get("status") == "success"

        worker = TestWorker.options(
            mode="asyncio",
            num_retries={"*": 0, "inner": 0},
            retry_until={"*": None, "inner": validate_status},
        ).init()

        try:
            # Call batch_process with values [1, 2, 42, 3]
            # The call with value=42 will fail validation
            with pytest.raises(RetryValidationError) as exc_info:
                worker.batch_process([1, 2, 42, 3]).result()

            # Verify exception is from the failing call
            error = exc_info.value
            assert error.method_name == "inner"
            assert error.attempts == 1
            assert error.all_results[0]["value"] == 42
            assert error.all_results[0]["status"] == "pending"
        finally:
            worker.stop()

    def test_async_gather_all_pass_validation(self):
        """Test async_gather with multiple internal calls where all pass validation.

        Positive test case: all calls succeed validation.
        """

        class TestWorker(Worker):
            async def inner(self, value: int) -> dict:
                """Inner method that returns a dict."""
                return {"value": value, "status": "success"}

            async def batch_process(self, values: list) -> list:
                """Batch method that calls inner multiple times via async_gather."""
                tasks = [self.inner(v) for v in values]
                results = await async_gather(tasks)
                return results

        def validate_status(result, **ctx):
            """Validator that requires status='success'."""
            return result.get("status") == "success"

        worker = TestWorker.options(
            mode="asyncio",
            num_retries={"*": 0, "inner": 0},
            retry_until={"*": None, "inner": validate_status},
        ).init()

        try:
            # All calls should pass validation
            results = worker.batch_process([1, 2, 3, 4]).result()

            # Verify all results returned successfully
            assert len(results) == 4
            assert all(r["status"] == "success" for r in results)
            assert [r["value"] for r in results] == [1, 2, 3, 4]
        finally:
            worker.stop()

    def test_async_gather_multiple_failures_first_raises(self):
        """Test async_gather where multiple calls fail, but first exception is raised.

        When multiple tasks fail, async_gather (with return_exceptions=False)
        should raise the first exception encountered.
        """

        class TestWorker(Worker):
            async def inner(self, value: int) -> dict:
                """Inner method that returns a dict."""
                # Values 10, 20, 30 will fail validation
                if value in [10, 20, 30]:
                    return {"value": value, "status": "pending"}
                return {"value": value, "status": "success"}

            async def batch_process(self, values: list) -> list:
                """Batch method that calls inner multiple times via async_gather."""
                tasks = [self.inner(v) for v in values]
                results = await async_gather(tasks)
                return results

        def validate_status(result, **ctx):
            """Validator that requires status='success'."""
            return result.get("status") == "success"

        worker = TestWorker.options(
            mode="asyncio",
            num_retries={"*": 0, "inner": 0},
            retry_until={"*": None, "inner": validate_status},
        ).init()

        try:
            # Multiple calls will fail validation
            # async_gather should raise exception from one of them
            with pytest.raises(RetryValidationError) as exc_info:
                worker.batch_process([1, 10, 20, 30, 2]).result()

            # Verify exception is raised
            error = exc_info.value
            assert error.method_name == "inner"
            assert error.attempts == 1
            # The failing value should be one of [10, 20, 30]
            assert error.all_results[0]["value"] in [10, 20, 30]
        finally:
            worker.stop()

    def test_basemodel_worker_decorator_with_partial_validator(self):
        """Test EXACT user scenario: @worker decorator + BaseModel + partial() validator.

        This is the exact scenario the user reported that exposed the composition
        wrapper bug where internal method calls bypassed retry validation.

        Key elements that trigger the bug (now fixed):
        - @worker(mode="asyncio") decorator
        - BaseModel inheritance (triggers composition wrapper)
        - partial() validator with num_retries=0
        - Internal method calls via async_gather
        """

        def validator_with_threshold(result, threshold: int, **kw) -> bool:
            """Validator that checks if result value > threshold."""
            if isinstance(result, dict) and "value" in result:
                value = result["value"]
            else:
                value = result
            return value > threshold

        @worker(mode="asyncio")
        class LLMWorker(BaseModel):
            """Mimics user's LLM worker with BaseModel."""

            multiplier: int

            async def call_llm(self, x: int) -> Dict[str, Any]:
                """Mimics user's call_llm - returns a dict."""
                result = x * self.multiplier
                return {"value": result, "response": f"Result is {result}"}

            async def call_batch(self, values: List[int]) -> List[Dict[str, Any]]:
                """Mimics user's call_batch - calls call_llm via async_gather."""
                tasks = [self.call_llm(v) for v in values]
                results = await async_gather(tasks)
                return results

        # Create worker with partial() validator - EXACT user configuration
        llm = LLMWorker.options(
            num_retries={"*": 0, "call_llm": 0},
            retry_until={"*": None, "call_llm": partial(validator_with_threshold, threshold=15)},
        ).init(multiplier=2)

        try:
            # Values: 5*2=10 (FAIL: < 15), 10*2=20 (PASS: > 15), 8*2=16 (PASS: > 15)
            # Should raise RetryValidationError for the first failing value
            with pytest.raises(RetryValidationError) as exc_info:
                llm.call_batch([5, 10, 8]).result()

            # Verify exception details
            error = exc_info.value
            assert error.method_name == "call_llm"
            assert error.attempts == 1  # num_retries=0
            assert error.all_results[0]["value"] == 10  # 5*2=10 failed validation

        finally:
            llm.stop()
