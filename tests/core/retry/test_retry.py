"""Unit tests for retry utilities."""

import pytest

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
        """Test default RetryConfig."""
        config = RetryConfig()
        assert config.num_retries == 0
        assert config.retry_on == [Exception]
        assert config.retry_algorithm == RetryAlgorithm.Exponential
        assert config.retry_wait == 1.0
        assert config.retry_jitter == 0.3
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
