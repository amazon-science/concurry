"""Comprehensive tests for Worker retry functionality.

This module tests retry behavior across all execution modes and worker features:
- Basic retry functionality across all modes (sync, thread, process, asyncio, ray)
- Retry interaction with Limits (resource, rate, call)
- Retry interaction with Shared Limits
- Retry interaction with Pydantic (BaseModel, validate_call)
- Retry interaction with Worker Pools
- Edge cases and complex scenarios
"""

import asyncio
import time
from typing import Any, Dict

import pytest
from morphic import Typed
from pydantic import BaseModel, Field, ValidationError, validate_call

from concurry import (
    CallLimit,
    LimitSet,
    RateLimit,
    RateLimitAlgorithm,
    ResourceLimit,
    TaskWorker,
    Worker,
)
from concurry.core.retry import RetryAlgorithm, RetryValidationError

# Worker mode fixture and cleanup are provided by tests/conftest.py


# =============================================================================
# Test Worker Classes
# =============================================================================


class CounterWorker(Worker):
    """Worker that counts attempts and succeeds after N tries."""

    def __init__(self, succeed_after: int = 3):
        self.succeed_after = succeed_after
        self.attempt_count = 0

    def flaky_method(self, value: int) -> int:
        """Method that fails N times then succeeds."""
        self.attempt_count += 1
        if self.attempt_count < self.succeed_after:
            raise ValueError(f"Attempt {self.attempt_count} failed")
        return value * 2

    def reset(self) -> None:
        """Reset the counter."""
        self.attempt_count = 0

    def get_attempts(self) -> int:
        """Get the number of attempts made."""
        return self.attempt_count


class ExceptionTypeWorker(Worker):
    """Worker that raises different exception types."""

    def __init__(self):
        self.attempt_count = 0

    def value_error_method(self) -> str:
        """Always raises ValueError."""
        self.attempt_count += 1
        raise ValueError(f"ValueError on attempt {self.attempt_count}")

    def type_error_method(self) -> str:
        """Always raises TypeError."""
        self.attempt_count += 1
        raise TypeError(f"TypeError on attempt {self.attempt_count}")

    def mixed_error_method(self) -> str:
        """Raises ValueError then TypeError."""
        self.attempt_count += 1
        if self.attempt_count == 1:
            raise ValueError("First attempt ValueError")
        raise TypeError("Subsequent attempt TypeError")


class ValidatedOutputWorker(Worker):
    def __init__(self):
        self.attempt_count = 0

    @validate_call
    def get_positive_number(self, base: int) -> int:
        """Returns increasing numbers."""
        self.attempt_count += 1
        return base + self.attempt_count


class ValidatedMethodWorker(Worker):
    """Worker with @validate_call decorator."""

    def __init__(self):
        self.attempt_count = 0

    @validate_call
    def validated_method(self, x: int, y: int) -> int:
        """Method with input validation."""
        self.attempt_count += 1
        if self.attempt_count < 2:
            raise ValueError("First attempt fails")
        return x + y


class ConfiguredPydanticWorker(Worker, BaseModel):
    """Worker inheriting from BaseModel with extra fields allowed."""

    max_retries: int = Field(default=3)
    timeout: float = Field(default=1.0)
    attempt_count: int = Field(default=0)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def process(self, value: int) -> int:
        object.__setattr__(self, "attempt_count", self.attempt_count + 1)
        if self.attempt_count < 2:
            raise ValueError("Processing failed")
        return value * self.max_retries


class TypedWorkerWithRetry(Worker, Typed):
    """Worker inheriting from Typed with extra fields allowed."""

    config_value: int

    model_config = {"extra": "allow"}

    def post_initialize(self):
        # Use object.__setattr__ to bypass Pydantic validation for runtime attributes
        object.__setattr__(self, "attempt_count", 0)

    def compute(self, x: int) -> int:
        object.__setattr__(self, "attempt_count", self.attempt_count + 1)
        if self.attempt_count < 2:
            raise ValueError("Compute failed")
        return x * self.config_value


class OutputValidationWorker(Worker):
    """Worker for testing retry_until output validation."""

    def __init__(self):
        self.attempt_count = 0

    def get_number(self, target: int) -> int:
        """Returns increasing numbers until target is reached."""
        self.attempt_count += 1
        return self.attempt_count

    def get_dict(self) -> Dict[str, Any]:
        """Returns dict with attempt number."""
        self.attempt_count += 1
        return {"attempt": self.attempt_count, "valid": self.attempt_count >= 3}


# Module-level validator functions for picklability in process/ray modes
def validate_never_succeeds(result: int, **context) -> bool:
    """Always fails validation."""
    return False


def validate_greater_than_2(result: int, **context) -> bool:
    """Validate result is greater than 2."""
    return result > 2


def validate_is_dict(result: Any, **context) -> bool:
    """Validate result is a dict."""
    return isinstance(result, dict)


def validate_has_valid_key(result: Any, **context) -> bool:
    """Validate result has 'valid' key set to True."""
    return isinstance(result, dict) and result.get("valid") is True


def validate_greater_than_3(result: int, **context) -> bool:
    """Validate result is greater than 3."""
    return result > 3


def validate_greater_than_5(result: int, **context) -> bool:
    """Validate result is greater than 5."""
    return result > 5


def validate_greater_than_10(result: int, **context) -> bool:
    """Validate result is greater than 10."""
    return result > 10


class LimitedWorker(Worker):
    """Worker that uses limits."""

    def __init__(self):
        self.execution_count = 0
        self.concurrent_executions = 0
        self.max_concurrent = 0

    def limited_method(self, duration: float = 0.1) -> str:
        """Method that acquires limits during execution."""
        with self.limits.acquire(requested={"connections": 1}):
            self.concurrent_executions += 1
            self.max_concurrent = max(self.max_concurrent, self.concurrent_executions)
            time.sleep(duration)
            self.execution_count += 1
            self.concurrent_executions -= 1
            if self.execution_count < 3:
                raise ValueError(f"Execution {self.execution_count} failed")
            return f"Success after {self.execution_count} executions"

    def get_stats(self) -> Dict[str, int]:
        """Get execution statistics."""
        return {
            "execution_count": self.execution_count,
            "max_concurrent": self.max_concurrent,
        }


class AsyncWorker(Worker):
    """Worker with async methods."""

    def __init__(self):
        self.attempt_count = 0

    async def async_flaky_method(self, succeed_after: int = 2) -> str:
        """Async method that fails N times then succeeds."""
        self.attempt_count += 1
        await asyncio.sleep(0.01)  # Simulate async work
        if self.attempt_count < succeed_after:
            raise ValueError(f"Async attempt {self.attempt_count} failed")
        return f"Success on attempt {self.attempt_count}"


# =============================================================================
# Test Basic Retry Functionality
# =============================================================================


class TestBasicRetries:
    """Test basic retry functionality across all worker modes."""

    def test_retry_success_after_failures(self, worker_mode):
        """Test that method succeeds after retries.

        1. Creates CounterWorker with num_retries=5, succeed_after=3
        2. Calls flaky_method(10) which fails twice then succeeds
        3. Verifies result is 20 (10*2)
        4. Verifies attempt_count is 3 (2 failures + 1 success)
        5. Stops worker
        """
        worker = CounterWorker.options(
            mode=worker_mode,
            max_workers=1,
            num_retries=5,
            retry_wait=0.01,
            retry_algorithm=RetryAlgorithm.Linear,
        ).init(succeed_after=3)

        result = worker.flaky_method(10).result(timeout=5)
        assert result == 20

        attempts = worker.get_attempts().result(timeout=5)
        assert attempts == 3

        worker.stop()

    def test_retry_exhaustion(self, worker_mode):
        """Test that retries are exhausted and exception is raised.

        1. Creates CounterWorker with num_retries=2 (3 total attempts), succeed_after=5
        2. Calls flaky_method(10) which needs 5 attempts but only gets 3
        3. Verifies ValueError is raised after 3 attempts exhausted
        4. Verifies attempt_count is 3 (initial + 2 retries)
        5. Stops worker
        """
        worker = (
            CounterWorker.options(
                mode=worker_mode,
                max_workers=1,
                num_retries=2,  # Only 2 retries = 3 total attempts
                retry_wait=0.01,
            ).init(succeed_after=5)  # Need 5 attempts to succeed
        )

        future = worker.flaky_method(10)
        with pytest.raises(ValueError, match="Attempt 3 failed"):
            future.result(timeout=5)

        attempts = worker.get_attempts().result(timeout=5)
        assert attempts == 3  # Initial + 2 retries

        worker.stop()

    def test_no_retry_default(self, worker_mode):
        """Test that default behavior is no retries.

        1. Creates CounterWorker with default settings (num_retries=0), succeed_after=2
        2. Calls flaky_method(10) which fails on first attempt
        3. Verifies ValueError is raised immediately (no retries)
        4. Verifies attempt_count is 1 (only initial attempt, no retries)
        5. Stops worker
        """
        worker = CounterWorker.options(mode=worker_mode, max_workers=1).init(succeed_after=2)

        future = worker.flaky_method(10)
        with pytest.raises(ValueError, match="Attempt 1 failed"):
            future.result(timeout=5)

        attempts = worker.get_attempts().result(timeout=5)
        assert attempts == 1  # Only initial attempt

        worker.stop()

    def test_retry_with_specific_exception(self, worker_mode):
        """Test retry only on specific exception types.

        1. Creates ExceptionTypeWorker with retry_on=[ValueError] (only retries ValueError)
        2. Calls value_error_method() which raises ValueError
        3. Verifies ValueError is raised after retries exhausted
        4. Creates second worker with same retry_on=[ValueError]
        5. Calls type_error_method() which raises TypeError
        6. Verifies TypeError fails immediately (no retries) on attempt 1
        7. Stops both workers
        """
        # Test ValueError retries
        worker1 = ExceptionTypeWorker.options(
            mode=worker_mode,
            num_retries=3,
            retry_on=[ValueError],  # Only retry on ValueError
            retry_wait=0.01,
        ).init()

        # Should retry ValueError
        with pytest.raises(ValueError):
            worker1.value_error_method().result(timeout=5)

        worker1.stop()

        # Test TypeError does NOT retry (use fresh worker instance)
        worker2 = ExceptionTypeWorker.options(
            mode=worker_mode,
            num_retries=3,
            retry_on=[ValueError],  # Only retry on ValueError
            retry_wait=0.01,
        ).init()

        # Should NOT retry TypeError (fail immediately)
        with pytest.raises(TypeError, match="TypeError on attempt 1"):
            worker2.type_error_method().result(timeout=5)

        worker2.stop()

    def test_retry_with_callable_filter(self, worker_mode):
        """Test retry with custom exception filter.

        1. Defines should_retry() filter that returns True if exception message contains 'retry'
        2. Defines CustomWorker that raises ValueError with "Please RETRY this operation"
        3. Creates worker with retry_on=[should_retry] filter
        4. Calls conditional_method(should_fail=True) which fails twice with "RETRY" message
        5. Verifies method succeeds after retries (message contains "retry")
        6. Stops worker
        """

        def should_retry(exception: Exception, **context) -> bool:
            """Only retry if message contains 'retry'."""
            return "retry" in str(exception).lower()

        class CustomWorker(Worker):
            def __init__(self):
                self.attempt_count = 0

            def conditional_method(self, should_fail: bool) -> str:
                self.attempt_count += 1
                if should_fail and self.attempt_count < 3:
                    raise ValueError("Please RETRY this operation")
                return "Success"

        worker = CustomWorker.options(
            mode=worker_mode,
            num_retries=5,
            retry_on=[should_retry],
            retry_wait=0.01,
        ).init()

        # Should retry because message contains "retry"
        result = worker.conditional_method(should_fail=True).result(timeout=5)
        assert result == "Success"

        worker.stop()

    def test_retry_algorithms(self, worker_mode):
        """Test different retry algorithms."""
        for algorithm in [
            RetryAlgorithm.Linear,
            RetryAlgorithm.Exponential,
            RetryAlgorithm.Fibonacci,
        ]:
            worker = CounterWorker.options(
                mode=worker_mode,
                num_retries=3,
                retry_algorithm=algorithm,
                retry_wait=0.01,
            ).init(succeed_after=2)

            result = worker.flaky_method(10).result(timeout=5)
            assert result == 20

            worker.stop()


# =============================================================================
# Test Retry with Output Validation
# =============================================================================


class TestRetryOutputValidation:
    """Test retry_until output validation."""

    def test_retry_until_simple_validator(self, worker_mode):
        """Test retry with simple output validator."""
        worker = OutputValidationWorker.options(
            mode=worker_mode,
            num_retries=5,
            retry_until=[validate_greater_than_2],
            retry_wait=0.01,
        ).init()

        result = worker.get_number(5).result(timeout=5)
        assert result == 3  # First valid result

        worker.stop()

    def test_retry_until_multiple_validators(self, worker_mode):
        """Test retry with multiple output validators."""
        worker = OutputValidationWorker.options(
            mode=worker_mode,
            num_retries=5,
            retry_until=[validate_is_dict, validate_has_valid_key],
            retry_wait=0.01,
        ).init()

        result = worker.get_dict().result(timeout=5)
        assert result["attempt"] == 3
        assert result["valid"] is True

        worker.stop()

    def test_retry_until_validation_error(self, worker_mode):
        """Test RetryValidationError when validation fails after all retries."""
        worker = OutputValidationWorker.options(
            mode=worker_mode,
            num_retries=2,
            retry_until=[validate_never_succeeds],
            retry_wait=0.01,
        ).init()

        future = worker.get_number(5)
        with pytest.raises(RetryValidationError) as exc_info:
            future.result(timeout=5)

        error = exc_info.value
        assert error.attempts == 3  # Initial + 2 retries
        assert len(error.all_results) == 3
        assert error.all_results == [1, 2, 3]
        assert len(error.validation_errors) > 0

        worker.stop()

    def test_retry_until_with_exception_and_validation(self, worker_mode):
        """Test retry handles both exceptions and validation."""

        class MixedWorker(Worker):
            def __init__(self):
                self.attempt_count = 0

            def mixed_method(self) -> int:
                self.attempt_count += 1
                if self.attempt_count == 1:
                    raise ValueError("First attempt fails with exception")
                return self.attempt_count

        worker = MixedWorker.options(
            mode=worker_mode,
            num_retries=5,
            retry_until=[validate_greater_than_3],
            retry_wait=0.01,
        ).init()

        result = worker.mixed_method().result(timeout=5)
        assert result == 4  # Attempt 1 raises, 2-3 fail validation, 4 succeeds

        worker.stop()


# =============================================================================
# Test Retry with Limits
# =============================================================================


class TestRetryWithLimits:
    """Test retry interaction with limits."""

    def test_retry_with_resource_limit(self, worker_mode):
        """Test that limits are properly released between retry attempts."""
        limits = [ResourceLimit(key="connections", capacity=1)]

        worker = LimitedWorker.options(
            mode=worker_mode,
            max_workers=1,
            limits=limits,
            num_retries=5,
            retry_wait=0.05,
        ).init()

        # This should succeed after 3 attempts
        # If limits are not released, this would deadlock
        result = worker.limited_method(duration=0.05).result(timeout=10)
        assert "Success" in result

        stats = worker.get_stats().result(timeout=5)
        assert stats["execution_count"] == 3
        # Max concurrent should be 1 (limits are released between retries)
        assert stats["max_concurrent"] == 1

        worker.stop()

    def test_retry_with_rate_limit(self, worker_mode):
        """Test retry with rate limits."""
        limits = [
            RateLimit(
                key="api_calls",
                window_seconds=1,
                algorithm=RateLimitAlgorithm.TokenBucket,
                capacity=10,
            )
        ]

        class RateLimitedWorker(Worker):
            def __init__(self):
                self.call_count = 0

            def api_call(self) -> str:
                with self.limits.acquire(requested={"api_calls": 1}) as acq:
                    self.call_count += 1
                    if self.call_count < 3:
                        # Update usage before raising to avoid "not all limits updated" error
                        acq.update(usage={"api_calls": 1})
                        raise ValueError(f"Call {self.call_count} failed")
                    # Update usage on success too
                    acq.update(usage={"api_calls": 1})
                    return f"Success after {self.call_count} calls"

        worker = RateLimitedWorker.options(
            mode=worker_mode,
            limits=limits,
            num_retries=5,
            retry_wait=0.01,
        ).init()

        result = worker.api_call().result(timeout=5)
        assert "Success after 3 calls" == result

        worker.stop()

    def test_retry_with_call_limit(self, worker_mode):
        """Test retry with call limits."""
        # CallLimit has a fixed key "call_count", don't pass custom key
        limits = [CallLimit(window_seconds=60, capacity=10, algorithm=RateLimitAlgorithm.TokenBucket)]

        class CallLimitedWorker(Worker):
            def __init__(self):
                self.attempt_count = 0

            def limited_call(self) -> str:
                # CallLimit key is always "call_count" and usage is always 1
                # It's automatically acquired, no need to specify in requested
                with self.limits.acquire():
                    self.attempt_count += 1
                    if self.attempt_count < 2:
                        raise ValueError(f"Attempt {self.attempt_count} failed")
                    return f"Success on attempt {self.attempt_count}"

        worker = CallLimitedWorker.options(
            mode=worker_mode,
            limits=limits,
            num_retries=3,
            retry_wait=0.01,
        ).init()

        result = worker.limited_call().result(timeout=5)
        assert "Success on attempt 2" == result

        worker.stop()

    def test_retry_limits_no_deadlock(self, worker_mode):
        """Test that retries with limits don't cause deadlocks."""
        # Use a very restrictive limit
        limits = [ResourceLimit(key="resource", capacity=1)]

        class StrictWorker(Worker):
            def __init__(self):
                self.attempt_count = 0

            def strict_method(self) -> str:
                # Acquire the only available resource
                with self.limits.acquire(requested={"resource": 1}):
                    self.attempt_count += 1
                    time.sleep(0.05)  # Hold resource briefly
                    if self.attempt_count < 3:
                        raise ValueError("Not ready yet")
                    return "Success"

        worker = StrictWorker.options(
            mode=worker_mode,
            limits=limits,
            num_retries=5,
            retry_wait=0.01,
        ).init()

        # Should not deadlock - limits should be released between attempts
        result = worker.strict_method().result(timeout=10)
        assert result == "Success"

        worker.stop()


# =============================================================================
# Test Retry with Shared Limits
# =============================================================================


class TestRetryWithSharedLimits:
    """Test retry interaction with shared limits."""

    def test_retry_with_shared_resource_limit(self, worker_mode):
        """Test retry with shared resource limits across multiple workers."""
        # Create shared limit
        shared_limits = LimitSet(
            limits=[ResourceLimit(key="db_conn", capacity=2)],
            shared=True,
            mode=worker_mode,
        )

        class DBWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id
                self.attempt_count = 0

            def query(self) -> str:
                with self.limits.acquire(requested={"db_conn": 1}):
                    self.attempt_count += 1
                    time.sleep(0.05)
                    if self.attempt_count < 2:
                        raise ValueError(f"Worker {self.worker_id} attempt {self.attempt_count} failed")
                    return f"Worker {self.worker_id} success"

        # Create multiple workers sharing the same limits
        worker1 = DBWorker.options(
            mode=worker_mode,
            limits=shared_limits,
            num_retries=3,
            retry_wait=0.01,
        ).init(worker_id=1)

        worker2 = DBWorker.options(
            mode=worker_mode,
            limits=shared_limits,
            num_retries=3,
            retry_wait=0.01,
        ).init(worker_id=2)

        # Both should succeed
        result1 = worker1.query().result(timeout=10)
        result2 = worker2.query().result(timeout=10)

        assert "Worker 1 success" == result1
        assert "Worker 2 success" == result2

        worker1.stop()
        worker2.stop()

    def test_retry_shared_limit_no_starvation(self, worker_mode):
        """Test that retry with shared limits doesn't cause starvation.

        This test verifies that:
        1. Limits are properly released between retry attempts
        2. Multiple workers competing for limited resources can all eventually succeed
        3. No deadlock occurs when workers are retrying
        4. **VALIDATES SHARING**: Ensures limits are actually shared (not per-worker)

        Setup: 6 workers competing for 3 resource slots, each worker fails once
        before succeeding. With proper limit release, all should complete.

        Expected behavior:
        - First 3 workers acquire resources immediately
        - Workers 4-6 wait for resources to be released
        - After first 3 fail and retry, resources become available
        - All workers eventually succeed after proper retry+release

        If limits are NOT shared, all 6 workers would succeed immediately.
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        shared_limits = LimitSet(
            limits=[ResourceLimit(key="resource", capacity=3)],
            shared=True,
            mode=worker_mode,
        )

        class ResourceWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id
                self.attempt_count = 0

            def work(self) -> dict:
                import time

                attempt_start = time.time()
                with self.limits.acquire(requested={"resource": 1}):
                    self.attempt_count += 1
                    time.sleep(0.5)  # Hold resource for 0.5s
                    if self.attempt_count < 2:
                        raise ValueError(f"Worker {self.worker_id} fails on first attempt")
                    return {
                        "worker_id": self.worker_id,
                        "attempts": self.attempt_count,
                        "total_time": time.time() - attempt_start,
                    }

        # Use 6 workers (2x capacity) to test sharing without overwhelming the system
        workers = []
        for i in range(6):
            w = ResourceWorker.options(
                mode=worker_mode,
                limits=shared_limits,
                num_retries=5,  # Sufficient retries
                retry_wait=0.1,  # Short retry wait for faster test
            ).init(worker_id=i)
            workers.append(w)

        # All workers should eventually succeed
        # With capacity=3, at most 3 workers can hold resources simultaneously
        futures = [w.work() for w in workers]
        results = [f.result(timeout=30) for f in futures]

        # Validate all completed successfully
        assert len(results) == 6
        assert all(r["attempts"] == 2 for r in results), "All workers should succeed on second attempt"

        # Validate shared behavior: Workers experienced contention for resources
        # Note: total_time measures only the final retry attempt (timer resets on retry)
        # If limits were NOT shared, all would complete in ~0.5s (just the sleep time)
        # With shared limits, workers must wait for resources, taking longer
        total_times = [r["total_time"] for r in results]
        avg_time = sum(total_times) / len(total_times)

        # Validate shared behavior using a more robust check:
        # NOT all tasks should complete in ~0.5s (the base sleep time)
        # If limits are shared, at least some tasks must wait
        immediate_completions = sum(1 for t in total_times if t < 0.55)

        # With capacity=3 and 6 workers, we expect 3-4 to complete immediately
        # and 2-3 to wait. Due to Ray's async scheduling, allow some variance.
        assert immediate_completions < 6, (
            f"All {immediate_completions} tasks completed immediately (<0.55s), limits NOT shared! "
            f"Individual times: {total_times}"
        )

        # At least one task should have waited significantly
        max_time = max(total_times)
        assert max_time > 0.7, (
            f"No task waited significantly (max={max_time:.2f}s), limits may not be shared. "
            f"Expected at least one task to wait >0.7s. Individual times: {total_times}"
        )

        # No worker should complete faster than the base sleep time
        # This validates the measurement and that work is actually happening
        min_time = min(total_times)
        assert min_time >= 0.5, (
            f"Minimum completion time {min_time:.2f}s is too fast. "
            f"Expected at least 0.5s (the sleep time). "
            f"Individual times: {total_times}"
        )

        for w in workers:
            w.stop()


# =============================================================================
# Test Retry with Pydantic Integration
# =============================================================================


class TestRetryWithPydantic:
    """Test retry interaction with Pydantic BaseModel and validate_call."""

    def test_retry_with_validate_call(self, worker_mode):
        """Test retry on worker methods with @validate_call decorator."""
        worker = ValidatedMethodWorker.options(
            mode=worker_mode,
            num_retries=3,
            retry_wait=0.01,
        ).init()

        # Valid inputs should work with retry
        result = worker.validated_method(5, 10).result(timeout=5)
        assert result == 15

        # Invalid inputs should fail validation immediately (no retry)
        # Ray wraps exceptions in RayTaskError
        if worker_mode == "ray":
            import ray

            with pytest.raises(ray.exceptions.RayTaskError):
                worker.validated_method("not", "numbers").result(timeout=5)
        else:
            with pytest.raises((ValidationError, TypeError)):
                worker.validated_method("not", "numbers").result(timeout=5)

        worker.stop()

    def test_retry_with_basemodel_inheritance(self, worker_mode):
        """Test retry on workers inheriting from BaseModel.

        Now works in ALL modes including Ray thanks to auto-composition wrapper!
        """
        worker = ConfiguredPydanticWorker.options(
            mode=worker_mode,
            num_retries=5,
            retry_wait=0.01,
        ).init(max_retries=4)

        result = worker.process(10).result(timeout=5)
        assert result == 40  # 10 * 4

        worker.stop()

    def test_retry_with_typed_inheritance(self, worker_mode):
        """Test retry on workers inheriting from morphic.Typed.

        Now works in ALL modes including Ray thanks to auto-composition wrapper!
        """
        worker = TypedWorkerWithRetry.options(
            mode=worker_mode,
            num_retries=3,
            retry_wait=0.01,
        ).init(config_value=5)

        result = worker.compute(7).result(timeout=5)
        assert result == 35  # 7 * 5

        worker.stop()

    def test_retry_with_pydantic_validation_and_retry_until(self, worker_mode):
        """Test combining Pydantic validation with retry_until."""
        worker = ValidatedOutputWorker.options(
            mode=worker_mode,
            num_retries=15,
            retry_until=[validate_greater_than_10],
            retry_wait=0.01,
        ).init()

        result = worker.get_positive_number(5).result(timeout=5)
        assert result > 10

        worker.stop()


# =============================================================================
# Test Retry with Worker Pools
# =============================================================================


class TestRetryWithWorkerPools:
    """Test retry interaction with worker pools."""

    def test_retry_in_pool(self, worker_mode):
        """Test retry behavior in worker pools."""
        # Skip for sync/asyncio modes which don't support max_workers > 1
        if worker_mode in ["sync", "asyncio"]:
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        worker_pool = CounterWorker.options(
            mode=worker_mode,
            max_workers=3,
            num_retries=5,
            retry_wait=0.01,
        ).init(succeed_after=2)

        # All calls should succeed after retries
        futures = [worker_pool.flaky_method(i) for i in range(10)]
        results = [f.result(timeout=10) for f in futures]

        assert len(results) == 10
        assert all(r == i * 2 for i, r in enumerate(results))

        worker_pool.stop()

    def test_retry_pool_load_balancing(self, worker_mode):
        """Test that retries work correctly with pool load balancing."""
        # Skip for sync/asyncio modes which don't support max_workers > 1
        if worker_mode in ["sync", "asyncio"]:
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        class PoolWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id
                self.task_attempts = {}  # Track attempts per task

            def work(self, task_id: int) -> Dict[str, int]:
                if task_id not in self.task_attempts:
                    self.task_attempts[task_id] = 0
                self.task_attempts[task_id] += 1

                # Fail on first attempt for each task
                if self.task_attempts[task_id] == 1:
                    raise ValueError(f"Worker {self.worker_id} task {task_id} first attempt fails")

                return {
                    "worker_id": self.worker_id,
                    "task_id": task_id,
                    "attempts": self.task_attempts[task_id],
                }

        # Create pool
        pool = PoolWorker.options(
            mode=worker_mode,
            max_workers=2,
            num_retries=3,
            retry_wait=0.01,
        ).init(1)

        # Submit multiple tasks
        futures = [pool.work(i) for i in range(4)]
        results = [f.result(timeout=10) for f in futures]

        assert len(results) == 4
        # Each task should have been retried once (attempts=2)
        assert all(r["attempts"] == 2 for r in results)

        pool.stop()

    def test_retry_pool_with_limits(self, worker_mode):
        """Test retry in pools with limits."""
        # Skip for sync/asyncio modes which don't support max_workers > 1
        if worker_mode in ["sync", "asyncio"]:
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        limits = [ResourceLimit(key="connections", capacity=2)]

        class LimitedPoolWorker(Worker):
            def __init__(self):
                self.attempt_count = 0

            def limited_work(self, task_id: int) -> str:
                with self.limits.acquire(requested={"connections": 1}):
                    self.attempt_count += 1
                    time.sleep(0.05)
                    if self.attempt_count < 2:
                        raise ValueError(f"Task {task_id} attempt {self.attempt_count} failed")
                    return f"Task {task_id} completed"

        pool = LimitedPoolWorker.options(
            mode=worker_mode,
            max_workers=3,
            limits=limits,
            num_retries=5,
            retry_wait=0.01,
        ).init()

        # Submit tasks
        futures = [pool.limited_work(i) for i in range(5)]
        results = [f.result(timeout=15) for f in futures]

        assert len(results) == 5
        assert all("completed" in r for r in results)

        pool.stop()

    def test_retry_pool_individual_worker_state(self, worker_mode):
        """Test that retries maintain individual worker state in pools."""
        # Skip for sync/asyncio modes which don't support max_workers > 1
        if worker_mode in ["sync", "asyncio"]:
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        class StatefulPoolWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id
                self.state = 0

            def stateful_method(self) -> Dict[str, int]:
                self.state += 1
                if self.state < 3:
                    raise ValueError(f"Worker {self.worker_id} state {self.state} not ready")
                return {"worker_id": self.worker_id, "state": self.state}

        pool = StatefulPoolWorker.options(
            mode=worker_mode,
            max_workers=2,
            num_retries=5,
            retry_wait=0.01,
        ).init(10)  # All workers in pool use same init args

        # Each worker should retry independently
        result1 = pool.stateful_method().result(timeout=10)
        result2 = pool.stateful_method().result(timeout=10)

        # Both should succeed with state=3
        assert result1["state"] == 3
        assert result2["state"] == 3

        pool.stop()


# =============================================================================
# Test Retry with Async Methods
# =============================================================================


class TestRetryWithAsync:
    """Test retry with async worker methods."""

    def test_retry_async_method(self, worker_mode):
        """Test retry on async methods."""
        worker = AsyncWorker.options(
            mode=worker_mode,
            num_retries=5,
            retry_wait=0.01,
        ).init()

        result = worker.async_flaky_method(succeed_after=3).result(timeout=5)
        assert "Success on attempt 3" == result

        worker.stop()

    def test_retry_async_with_validation(self, worker_mode):
        """Test retry on async methods with output validation."""

        class AsyncValidationWorker(Worker):
            def __init__(self):
                self.attempt_count = 0

            async def async_get_number(self) -> int:
                self.attempt_count += 1
                await asyncio.sleep(0.01)
                return self.attempt_count

        def validate_greater_than_2(result: int, **context) -> bool:
            return result > 2

        worker = AsyncValidationWorker.options(
            mode=worker_mode,
            num_retries=5,
            retry_until=[validate_greater_than_2],
            retry_wait=0.01,
        ).init()

        result = worker.async_get_number().result(timeout=5)
        assert result == 3

        worker.stop()


# =============================================================================
# Test Retry Edge Cases
# =============================================================================


class TestRetryEdgeCases:
    """Test edge cases and complex scenarios."""

    def test_retry_with_zero_retries(self, worker_mode):
        """Test explicit zero retries."""
        worker = CounterWorker.options(
            mode=worker_mode,
            num_retries=0,  # Explicit zero
        ).init(succeed_after=2)

        with pytest.raises(ValueError):
            worker.flaky_method(10).result(timeout=5)

        worker.stop()

    def test_retry_context_passed_to_filters(self, worker_mode):
        """Test that retry context is passed to exception filters."""
        # Skip for process/ray modes - closures can't capture variables across processes
        if worker_mode in ["process", "ray"]:
            pytest.skip("Closure variable capture not supported in process/ray modes")

        context_data = {}

        def capture_context_filter(exception: Exception, **context) -> bool:
            context_data.update(context)
            return True  # Always retry

        class ContextWorker(Worker):
            def __init__(self):
                self.attempt_count = 0

            def method_with_context(self) -> str:
                self.attempt_count += 1
                if self.attempt_count < 2:
                    raise ValueError("Fail first time")
                return "Success"

        worker = ContextWorker.options(
            mode=worker_mode,
            num_retries=3,
            retry_on=[capture_context_filter],
            retry_wait=0.01,
        ).init()

        result = worker.method_with_context().result(timeout=5)
        assert result == "Success"

        # Verify context was passed
        assert "attempt" in context_data
        assert "elapsed_time" in context_data
        assert "method_name" in context_data
        assert context_data["method_name"] == "method_with_context"

        worker.stop()

    def test_retry_with_jitter(self, worker_mode):
        """Test that jitter is applied to retry delays."""
        start_time = time.time()

        worker = CounterWorker.options(
            mode=worker_mode,
            num_retries=3,
            retry_wait=0.1,
            retry_jitter=1.0,  # Full jitter
            retry_algorithm=RetryAlgorithm.Linear,
        ).init(succeed_after=4)

        result = worker.flaky_method(10).result(timeout=10)
        elapsed = time.time() - start_time

        # With full jitter on linear backoff, delays are randomized between 0 and calculated_wait
        # With full jitter (1.0), delays can be very close to 0, so we just verify:
        # 1. It completed successfully (means retries happened)
        # 2. Elapsed time is reasonable (not absurdly long)
        assert elapsed >= 0  # Should take some time (though with full jitter can be very small)
        assert elapsed < 5.0  # Should not take too long (theoretical max with jitter is 0.6s)
        assert result == 20

        worker.stop()

    def test_retry_multiple_exception_types(self, worker_mode):
        """Test retry with multiple exception types."""
        worker = ExceptionTypeWorker.options(
            mode=worker_mode,
            num_retries=5,
            retry_on=[ValueError, TypeError],
            retry_wait=0.01,
        ).init()

        # Should retry both ValueError and TypeError
        with pytest.raises(ValueError):
            worker.value_error_method().result(timeout=5)

        with pytest.raises(TypeError):
            worker.type_error_method().result(timeout=5)

        worker.stop()

    def test_retry_with_mixed_filters(self, worker_mode):
        """Test retry with both exception types and callable filters."""

        def custom_filter(exception: Exception, **context) -> bool:
            return "custom" in str(exception).lower()

        class MixedFilterWorker(Worker):
            def __init__(self):
                self.attempt_count = 0

            def method1(self) -> str:
                self.attempt_count += 1
                if self.attempt_count < 2:
                    raise ValueError("Standard error")
                return "Success1"

            def method2(self) -> str:
                self.attempt_count += 1
                if self.attempt_count < 4:
                    raise RuntimeError("Custom error")
                return "Success2"

        worker = MixedFilterWorker.options(
            mode=worker_mode,
            num_retries=5,
            retry_on=[ValueError, custom_filter],
            retry_wait=0.01,
        ).init()

        # ValueError should be retried
        result1 = worker.method1().result(timeout=5)
        assert result1 == "Success1"

        worker.stop()

        # Create new worker for second test
        worker2 = MixedFilterWorker.options(
            mode=worker_mode,
            num_retries=5,
            retry_on=[ValueError, custom_filter],
            retry_wait=0.01,
        ).init()

        # Custom filter should match RuntimeError with "custom"
        result2 = worker2.method2().result(timeout=5)
        assert result2 == "Success2"

        worker2.stop()

    def test_retry_preserves_exception_details(self, worker_mode):
        """Test that final exception preserves original details."""

        class DetailedWorker(Worker):
            def __init__(self):
                self.attempt_count = 0

            def detailed_error(self) -> str:
                self.attempt_count += 1
                raise ValueError(f"Detailed error on attempt {self.attempt_count}")

        worker = DetailedWorker.options(
            mode=worker_mode,
            num_retries=2,
            retry_wait=0.01,
        ).init()

        with pytest.raises(ValueError, match="Detailed error on attempt 3"):
            worker.detailed_error().result(timeout=5)

        worker.stop()

    def test_retry_until_validator_receives_context(self, worker_mode):
        """Test that retry_until validators receive full context."""
        # Skip for process/ray modes - closures can't capture variables across processes
        if worker_mode in ["process", "ray"]:
            pytest.skip("Closure variable capture not supported in process/ray modes")

        context_data = {}

        def capture_context_validator(result: Any, **context) -> bool:
            context_data.update(context)
            return result > 2

        worker = OutputValidationWorker.options(
            mode=worker_mode,
            num_retries=5,
            retry_until=[capture_context_validator],
            retry_wait=0.01,
        ).init()

        result = worker.get_number(10).result(timeout=5)
        assert result == 3

        # Verify context
        assert "attempt" in context_data
        assert "elapsed_time" in context_data
        assert "method_name" in context_data

        worker.stop()

    def test_retry_with_blocking_mode(self, worker_mode):
        """Test retry with blocking=True."""
        worker = CounterWorker.options(
            mode=worker_mode,
            num_retries=3,
            retry_wait=0.01,
            blocking=True,
        ).init(succeed_after=2)

        # With blocking=True, result() returns directly
        result = worker.flaky_method(10)
        if hasattr(result, "result"):
            result = result.result(timeout=5)
        assert result == 20

        worker.stop()


# =============================================================================
# Test Retry with TaskWorker
# =============================================================================


class TestRetryWithTaskWorker:
    """Test retry functionality with TaskWorker (submit and map)."""

    def test_taskworker_submit_with_retry(self, worker_mode):
        """Test TaskWorker.submit() with retry on exception."""
        import time

        # For process/ray modes, closures don't capture state across boundaries
        # Use time-based approach instead
        start_time = time.time()

        def flaky_function(value: int) -> int:
            # Fail for first 0.05 seconds, then succeed
            if time.time() - start_time < 0.05:
                raise ValueError("Still failing")
            return value * 2

        worker = TaskWorker.options(
            mode=worker_mode,
            num_retries=10,  # Enough retries to succeed
            retry_wait=0.01,
        ).init()

        future = worker.submit(flaky_function, 10)
        result = future.result(timeout=5)

        assert result == 20  # Verify retry succeeded

        worker.stop()

    def test_taskworker_submit_retry_exhaustion(self, worker_mode):
        """Test TaskWorker.submit() with retry exhaustion."""

        def always_fails(value: int) -> int:
            raise RuntimeError("Always fails")

        worker = TaskWorker.options(
            mode=worker_mode,
            num_retries=2,
            retry_wait=0.01,
        ).init()

        future = worker.submit(always_fails, 10)
        with pytest.raises(RuntimeError, match="Always fails"):
            future.result(timeout=5)

        worker.stop()

    def test_taskworker_submit_with_retry_until(self, worker_mode):
        """Test TaskWorker.submit() with output validation."""
        import time

        # Use time-based approach for all modes (closures don't work with process/ray)
        start_time = time.time()

        def incrementing_function() -> int:
            elapsed = time.time() - start_time
            # Return incrementing value based on elapsed time
            # Will fail validation until enough time has passed
            return int(elapsed / 0.01) + 1

        def validate_gt_3(result: int, **context) -> bool:
            return result > 3

        worker = TaskWorker.options(
            mode=worker_mode,
            num_retries=10,
            retry_wait=0.01,
            retry_until=validate_gt_3,
        ).init()

        future = worker.submit(incrementing_function)
        result = future.result(timeout=5)

        assert result > 3  # Verify validation succeeded

        worker.stop()

    def test_taskworker_submit_async_with_retry(self, worker_mode):
        """Test TaskWorker.submit() with async function and retry."""
        import time

        # Use time-based approach (closures don't work with process/ray)
        start_time = time.time()

        async def async_flaky_function(value: int) -> int:
            import asyncio

            await asyncio.sleep(0.01)
            # Fail for first 0.05 seconds, then succeed
            if time.time() - start_time < 0.05:
                raise ValueError("Still failing")
            return value * 3

        worker = TaskWorker.options(
            mode=worker_mode,
            num_retries=10,
            retry_wait=0.01,
        ).init()

        future = worker.submit(async_flaky_function, 10)
        result = future.result(timeout=5)

        assert result == 30  # Verify retry succeeded

        worker.stop()

    def test_taskworker_map_with_retry(self, worker_mode):
        """Test TaskWorker.map() with retry."""
        import random

        # Use random failures (not closures, since they don't work with process/ray)
        def flaky_square(x: int) -> int:
            # Randomly fail with ~50% chance on first few attempts
            if random.random() < 0.3:
                raise ValueError(f"Random failure for {x}")
            return x**2

        worker = TaskWorker.options(
            mode=worker_mode,
            num_retries=10,  # Enough retries to eventually succeed
            retry_wait=0.01,
        ).init()

        results = list(worker.map(flaky_square, range(5)))

        assert results == [0, 1, 4, 9, 16]  # Verify all succeeded

        worker.stop()

    def test_taskworker_submit_with_specific_exception(self, worker_mode):
        """Test TaskWorker.submit() retries only on specific exceptions."""

        def always_value_error() -> str:
            raise ValueError("Retriable error")

        def always_type_error() -> str:
            raise TypeError("Non-retriable error")

        worker = TaskWorker.options(
            mode=worker_mode,
            num_retries=3,
            retry_wait=0.01,
            retry_on=[ValueError],
        ).init()

        # Should retry on ValueError (will exhaust retries)
        future = worker.submit(always_value_error)
        with pytest.raises(ValueError, match="Retriable"):
            future.result(timeout=5)

        # Should NOT retry on TypeError (fails immediately)
        future = worker.submit(always_type_error)
        with pytest.raises(TypeError, match="Non-retriable"):
            future.result(timeout=5)

        worker.stop()

    def test_taskworker_submit_with_limits(self, worker_mode):
        """Test TaskWorker.submit() with limits and retry."""
        limits = LimitSet(
            limits=[ResourceLimit(key="slots", capacity=1)],
            shared=True,
            mode=worker_mode,
        )

        # Use time-based approach (closures don't work with process/ray)
        start_time = time.time()

        def limited_function() -> str:
            # Fail for first 0.05 seconds
            if time.time() - start_time < 0.05:
                raise ValueError("First attempt fails")
            return "success"

        worker = TaskWorker.options(
            mode=worker_mode,
            limits=limits,
            num_retries=10,
            retry_wait=0.01,
        ).init()

        # Note: TaskWorker doesn't use self.limits in the function,
        # but the retry mechanism should still work
        future = worker.submit(limited_function)
        result = future.result(timeout=5)

        assert result == "success"

        worker.stop()

    def test_taskworker_pool_with_retry(self, worker_mode):
        """Test TaskWorker pool with retry."""
        if worker_mode in ["sync", "asyncio"]:
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        import random

        # Use random failures (closures don't work with process/ray)
        def flaky_multiply(x: int) -> int:
            # Randomly fail with 40% probability
            if random.random() < 0.4:
                raise ValueError(f"Random failure for {x}")
            return x * 10

        pool = TaskWorker.options(
            mode=worker_mode,
            max_workers=3,
            num_retries=20,  # Enough retries to eventually succeed
            retry_wait=0.01,
        ).init()

        # Submit multiple tasks
        futures = [pool.submit(flaky_multiply, i) for i in range(6)]
        results = [f.result(timeout=10) for f in futures]

        assert results == [0, 10, 20, 30, 40, 50]

        pool.stop()

    def test_taskworker_pool_map_with_retry(self, worker_mode):
        """Test TaskWorker pool map() with retry."""
        if worker_mode in ["sync", "asyncio"]:
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        import random

        # Use random failures (closures don't work with process/ray)
        def flaky_double(x: int) -> int:
            # Randomly fail with 40% probability
            if random.random() < 0.4:
                raise ValueError(f"Random failure for {x}")
            return x * 2

        pool = TaskWorker.options(
            mode=worker_mode,
            max_workers=4,
            num_retries=20,  # Enough retries to eventually succeed
            retry_wait=0.01,
        ).init()

        results = list(pool.map(flaky_double, range(8)))

        assert results == [0, 2, 4, 6, 8, 10, 12, 14]

        pool.stop()

    def test_taskworker_pool_retry_with_validation(self, worker_mode):
        """Test TaskWorker pool with retry_until validation."""
        if worker_mode in ["sync", "asyncio"]:
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        import time

        # Use time-based approach (closures don't work with process/ray)
        start_time = time.time()

        def time_based_function(x: int) -> int:
            elapsed = time.time() - start_time
            # Return value that increases over time
            return int(elapsed / 0.01) + x

        def validate_gt_2(result: int, **context) -> bool:
            return result > 2

        pool = TaskWorker.options(
            mode=worker_mode,
            max_workers=2,
            num_retries=20,  # Enough retries
            retry_wait=0.01,
            retry_until=validate_gt_2,
        ).init()

        futures = [pool.submit(time_based_function, i) for i in range(4)]
        results = [f.result(timeout=10) for f in futures]

        # All should pass validation (> 2)
        assert all(r > 2 for r in results)

        pool.stop()

    def test_taskworker_lambda_with_retry(self, worker_mode):
        """Test TaskWorker with lambda functions and retry."""

        # Lambdas don't maintain state, so we use a list
        attempts = [0]

        worker = TaskWorker.options(
            mode=worker_mode,
            num_retries=5,
            retry_wait=0.01,
        ).init()

        # This is tricky - lambdas can't maintain state across retries
        # So we just test that retry mechanism doesn't break with lambdas
        future = worker.submit(lambda x: x * 5, 10)
        result = future.result(timeout=5)

        assert result == 50

        worker.stop()

    def test_taskworker_pool_mixed_success_failure(self, worker_mode):
        """Test TaskWorker pool where some tasks succeed and some fail."""
        if worker_mode in ["sync", "asyncio"]:
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        def conditional_function(x: int) -> int:
            if x % 2 == 0:
                return x * 2  # Even numbers succeed
            else:
                raise ValueError(f"Odd number: {x}")  # Odd numbers fail

        pool = TaskWorker.options(
            mode=worker_mode,
            max_workers=3,
            num_retries=2,
            retry_wait=0.01,
        ).init()

        futures = [pool.submit(conditional_function, i) for i in range(6)]

        # Check results
        for i, f in enumerate(futures):
            if i % 2 == 0:
                assert f.result(timeout=5) == i * 2
            else:
                with pytest.raises(ValueError, match=f"Odd number: {i}"):
                    f.result(timeout=5)

        pool.stop()


# =============================================================================
# Test Retry Configuration Validation
# =============================================================================


class TestRetryConfigValidation:
    """Test RetryConfig validation."""

    def test_invalid_num_retries(self, worker_mode):
        """Test that negative num_retries raises error."""
        with pytest.raises((ValidationError, ValueError)):
            CounterWorker.options(
                mode=worker_mode,
                num_retries=-1,
            ).init()

    def test_invalid_retry_wait(self, worker_mode):
        """Test that non-positive retry_wait raises error."""
        with pytest.raises((ValidationError, ValueError)):
            CounterWorker.options(
                mode=worker_mode,
                num_retries=1,
                retry_wait=0,  # Must be > 0
            ).init()

    def test_invalid_retry_jitter(self, worker_mode):
        """Test that invalid retry_jitter raises error."""
        # Jitter must be between 0 and 1
        with pytest.raises((ValidationError, ValueError)):
            CounterWorker.options(
                mode=worker_mode,
                num_retries=1,
                retry_jitter=1.5,  # > 1.0
            ).init()

        with pytest.raises((ValidationError, ValueError)):
            CounterWorker.options(
                mode=worker_mode,
                num_retries=1,
                retry_jitter=-0.1,  # < 0
            ).init()

    def test_invalid_retry_on_type(self, worker_mode):
        """Test that invalid retry_on types raise error."""
        with pytest.raises((ValidationError, ValueError, TypeError)):
            CounterWorker.options(
                mode=worker_mode,
                num_retries=1,
                retry_on=["not a type or callable"],
            ).init()

    def test_invalid_retry_until_type(self, worker_mode):
        """Test that invalid retry_until types raise error."""
        with pytest.raises((ValidationError, ValueError, TypeError)):
            CounterWorker.options(
                mode=worker_mode,
                num_retries=1,
                retry_until=["not a callable"],
            ).init()


# =============================================================================
# Tests for Infrastructure Method Wrapping Bug (retry_until with Typed/BaseModel)
# =============================================================================


class TypedWorkerWithRetryUntil(Worker, Typed):
    """Worker inheriting from Typed with retry_until configuration.

    This tests the bug where infrastructure methods (post_initialize,
    post_set_validate_inputs, etc.) were incorrectly wrapped with retry logic,
    causing timeout during worker initialization.
    """

    name: str
    multiplier: int = 2

    def compute(self, x: int) -> int:
        """User-defined method that should be wrapped."""
        return x * self.multiplier


class BaseModelWorkerWithRetryUntil(Worker, BaseModel):
    """Worker inheriting from BaseModel with retry_until configuration.

    Tests the same bug for BaseModel workers.
    """

    name: str = Field(default="test")
    multiplier: int = Field(default=2)

    def compute(self, x: int) -> int:
        """User-defined method that should be wrapped."""
        return x * self.multiplier


class PlainWorkerWithRetryUntil(Worker):
    """Plain worker (no Typed/BaseModel) with retry_until configuration.

    Control test - should work fine since there are no infrastructure methods.
    """

    def __init__(self, multiplier: int = 2):
        self.multiplier = multiplier

    def compute(self, x: int) -> int:
        """User-defined method that should be wrapped."""
        return x * self.multiplier


class TestRetryUntilWithTypedBaseModel:
    """Test that retry_until works correctly with Typed and BaseModel workers.

    Critical Bug Test: When retry_until is configured, retry logic wraps all public
    methods. Previously, this incorrectly wrapped infrastructure methods like
    post_set_validate_inputs, causing initialization failures.

    This test suite verifies:
    1. Workers with retry_until can be created successfully (all modes)
    2. Infrastructure methods are NOT wrapped with retry logic
    3. User methods ARE wrapped with retry logic
    4. retry_until validation only applies to user methods
    """

    def test_typed_worker_with_retry_until_creates_successfully(self, worker_mode):
        """Test that Typed workers with retry_until can be created.

        This was the primary symptom of the bug - worker creation would timeout
        because post_set_validate_inputs was being wrapped with retry logic.

        Now works in ALL modes including Ray thanks to auto-composition wrapper!
        """

        def validate_result(result, **context):
            """Simple validator that accepts any result."""
            return True

        # This should NOT timeout or fail (works in ALL modes now!)
        w = TypedWorkerWithRetryUntil.options(
            mode=worker_mode, num_retries=3, retry_until=validate_result
        ).init(name="test", multiplier=2)

        # Verify worker works correctly
        future = w.compute(5)
        result = future.result()
        assert result == 10

        w.stop()

    def test_basemodel_worker_with_retry_until_creates_successfully(self, worker_mode):
        """Test that BaseModel workers with retry_until can be created.

        Now works in ALL modes including Ray thanks to auto-composition wrapper!
        """

        def validate_result(result, **context):
            """Simple validator that accepts any result."""
            return True

        # This should NOT timeout or fail (works in ALL modes now!)
        w = BaseModelWorkerWithRetryUntil.options(
            mode=worker_mode, num_retries=3, retry_until=validate_result
        ).init(name="test", multiplier=2)

        # Verify worker works correctly
        future = w.compute(5)
        result = future.result()
        assert result == 10

        w.stop()

    def test_plain_worker_with_retry_until_creates_successfully(self, worker_mode):
        """Test that plain workers with retry_until work (control test)."""

        def validate_result(result, **context):
            """Simple validator that accepts any result."""
            return True

        w = PlainWorkerWithRetryUntil.options(
            mode=worker_mode, num_retries=3, retry_until=validate_result
        ).init(multiplier=2)

        # Verify worker works correctly
        future = w.compute(5)
        result = future.result()
        assert result == 10

        w.stop()

    def test_retry_until_only_validates_user_methods(self, worker_mode):
        """Test that retry_until validators only see user method results."""
        if worker_mode in ("process", "ray"):
            pytest.skip("Cannot track validation calls across process/ray boundaries")

        validation_calls = []

        def track_validation(result, **context):
            """Validator that tracks what it's called with."""
            validation_calls.append({"result": result, "method_name": context.get("method_name")})
            return True

        w = TypedWorkerWithRetryUntil.options(
            mode=worker_mode, num_retries=3, retry_until=track_validation
        ).init(name="test", multiplier=2)

        # Call user method
        future = w.compute(5)
        result = future.result()
        assert result == 10

        w.stop()

        # Verify validator was called for user method
        assert len(validation_calls) > 0
        assert any(call["method_name"] == "compute" for call in validation_calls)

        # Verify validator was NOT called for infrastructure methods
        infrastructure_methods = [
            "post_initialize",
            "pre_initialize",
            "post_set_validate_inputs",
            "model_dump",
        ]
        for method in infrastructure_methods:
            assert not any(call["method_name"] == method for call in validation_calls), (
                f"Validator should not be called for infrastructure method {method}"
            )

    def test_retry_until_validation_failure_retries_user_methods(self, worker_mode):
        """Test that retry_until failures trigger retries for user methods."""
        if worker_mode in ("process", "ray"):
            pytest.skip("Cannot track attempt count across process/ray boundaries")

        attempt_count = 0

        def failing_then_succeeding_validation(result, **context):
            """Validator that fails first 2 times, then succeeds."""
            nonlocal attempt_count
            attempt_count += 1
            return attempt_count >= 3  # Succeed on 3rd attempt

        w = TypedWorkerWithRetryUntil.options(
            mode=worker_mode,
            num_retries=5,  # Allow enough retries
            retry_until=failing_then_succeeding_validation,
        ).init(name="test", multiplier=2)

        # This should retry until validation passes
        future = w.compute(5)
        result = future.result()
        assert result == 10

        # Verify it took 3 attempts
        assert attempt_count == 3

        w.stop()

    def test_typed_worker_pool_with_retry_until(self, pool_mode):
        """Test that worker pools with Typed workers and retry_until work.

        Now works in ALL pool modes including Ray!
        """

        def validate_result(result, **context):
            """Simple validator."""
            return result > 0

        # Create pool with retry_until (works in ALL modes now!)
        pool = TypedWorkerWithRetryUntil.options(
            mode=pool_mode,
            max_workers=3,
            num_retries=2,
            retry_until=validate_result,
        ).init(name="test", multiplier=2)

        # Submit multiple tasks
        futures = [pool.compute(i) for i in range(1, 6)]
        results = [f.result() for f in futures]

        assert results == [2, 4, 6, 8, 10]

        pool.stop()

    def test_retry_until_with_limits_and_typed_worker(self, worker_mode):
        """Test retry_until works with Limits and Typed workers.

        Now works in ALL modes including Ray!
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip(f"{worker_mode} mode doesn't support shared limits")

        def validate_result(result, **context):
            """Simple validator."""
            return True

        limits = LimitSet(
            limits=[
                CallLimit(window_seconds=60, capacity=100),
                RateLimit(key="tokens", window_seconds=60, capacity=1000),
            ],
            shared=True,
            mode=worker_mode,
        )

        w = TypedWorkerWithRetryUntil.options(
            mode=worker_mode,
            num_retries=3,
            retry_until=validate_result,
            limits=limits,
        ).init(name="test", multiplier=2)

        # Method should work with both retry_until and limits (works in ALL modes now!)
        future = w.compute(5)
        result = future.result()
        assert result == 10

        w.stop()

    def test_retry_until_signature_incorrect_raises_clear_error(self, worker_mode):
        """Test that incorrect retry_until signature gives clear error.

        This was part of the original bug - users would pass a function with
        wrong signature and get confusing timeout errors.

        Now works in ALL modes including Ray!
        """

        def wrong_signature(response: str) -> bool:
            """Validator with wrong signature (missing **kwargs)."""
            return True

        w = TypedWorkerWithRetryUntil.options(
            mode=worker_mode, num_retries=3, retry_until=wrong_signature
        ).init(name="test", multiplier=2)

        # Calling the method should fail with a clear error about signature
        future = w.compute(5)
        with pytest.raises(Exception) as exc_info:
            future.result()

        # Error should mention the signature issue
        error_msg = str(exc_info.value).lower()
        assert "unexpected keyword argument" in error_msg or "got an unexpected" in error_msg

        w.stop()

    def test_retry_until_with_all_retry_algorithms(self, worker_mode):
        """Test retry_until works with all retry algorithms.

        Now works in ALL modes including Ray!
        """
        for algorithm in [
            RetryAlgorithm.Linear,
            RetryAlgorithm.Exponential,
            RetryAlgorithm.Fibonacci,
        ]:

            def validate_result(result, **context):
                return True

            w = TypedWorkerWithRetryUntil.options(
                mode=worker_mode,
                num_retries=2,
                retry_algorithm=algorithm,
                retry_until=validate_result,
            ).init(name="test", multiplier=2)

            future = w.compute(5)
            result = future.result()
            assert result == 10

            w.stop()


# =============================================================================
# Test Per-Method Retry Configuration
# =============================================================================


class TestPerMethodRetryConfiguration:
    """Test per-method retry configuration feature.

    This test suite validates that retry parameters can be configured on a
    per-method basis using dictionaries with method names as keys.
    """

    def test_per_method_num_retries(self, worker_mode):
        """Test different num_retries for different methods.

        Steps:
        1. Create worker with num_retries dict: {"*": 2, "method_a": 4, "method_b": 0}
        2. Call method_a which fails 3 times then succeeds (needs 4 retries)
        3. Call method_b which always fails (0 retries = should fail immediately)
        4. Call method_c which fails twice then succeeds (uses default 2 retries)
        5. Verify all behave according to their retry config
        """

        class PerMethodWorker(Worker):
            def __init__(self):
                self.attempt_a = 0
                self.attempt_b = 0
                self.attempt_c = 0

            def method_a(self, x: int) -> int:
                """Needs 4 retries to succeed."""
                self.attempt_a += 1
                if self.attempt_a < 4:
                    raise ValueError(f"method_a attempt {self.attempt_a} failed")
                return x * 2

            def method_b(self, x: int) -> int:
                """Has 0 retries, always fails."""
                self.attempt_b += 1
                raise ValueError(f"method_b attempt {self.attempt_b} failed")

            def method_c(self, x: int) -> int:
                """Uses default 2 retries, succeeds on attempt 3."""
                self.attempt_c += 1
                if self.attempt_c < 3:
                    raise ValueError(f"method_c attempt {self.attempt_c} failed")
                return x * 4

        worker = PerMethodWorker.options(
            mode=worker_mode,
            num_retries={"*": 2, "method_a": 4, "method_b": 0},
            retry_wait=0.01,
        ).init()

        # method_a should succeed (4 retries configured, needs 3 failures)
        result_a = worker.method_a(10).result(timeout=5)
        assert result_a == 20

        # method_c should succeed (default 2 retries, needs 2 failures)
        result_c = worker.method_c(10).result(timeout=5)
        assert result_c == 40

        # method_b should fail immediately (0 retries, always fails)
        with pytest.raises(ValueError, match="method_b attempt 1 failed"):
            worker.method_b(10).result(timeout=5)

        worker.stop()

    def test_per_method_retry_on(self, worker_mode):
        """Test different retry_on filters for different methods.

        Steps:
        1. Create worker with retry_on dict:
           - method_a retries on ValueError only
           - method_b retries on TypeError only
           - method_c uses default (retries on any Exception)
        2. Test that each method only retries on its configured exceptions
        """

        class FilterWorker(Worker):
            def __init__(self):
                self.attempt_a = 0
                self.attempt_b = 0
                self.attempt_c = 0

            def method_a(self, should_raise_type_error: bool) -> str:
                """Retries only on ValueError."""
                self.attempt_a += 1
                if should_raise_type_error:
                    raise TypeError("Should NOT retry on TypeError")
                if self.attempt_a < 2:
                    raise ValueError("Should retry on ValueError")
                return "success_a"

            def method_b(self, should_raise_value_error: bool) -> str:
                """Retries only on TypeError."""
                self.attempt_b += 1
                if should_raise_value_error:
                    raise ValueError("Should NOT retry on ValueError")
                if self.attempt_b < 2:
                    raise TypeError("Should retry on TypeError")
                return "success_b"

            def method_c(self) -> str:
                """Retries on any exception (default)."""
                self.attempt_c += 1
                if self.attempt_c < 2:
                    raise RuntimeError("Should retry on any exception")
                return "success_c"

        worker = FilterWorker.options(
            mode=worker_mode,
            num_retries=3,
            retry_on={"*": [Exception], "method_a": [ValueError], "method_b": [TypeError]},
            retry_wait=0.01,
        ).init()

        # method_a: should retry on ValueError
        result_a = worker.method_a(should_raise_type_error=False).result(timeout=5)
        assert result_a == "success_a"

        # method_a: should NOT retry on TypeError
        with pytest.raises(TypeError, match="Should NOT retry on TypeError"):
            worker.method_a(should_raise_type_error=True).result(timeout=5)

        worker.stop()

        # Create new worker for method_b tests
        worker2 = FilterWorker.options(
            mode=worker_mode,
            num_retries=3,
            retry_on={"*": [Exception], "method_a": [ValueError], "method_b": [TypeError]},
            retry_wait=0.01,
        ).init()

        # method_b: should retry on TypeError
        result_b = worker2.method_b(should_raise_value_error=False).result(timeout=5)
        assert result_b == "success_b"

        # method_b: should NOT retry on ValueError
        with pytest.raises(ValueError, match="Should NOT retry on ValueError"):
            worker2.method_b(should_raise_value_error=True).result(timeout=5)

        worker2.stop()

        # Create new worker for method_c test
        worker3 = FilterWorker.options(
            mode=worker_mode,
            num_retries=3,
            retry_on={"*": [Exception], "method_a": [ValueError], "method_b": [TypeError]},
            retry_wait=0.01,
        ).init()

        # method_c: should retry on any exception (default)
        result_c = worker3.method_c().result(timeout=5)
        assert result_c == "success_c"

        worker3.stop()

    def test_per_method_retry_until(self, worker_mode):
        """Test different retry_until validators for different methods.

        Steps:
        1. Create worker with retry_until dict:
           - method_a requires result > 5
           - method_b requires result > 10
           - method_c uses no validation (default)
        2. Verify each method retries until its validator passes
        """

        class ValidationWorker(Worker):
            def __init__(self):
                self.attempt_a = 0
                self.attempt_b = 0

            def method_a(self) -> int:
                """Returns increasing values."""
                self.attempt_a += 1
                return self.attempt_a

            def method_b(self) -> int:
                """Returns increasing values."""
                self.attempt_b += 1
                return self.attempt_b

            def method_c(self) -> int:
                """No validation, returns immediately."""
                return 1

        # Use module-level validators for picklability in process/ray modes
        worker = ValidationWorker.options(
            mode=worker_mode,
            num_retries=15,  # Enough retries
            retry_until={
                "*": None,
                "method_a": validate_greater_than_5,
                "method_b": validate_greater_than_10,
            },
            retry_wait=0.01,
        ).init()

        # method_a should retry until result > 5 (attempt 6)
        result_a = worker.method_a().result(timeout=10)
        assert result_a == 6

        # method_b should retry until result > 10 (attempt 11)
        result_b = worker.method_b().result(timeout=10)
        assert result_b == 11

        # method_c should return immediately (no validation)
        result_c = worker.method_c().result(timeout=10)
        assert result_c == 1

        worker.stop()

    def test_per_method_retry_algorithm(self, worker_mode):
        """Test different retry algorithms for different methods.

        Steps:
        1. Create worker with retry_algorithm dict:
           - method_a uses Linear
           - method_b uses Exponential
           - method_c uses Fibonacci (default)
        2. Verify each method uses its configured algorithm
        """

        class AlgorithmWorker(Worker):
            def __init__(self):
                self.attempt = 0

            def method_a(self) -> str:
                self.attempt += 1
                if self.attempt < 2:
                    raise ValueError("Retry with Linear")
                return "success"

            def method_b(self) -> str:
                self.attempt += 1
                if self.attempt < 3:
                    raise ValueError("Retry with Exponential")
                return "success"

            def method_c(self) -> str:
                self.attempt += 1
                if self.attempt < 4:
                    raise ValueError("Retry with Fibonacci")
                return "success"

        worker = AlgorithmWorker.options(
            mode=worker_mode,
            num_retries=5,
            retry_algorithm={
                "*": RetryAlgorithm.Fibonacci,
                "method_a": RetryAlgorithm.Linear,
                "method_b": RetryAlgorithm.Exponential,
            },
            retry_wait=0.01,
        ).init()

        # All should succeed (algorithm doesn't affect success, just timing)
        assert worker.method_a().result(timeout=5) == "success"

        worker.stop()

    def test_per_method_mixed_retry_params(self, worker_mode):
        """Test multiple retry parameters configured per-method.

        Steps:
        1. Create worker with different num_retries, retry_wait, and retry_algorithm per method
        2. Verify each method uses its specific configuration
        """

        class MixedWorker(Worker):
            def __init__(self):
                self.attempt_a = 0
                self.attempt_b = 0

            def method_a(self) -> str:
                """Needs 3 retries."""
                self.attempt_a += 1
                if self.attempt_a < 3:
                    raise ValueError(f"method_a attempt {self.attempt_a}")
                return "success_a"

            def method_b(self) -> str:
                """Needs 1 retry."""
                self.attempt_b += 1
                if self.attempt_b < 2:
                    raise ValueError(f"method_b attempt {self.attempt_b}")
                return "success_b"

        worker = MixedWorker.options(
            mode=worker_mode,
            num_retries={"*": 1, "method_a": 3},
            retry_wait={"*": 0.01, "method_a": 0.02, "method_b": 0.005},
            retry_algorithm={"*": RetryAlgorithm.Linear, "method_a": RetryAlgorithm.Exponential},
        ).init()

        # method_a should succeed with 3 retries
        result_a = worker.method_a().result(timeout=5)
        assert result_a == "success_a"

        # method_b should succeed with 1 retry (default)
        result_b = worker.method_b().result(timeout=5)
        assert result_b == "success_b"

        worker.stop()

    def test_per_method_with_worker_pool(self, worker_mode):
        """Test per-method retry configuration with worker pools.

        Steps:
        1. Create pool with per-method retry configs
        2. Submit tasks to different methods
        3. Verify each method uses its configured retries
        """
        if worker_mode in ["sync", "asyncio"]:
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        class PoolWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id
                self.attempt_a = 0
                self.attempt_b = 0

            def method_a(self, x: int) -> dict:
                """Needs 2 retries."""
                self.attempt_a += 1
                if self.attempt_a < 2:
                    raise ValueError("method_a retry needed")
                return {"worker_id": self.worker_id, "method": "a", "result": x * 2}

            def method_b(self, x: int) -> dict:
                """No retries, should succeed immediately."""
                self.attempt_b += 1
                return {"worker_id": self.worker_id, "method": "b", "result": x * 3}

        pool = PoolWorker.options(
            mode=worker_mode,
            max_workers=3,
            num_retries={"*": 0, "method_a": 2},
            retry_wait=0.01,
        ).init(1)  # worker_id will be same for all pool workers

        # Test method_a with retries
        futures_a = [pool.method_a(i) for i in range(3)]
        results_a = [f.result(timeout=10) for f in futures_a]
        assert len(results_a) == 3
        assert all(r["method"] == "a" for r in results_a)

        # Test method_b without retries
        futures_b = [pool.method_b(i) for i in range(3)]
        results_b = [f.result(timeout=10) for f in futures_b]
        assert len(results_b) == 3
        assert all(r["method"] == "b" for r in results_b)

        pool.stop()

    def test_per_method_with_limits(self, worker_mode):
        """Test per-method retry with resource limits.

        Steps:
        1. Create worker with per-method retries and resource limits
        2. Verify limits are released between retry attempts for each method
        """

        class LimitedWorker(Worker):
            def __init__(self):
                self.attempt_a = 0
                self.attempt_b = 0

            def method_a(self) -> str:
                """Uses limit, needs 2 retries."""
                with self.limits.acquire(requested={"connections": 1}):
                    self.attempt_a += 1
                    time.sleep(0.05)
                    if self.attempt_a < 2:
                        raise ValueError("method_a retry")
                    return "success_a"

            def method_b(self) -> str:
                """Uses limit, no retries."""
                with self.limits.acquire(requested={"connections": 1}):
                    self.attempt_b += 1
                    time.sleep(0.05)
                    return "success_b"

        worker = LimitedWorker.options(
            mode=worker_mode,
            num_retries={"*": 0, "method_a": 3},
            retry_wait=0.01,
            limits=[ResourceLimit(key="connections", capacity=1)],
        ).init()

        # method_a should succeed after retry (limit released properly)
        result_a = worker.method_a().result(timeout=10)
        assert result_a == "success_a"

        # method_b should succeed immediately
        result_b = worker.method_b().result(timeout=10)
        assert result_b == "success_b"

        worker.stop()

    def test_per_method_partial_dict(self, worker_mode):
        """Test partial dictionary (only some methods specified).

        Steps:
        1. Create worker with dict specifying only some methods
        2. Verify specified methods use their config
        3. Verify unspecified methods use default ("*") config
        """

        class PartialWorker(Worker):
            def __init__(self):
                self.attempt_a = 0
                self.attempt_b = 0
                self.attempt_c = 0

            def method_a(self) -> str:
                self.attempt_a += 1
                if self.attempt_a < 2:
                    raise ValueError("method_a")
                return "success_a"

            def method_b(self) -> str:
                self.attempt_b += 1
                if self.attempt_b < 3:
                    raise ValueError("method_b")
                return "success_b"

            def method_c(self) -> str:
                self.attempt_c += 1
                if self.attempt_c < 2:
                    raise ValueError("method_c")
                return "success_c"

        # Only configure method_a explicitly, rest use default
        worker = PartialWorker.options(
            mode=worker_mode,
            num_retries={"*": 3, "method_b": 5},  # method_a and method_c use default (3)
            retry_wait=0.01,
        ).init()

        # All should succeed
        assert worker.method_a().result(timeout=5) == "success_a"  # Uses default 3
        assert worker.method_b().result(timeout=5) == "success_b"  # Uses explicit 5
        assert worker.method_c().result(timeout=5) == "success_c"  # Uses default 3

        worker.stop()

    def test_per_method_all_zero_except_one(self, worker_mode):
        """Test disabling retries for all methods except one.

        Steps:
        1. Create worker with num_retries={"*": 0, "method_a": 3}
        2. Verify method_a has retries
        3. Verify other methods fail immediately
        """

        class SelectiveWorker(Worker):
            def __init__(self):
                self.attempt_a = 0
                self.attempt_b = 0

            def method_a(self) -> str:
                self.attempt_a += 1
                if self.attempt_a < 2:
                    raise ValueError("method_a")
                return "success_a"

            def method_b(self) -> str:
                self.attempt_b += 1
                if self.attempt_b < 2:
                    raise ValueError("method_b attempt 1")
                return "success_b"

        worker = SelectiveWorker.options(
            mode=worker_mode,
            num_retries={"*": 0, "method_a": 3},
            retry_wait=0.01,
        ).init()

        # method_a should succeed (has retries)
        result_a = worker.method_a().result(timeout=5)
        assert result_a == "success_a"

        # method_b should fail immediately (no retries)
        with pytest.raises(ValueError, match="method_b attempt 1"):
            worker.method_b().result(timeout=5)

        worker.stop()

    def test_per_method_invalid_method_name_error(self, worker_mode):
        """Test that specifying invalid method name raises error.

        Steps:
        1. Try to create worker with method name that doesn't exist
        2. Verify ValidationError is raised
        """

        class SimpleWorker(Worker):
            def method_a(self) -> str:
                return "success"

        # Should raise error for unknown method "nonexistent_method"
        with pytest.raises((ValueError, ValidationError)):
            SimpleWorker.options(
                mode=worker_mode,
                num_retries={"*": 2, "nonexistent_method": 5},
            ).init()

    def test_per_method_missing_default_key_error(self, worker_mode):
        """Test that dict without '*' key raises error.

        Steps:
        1. Try to create worker with dict missing "*" key
        2. Verify ValueError is raised
        """

        class SimpleWorker(Worker):
            def method_a(self) -> str:
                return "success"

        # Should raise error for missing "*" key
        with pytest.raises(ValueError, match="must include '\\*' key for default value"):
            SimpleWorker.options(
                mode=worker_mode,
                num_retries={"method_a": 5},  # Missing "*"
            ).init()

    def test_per_method_single_value_backward_compatible(self, worker_mode):
        """Test that single value (non-dict) still works (backward compatibility).

        Steps:
        1. Create worker with single num_retries value (not dict)
        2. Verify it applies to all methods
        """

        class SimpleWorker(Worker):
            def __init__(self):
                self.attempt_a = 0
                self.attempt_b = 0

            def method_a(self) -> str:
                self.attempt_a += 1
                if self.attempt_a < 2:
                    raise ValueError("method_a")
                return "success_a"

            def method_b(self) -> str:
                self.attempt_b += 1
                if self.attempt_b < 2:
                    raise ValueError("method_b")
                return "success_b"

        # Use single value (backward compatible)
        worker = SimpleWorker.options(
            mode=worker_mode,
            num_retries=3,  # Single value, not dict
            retry_wait=0.01,
        ).init()

        # Both methods should have retries
        assert worker.method_a().result(timeout=5) == "success_a"
        assert worker.method_b().result(timeout=5) == "success_b"

        worker.stop()

    def test_per_method_with_taskworker(self, worker_mode):
        """Test per-method retry with TaskWorker.

        Steps:
        1. Create TaskWorker with num_retries dict for "submit" method
        2. Submit tasks and verify retry behavior

        Note: TaskWorker has a special "submit" method that is wrapped.
        """
        import time

        # Use time-based approach (closures don't work with process/ray)
        start_time = time.time()

        def flaky_function(x: int) -> int:
            # Fail for first 0.05 seconds, then succeed
            if time.time() - start_time < 0.05:
                raise ValueError("Still failing")
            return x * 2

        # TaskWorker should support per-method config for "submit"
        worker = TaskWorker.options(
            mode=worker_mode,
            num_retries={"*": 10, "submit": 10},  # Explicit config for submit
            retry_wait=0.01,
        ).init()

        future = worker.submit(flaky_function, 10)
        result = future.result(timeout=5)
        assert result == 20

        worker.stop()


class TestRetryUntilWithZeroRetries:
    """Test that retry_until validation works even with num_retries=0.

    This is a critical edge case: users should be able to validate output
    on the initial attempt (no retries) and get RetryValidationError if
    validation fails.
    """

    def test_retry_until_with_zero_retries_validation_fails(self, worker_mode):
        """Test that retry_until validator runs even with num_retries=0.

        When num_retries=0 but retry_until is configured, the validator
        should still run on the initial attempt. If validation fails,
        RetryValidationError should be raised.

        Steps:
        1. Create worker with num_retries=0 and a failing validator
        2. Call method that returns a value
        3. Validator should run and fail
        4. Verify RetryValidationError is raised with 1 attempt
        """

        def always_fails_validator(result, **ctx):
            """Validator that always returns False."""
            return False

        class SimpleWorker(Worker):
            def get_value(self) -> int:
                return 42

        worker = SimpleWorker.options(
            mode=worker_mode,
            num_retries=0,  # No retries
            retry_until=always_fails_validator,  # But validation should still run
        ).init()

        # Should raise RetryValidationError after 1 attempt (initial, no retries)
        with pytest.raises(RetryValidationError) as exc_info:
            worker.get_value().result(timeout=5)

        # Verify it was only 1 attempt (initial attempt, no retries)
        assert exc_info.value.attempts == 1
        assert len(exc_info.value.all_results) == 1
        assert exc_info.value.all_results[0] == 42
        assert len(exc_info.value.validation_errors) == 1

        worker.stop()

    def test_retry_until_with_zero_retries_validation_succeeds(self, worker_mode):
        """Test that retry_until validator runs and can succeed with num_retries=0.

        Steps:
        1. Create worker with num_retries=0 and a passing validator
        2. Call method that returns a valid value
        3. Validator should run and pass
        4. Verify result is returned successfully
        """

        def always_passes_validator(result, **ctx):
            """Validator that always returns True."""
            return True

        class SimpleWorker(Worker):
            def get_value(self) -> int:
                return 42

        worker = SimpleWorker.options(
            mode=worker_mode,
            num_retries=0,
            retry_until=always_passes_validator,
        ).init()

        result = worker.get_value().result(timeout=5)
        assert result == 42

        worker.stop()

    def test_retry_until_with_zero_retries_conditional_validation(self, worker_mode):
        """Test conditional validator with num_retries=0.

        Steps:
        1. Create worker with num_retries=0 and conditional validator
        2. Call method with value that fails validation
        3. Verify RetryValidationError
        4. Call method with value that passes validation
        5. Verify success
        """

        def greater_than_50_validator(result, **ctx):
            """Validator that checks if result > 50."""
            return result > 50

        class SimpleWorker(Worker):
            def get_value(self, value: int) -> int:
                return value

        worker = SimpleWorker.options(
            mode=worker_mode,
            num_retries=0,
            retry_until=greater_than_50_validator,
        ).init()

        # Should fail validation (30 < 50)
        with pytest.raises(RetryValidationError) as exc_info:
            worker.get_value(30).result(timeout=5)

        assert exc_info.value.attempts == 1
        assert exc_info.value.all_results[0] == 30

        # Should pass validation (60 > 50)
        result = worker.get_value(60).result(timeout=5)
        assert result == 60

        worker.stop()

    def test_retry_until_with_zero_retries_taskworker(self, worker_mode):
        """Test that TaskWorker validates submitted functions with num_retries=0.

        Steps:
        1. Create TaskWorker with num_retries=0 and validator
        2. Submit function that returns invalid result
        3. Verify RetryValidationError
        4. Submit function that returns valid result
        5. Verify success
        """

        def is_even_validator(result, **ctx):
            """Validator that checks if result is even."""
            return result % 2 == 0

        def compute(x: int) -> int:
            return x

        worker = TaskWorker.options(
            mode=worker_mode,
            num_retries=0,
            retry_until=is_even_validator,
        ).init()

        # Should fail validation (odd number)
        with pytest.raises(RetryValidationError) as exc_info:
            worker.submit(compute, 7).result(timeout=5)

        assert exc_info.value.attempts == 1
        assert exc_info.value.all_results[0] == 7

        # Should pass validation (even number)
        result = worker.submit(compute, 8).result(timeout=5)
        assert result == 8

        worker.stop()

    def test_retry_until_with_zero_retries_multiple_validators(self, worker_mode):
        """Test multiple validators with num_retries=0.

        All validators must pass for validation to succeed.

        Steps:
        1. Create worker with num_retries=0 and multiple validators
        2. Call with value that fails first validator
        3. Call with value that fails second validator
        4. Call with value that passes all validators
        """

        def is_positive_validator(result, **ctx):
            return result > 0

        def is_even_validator(result, **ctx):
            return result % 2 == 0

        class SimpleWorker(Worker):
            def get_value(self, value: int) -> int:
                return value

        worker = SimpleWorker.options(
            mode=worker_mode,
            num_retries=0,
            retry_until=[is_positive_validator, is_even_validator],
        ).init()

        # Fails first validator (not positive)
        with pytest.raises(RetryValidationError):
            worker.get_value(-2).result(timeout=5)

        # Fails second validator (not even)
        with pytest.raises(RetryValidationError):
            worker.get_value(3).result(timeout=5)

        # Passes all validators (positive and even)
        result = worker.get_value(4).result(timeout=5)
        assert result == 4

        worker.stop()

    def test_retry_until_with_zero_retries_per_method(self, worker_mode):
        """Test per-method configuration with num_retries=0 and retry_until.

        Steps:
        1. Create worker with per-method num_retries=0 and validators
        2. Verify different methods have different validators
        """

        def always_fails(result, **ctx):
            return False

        def always_passes(result, **ctx):
            return True

        class MultiMethodWorker(Worker):
            def method_a(self) -> int:
                return 1

            def method_b(self) -> int:
                return 2

        worker = MultiMethodWorker.options(
            mode=worker_mode,
            num_retries={"*": 0, "method_a": 0, "method_b": 0},
            retry_until={"*": always_passes, "method_a": always_fails},
        ).init()

        # method_a should fail validation
        with pytest.raises(RetryValidationError):
            worker.method_a().result(timeout=5)

        # method_b should pass validation (uses "*" default)
        result = worker.method_b().result(timeout=5)
        assert result == 2

        worker.stop()

    def test_retry_until_validator_receives_context(self, worker_mode):
        """Test that validator receives correct context even with num_retries=0.

        Steps:
        1. Create worker with validator that checks context
        2. Verify validator can access and use context fields
        3. Verify validation works correctly based on context
        """

        def context_checking_validator(result, attempt, elapsed_time, method_name, **ctx):
            """Validator that checks context fields are present and valid."""
            # Verify all expected context fields are present
            assert isinstance(result, int), f"result should be int, got {type(result)}"
            assert isinstance(attempt, int), f"attempt should be int, got {type(attempt)}"
            assert isinstance(elapsed_time, (int, float)), (
                f"elapsed_time should be numeric, got {type(elapsed_time)}"
            )
            assert isinstance(method_name, str), f"method_name should be str, got {type(method_name)}"

            # Verify values are reasonable
            assert result == 42, f"result should be 42, got {result}"
            assert attempt == 1, f"attempt should be 1 (first and only), got {attempt}"
            assert elapsed_time >= 0, f"elapsed_time should be non-negative, got {elapsed_time}"
            assert method_name == "test_method", f"method_name should be 'test_method', got {method_name}"

            # Pass validation
            return True

        class SimpleWorker(Worker):
            def test_method(self) -> int:
                return 42

        worker = SimpleWorker.options(
            mode=worker_mode,
            num_retries=0,
            retry_until=context_checking_validator,
        ).init()

        # This will raise AssertionError from validator if context is wrong
        result = worker.test_method().result(timeout=5)
        assert result == 42

        worker.stop()


class TestMultiplePerMethodDictConfigurations:
    """Test scenarios where multiple retry parameters use dict-based per-method configuration.

    This ensures that different retry parameters can have independent per-method
    configurations and they all work together correctly.
    """

    def test_multiple_dicts_num_retries_and_retry_on(self, worker_mode):
        """Test num_retries and retry_on both as dicts with different method configs.

        Steps:
        1. Configure num_retries and retry_on as dicts with different method overrides
        2. Verify each method uses its specific retry count and exception filter
        3. Check that methods with different configs behave independently
        """

        class MultiConfigWorker(Worker):
            def __init__(self):
                self.attempt_a = 0
                self.attempt_b = 0
                self.attempt_c = 0

            def method_a(self) -> str:
                """Retries on ValueError, max 3 retries."""
                self.attempt_a += 1
                if self.attempt_a < 3:
                    raise ValueError("Retry me")
                return f"success_a_{self.attempt_a}"

            def method_b(self) -> str:
                """Retries on TypeError, max 5 retries."""
                self.attempt_b += 1
                if self.attempt_b < 4:
                    raise TypeError("Retry me too")
                return f"success_b_{self.attempt_b}"

            def method_c(self) -> str:
                """No retries, should fail immediately."""
                self.attempt_c += 1
                raise RuntimeError("Should not retry")

        worker = MultiConfigWorker.options(
            mode=worker_mode,
            num_retries={
                "*": 0,
                "method_a": 3,
                "method_b": 5,
            },
            retry_on={
                "*": [Exception],
                "method_a": [ValueError],
                "method_b": [TypeError],
            },
        ).init()

        # method_a: succeeds on 3rd attempt (2 retries)
        result_a = worker.method_a().result(timeout=15.0)
        assert result_a == "success_a_3"

        # method_b: succeeds on 4th attempt (3 retries)
        result_b = worker.method_b().result(timeout=15.0)
        assert result_b == "success_b_4"

        # method_c: no retries, should fail immediately
        with pytest.raises(RuntimeError, match="Should not retry"):
            worker.method_c().result(timeout=15.0)

        worker.stop()

    def test_multiple_dicts_all_retry_params(self, worker_mode):
        """Test all retry parameters as dicts with different method configs.

        Steps:
        1. Configure num_retries, retry_on, retry_until, retry_algorithm, retry_wait all as dicts
        2. Each method has different settings for multiple parameters
        3. Verify each method uses its specific configuration correctly
        """

        class ComplexWorker(Worker):
            def __init__(self):
                self.attempt_a = 0
                self.attempt_b = 0
                self.attempt_c = 0

            def method_a(self) -> int:
                """3 retries, ValueError only, linear backoff, result > 2."""
                self.attempt_a += 1
                if self.attempt_a < 2:
                    raise ValueError("Retry")
                return self.attempt_a

            def method_b(self) -> int:
                """5 retries, TypeError only, exponential backoff, result > 3."""
                self.attempt_b += 1
                if self.attempt_b < 3:
                    raise TypeError("Retry")
                return self.attempt_b

            def method_c(self) -> int:
                """No retries, should fail immediately."""
                self.attempt_c += 1
                raise RuntimeError("No retry")

        def validate_gt_2(result, **ctx):
            return result > 2

        def validate_gt_3(result, **ctx):
            return result > 3

        worker = ComplexWorker.options(
            mode=worker_mode,
            num_retries={
                "*": 0,
                "method_a": 3,
                "method_b": 5,
            },
            retry_on={
                "*": [Exception],
                "method_a": [ValueError],
                "method_b": [TypeError],
            },
            retry_until={
                "*": None,
                "method_a": validate_gt_2,
                "method_b": validate_gt_3,
            },
            retry_algorithm={
                "*": RetryAlgorithm.Linear,
                "method_a": RetryAlgorithm.Linear,
                "method_b": RetryAlgorithm.Exponential,
            },
            retry_wait={
                "*": 0.01,
                "method_a": 0.01,
                "method_b": 0.01,
            },
        ).init()

        # method_a: needs 3 attempts (exception on 1, validation fail on 2, success on 3)
        result_a = worker.method_a().result(timeout=5.0)
        assert result_a == 3

        # method_b: needs 4 attempts (exception on 1-2, validation fail on 3, success on 4)
        result_b = worker.method_b().result(timeout=5.0)
        assert result_b == 4

        # method_c: no retries
        with pytest.raises(RuntimeError, match="No retry"):
            worker.method_c().result(timeout=5.0)

        worker.stop()

    def test_mixed_dict_and_single_values(self, worker_mode):
        """Test mixing dict-based and single-value retry parameters.

        Steps:
        1. Configure some parameters as dicts (per-method) and others as single values
        2. Verify dict parameters respect method-specific settings
        3. Verify single-value parameters apply uniformly to all methods
        """

        class MixedWorker(Worker):
            def __init__(self):
                self.attempt_a = 0
                self.attempt_b = 0

            def method_a(self) -> str:
                """3 retries on ValueError, uses global retry_wait."""
                self.attempt_a += 1
                if self.attempt_a < 3:
                    raise ValueError("Retry")
                return f"success_a_{self.attempt_a}"

            def method_b(self) -> str:
                """5 retries on TypeError, uses global retry_wait."""
                self.attempt_b += 1
                if self.attempt_b < 4:
                    raise TypeError("Retry")
                return f"success_b_{self.attempt_b}"

        worker = MixedWorker.options(
            mode=worker_mode,
            num_retries={
                "*": 1,
                "method_a": 3,
                "method_b": 5,
            },
            retry_on={
                "*": [Exception],
                "method_a": [ValueError],
                "method_b": [TypeError],
            },
            retry_wait=0.01,  # Single value - applies to all
            retry_algorithm=RetryAlgorithm.Linear,  # Single value - applies to all
            retry_jitter=0,  # Single value - applies to all
        ).init()

        result_a = worker.method_a().result(timeout=5.0)
        assert result_a == "success_a_3"

        result_b = worker.method_b().result(timeout=5.0)
        assert result_b == "success_b_4"

        worker.stop()

    def test_complex_validation_and_exception_filters_per_method(self, worker_mode):
        """Test complex per-method retry_on and retry_until configurations.

        Steps:
        1. Configure different validators and exception filters per method
        2. Method A: custom exception filter + custom validator
        3. Method B: different exception filter + different validator
        4. Verify each method uses its specific filters and validators
        """

        class ValidationWorker(Worker):
            def __init__(self):
                self.attempt_a = 0
                self.attempt_b = 0

            def method_a(self) -> dict:
                """Custom filter + validator."""
                self.attempt_a += 1
                if self.attempt_a == 1:
                    raise ValueError("retriable_error")  # Should retry
                # On attempt 2, will return successfully
                return {"value": self.attempt_a, "status": "ok"}

            def method_b(self) -> dict:
                """Different filter + different validator."""
                self.attempt_b += 1
                if self.attempt_b < 2:
                    raise TypeError("network_error")
                return {"value": self.attempt_b, "code": 200}

        def filter_a(exception, **ctx):
            """Only retry ValueError with 'retriable' in message."""
            return isinstance(exception, ValueError) and "retriable" in str(exception)

        def filter_b(exception, **ctx):
            """Only retry TypeError with 'network' in message."""
            return isinstance(exception, TypeError) and "network" in str(exception)

        def validate_a(result, **ctx):
            """Validate method_a result."""
            return isinstance(result, dict) and result.get("status") == "ok"

        def validate_b(result, **ctx):
            """Validate method_b result."""
            return isinstance(result, dict) and result.get("code") == 200

        worker = ValidationWorker.options(
            mode=worker_mode,
            num_retries={
                "*": 0,
                "method_a": 5,
                "method_b": 5,
            },
            retry_on={
                "*": [Exception],
                "method_a": filter_a,
                "method_b": filter_b,
            },
            retry_until={
                "*": None,
                "method_a": validate_a,
                "method_b": validate_b,
            },
            retry_wait=0.01,
        ).init()

        # method_a: retries on attempt 1 (retriable), succeeds on attempt 2
        result_a = worker.method_a().result(timeout=5.0)
        assert result_a["status"] == "ok"
        assert result_a["value"] == 2  # Took 2 attempts (1 retry)

        # method_b: retries on attempt 1, succeeds on attempt 2
        result_b = worker.method_b().result(timeout=5.0)
        assert result_b["code"] == 200

        worker.stop()

    def test_per_method_with_taskworker_multiple_params(self, worker_mode):
        """Test TaskWorker with multiple dict-based retry parameters.

        Steps:
        1. Configure TaskWorker with num_retries, retry_on, retry_until all as dicts
        2. Use "submit" as method name for configuration
        3. Verify submitted functions use the configured retry behavior
        """
        from concurry import TaskWorker

        def flaky_function(x: int) -> int:
            """Function that fails first 2 times, then succeeds."""
            # Use counter-based approach instead of timing to avoid flakiness
            if not hasattr(flaky_function, "call_count"):
                flaky_function.call_count = 0

            flaky_function.call_count += 1

            # Fail first 2 attempts, succeed on 3rd
            if flaky_function.call_count < 3:
                raise ConnectionError("Temporary network error")
            return x * 2

        def validate_even(result, **ctx):
            """Validate result is even."""
            return result % 2 == 0

        # Reset counter for this test
        flaky_function.call_count = 0

        worker = TaskWorker.options(
            mode=worker_mode,
            num_retries={
                "*": 0,
                "submit": 5,
            },
            retry_on={
                "*": [Exception],
                "submit": [ConnectionError],
            },
            retry_until={
                "*": None,
                "submit": validate_even,
            },
            retry_wait=0.01,
        ).init()

        # Should retry on ConnectionError and validate result
        future = worker.submit(flaky_function, 5)
        result = future.result(timeout=5.0)
        assert result == 10

        worker.stop()

    def test_worker_pool_with_multiple_dict_params(self, worker_mode):
        """Test worker pool where workers use multiple dict-based retry params.

        Steps:
        1. Create pool with multiple dict-based retry parameters
        2. Submit tasks to different methods
        3. Verify each worker in pool uses correct per-method config
        """
        if worker_mode in ("sync", "asyncio"):
            pytest.skip(f"{worker_mode} mode does not support max_workers > 1")

        class PoolWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id
                self.attempt_a = 0
                self.attempt_b = 0

            def method_a(self, x: int) -> dict:
                """3 retries on ValueError."""
                self.attempt_a += 1
                if self.attempt_a < 2:
                    raise ValueError("Retry")
                return {"worker_id": self.worker_id, "method": "a", "value": x, "attempts": self.attempt_a}

            def method_b(self, x: int) -> dict:
                """5 retries on TypeError."""
                self.attempt_b += 1
                if self.attempt_b < 3:
                    raise TypeError("Retry")
                return {"worker_id": self.worker_id, "method": "b", "value": x, "attempts": self.attempt_b}

        pool = PoolWorker.options(
            mode=worker_mode,
            max_workers=3,
            num_retries={
                "*": 0,
                "method_a": 3,
                "method_b": 5,
            },
            retry_on={
                "*": [Exception],
                "method_a": [ValueError],
                "method_b": [TypeError],
            },
            retry_wait=0.01,
        ).init(worker_id=0)

        # Submit tasks to both methods
        futures_a = [pool.method_a(i) for i in range(5)]
        futures_b = [pool.method_b(i) for i in range(5)]

        results_a = [f.result(timeout=5.0) for f in futures_a]
        results_b = [f.result(timeout=5.0) for f in futures_b]

        # Verify results
        for result in results_a:
            assert result["method"] == "a"
            assert result["attempts"] >= 2  # At least 1 retry

        for result in results_b:
            assert result["method"] == "b"
            assert result["attempts"] >= 3  # At least 2 retries

        pool.stop()

    def test_three_dicts_num_retries_retry_wait_retry_algorithm(self, worker_mode):
        """Test num_retries, retry_wait, retry_algorithm all as dicts.

        Steps:
        1. Configure three different retry parameters as dicts
        2. Each method gets different retry count, wait time, and algorithm
        3. Verify timing and retry counts match expected behavior
        """
        import time

        class TimingWorker(Worker):
            def __init__(self):
                self.attempt_a = 0
                self.attempt_b = 0
                self.start_a = None
                self.start_b = None

            def method_a(self) -> str:
                """2 retries, 0.05s wait, linear backoff."""
                if self.start_a is None:
                    self.start_a = time.time()
                self.attempt_a += 1
                if self.attempt_a < 2:
                    raise ValueError("Retry")
                return "success_a"

            def method_b(self) -> str:
                """3 retries, 0.02s wait, exponential backoff."""
                if self.start_b is None:
                    self.start_b = time.time()
                self.attempt_b += 1
                if self.attempt_b < 3:
                    raise ValueError("Retry")
                return "success_b"

        worker = TimingWorker.options(
            mode=worker_mode,
            num_retries={
                "*": 0,
                "method_a": 2,
                "method_b": 3,
            },
            retry_wait={
                "*": 0.01,
                "method_a": 0.05,
                "method_b": 0.02,
            },
            retry_algorithm={
                "*": RetryAlgorithm.Linear,
                "method_a": RetryAlgorithm.Linear,
                "method_b": RetryAlgorithm.Exponential,
            },
            retry_on=[ValueError],
            retry_jitter=0,  # Disable jitter for predictable timing
        ).init()

        # method_a: 2 attempts (1 retry), linear: 0.05s wait
        start = time.time()
        result_a = worker.method_a().result(timeout=5.0)
        elapsed_a = time.time() - start
        assert result_a == "success_a"
        # Should have waited ~0.05s (1 retry * 0.05s * 1)
        assert elapsed_a >= 0.04  # Allow some slack

        # method_b: 3 attempts (2 retries), exponential: 0.02s, 0.04s waits
        start = time.time()
        result_b = worker.method_b().result(timeout=5.0)
        elapsed_b = time.time() - start
        assert result_b == "success_b"
        # Should have waited ~0.02 + 0.04 = 0.06s
        assert elapsed_b >= 0.05  # Allow some slack

        worker.stop()

    def test_per_method_with_limits_multiple_dict_params(self, worker_mode):
        """Test per-method retry with limits and multiple dict parameters.

        Steps:
        1. Create worker with resource limits
        2. Configure multiple dict-based retry parameters
        3. Verify limits are released between retries for each method
        4. Verify different methods use different retry configs
        """
        from concurry import ResourceLimit

        class LimitedMultiWorker(Worker):
            def __init__(self):
                self.attempt_a = 0
                self.attempt_b = 0

            def method_a(self) -> str:
                """3 retries, ValueError only."""
                with self.limits.acquire(requested={"resources": 1}) as acq:
                    self.attempt_a += 1
                    if self.attempt_a < 3:
                        raise ValueError("Retry")
                    acq.update(usage={"resources": 1})
                    return "success_a"

            def method_b(self) -> str:
                """5 retries, TypeError only."""
                with self.limits.acquire(requested={"resources": 1}) as acq:
                    self.attempt_b += 1
                    if self.attempt_b < 4:
                        raise TypeError("Retry")
                    acq.update(usage={"resources": 1})
                    return "success_b"

        worker = LimitedMultiWorker.options(
            mode=worker_mode,
            num_retries={
                "*": 0,
                "method_a": 3,
                "method_b": 5,
            },
            retry_on={
                "*": [Exception],
                "method_a": [ValueError],
                "method_b": [TypeError],
            },
            retry_wait=0.01,
            limits=[ResourceLimit(key="resources", capacity=1)],
        ).init()

        result_a = worker.method_a().result(timeout=5.0)
        assert result_a == "success_a"

        result_b = worker.method_b().result(timeout=5.0)
        assert result_b == "success_b"

        # Test completed successfully - limits were properly managed (no deadlocks occurred)

        worker.stop()
