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

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **data):
        super().__init__(**data)
        # Use object.__setattr__ to bypass Pydantic validation for runtime attributes
        object.__setattr__(self, "attempt_count", 0)

    def process(self, value: int) -> int:
        object.__setattr__(self, "attempt_count", self.attempt_count + 1)
        if self.attempt_count < 2:
            raise ValueError("Processing failed")
        return value * self.max_retries


class TypedWorkerWithRetry(Worker, Typed):
    """Worker inheriting from Typed with extra fields allowed."""

    config_value: int

    model_config = {"extra": "allow"}

    def __init__(self, config_value: int):
        super().__init__(config_value=config_value)
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
        """Test that method succeeds after retries."""
        worker = CounterWorker.options(
            mode=worker_mode,
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
        """Test that retries are exhausted and exception is raised."""
        worker = (
            CounterWorker.options(
                mode=worker_mode,
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
        """Test that default behavior is no retries."""
        worker = CounterWorker.options(mode=worker_mode).init(succeed_after=2)

        future = worker.flaky_method(10)
        with pytest.raises(ValueError, match="Attempt 1 failed"):
            future.result(timeout=5)

        attempts = worker.get_attempts().result(timeout=5)
        assert attempts == 1  # Only initial attempt

        worker.stop()

    def test_retry_with_specific_exception(self, worker_mode):
        """Test retry only on specific exception types."""
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
        """Test retry with custom exception filter."""

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

        Setup: 10 workers competing for 3 resource slots, each worker fails once
        before succeeding. With proper limit release, all should complete.
        """
        shared_limits = LimitSet(
            limits=[ResourceLimit(key="resource", capacity=3)],
            shared=True,
            mode=worker_mode,
        )

        class ResourceWorker(Worker):
            def __init__(self, worker_id: int):
                self.worker_id = worker_id
                self.attempt_count = 0

            def work(self) -> str:
                with self.limits.acquire(requested={"resource": 1}):
                    self.attempt_count += 1
                    time.sleep(0.05)  # Simulate work
                    if self.attempt_count < 2:
                        raise ValueError(f"Worker {self.worker_id} can only succeed on the second attempt.")
                    return f"Worker {self.worker_id} completed successfully"

        workers = []
        for i in range(10):
            w = ResourceWorker.options(
                mode=worker_mode,
                limits=shared_limits,
                num_retries=10,  # Plenty of retries
                retry_wait=1.0,  # 1 second base wait to avoid timing issues
            ).init(worker_id=i)
            workers.append(w)

        # All workers should eventually succeed
        # With capacity=3, at most 3 workers can hold resources simultaneously
        # Each worker needs 2 attempts total (1 initial + 1 retry)
        futures = [w.work() for w in workers]
        results = [f.result(timeout=60) for f in futures]

        assert len(results) == 10
        assert all("completed successfully" in r for r in results)

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
        """Test retry on workers inheriting from BaseModel."""
        if worker_mode == "ray":
            pytest.skip("Ray mode not supported with Pydantic BaseModel")

        worker = ConfiguredPydanticWorker.options(
            mode=worker_mode,
            num_retries=5,
            retry_wait=0.01,
        ).init(max_retries=4)

        result = worker.process(10).result(timeout=5)
        assert result == 40  # 10 * 4

        worker.stop()

    def test_retry_with_typed_inheritance(self, worker_mode):
        """Test retry on workers inheriting from morphic.Typed."""
        if worker_mode == "ray":
            pytest.skip("Ray mode not supported with morphic.Typed")

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
        worker_pool = CounterWorker.options(
            mode=worker_mode,
            num_pool_members=3,
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
            pytest.skip("Sync and asyncio modes don't support max_workers > 1")

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
            num_pool_members=3,
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
            pytest.skip("Sync and asyncio modes don't support max_workers > 1")

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

        # With full jitter on linear backoff, delays are randomized
        # We can't predict exact timing, but it should be > 0 and < theoretical max
        assert elapsed > 0.05  # Some delay occurred
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

        from concurry import TaskWorker

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
        from concurry import TaskWorker

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

        from concurry import TaskWorker

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

        from concurry import TaskWorker

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

        from concurry import TaskWorker

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
        from concurry import TaskWorker

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
        import time

        from concurry import LimitSet, ResourceLimit, TaskWorker

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
            pytest.skip("Sync and asyncio modes don't support max_workers > 1")

        import random

        from concurry import TaskWorker

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
            pytest.skip("Sync and asyncio modes don't support max_workers > 1")

        import random

        from concurry import TaskWorker

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
            pytest.skip("Sync and asyncio modes don't support max_workers > 1")

        import time

        from concurry import TaskWorker

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
        from concurry import TaskWorker

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
            pytest.skip("Sync and asyncio modes don't support max_workers > 1")

        from concurry import TaskWorker

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
