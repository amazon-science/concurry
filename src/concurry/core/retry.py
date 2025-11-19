"""Retry utilities for concurry workers.

This module provides retry functionality with configurable strategies, exception filtering,
and output validation for worker method calls.
"""

import asyncio
import inspect
import random
import time
from typing import Any, Callable, Dict, List, Optional, Union

from morphic import Typed
from pydantic import Field, confloat, conint, field_validator

from ..utils import _NO_ARG, _NO_ARG_TYPE
from .constants import RetryAlgorithm


class RetryValidationError(Exception):
    """Raised when retry_until validation fails after all retries.

    This exception contains all results from retry attempts and validation
    failure reasons, useful for debugging why validation failed.

    Attributes:
        attempts: Number of attempts made (including initial attempt)
        all_results: List of all output values from each attempt
        validation_errors: List of validation failure reasons for each attempt
        method_name: Name of the method that was retried

    Example:
        ```python
        try:
            result = worker.generate_json(prompt).result()
        except RetryValidationError as e:
            print(f"Failed after {e.attempts} attempts")
            print(f"All results: {e.all_results}")
            print(f"Errors: {e.validation_errors}")
            # Use the last result even though validation failed
            last_output = e.all_results[-1]
        ```
    """

    def __init__(
        self,
        attempts: int,
        all_results: List[Any],
        validation_errors: List[str],
        method_name: str = "unknown",
    ):
        self.attempts = attempts
        self.all_results = all_results
        self.validation_errors = validation_errors
        self.method_name = method_name

        all_results_str = "".join([f"Attempt {i + 1}:\n{result}\n" for i, result in enumerate(all_results)])

        # Create informative error message
        message = (
            f"Validation failed for method '{method_name}' after {attempts} attempts.\n"
            f"Validation errors: {validation_errors}\n"
            f"Results from all attempts:\n{all_results_str}"
        )
        super().__init__(message)

    def __reduce__(self):
        """Support pickling for multiprocessing."""
        return (
            self.__class__,
            (self.attempts, self.all_results, self.validation_errors, self.method_name),
        )


class RetryConfig(Typed):
    """Configuration for retry behavior.

    This class encapsulates all retry-related settings for worker method calls.

    Attributes:
        num_retries: Maximum number of retry attempts after initial failure.
            Total attempts = num_retries + 1 (initial attempt).
            Default value is determined by global_config.defaults.num_retries
        retry_on: Exception types or callables that trigger retries.
            Can be a single exception class, a callable, or a list of either.
            Callables receive context as kwargs and should return bool.
            Default: [Exception] (retry on all exceptions).
        retry_algorithm: Backoff strategy for wait times.
            Default value is determined by global_config.defaults.retry_algorithm
        retry_wait: Minimum wait time between retries in seconds.
            This is the base wait time before applying strategy and jitter.
            Default value is determined by global_config.defaults.retry_wait
        retry_jitter: Jitter factor between 0 and 1.
            Uses Full Jitter algorithm from AWS: sleep = random(0, calculated_wait).
            Set to 0 to disable jitter.
            Default value is determined by global_config.defaults.retry_jitter
        retry_until: Validation functions for output (default: None).
            Can be a single callable or list of callables. All must return True.
            Callables receive result and context as kwargs.
            If validation fails, triggers retry even without exception.

    Example:
        ```python
        from concurry import RetryConfig, RetryAlgorithm

        # Basic exponential backoff
        config = RetryConfig(
            num_retries=3,
            retry_algorithm=RetryAlgorithm.Exponential,
            retry_wait=1.0,
            retry_jitter=0.3
        )

        # Retry only on specific exceptions
        config = RetryConfig(
            num_retries=5,
            retry_on=[ConnectionError, TimeoutError],
            retry_algorithm=RetryAlgorithm.Linear
        )

        # Custom exception filter
        config = RetryConfig(
            num_retries=3,
            retry_on=lambda exception, **ctx: (
                isinstance(exception, ValueError) and "retry" in str(exception)
            )
        )

        # Output validation (e.g., for LLM responses)
        config = RetryConfig(
            num_retries=5,
            retry_until=lambda result, **ctx: (
                isinstance(result, dict) and "data" in result
            )
        )
        ```
    """

    num_retries: Union[conint(ge=0), _NO_ARG_TYPE] = _NO_ARG
    retry_on: Union[type, Callable, List[Union[type, Callable]]] = Field(default_factory=lambda: [Exception])
    retry_algorithm: Union[RetryAlgorithm, _NO_ARG_TYPE] = _NO_ARG
    retry_wait: Union[confloat(gt=0), _NO_ARG_TYPE] = _NO_ARG
    retry_jitter: Union[confloat(ge=0, le=1), _NO_ARG_TYPE] = _NO_ARG
    retry_until: Optional[Union[Callable, List[Callable]]] = None

    def post_initialize(self) -> None:
        """Set defaults from global config for _NO_ARG values."""
        from ..config import global_config

        # Clone config
        local_config = global_config.clone()
        defaults = local_config.defaults

        # Set defaults if not provided (use object.__setattr__ for frozen instances)
        if self.num_retries is _NO_ARG:
            object.__setattr__(self, "num_retries", defaults.num_retries)
        if self.retry_algorithm is _NO_ARG:
            object.__setattr__(self, "retry_algorithm", defaults.retry_algorithm)
        if self.retry_wait is _NO_ARG:
            object.__setattr__(self, "retry_wait", defaults.retry_wait)
        if self.retry_jitter is _NO_ARG:
            object.__setattr__(self, "retry_jitter", defaults.retry_jitter)

    @field_validator("num_retries")
    @classmethod
    def validate_num_retries(cls, v):
        """Validate num_retries is non-negative or _NO_ARG."""
        if v is _NO_ARG:
            return v
        if not isinstance(v, int) or v < 0:
            raise ValueError(f"num_retries must be >= 0, got {v}")
        return v

    @field_validator("retry_wait")
    @classmethod
    def validate_retry_wait(cls, v):
        """Validate retry_wait is positive or _NO_ARG."""
        if v is _NO_ARG:
            return v
        if not isinstance(v, (int, float)) or v <= 0:
            raise ValueError(f"retry_wait must be > 0, got {v}")
        return v

    @field_validator("retry_jitter")
    @classmethod
    def validate_retry_jitter(cls, v):
        """Validate retry_jitter is in [0, 1] or _NO_ARG."""
        if v is _NO_ARG:
            return v
        if not isinstance(v, (int, float)) or not (0 <= v <= 1):
            raise ValueError(f"retry_jitter must be in [0, 1], got {v}")
        return v

    @field_validator("retry_on")
    @classmethod
    def validate_retry_on(cls, v):
        """Ensure retry_on is a list of exception types or callables."""
        if not isinstance(v, list):
            v = [v]

        for item in v:
            if isinstance(item, type):
                if not issubclass(item, BaseException):
                    raise ValueError(
                        f"retry_on exception types must be subclasses of BaseException, got {item}"
                    )
            elif not callable(item):
                raise ValueError(f"retry_on items must be exception types or callables, got {type(item)}")

        return v

    @field_validator("retry_until")
    @classmethod
    def validate_retry_until(cls, v):
        """Ensure retry_until is a list of callables."""
        if v is None:
            return None

        if not isinstance(v, list):
            v = [v]

        for item in v:
            if not callable(item):
                raise ValueError(f"retry_until items must be callables, got {type(item)}")

        return v


def _fibonacci(n: int) -> int:
    """Calculate nth Fibonacci number (1-indexed for retry attempts).

    For retry attempts, we use: fib(1)=1, fib(2)=1, fib(3)=2, fib(4)=3, fib(5)=5, etc.

    Args:
        n: Attempt number (1-indexed, 1 = first attempt)

    Returns:
        Fibonacci number at attempt n
    """
    if n <= 0:
        return 1
    if n == 1:
        return 1
    if n == 2:
        return 1

    a, b = 1, 1
    for _ in range(n - 2):
        a, b = b, a + b
    return b


def calculate_retry_wait(attempt: int, config: RetryConfig) -> float:
    """Calculate wait time for a retry attempt with strategy and jitter.

    This function implements three backoff strategies and applies Full Jitter
    as described in the AWS blog post on exponential backoff:
    https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

    Args:
        attempt: Retry attempt number (1-indexed, 1 = first retry)
        config: Retry configuration with strategy, wait time, and jitter settings

    Returns:
        Wait time in seconds (always >= 0)

    Example:
        ```python
        config = RetryConfig(
            retry_wait=1.0,
            retry_algorithm=RetryAlgorithm.Exponential,
            retry_jitter=0.3
        )

        wait = calculate_retry_wait(1, config)  # ~0-2 seconds
        wait = calculate_retry_wait(2, config)  # ~0-4 seconds
        wait = calculate_retry_wait(3, config)  # ~0-8 seconds
        ```
    """
    base_wait = config.retry_wait

    # Apply backoff strategy
    if config.retry_algorithm == RetryAlgorithm.Linear:
        calculated_wait = base_wait * attempt
    elif config.retry_algorithm == RetryAlgorithm.Exponential:
        calculated_wait = base_wait * (2 ** (attempt - 1))
    elif config.retry_algorithm == RetryAlgorithm.Fibonacci:
        calculated_wait = base_wait * _fibonacci(attempt)
    else:
        # Fallback to exponential
        calculated_wait = base_wait * (2 ** (attempt - 1))

    # Apply Full Jitter: sleep = random_between(0, calculated_wait)
    if config.retry_jitter > 0:
        wait = random.uniform(0, calculated_wait)
    else:
        wait = calculated_wait

    return max(0, wait)


def _should_retry_on_exception(
    exception: BaseException,
    retry_on_filters: List[Union[type, Callable]],
    context: Dict[str, Any],
) -> bool:
    """Check if an exception should trigger a retry.

    Args:
        exception: The exception that was raised
        retry_on_filters: List of exception types or callables
        context: Context dict with attempt, elapsed_time, method_name, args, kwargs

    Returns:
        True if exception matches any filter
    """
    for filter_item in retry_on_filters:
        if isinstance(filter_item, type) and issubclass(filter_item, BaseException):
            # Exception type filter
            if isinstance(exception, filter_item):
                return True
        elif callable(filter_item):
            # Callable filter - pass exception and context
            try:
                if filter_item(exception=exception, **context):
                    return True
            except Exception:
                # If filter raises, don't retry
                continue

    return False


def _validate_result(
    result: Any,
    validators: List[Callable],
    context: Dict[str, Any],
) -> tuple[bool, Optional[str]]:
    """Validate result against validation functions.

    All validators must return True for validation to succeed.

    Args:
        result: The result to validate
        validators: List of validation functions
        context: Context dict with attempt, elapsed_time, method_name, args, kwargs

    Returns:
        Tuple of (is_valid, error_message)
    """
    for validator in validators:
        try:
            # Pass result and context to validator
            if not validator(result, **context):
                validator_name = getattr(validator, "__name__", str(validator))
                return False, f"Validator '{validator_name}' returned False"
        except Exception as e:
            validator_name = getattr(validator, "__name__", str(validator))
            return False, f"Validator '{validator_name}' raised: {e}"

    return True, None


def execute_with_retry(
    func: Callable,
    args: tuple,
    kwargs: dict,
    config: RetryConfig,
    context: Dict[str, Any],
) -> Any:
    """Execute function with retry logic.

    This function handles the retry loop for synchronous functions, including:
    - Exception filtering with retry_on
    - Output validation with retry_until
    - Backoff strategies with jitter
    - Collection of all results and errors

    Args:
        func: Function to execute
        args: Positional arguments for func
        kwargs: Keyword arguments for func
        config: Retry configuration
        context: Context dict with method_name, worker_class, etc.

    Returns:
        Result from successful function execution

    Raises:
        RetryValidationError: If retry_until validation fails after all retries
        Exception: The last exception if retries exhausted

    Example:
        ```python
        def flaky_function(x):
            if random.random() < 0.5:
                raise ConnectionError("Transient error")
            return x * 2

        config = RetryConfig(num_retries=3, retry_on=[ConnectionError])
        context = {"method_name": "flaky_function"}

        result = execute_with_retry(
            flaky_function,
            (5,),
            {},
            config,
            context
        )
        ```
    """
    start_time = time.time()
    method_name = context.get("method_name", "unknown")

    # Track results and errors from all attempts
    all_results = []
    all_exceptions = []
    validation_errors = []

    # Total attempts = initial + retries
    max_attempts = config.num_retries + 1

    for attempt in range(1, max_attempts + 1):
        # Update context with current attempt info
        current_context = {
            **context,
            "attempt": attempt,
            "elapsed_time": time.time() - start_time,
        }

        try:
            # Execute the function
            result = func(*args, **kwargs)

            # If we have validators, check the result
            if config.retry_until is not None:
                is_valid, error_msg = _validate_result(result, config.retry_until, current_context)

                if is_valid:
                    # Validation passed - return result
                    return result
                else:
                    # Validation failed - collect result and error
                    all_results.append(result)
                    validation_errors.append(error_msg)

                    # If this was the last attempt, raise RetryValidationError
                    if attempt == max_attempts:
                        raise RetryValidationError(
                            attempts=attempt,
                            all_results=all_results,
                            validation_errors=validation_errors,
                            method_name=method_name,
                        )

                    # Otherwise, wait and retry
                    wait = calculate_retry_wait(attempt, config)
                    time.sleep(wait)
                    continue
            else:
                # No validators - return result immediately
                return result

        except Exception as e:
            # Collect exception
            all_exceptions.append(e)

            # Check if we should retry on this exception
            should_retry = _should_retry_on_exception(e, config.retry_on, current_context)

            if not should_retry:
                # Exception not in retry_on - raise immediately
                raise

            # If this was the last attempt, raise the exception
            if attempt == max_attempts:
                raise

            # Wait before retry
            wait = calculate_retry_wait(attempt, config)
            time.sleep(wait)

    # Should never reach here, but just in case
    if len(all_exceptions) > 0:
        raise all_exceptions[-1]
    elif len(validation_errors) > 0:
        raise RetryValidationError(
            attempts=max_attempts,
            all_results=all_results,
            validation_errors=validation_errors,
            method_name=method_name,
        )
    else:
        raise RuntimeError(f"Unexpected state in retry logic for method '{method_name}'")


async def execute_with_retry_async(
    async_func: Callable,
    args: tuple,
    kwargs: dict,
    config: RetryConfig,
    context: Dict[str, Any],
) -> Any:
    """Execute async function with retry logic.

    This is the async version of execute_with_retry, using asyncio.sleep()
    instead of time.sleep() for wait times.

    Args:
        async_func: Async function to execute
        args: Positional arguments for async_func
        kwargs: Keyword arguments for async_func
        config: Retry configuration
        context: Context dict with method_name, worker_class, etc.

    Returns:
        Result from successful function execution

    Raises:
        RetryValidationError: If retry_until validation fails after all retries
        Exception: The last exception if retries exhausted

    Example:
        ```python
        async def async_flaky_function(x):
            if random.random() < 0.5:
                raise ConnectionError("Transient error")
            await asyncio.sleep(0.01)
            return x * 2

        config = RetryConfig(num_retries=3, retry_on=[ConnectionError])
        context = {"method_name": "async_flaky_function"}

        result = await execute_with_retry_async(
            async_flaky_function,
            (5,),
            {},
            config,
            context
        )
        ```
    """
    start_time = time.time()
    method_name = context.get("method_name", "unknown")

    # Track results and errors from all attempts
    all_results = []
    all_exceptions = []
    validation_errors = []

    # Total attempts = initial + retries
    max_attempts = config.num_retries + 1

    for attempt in range(1, max_attempts + 1):
        # Update context with current attempt info
        current_context = {
            **context,
            "attempt": attempt,
            "elapsed_time": time.time() - start_time,
        }

        try:
            # Execute the async function
            result = await async_func(*args, **kwargs)

            # If we have validators, check the result
            if config.retry_until is not None:
                is_valid, error_msg = _validate_result(result, config.retry_until, current_context)

                if is_valid:
                    # Validation passed - return result
                    return result
                else:
                    # Validation failed - collect result and error
                    all_results.append(result)
                    validation_errors.append(error_msg)

                    # If this was the last attempt, raise RetryValidationError
                    if attempt == max_attempts:
                        raise RetryValidationError(
                            attempts=attempt,
                            all_results=all_results,
                            validation_errors=validation_errors,
                            method_name=method_name,
                        )

                    # Otherwise, wait and retry
                    wait = calculate_retry_wait(attempt, config)
                    await asyncio.sleep(wait)
                    continue
            else:
                # No validators - return result immediately
                return result

        except Exception as e:
            # Collect exception
            all_exceptions.append(e)

            # Check if we should retry on this exception
            should_retry = _should_retry_on_exception(e, config.retry_on, current_context)

            if not should_retry:
                # Exception not in retry_on - raise immediately
                raise

            # If this was the last attempt, raise the exception
            if attempt == max_attempts:
                raise

            # Wait before retry
            wait = calculate_retry_wait(attempt, config)
            await asyncio.sleep(wait)

    # Should never reach here, but just in case
    if len(all_exceptions) > 0:
        raise all_exceptions[-1]
    elif len(validation_errors) > 0:
        raise RetryValidationError(
            attempts=max_attempts,
            all_results=all_results,
            validation_errors=validation_errors,
            method_name=method_name,
        )
    else:
        raise RuntimeError(f"Unexpected state in retry logic for method '{method_name}'")


def execute_with_retry_auto(
    fn: Callable,
    args: tuple,
    kwargs: dict,
    config: RetryConfig,
    context: Dict[str, Any],
) -> Any:
    """Automatically execute function with retry, handling both sync and async functions.

    This is a convenience function for TaskWorker that automatically detects whether
    the function is sync or async and uses the appropriate retry mechanism.

    For async functions in sync contexts (Sync/Thread/Process/Ray workers), this will
    run the async retry logic using asyncio.run().

    For async functions in async contexts (AsyncioWorker), the caller should use
    execute_with_retry_async directly instead of this function.

    Args:
        fn: Function to execute (sync or async)
        args: Positional arguments
        kwargs: Keyword arguments
        config: Retry configuration
        context: Context dict with method_name, worker_class, etc.

    Returns:
        Result from successful function execution

    Raises:
        RetryValidationError: If retry_until validation fails after all retries
        Exception: The last exception if retries exhausted

    Example:
        ```python
        # Works for both sync and async functions
        result = execute_with_retry_auto(
            some_function,  # Can be sync or async
            (arg1, arg2),
            {"key": "value"},
            retry_config,
            {"method_name": "some_function"}
        )
        ```
    """
    if inspect.iscoroutinefunction(fn):
        # Async function - run with asyncio.run() for sync contexts
        return asyncio.run(execute_with_retry_async(fn, args, kwargs, config, context))
    else:
        # Sync function - use regular retry
        return execute_with_retry(fn, args, kwargs, config, context)


def create_retry_wrapper(
    method: Callable,
    config: RetryConfig,
    method_name: str,
    worker_class_name: str,
) -> Callable:
    """Create a wrapper function that adds retry logic to a method.

    This function handles both sync and async methods automatically.

    Args:
        method: The method to wrap
        config: Retry configuration
        method_name: Name of the method (for error messages)
        worker_class_name: Name of the worker class (for context)

    Returns:
        Wrapped method with retry logic
    """
    # Check if method is async
    is_async = inspect.iscoroutinefunction(method)

    if is_async:

        async def async_wrapper(*args, **kwargs):
            context = {
                "method_name": method_name,
                "worker_class": worker_class_name,
                "args": args,
                "kwargs": kwargs,
            }
            return await execute_with_retry_async(method, args, kwargs, config, context)

        # Preserve method attributes
        async_wrapper.__name__ = method.__name__
        async_wrapper.__doc__ = method.__doc__
        return async_wrapper
    else:

        def sync_wrapper(*args, **kwargs):
            context = {
                "method_name": method_name,
                "worker_class": worker_class_name,
                "args": args,
                "kwargs": kwargs,
            }
            return execute_with_retry(method, args, kwargs, config, context)

        # Preserve method attributes
        sync_wrapper.__name__ = method.__name__
        sync_wrapper.__doc__ = method.__doc__
        return sync_wrapper
