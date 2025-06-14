"""Retry utility with exponential backoff and jitter."""

import random
import time
from typing import Any, Callable, Tuple, Type, Union


def retry(
    fn: Callable,
    *args,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = (Exception,),
    **kwargs
) -> Any:
    """Retry a function call with exponential backoff and jitter.
    
    Args:
        fn: Function to retry
        *args: Positional arguments for the function
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential backoff calculation
        jitter: Whether to add random jitter to delays
        retryable_exceptions: Tuple of exception types that should trigger retries
        **kwargs: Keyword arguments for the function
        
    Returns:
        Result of the function call
        
    Raises:
        Exception: The last exception if all retries fail
        
    Example:
        # Retry with default settings
        result = retry(flaky_function, arg1, arg2, kwarg1=value1)
        
        # Custom retry configuration
        result = retry(
            api_call,
            url="https://api.example.com",
            max_retries=5,
            initial_delay=0.5,
            max_delay=30.0,
            retryable_exceptions=(ConnectionError, TimeoutError)
        )
    """
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return fn(*args, **kwargs)
        except retryable_exceptions as e:
            last_exception = e
            
            if attempt == max_retries:
                # Last attempt failed, re-raise the exception
                raise
            
            # Calculate delay for next attempt
            delay = min(initial_delay * (exponential_base ** attempt), max_delay)
            
            # Add jitter if enabled
            if jitter:
                delay = delay * (0.5 + random.random() * 0.5)
            
            time.sleep(delay)
    
    # This should never be reached, but just in case
    if last_exception:
        raise last_exception
    else:
        raise RuntimeError("Retry function failed unexpectedly") 