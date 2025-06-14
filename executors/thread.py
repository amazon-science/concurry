"""Thread-based executor implementation."""

import time
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Iterator, Optional

from ..core.executor import Executor
from ..core.future import Future, ConcurrentFutureWrapper


class RateLimitedThreadPoolExecutor(ThreadPoolExecutor):
    """ThreadPoolExecutor with rate limiting support."""
    
    def __init__(self, max_workers, max_calls_per_second=float('inf'), **kwargs):
        super().__init__(max_workers=max_workers, **kwargs)
        self.max_calls_per_second = max_calls_per_second
        self.min_interval = 1.0 / max_calls_per_second if max_calls_per_second != float('inf') else 0
        self.last_submit_time = 0
        self.submit_lock = threading.Lock()
    
    def submit(self, fn, *args, **kwargs):
        """Submit with rate limiting."""
        if self.min_interval > 0:
            with self.submit_lock:
                now = time.time()
                time_since_last = now - self.last_submit_time
                if time_since_last < self.min_interval:
                    sleep_time = self.min_interval - time_since_last
                    time.sleep(sleep_time)
                self.last_submit_time = time.time()
        
        return super().submit(fn, *args, **kwargs)


class ThreadExecutor(Executor):
    """Executor that runs tasks using threads."""
    
    def __init__(self, config):
        super().__init__(config)
        self._executor: Optional[ThreadPoolExecutor] = None
        self._create_executor()
    
    def _create_executor(self):
        """Create the underlying ThreadPoolExecutor."""
        if self.config.max_calls_per_second != float('inf'):
            self._executor = RateLimitedThreadPoolExecutor(
                max_workers=self.config.max_workers,
                max_calls_per_second=self.config.max_calls_per_second,
                thread_name_prefix="concurry-thread"
            )
        else:
            self._executor = ThreadPoolExecutor(
                max_workers=self.config.max_workers,
                thread_name_prefix="concurry-thread"
            )
    
    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """Submit a function for execution in a thread.
        
        Args:
            fn: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Future representing the computation
        """
        if self._executor is None:
            raise RuntimeError("Executor has been shut down")
        
        # Apply retry logic if configured
        if self.config.retry_config:
            fn = self._wrap_with_retry(fn)
        
        # Apply timeout if configured
        if self.config.timeout:
            fn = self._wrap_with_timeout(fn, self.config.timeout)
        
        future = self._executor.submit(fn, *args, **kwargs)
        return ConcurrentFutureWrapper(future)
    
    def map(self, fn: Callable, *iterables, **kwargs) -> Iterator[Any]:
        """Apply function to iterables using threads.
        
        Args:
            fn: Function to apply
            *iterables: Iterables to process
            **kwargs: Additional arguments
            
        Returns:
            Iterator of results
        """
        if self._executor is None:
            raise RuntimeError("Executor has been shut down")
        
        # Apply retry logic if configured
        if self.config.retry_config:
            fn = self._wrap_with_retry(fn)
        
        # Apply timeout if configured
        if self.config.timeout:
            fn = self._wrap_with_timeout(fn, self.config.timeout)
        
        chunksize = kwargs.get('chunksize', 1)
        timeout = kwargs.get('timeout', None)
        
        return self._executor.map(fn, *iterables, chunksize=chunksize, timeout=timeout)
    
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the thread pool executor.
        
        Args:
            wait: Whether to wait for pending tasks to complete
        """
        if self._executor is not None:
            self._executor.shutdown(wait=wait)
            self._executor = None
    
    def _wrap_with_retry(self, fn: Callable) -> Callable:
        """Wrap function with retry logic."""
        from ..utils.retry import retry
        
        retry_config = self.config.retry_config
        
        def wrapped(*args, **kwargs):
            return retry(
                fn,
                *args,
                max_retries=retry_config.max_retries,
                initial_delay=retry_config.initial_delay,
                max_delay=retry_config.max_delay,
                exponential_base=retry_config.exponential_base,
                jitter=retry_config.jitter,
                retryable_exceptions=retry_config.retryable_exceptions,
                **kwargs
            )
        
        return wrapped
    
    def _wrap_with_timeout(self, fn: Callable, timeout: float) -> Callable:
        """Wrap function with timeout logic."""
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Function execution exceeded {timeout} seconds")
        
        def wrapped(*args, **kwargs):
            # Set up timeout signal (Unix only)
            try:
                old_handler = signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(int(timeout))
                try:
                    result = fn(*args, **kwargs)
                    return result
                finally:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, old_handler)
            except AttributeError:
                # Windows doesn't support SIGALRM, fallback to no timeout
                return fn(*args, **kwargs)
        
        return wrapped 