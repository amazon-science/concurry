"""Synchronous executor implementation."""

from typing import Any, Callable, Iterator

from ..core.executor import Executor
from ..core.future import Future, SyncFuture


class SyncExecutor(Executor):
    """Executor that runs tasks synchronously (no parallelism)."""
    
    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """Execute function synchronously and return completed future.
        
        Args:
            fn: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            SyncFuture with the result or exception
        """
        try:
            result = fn(*args, **kwargs)
            return SyncFuture(result=result)
        except Exception as e:
            return SyncFuture(exception=e)
    
    def map(self, fn: Callable, *iterables, **kwargs) -> Iterator[Any]:
        """Apply function to iterables synchronously.
        
        Args:
            fn: Function to apply
            *iterables: Iterables to process
            **kwargs: Additional arguments (ignored for sync)
            
        Returns:
            Iterator of results
        """
        # Use built-in map for synchronous execution
        return map(fn, *iterables)
    
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown executor (no-op for sync executor).
        
        Args:
            wait: Ignored for sync executor
        """
        pass  # Nothing to shutdown for synchronous execution 