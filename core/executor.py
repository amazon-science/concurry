"""Unified Executor interface for concurry."""

import inspect
from abc import ABC, abstractmethod
from typing import Any, Callable, Iterator, List, Optional, Union

from .config import ExecutorConfig, ExecutionMode
from .future import Future


class Executor(ABC):
    """Abstract base class for unified executor interface."""
    
    def __init__(self, config: ExecutorConfig):
        self.config = config
    
    @abstractmethod
    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """Submit a function for execution.
        
        Args:
            fn: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Future representing the computation
        """
        pass
    
    @abstractmethod
    def map(self, fn: Callable, *iterables, **kwargs) -> Iterator[Any]:
        """Apply function to iterables in parallel.
        
        Args:
            fn: Function to apply to each item
            *iterables: Iterables to process
            **kwargs: Additional arguments
            
        Returns:
            Iterator of results
        """
        pass
    
    @abstractmethod
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the executor and clean up resources.
        
        Args:
            wait: Whether to wait for pending tasks to complete
        """
        pass
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.shutdown(wait=True)


def create_executor(config: ExecutorConfig) -> Executor:
    """Create an executor based on the configuration.
    
    Args:
        config: Configuration specifying execution mode and parameters
        
    Returns:
        Executor instance for the specified mode
        
    Raises:
        ValueError: If execution mode is not supported
        ImportError: If required dependencies are not available
    """
    mode = config.mode
    
    # Auto-detect mode if requested
    if mode == ExecutionMode.AUTO:
        mode = _auto_detect_mode(config)
        config.mode = mode
    
    # Import and create the appropriate executor
    if mode == ExecutionMode.SYNC:
        from ..executors.sync import SyncExecutor
        return SyncExecutor(config)
    elif mode == ExecutionMode.THREADS:
        from ..executors.thread import ThreadExecutor
        return ThreadExecutor(config)
    elif mode == ExecutionMode.PROCESSES:
        from ..executors.process import ProcessExecutor
        return ProcessExecutor(config)
    elif mode == ExecutionMode.ASYNCIO:
        from ..executors.asyncio import AsyncioExecutor
        return AsyncioExecutor(config)
    elif mode == ExecutionMode.RAY:
        from ..executors.ray import RayExecutor
        return RayExecutor(config)
    else:
        raise ValueError(f"Unsupported execution mode: {mode}")


def _auto_detect_mode(config: ExecutorConfig) -> ExecutionMode:
    """Auto-detect the best execution mode based on configuration and environment.
    
    Args:
        config: Configuration object with hints about workload
        
    Returns:
        Best execution mode for the given configuration
    """
    # Check if Ray is available and configured
    try:
        import ray
        if ray.is_initialized() or config.ray_address:
            return ExecutionMode.RAY
    except ImportError:
        pass
    
    # Default heuristics based on max_workers and system characteristics
    import os
    cpu_count = os.cpu_count() or 4
    
    # If max_workers is 1 or None, use sync
    if config.max_workers == 1:
        return ExecutionMode.SYNC
    
    # For high concurrency (more than CPU count), prefer threads (I/O bound)
    if config.max_workers and config.max_workers > cpu_count * 2:
        return ExecutionMode.THREADS
    
    # For CPU-bound workloads, prefer processes
    if config.max_workers and config.max_workers <= cpu_count:
        return ExecutionMode.PROCESSES
    
    # Default to threads for most use cases
    return ExecutionMode.THREADS


def _detect_function_characteristics(fn: Callable) -> dict:
    """Analyze function to provide hints for execution mode selection.
    
    Args:
        fn: Function to analyze
        
    Returns:
        Dictionary with characteristics like io_bound, cpu_bound, async_native
    """
    characteristics = {
        'io_bound': False,
        'cpu_bound': False,
        'async_native': False,
    }
    
    # Check if function is async
    if inspect.iscoroutinefunction(fn):
        characteristics['async_native'] = True
        characteristics['io_bound'] = True  # Async usually means I/O bound
        return characteristics
    
    # Analyze function source for common patterns (basic heuristics)
    try:
        source = inspect.getsource(fn)
        
        # I/O bound indicators
        io_indicators = [
            'requests.', 'urllib', 'http', 'socket', 'open(',
            'read(', 'write(', 'json.loads', 'json.dumps',
            'time.sleep', 'asyncio', 'aiohttp'
        ]
        
        # CPU bound indicators
        cpu_indicators = [
            'numpy', 'pandas', 'scipy', 'sklearn', 'torch',
            'tensorflow', 'for _ in range', 'while', 'math.',
            'calculate', 'compute', 'process'
        ]
        
        io_score = sum(1 for indicator in io_indicators if indicator in source)
        cpu_score = sum(1 for indicator in cpu_indicators if indicator in source)
        
        if io_score > cpu_score:
            characteristics['io_bound'] = True
        elif cpu_score > io_score:
            characteristics['cpu_bound'] = True
        
    except (OSError, TypeError):
        # Cannot analyze source, use safe defaults
        pass
    
    return characteristics 