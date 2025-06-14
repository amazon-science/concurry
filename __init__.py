"""
Concurry - A unified, delightful Python concurrency library

Concurry provides a consistent API for parallel and concurrent execution
across threads, processes, asyncio, and distributed systems.

Key features:
- Single API for all execution modes
- Automatic best-practice defaults
- Built-in progress tracking and error handling
- Easy switching between execution strategies
- Composable and testable design

Basic usage:
    import concurry
    
    # Simple parallel execution
    @concurry.run(mode='threads')
    def fetch_url(url):
        return requests.get(url).text
    
    # Parallel map operation
    results = concurry.map(process_item, items, mode='processes')
    
    # Streaming results
    for result in concurry.map(process_item, items, stream=True):
        print(result)
    
    # Background daemon
    @concurry.daemon(interval=60)
    def cleanup_task():
        cleanup_old_files()
"""

__version__ = "0.1.0"
__author__ = "Concurry Team"

from .core.config import ExecutorConfig, ExecutionMode, RetryConfig
from .core.executor import Executor, create_executor
from .core.future import Future
from .patterns.map import map, map_reduce
from .patterns.pipeline import Pipeline
from .patterns.daemon import daemon, start_daemon, stop_daemon
from .utils.progress import ProgressTracker
from .utils.retry import retry
from .utils.decorators import run

# Public API
__all__ = [
    # Core execution
    'run',
    'create_executor',
    'Executor',
    'Future',
    
    # Configuration
    'ExecutorConfig',
    'ExecutionMode', 
    'RetryConfig',
    
    # High-level patterns
    'map',
    'map_reduce',
    'Pipeline',
    'daemon',
    'start_daemon',
    'stop_daemon',
    
    # Utilities
    'ProgressTracker',
    'retry',
]

# Convenience functions for quick access
def executor(mode=ExecutionMode.AUTO, **kwargs):
    """Create an executor with the specified configuration.
    
    Args:
        mode: Execution mode (auto, sync, threads, processes, asyncio, ray)
        **kwargs: Additional configuration parameters
        
    Returns:
        Executor instance configured for the specified mode
        
    Example:
        with concurry.executor(mode='threads', max_workers=4) as executor:
            futures = [executor.submit(task, data) for data in dataset]
            results = [f.result() for f in futures]
    """
    config = ExecutorConfig(mode=mode, **kwargs)
    return create_executor(config) 