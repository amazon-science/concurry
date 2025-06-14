"""Process-based executor implementation."""

import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Callable, Iterator, Optional

from ..core.executor import Executor
from ..core.future import Future, ConcurrentFutureWrapper


def _init_worker(init_fn, init_args):
    """Initialize worker process with custom initialization function."""
    if init_fn is not None:
        init_fn(*init_args)


class ProcessExecutor(Executor):
    """Executor that runs tasks using processes."""
    
    def __init__(self, config):
        super().__init__(config)
        self._executor: Optional[ProcessPoolExecutor] = None
        self._create_executor()
    
    def _create_executor(self):
        """Create the underlying ProcessPoolExecutor."""
        # Determine multiprocessing context
        if self.config.context:
            ctx = mp.get_context(self.config.context)
        else:
            # Use 'spawn' as default for better cross-platform compatibility
            ctx = mp.get_context('spawn')
        
        kwargs = {
            'max_workers': self.config.max_workers,
            'mp_context': ctx,
        }
        
        # Add worker initialization if configured
        if self.config.worker_init_fn:
            kwargs['initializer'] = _init_worker
            kwargs['initargs'] = (self.config.worker_init_fn, self.config.worker_init_args)
        
        self._executor = ProcessPoolExecutor(**kwargs)
    
    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """Submit a function for execution in a process.
        
        Args:
            fn: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Future representing the computation
        """
        if self._executor is None:
            raise RuntimeError("Executor has been shut down")
        
        # Note: Retry and timeout wrapping is more complex with processes
        # due to pickling requirements. For now, we'll keep it simple.
        
        future = self._executor.submit(fn, *args, **kwargs)
        return ConcurrentFutureWrapper(future)
    
    def map(self, fn: Callable, *iterables, **kwargs) -> Iterator[Any]:
        """Apply function to iterables using processes.
        
        Args:
            fn: Function to apply
            *iterables: Iterables to process
            **kwargs: Additional arguments
            
        Returns:
            Iterator of results
        """
        if self._executor is None:
            raise RuntimeError("Executor has been shut down")
        
        chunksize = kwargs.get('chunksize', 1)
        timeout = kwargs.get('timeout', None)
        
        return self._executor.map(fn, *iterables, chunksize=chunksize, timeout=timeout)
    
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the process pool executor.
        
        Args:
            wait: Whether to wait for pending tasks to complete
        """
        if self._executor is not None:
            self._executor.shutdown(wait=wait)
            self._executor = None 