"""AsyncIO-based executor implementation."""

import asyncio
import inspect
import threading
from typing import Any, Callable, Iterator, Optional
from functools import partial

from ..core.executor import Executor
from ..core.future import Future, AsyncioFutureWrapper


class AsyncioExecutor(Executor):
    """Executor that runs tasks using asyncio."""
    
    def __init__(self, config):
        super().__init__(config)
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None
        self._running = True
        self._create_loop()
    
    def _create_loop(self):
        """Create and start the asyncio event loop in a separate thread."""
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._run_loop,
            name="concurry-asyncio-loop",
            daemon=True
        )
        self._loop_thread.start()
    
    def _run_loop(self):
        """Run the asyncio event loop."""
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()
    
    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """Submit a function for execution using asyncio.
        
        Args:
            fn: Function to execute (can be sync or async)
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Future representing the computation
        """
        if self._loop is None or not self._running:
            raise RuntimeError("Executor has been shut down")
        
        # Create coroutine based on function type
        if inspect.iscoroutinefunction(fn):
            # Async function - run directly
            coro = fn(*args, **kwargs)
        else:
            # Sync function - run in executor
            coro = self._run_sync_in_executor(fn, *args, **kwargs)
        
        # Schedule coroutine on the event loop
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return AsyncioFutureWrapper(future)
    
    async def _run_sync_in_executor(self, fn: Callable, *args, **kwargs):
        """Run synchronous function in the default executor."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, partial(fn, *args, **kwargs))
    
    def map(self, fn: Callable, *iterables, **kwargs) -> Iterator[Any]:
        """Apply function to iterables using asyncio.
        
        Args:
            fn: Function to apply (can be sync or async)
            *iterables: Iterables to process
            **kwargs: Additional arguments
            
        Returns:
            Iterator of results
        """
        if self._loop is None or not self._running:
            raise RuntimeError("Executor has been shut down")
        
        # Gather all items
        items = list(zip(*iterables))
        
        # Submit all tasks
        futures = []
        for item_args in items:
            future = self.submit(fn, *item_args)
            futures.append(future)
        
        # Return iterator over results
        return (future.result() for future in futures)
    
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the asyncio executor.
        
        Args:
            wait: Whether to wait for pending tasks to complete
        """
        if self._loop is not None and self._running:
            self._running = False
            
            # Stop the event loop
            self._loop.call_soon_threadsafe(self._loop.stop)
            
            if wait and self._loop_thread:
                self._loop_thread.join(timeout=30)  # Give it time to shutdown
            
            if self._loop and not self._loop.is_closed():
                self._loop.close()
            
            self._loop = None
            self._loop_thread = None 