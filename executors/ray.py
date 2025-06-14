"""Ray-based executor implementation."""

from typing import Any, Callable, Iterator, Optional

from ..core.executor import Executor
from ..core.future import Future, RayFutureWrapper


try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False
    ray = None


@ray.remote
def _execute_function(fn_bytes, args, kwargs):
    """Remote function to execute serialized functions."""
    import cloudpickle
    fn = cloudpickle.loads(fn_bytes)
    return fn(*args, **kwargs)


class RayExecutor(Executor):
    """Executor that runs tasks using Ray."""
    
    def __init__(self, config):
        if not RAY_AVAILABLE:
            raise ImportError("Ray is not installed. Install with: pip install ray")
        
        super().__init__(config)
        self._initialize_ray()
    
    def _initialize_ray(self):
        """Initialize Ray if not already initialized."""
        if not ray.is_initialized():
            init_kwargs = {}
            
            if self.config.ray_address:
                init_kwargs['address'] = self.config.ray_address
            
            # Add resource configuration if available
            if hasattr(self.config, 'num_cpus') and self.config.num_cpus:
                init_kwargs['num_cpus'] = self.config.num_cpus
            
            if hasattr(self.config, 'num_gpus') and self.config.num_gpus:
                init_kwargs['num_gpus'] = self.config.num_gpus
            
            ray.init(**init_kwargs)
    
    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """Submit a function for execution using Ray.
        
        Args:
            fn: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Future representing the computation
        """
        # Extract Ray-specific options from kwargs
        ray_options = {}
        if 'num_cpus' in kwargs:
            ray_options['num_cpus'] = kwargs.pop('num_cpus')
        elif self.config.num_cpus:
            ray_options['num_cpus'] = self.config.num_cpus
        
        if 'num_gpus' in kwargs:
            ray_options['num_gpus'] = kwargs.pop('num_gpus')
        elif self.config.num_gpus:
            ray_options['num_gpus'] = self.config.num_gpus
        
        # Serialize function for remote execution
        import cloudpickle
        fn_bytes = cloudpickle.dumps(fn)
        
        # Create remote function with options
        remote_fn = _execute_function.options(**ray_options)
        
        # Submit task
        object_ref = remote_fn.remote(fn_bytes, args, kwargs)
        return RayFutureWrapper(object_ref)
    
    def map(self, fn: Callable, *iterables, **kwargs) -> Iterator[Any]:
        """Apply function to iterables using Ray.
        
        Args:
            fn: Function to apply
            *iterables: Iterables to process
            **kwargs: Additional arguments
            
        Returns:
            Iterator of results
        """
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
        """Shutdown the Ray executor.
        
        Args:
            wait: Whether to wait for pending tasks to complete
        """
        # Note: We don't shutdown Ray as it might be used by other components
        # This is just a no-op for compatibility
        pass 