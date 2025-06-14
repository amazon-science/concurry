"""Configuration classes for concurry executors."""

import os
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field


class ExecutionMode(Enum):
    """Execution modes supported by concurry."""
    
    AUTO = "auto"          # Auto-detect best mode based on function characteristics
    SYNC = "sync"          # Synchronous execution (no parallelism)
    THREADS = "threads"    # Thread-based execution (good for I/O bound tasks)
    PROCESSES = "processes" # Process-based execution (good for CPU bound tasks)
    ASYNCIO = "asyncio"    # AsyncIO execution (good for async I/O)
    RAY = "ray"            # Ray distributed execution (good for distributed tasks)


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    
    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_exceptions: tuple = (Exception,)
    
    def __post_init__(self):
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.initial_delay <= 0:
            raise ValueError("initial_delay must be positive")
        if self.max_delay <= 0:
            raise ValueError("max_delay must be positive")
        if self.exponential_base <= 1:
            raise ValueError("exponential_base must be greater than 1")


@dataclass
class ExecutorConfig:
    """Unified configuration for all execution modes."""
    
    # Core execution settings
    mode: ExecutionMode = ExecutionMode.AUTO
    max_workers: Optional[int] = None
    max_calls_per_second: float = float('inf')
    timeout: Optional[float] = None
    
    # Resource constraints
    max_memory_mb: Optional[int] = None
    max_cpu_percent: Optional[float] = None
    
    # Retry configuration
    retry_config: Optional[RetryConfig] = None
    
    # Progress and monitoring
    progress: bool = True
    progress_desc: str = ""
    
    # Ray-specific settings
    num_cpus: Optional[Union[int, float]] = None
    num_gpus: Optional[Union[int, float]] = None
    ray_address: Optional[str] = None
    
    # Advanced settings
    worker_init_fn: Optional[callable] = None
    worker_init_args: tuple = field(default_factory=tuple)
    context: Optional[str] = None  # For multiprocessing context
    
    def __post_init__(self):
        """Validate configuration and set defaults."""
        if self.max_workers is not None and self.max_workers <= 0:
            raise ValueError("max_workers must be positive")
        
        if self.max_calls_per_second <= 0:
            raise ValueError("max_calls_per_second must be positive")
        
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be positive")
        
        if self.max_memory_mb is not None and self.max_memory_mb <= 0:
            raise ValueError("max_memory_mb must be positive")
        
        if self.max_cpu_percent is not None and (self.max_cpu_percent <= 0 or self.max_cpu_percent > 100):
            raise ValueError("max_cpu_percent must be between 0 and 100")
        
        # Set reasonable defaults based on mode
        if self.max_workers is None:
            self.max_workers = self._get_default_max_workers()
        
        # Load from environment if available
        self._load_from_environment()
    
    def _get_default_max_workers(self) -> Optional[int]:
        """Get sensible default for max_workers based on execution mode."""
        if self.mode in (ExecutionMode.SYNC, ExecutionMode.ASYNCIO):
            return None
        
        # For threads and processes, use CPU count as baseline
        import os
        cpu_count = os.cpu_count() or 4
        
        if self.mode == ExecutionMode.THREADS:
            # I/O bound tasks can use more threads
            return min(32, cpu_count * 4)
        elif self.mode == ExecutionMode.PROCESSES:
            # CPU bound tasks should match CPU count
            return cpu_count
        elif self.mode == ExecutionMode.RAY:
            # Ray can handle more parallelism
            return None  # Let Ray decide
        
        return cpu_count
    
    def _load_from_environment(self):
        """Load configuration from environment variables."""
        env_mapping = {
            'CONCURRY_MAX_WORKERS': ('max_workers', int),
            'CONCURRY_MAX_CALLS_PER_SECOND': ('max_calls_per_second', float),
            'CONCURRY_TIMEOUT': ('timeout', float),
            'CONCURRY_MAX_MEMORY_MB': ('max_memory_mb', int),
            'CONCURRY_MAX_CPU_PERCENT': ('max_cpu_percent', float),
            'CONCURRY_PROGRESS': ('progress', lambda x: x.lower() == 'true'),
            'CONCURRY_RAY_ADDRESS': ('ray_address', str),
        }
        
        for env_key, (attr_name, converter) in env_mapping.items():
            if env_key in os.environ:
                try:
                    value = converter(os.environ[env_key])
                    setattr(self, attr_name, value)
                except (ValueError, TypeError):
                    # Ignore invalid environment values
                    pass
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, Enum):
                result[key] = value.value
            elif isinstance(value, RetryConfig):
                result[key] = value.__dict__
            else:
                result[key] = value
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExecutorConfig':
        """Create configuration from dictionary."""
        # Convert string mode to enum
        if 'mode' in data and isinstance(data['mode'], str):
            data['mode'] = ExecutionMode(data['mode'])
        
        # Convert retry config
        if 'retry_config' in data and isinstance(data['retry_config'], dict):
            data['retry_config'] = RetryConfig(**data['retry_config'])
        
        return cls(**data) 