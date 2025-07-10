"""Configuration classes for concurry executors."""

from dataclasses import dataclass, fields
from typing import Any, Dict, Optional, Type, TypeVar

from autoenum import AutoEnum, alias, auto

# Environment variable names for configuration
ENV_MAX_THREADS = "CONCURRY_MAX_THREADS"
ENV_MAX_PROCESSES = "CONCURRY_MAX_PROCESSES"

T = TypeVar("T", bound="_Config")


class _Config:
    """Base class for all configuration classes with dict conversion support."""

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """Create config instance from dictionary with automatic type conversion."""
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")

        # Get field information for this dataclass
        field_info = {field.name: field for field in fields(cls)}
        constructor_inputs = {}

        for field_name, value in data.items():
            if field_name not in field_info:
                # Skip extra fields that don't exist in the dataclass
                continue

            field = field_info[field_name]

            # Handle enum conversion
            if hasattr(field.type, "__bases__") and AutoEnum in field.type.__bases__:
                if isinstance(value, str):
                    constructor_inputs[field_name] = field.type(value)
                else:
                    constructor_inputs[field_name] = value
            else:
                constructor_inputs[field_name] = value

        return cls(**constructor_inputs)


class ExecutionMode(AutoEnum):
    """Execution modes supported by concurry."""

    Auto = auto()  # Auto-detect best mode based on function characteristics
    Sync = alias("synchronous")  # Synchronous execution (no parallelism)
    Asyncio = alias("async", "asynchronous")  # AsyncIO execution (good for I/O)
    Threads = alias("thread")  # Thread-based execution (good for I/O bound tasks)
    Processes = alias("proc", "procs", "process")  # Process-based execution (good for CPU bound tasks)
    Ray = auto()  # Ray distributed execution (good for distributed tasks)


class RateLimitAlgorithm(AutoEnum):
    """Rate limiting algorithms."""

    FixedWindow = alias("fixed")  # Simple fixed time windows
    SlidingWindow = alias("sliding")  # Rolling time windows (more fair)
    TokenBucket = alias("token")  # Allows controlled bursts
    LeakyBucket = alias("leaky")  # Smooth traffic shaping


@dataclass
class RateLimitConfig(_Config):
    """Comprehensive rate limiting configuration."""

    # Core rate limiting parameters
    max_calls: int  # Number of calls allowed
    time_window: float  # Time window in seconds (configurable!)
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.SlidingWindow

    # Algorithm-specific parameters
    burst_capacity: Optional[int] = None  # For token bucket - max burst size
    refill_rate: Optional[float] = None  # For token bucket - tokens per second
    leak_rate: Optional[float] = None  # For leaky bucket

    def __post_init__(self):
        """Validate and set defaults based on algorithm."""
        if self.max_calls <= 0:
            raise ValueError("max_calls must be positive")
        if self.time_window <= 0:
            raise ValueError("time_window must be positive")

        # Set sensible defaults based on algorithm
        if self.algorithm == RateLimitAlgorithm.TokenBucket:
            if self.burst_capacity is None:
                self.burst_capacity = self.max_calls  # Allow full window as burst
            if self.refill_rate is None:
                self.refill_rate = self.max_calls / self.time_window

    @property
    def calls_per_second(self) -> float:
        """Backward compatibility property."""
        return self.max_calls / self.time_window

    @classmethod
    def per_second(cls, max_calls: int, **kwargs) -> "RateLimitConfig":
        """Convenience constructor for per-second limits."""
        return cls(max_calls=max_calls, time_window=1.0, **kwargs)

    @classmethod
    def per_minute(cls, max_calls: int, **kwargs) -> "RateLimitConfig":
        """Convenience constructor for per-minute limits."""
        return cls(max_calls=max_calls, time_window=60.0, **kwargs)

    @classmethod
    def per_hour(cls, max_calls: int, **kwargs) -> "RateLimitConfig":
        """Convenience constructor for per-hour limits."""
        return cls(max_calls=max_calls, time_window=3600.0, **kwargs)


@dataclass
class RetryConfig(_Config):
    """Configuration for retry behavior."""

    max_retries: int
    initial_delay: float = 0.0
    exponential_base: float = 2.0
    jitter: float = 0.5
    retryable_exceptions: tuple = (Exception,)

    def __post_init__(self):
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.initial_delay < 0:
            raise ValueError("initial_delay must be positive")
        if self.exponential_base <= 1:
            raise ValueError("exponential_base must be greater than 1")


@dataclass
class ExecutorConfig(_Config):
    """Unified configuration for all execution modes."""

    # Core execution settings
    mode: ExecutionMode = ExecutionMode.Auto
    max_workers: Optional[int] = None
    timeout: Optional[float] = None

    # Rate limiting configuration
    rate_limit: Optional[RateLimitConfig] = None

    # Retry configuration
    retry_config: Optional[RetryConfig] = None

    def __post_init__(self):
        """Validate configuration and set defaults."""
        # Convert string mode to ExecutionMode enum if needed
        if isinstance(self.mode, str):
            self.mode = ExecutionMode(self.mode)

        if self.max_workers is not None and self.max_workers <= 0:
            raise ValueError("max_workers must be positive")

        # Set reasonable defaults based on mode
        if self.max_workers is None:
            self.max_workers = self._get_default_max_workers()

        # Auto-convert dictionaries to config objects (handled by _Config.from_dict now)
        if self.rate_limit is not None and isinstance(self.rate_limit, dict):
            self.rate_limit = RateLimitConfig.from_dict(self.rate_limit)

        if self.retry_config is not None and isinstance(self.retry_config, dict):
            self.retry_config = RetryConfig.from_dict(self.retry_config)

        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be positive")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutorConfig":
        """Create ExecutorConfig from dictionary with automatic nested config conversion."""
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")

        # Make a copy to avoid mutating the original
        data = data.copy()

        # Handle nested config conversion before calling parent
        if "rate_limit" in data and isinstance(data["rate_limit"], dict):
            data["rate_limit"] = RateLimitConfig.from_dict(data["rate_limit"])

        if "retry_config" in data and isinstance(data["retry_config"], dict):
            data["retry_config"] = RetryConfig.from_dict(data["retry_config"])

        # Handle mode enum conversion
        if "mode" in data and isinstance(data["mode"], str):
            data["mode"] = ExecutionMode(data["mode"])

        return cls(**data)

    def _get_default_max_workers(self) -> Optional[int]:
        """Get sensible default for max_workers based on execution mode."""
        if self.mode in (ExecutionMode.Auto, ExecutionMode.Sync, ExecutionMode.Asyncio, ExecutionMode.Ray):
            return None

        # For threads and processes, max_workers must be explicitly provided
        if self.mode == ExecutionMode.Threads:
            raise ValueError("max_workers must be explicitly provided for Threads execution mode")

        elif self.mode == ExecutionMode.Processes:
            raise ValueError("max_workers must be explicitly provided for Processes execution mode")

        raise NotImplementedError(f"Unsupported execution mode: {self.mode}")
