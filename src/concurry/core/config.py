"""Configuration classes for concurry executors."""

from typing import Optional

from morphic import AutoEnum, Typed, alias, auto
from pydantic import conint, field_validator, model_validator

# Environment variable names for configuration
ENV_MAX_THREADS = "CONCURRY_MAX_THREADS"
ENV_MAX_PROCESSES = "CONCURRY_MAX_PROCESSES"


class ExecutionMode(AutoEnum):
    """Execution modes supported by concurry."""

    Auto = auto()  # Auto-detect best mode based on function characteristics
    Sync = alias("synchronous")  # Synchronous execution (no parallelism)
    Asyncio = alias("async", "asynchronous")  # AsyncIO execution (good for I/O)
    Threads = alias("thread")  # Thread-based execution (good for I/O bound tasks)
    Processes = alias("proc", "procs", "process")  # Process-based execution (good for CPU bound tasks)
    Ray = auto()  # Ray distributed execution (good for distributed tasks)


class LoadBalancingAlgorithm(AutoEnum):
    """Load balancing algorithms for worker pools."""

    RoundRobin = alias("rr")  # Distribute requests in round-robin fashion
    LeastActiveLoad = alias("active")  # Select worker with fewest active calls
    LeastTotalLoad = alias("total")  # Select worker with fewest total calls
    Random = alias("rand")  # Random worker selection


class RateLimitAlgorithm(AutoEnum):
    """Rate limiting algorithms."""

    FixedWindow = alias("fixed")  # Simple fixed time windows
    SlidingWindow = alias("sliding")  # Rolling time windows (more fair)
    TokenBucket = alias("token")  # Allows controlled bursts
    LeakyBucket = alias("leaky")  # Smooth traffic shaping


class RateLimitConfig(Typed):
    """Comprehensive rate limiting configuration."""

    # Core rate limiting parameters
    max_calls: int  # Number of calls allowed
    time_window: float  # Time window in seconds (configurable!)
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.SlidingWindow

    # Algorithm-specific parameters
    burst_capacity: Optional[int] = None  # For token bucket - max burst size
    refill_rate: Optional[float] = None  # For token bucket - tokens per second
    leak_rate: Optional[float] = None  # For leaky bucket

    @field_validator("max_calls")
    @classmethod
    def validate_max_calls(cls, v):
        if v <= 0:
            raise ValueError("max_calls must be positive")
        return v

    @field_validator("time_window")
    @classmethod
    def validate_time_window(cls, v):
        if v <= 0:
            raise ValueError("time_window must be positive")
        return v

    @model_validator(mode="after")
    def set_defaults_by_algorithm(self):
        """Set sensible defaults based on algorithm."""
        if self.algorithm == RateLimitAlgorithm.TokenBucket:
            if self.burst_capacity is None:
                # Create a copy with the updated value since the model is frozen
                return self.model_copy(update={"burst_capacity": self.max_calls})
            if self.refill_rate is None:
                # Create a copy with the updated value since the model is frozen
                return self.model_copy(update={"refill_rate": self.max_calls / self.time_window})
        return self

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


class RetryConfig(Typed):
    """Configuration for retry behavior."""

    max_retries: int
    initial_delay: float = 0.0
    exponential_base: float = 2.0
    jitter: float = 0.5
    retryable_exceptions: tuple = (Exception,)

    @field_validator("max_retries")
    @classmethod
    def validate_max_retries(cls, v):
        if v < 0:
            raise ValueError("max_retries must be non-negative")
        return v

    @field_validator("initial_delay")
    @classmethod
    def validate_initial_delay(cls, v):
        if v < 0:
            raise ValueError("initial_delay must be positive")
        return v

    @field_validator("exponential_base")
    @classmethod
    def validate_exponential_base(cls, v):
        if v <= 1:
            raise ValueError("exponential_base must be greater than 1")
        return v


class ExecutorConfig(Typed):
    """Unified configuration for all execution modes."""

    # Core execution settings
    mode: ExecutionMode = ExecutionMode.Auto
    max_workers: Optional[conint(ge=0)] = None
    timeout: Optional[float] = None

    # Rate limiting configuration
    rate_limit: Optional[RateLimitConfig] = None

    # Retry configuration
    retry_config: Optional[RetryConfig] = None

    @field_validator("mode", mode="before")
    @classmethod
    def validate_mode(cls, v):
        """Convert string mode to ExecutionMode enum if needed."""
        if isinstance(v, str):
            return ExecutionMode(v)
        return v

    @field_validator("rate_limit", mode="before")
    @classmethod
    def validate_rate_limit(cls, v):
        """Auto-convert dictionaries to config objects."""
        if v is not None and isinstance(v, dict):
            return RateLimitConfig(**v)
        return v

    @field_validator("retry_config", mode="before")
    @classmethod
    def validate_retry_config(cls, v):
        """Auto-convert dictionaries to config objects."""
        if v is not None and isinstance(v, dict):
            return RetryConfig(**v)
        return v

    @field_validator("max_workers")
    @classmethod
    def validate_max_workers(cls, v):
        if v is not None and v <= 0:
            raise ValueError("max_workers must be positive")
        return v

    @field_validator("timeout")
    @classmethod
    def validate_timeout(cls, v):
        if v is not None and v <= 0:
            raise ValueError("timeout must be positive")
        return v

    @model_validator(mode="after")
    def set_default_max_workers(self):
        """Set reasonable defaults based on mode."""
        if self.max_workers is None:
            default_workers = self._get_default_max_workers()
            if default_workers is not None:
                return self.model_copy(update={"max_workers": default_workers})
        return self

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
