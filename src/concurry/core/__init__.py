"""Core functionality for concurry."""

from .config import (
    ExecutionMode,
    LoadBalancingAlgorithm,
    RateLimitAlgorithm,
    RetryAlgorithm,
)
from .future import (
    AsyncioFuture,
    BaseFuture,
    ConcurrentFuture,
    SyncFuture,
    wrap_future,
)
from .limit import (
    Acquisition,
    CallLimit,
    Limit,
    LimitSet,
    LimitSetAcquisition,
    RateLimit,
    ResourceLimit,
)
from .retry import (
    RetryConfig,
    RetryValidationError,
    calculate_retry_wait,
    create_retry_wrapper,
    execute_with_retry,
    execute_with_retry_async,
)
from .worker import TaskWorker, Worker, worker

__all__ = [
    # Future types
    "BaseFuture",
    "SyncFuture",
    "ConcurrentFuture",
    "AsyncioFuture",
    "wrap_future",
    # Config types
    "ExecutionMode",
    "LoadBalancingAlgorithm",
    "RateLimitAlgorithm",
    "RetryAlgorithm",
    # Retry functions
    "RetryConfig",
    "RetryValidationError",
    "calculate_retry_wait",
    "create_retry_wrapper",
    "execute_with_retry",
    "execute_with_retry_async",
    # Worker types
    "TaskWorker",
    "Worker",
    "worker",
    # Algorithms
    "RateLimitAlgorithm",
    # Limit types
    "Limit",
    "RateLimit",
    "CallLimit",
    "ResourceLimit",
    "Acquisition",
    "LimitSetAcquisition",
    "LimitSet",
]

# Conditionally export RayFuture if Ray is installed
try:
    from .future import RayFuture

    __all__.append("RayFuture")
except ImportError:
    pass
