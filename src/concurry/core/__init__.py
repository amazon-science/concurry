"""Core functionality for concurry."""

from .constants import (
    ExecutionMode,
    LoadBalancingAlgorithm,
    PollingAlgorithm,
    RateLimitAlgorithm,
    RetryAlgorithm,
    ReturnWhen,
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
    LimitPool,
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
from .synch import (
    ALL_COMPLETED,
    FIRST_COMPLETED,
    FIRST_EXCEPTION,
    gather,
    wait,
)

# Also export ReturnWhen for modern usage
__all_imports = [
    "ReturnWhen",
]
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
    "PollingAlgorithm",
    "RateLimitAlgorithm",
    "RetryAlgorithm",
    "ReturnWhen",
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
    # Synchronization
    "wait",
    "gather",
    "ALL_COMPLETED",
    "FIRST_COMPLETED",
    "FIRST_EXCEPTION",
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
    "LimitPool",
]

# Conditionally export RayFuture if Ray is installed
try:
    from .future import RayFuture

    __all__.append("RayFuture")
except ImportError:
    pass
