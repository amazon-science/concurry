"""Core functionality for concurry."""

from .config import (
    ExecutionMode,
    ExecutorConfig,
    LoadBalancingAlgorithm,
    RateLimitAlgorithm,
    RateLimitConfig,
    RetryConfig,
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
    RateLimiterAlgorithm,
    ResourceLimit,
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
    "RateLimitConfig",
    "RetryConfig",
    "ExecutorConfig",
    # Worker types
    "TaskWorker",
    "Worker",
    "worker",
    # Algorithms
    "RateLimiterAlgorithm",
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
