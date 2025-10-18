"""
Concurry - A delicious way to parallelize your code.

Concurry provides a consistent API for parallel and concurrent execution
across asyncio, threads, processes and distributed systems.
"""

# Core types
from .core import (
    ALL_COMPLETED,
    FIRST_COMPLETED,
    FIRST_EXCEPTION,
    Acquisition,
    AsyncioFuture,
    BaseFuture,
    CallLimit,
    ConcurrentFuture,
    ExecutionMode,
    Limit,
    LimitPool,
    LimitSet,
    LimitSetAcquisition,
    LoadBalancingAlgorithm,
    PollingAlgorithm,
    RateLimit,
    RateLimitAlgorithm,
    ResourceLimit,
    RetryAlgorithm,
    RetryConfig,
    RetryValidationError,
    ReturnWhen,
    SyncFuture,
    TaskWorker,
    Worker,
    gather,
    wait,
    worker,
    wrap_future,
)

# Executor function
from .executor import Executor

# Utilities
from .utils.progress import ProgressBar

# Public API
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
    "ReturnWhen",
    # Worker types
    "TaskWorker",
    "Worker",
    "worker",
    "Executor",
    # Synchronization
    "wait",
    "gather",
    "ALL_COMPLETED",
    "FIRST_COMPLETED",
    "FIRST_EXCEPTION",
    # Algorithms
    "RateLimitAlgorithm",
    "RetryAlgorithm",
    # Limit types
    "Limit",
    "RateLimit",
    "CallLimit",
    "ResourceLimit",
    "Acquisition",
    "LimitSetAcquisition",
    "LimitSet",
    "LimitPool",
    # Retry types
    "RetryConfig",
    "RetryValidationError",
    # Utilities
    "ProgressBar",
]

# Conditionally export RayFuture if Ray is installed
try:
    from .core import RayFuture

    __all__.append("RayFuture")
except ImportError:
    pass
