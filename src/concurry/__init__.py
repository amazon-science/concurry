"""
Concurry - A delicious way to parallelize your code.

Concurry provides a consistent API for parallel and concurrent execution
across asyncio, threads, processes and distributed systems.
"""

# Core types
from .core import (
    Acquisition,
    AsyncioFuture,
    BaseFuture,
    CallLimit,
    ConcurrentFuture,
    ExecutionMode,
    Limit,
    LimitSet,
    LimitSetAcquisition,
    RateLimit,
    RateLimiterAlgorithm,
    ResourceLimit,
    SyncFuture,
    TaskWorker,
    Worker,
    worker,
    wrap_future,
)

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
    # Worker types
    "TaskWorker",
    "Worker",
    "worker",
    # Rate limiter
    "RateLimiterAlgorithm",
    # Limit types
    "Limit",
    "RateLimit",
    "CallLimit",
    "ResourceLimit",
    "Acquisition",
    "LimitSetAcquisition",
    "LimitSet",
    # Utilities
    "ProgressBar",
]

# Conditionally export RayFuture if Ray is installed
try:
    from .core import RayFuture

    __all__.append("RayFuture")
except ImportError:
    pass
