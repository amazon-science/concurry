"""Core functionality for concurry."""

from .future import (
    BaseFuture,
    SyncFuture,
    ConcurrentFuture,
    AsyncioFuture,
    wrap_future,
)
from .config import (
    ExecutionMode,
    RateLimitAlgorithm,
    RateLimitConfig,
    RetryConfig,
    ExecutorConfig,
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
    "RateLimitAlgorithm",
    "RateLimitConfig",
    "RetryConfig",
    "ExecutorConfig",
    # Worker types
    "TaskWorker",
    "Worker",
    "worker",
]

# Conditionally export RayFuture if Ray is installed
try:
    from .future import RayFuture

    __all__.append("RayFuture")
except ImportError:
    pass
