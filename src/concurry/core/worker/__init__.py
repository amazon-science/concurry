"""Worker module for concurry - actor pattern implementation."""

from .base_worker import TaskWorker, Worker, WorkerProxy, worker
from .sync_worker import SyncWorkerProxy
from .thread_worker import ThreadWorkerProxy
from .process_worker import ProcessWorkerProxy
from .asyncio_worker import AsyncioWorkerProxy

__all__ = [
    "TaskWorker",
    "Worker",
    "WorkerProxy",
    "worker",
    "SyncWorkerProxy",
    "ThreadWorkerProxy",
    "ProcessWorkerProxy",
    "AsyncioWorkerProxy",
]

# Conditionally export RayWorkerProxy if Ray is installed
try:
    from .ray_worker import RayWorkerProxy

    __all__.append("RayWorkerProxy")
except ImportError:
    pass
