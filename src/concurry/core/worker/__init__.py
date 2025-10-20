"""Worker module for concurry - actor pattern implementation."""

from .asyncio_worker import AsyncioWorkerProxy
from .base_worker import Worker, WorkerProxy, worker
from .process_worker import ProcessWorkerProxy
from .sync_worker import SyncWorkerProxy
from .task_decorator import task
from .task_worker import TaskWorker, TaskWorkerMixin, TaskWorkerPoolMixin
from .thread_worker import ThreadWorkerProxy

__all__ = [
    "TaskWorker",
    "Worker",
    "WorkerProxy",
    "worker",
    "task",
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
