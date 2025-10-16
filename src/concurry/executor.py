"""Executor function for creating TaskWorker executors.

This module provides a simple Executor() function that mimics the
concurrent.futures.Executor interface but returns TaskWorker instances.
"""

from typing import Any, Optional, Union


def Executor(
    mode: str = "thread",
    max_workers: Optional[int] = None,
    load_balancing: Optional[str] = None,
    on_demand: bool = False,
    limits: Optional[Any] = None,
    **kwargs: Any,
) -> Any:
    """Create a TaskWorker executor with concurrent.futures-like interface.

    This function provides a simple API similar to concurrent.futures.Executor
    for creating task executors with different execution modes. It delegates
    to TaskWorker.options(...).init() and supports all the same features
    including worker pools, load balancing, and resource limits.

    Args:
        mode: Execution mode (thread, process, asyncio, ray, sync)
            Default: "thread"
        max_workers: Maximum number of workers in pool (optional)
            - If None or 1: Creates single worker
            - If > 1: Creates worker pool with specified size
            - Sync/Asyncio: Must be 1 or None (raises error otherwise)
            - Thread: Default 24 when pool requested
            - Process: Default 4 when pool requested
            - Ray: Default 0 (unlimited for on-demand)
        load_balancing: Load balancing algorithm (optional)
            - "round_robin": Distribute requests evenly (default for pools)
            - "least_active": Select worker with fewest active calls
            - "least_total": Select worker with fewest total calls
            - "random": Random selection (default for on-demand)
        on_demand: If True, create workers on-demand per task (default: False)
            - Workers are created for each task and destroyed after completion
            - Useful for bursty workloads or resource-constrained environments
            - Cannot be used with Sync/Asyncio modes
        limits: LimitSet or List[Limit] for resource protection (optional)
            - Pass LimitSet: Workers share the same limit pool
            - Pass List[Limit]: Each worker gets private limits (creates shared LimitSet for pools)
        **kwargs: Additional mode-specific options
            - For ray: num_cpus, num_gpus, resources, etc.
            - For process: mp_context (fork, spawn, forkserver)

    Returns:
        TaskWorker instance (single worker or pool) that can be used like
        a concurrent.futures.Executor

    Examples:
        Simple Thread Pool:
            ```python
            from concurry import Executor

            # Create a thread pool with 10 workers
            executor = Executor(mode="thread", max_workers=10)
            future = executor.submit(lambda x: x * 2, 5)
            result = future.result()  # 10
            executor.stop()
            ```

        Process Pool with Load Balancing:
            ```python
            # Create a process pool with least-active load balancing
            executor = Executor(
                mode="process",
                max_workers=4,
                load_balancing="least_active"
            )

            def compute(x):
                return x ** 2

            results = list(executor.map(compute, range(10)))
            executor.stop()
            ```

        On-Demand Ray Workers:
            ```python
            import ray
            ray.init()

            # Create on-demand Ray workers (unlimited)
            executor = Executor(mode="ray", on_demand=True, max_workers=0)
            future = executor.submit(lambda x: x + 10, 5)
            result = future.result()  # 15
            executor.stop()
            ```

        With Rate Limits:
            ```python
            from concurry import Executor, RateLimit, RateLimitAlgorithm

            limits = [
                RateLimit(
                    key="api_calls",
                    window_seconds=60,
                    algorithm=RateLimitAlgorithm.TokenBucket,
                    capacity=100
                )
            ]

            executor = Executor(
                mode="thread",
                max_workers=5,
                limits=limits
            )

            # All workers share the 100 calls/min limit
            future = executor.submit(call_external_api, url="https://api.example.com")
            executor.stop()
            ```

        Blocking Mode:
            ```python
            # Create blocking executor (returns results directly)
            executor = Executor(mode="thread", max_workers=5, blocking=True)
            result = executor.submit(lambda x: x * 3, 7)  # Returns 21 directly
            executor.stop()
            ```

    See Also:
        - TaskWorker: The underlying worker class
        - Worker: Base class for custom workers
        - Worker Pools: User guide on worker pools
    """
    from .core.worker.task_worker import TaskWorker

    return TaskWorker.options(
        mode=mode,
        max_workers=max_workers,
        load_balancing=load_balancing,
        on_demand=on_demand,
        limits=limits,
        **kwargs,
    ).init()
