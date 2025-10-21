"""Limit system for resource protection and rate limiting.

This module provides a comprehensive limit system for controlling resource usage
and enforcing rate limits in concurry applications. Limits are simple data containers
that must be used within a LimitSet for thread-safe acquisition and management.

Main Components:
    Limit Types (NOT thread-safe, use within LimitSet):
        - RateLimit: Time-based rate limiting with multiple algorithms
        - CallLimit: Call counting (usage always 1)
        - ResourceLimit: Semaphore-based resource limiting

    Limit Management (thread-safe):
        - LimitSet: Thread-safe atomic acquisition of multiple limits

    Acquisition Tracking:
        - Acquisition: Tracks individual limit usage
        - LimitSetAcquisition: Tracks multi-limit usage

    Algorithms:
        - RateLimitAlgorithm: Enum of available rate limiting algorithms

Architecture:
    - **Limits**: Simple data containers that define constraints (NOT thread-safe)
    - **LimitSet**: Thread-safe manager that handles acquisition/release
    - **Shared vs Non-Shared**:
      - Non-shared (default): Private LimitSet per worker with threading.Lock
      - Shared: LimitSet shared across workers with appropriate backend
        (InMemory for sync/asyncio/thread, Multiprocess for process, Ray for ray)

Quick Start:
    Basic LimitSet usage::

        from concurry import LimitSet, RateLimit, RateLimitAlgorithm

        limits = LimitSet(limits=[
            RateLimit(
                key="api_tokens",
                window_seconds=60,
                algorithm=RateLimitAlgorithm.TokenBucket,
                capacity=1000
            )
        ])

        with limits.acquire(requested={"api_tokens": 100}) as acq:
            result = call_api()
            acq.update(usage={"api_tokens": result.actual_tokens})

    Multi-dimensional limiting::

        from concurry import LimitSet, RateLimit, ResourceLimit, RateLimitAlgorithm

        limits = LimitSet(limits=[
            RateLimit(
                key="tokens",
                window_seconds=60,
                algorithm=RateLimitAlgorithm.TokenBucket,
                capacity=1000
            ),
            ResourceLimit(key="connections", capacity=10)
        ])

        with limits.acquire(requested={"tokens": 100, "connections": 2}) as acq:
            result = process_data()
            acq.update(usage={"tokens": result.tokens})

    Worker integration::

        from concurry import Worker, LimitSet, RateLimit, RateLimitAlgorithm

        # Non-shared: pass list of Limits (creates private LimitSet per worker)
        worker = MyWorker.options(
            mode="thread",
            limits=[RateLimit(...), ResourceLimit(...)]
        ).init()

        # Shared: create shared LimitSet for multiple workers
        shared_limits = LimitSet(
            limits=[RateLimit(...), ResourceLimit(...)],
            shared=True,
            mode="thread"  # Must match worker mode
        )
        worker1 = MyWorker.options(mode="thread", limits=shared_limits).init()
        worker2 = MyWorker.options(mode="thread", limits=shared_limits).init()
        # worker1 and worker2 now share the same limits!

See Also:
    - User Guide: docs/user-guide/limits.md
    - Examples: examples/limit_example.py
"""

from .acquisition import Acquisition, LimitSetAcquisition
from .limit import CallLimit, Limit, RateLimit, ResourceLimit
from .limit_pool import LimitPool
from .limit_set import LimitSet
from ..algorithms import RateLimiter
from ..constants import RateLimitAlgorithm

__all__ = [
    # Base classes
    "Limit",
    "RateLimit",
    "CallLimit",
    "ResourceLimit",
    "RateLimitAlgorithm",
    # Acquisition
    "Acquisition",
    "LimitSetAcquisition",
    # Limit sets and pools
    "LimitSet",
    "LimitPool",
    # Factory
    "RateLimiter",
]
