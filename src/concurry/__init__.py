"""
Concurry - A delicious way to parallelize your code.

Concurry provides a consistent API for parallel and concurrent execution
across asyncio, threads, processes and distributed systems.
"""

# Utilities
from .utils.progress import ProgressBar

# Public API
__all__ = ["ProgressBar"]
