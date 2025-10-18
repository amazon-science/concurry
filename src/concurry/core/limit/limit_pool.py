"""LimitPool for load-balanced selection across multiple LimitSets.

This module provides LimitPool, a private (non-shared) wrapper around multiple
LimitSets that enables scalable multi-account/multi-region resource management.

Key Characteristics:
    - NOT a shared object (each worker has its own LimitPool instance)
    - LimitSets within the pool ARE shared (all workers use same LimitSets)
    - Uses load balancing (random or round-robin) to select LimitSets
    - No cross-worker synchronization (fast, scalable)
    - Config from selected LimitSet available via acquisition.config

Architecture:
    LimitPool selects a LimitSet using load balancing, then delegates the
    acquisition call to that LimitSet. Each LimitSet can have different config
    (e.g., account ID, region) accessible via acquisition.config.

    Example scenario: 10 AWS accounts x 5 regions = 50 LimitSets
    - Each LimitSet has independent limits for that account/region
    - Each LimitSet has config with account_id and region
    - Workers select LimitSets via round-robin or random
    - No synchronization needed in selection (fast, scalable)

Example:
    Multi-region API with different rate limits per region::

        from concurry import LimitSet, LimitPool, RateLimit, RateLimitAlgorithm

        # Create LimitSet for each region
        limitset_us_east = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
            shared=True,
            mode="thread",
            config={"region": "us-east-1", "endpoint": "https://api.us-east-1.example.com"}
        )

        limitset_us_west = LimitSet(
            limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)],
            shared=True,
            mode="thread",
            config={"region": "us-west-2", "endpoint": "https://api.us-west-2.example.com"}
        )

        # Create LimitPool (each worker gets private instance)
        limit_pool = LimitPool(
            limit_sets=[limitset_us_east, limitset_us_west],
            load_balancing=LoadBalancingAlgorithm.RoundRobin
        )

        # Use in worker
        class APIWorker(Worker):
            def call_api(self, prompt: str):
                with self.limits.acquire(requested={"tokens": 100}) as acq:
                    # Get config from selected LimitSet
                    endpoint = acq.config["endpoint"]
                    result = make_request(endpoint, prompt)
                    acq.update(usage={"tokens": result.tokens})
                    return result.text

See Also:
    - LimitSet: Thread-safe atomic multi-limit acquisition
    - LimitSetAcquisition: Tracks usage with config access
    - User Guide: docs/user-guide/limits.md
"""

from typing import Any, Dict, List, NoReturn, Optional

from morphic import Typed
from pydantic import PrivateAttr

from ..algorithms.load_balancing import RandomBalancer, RoundRobinBalancer
from ..constants import LoadBalancingAlgorithm
from .acquisition import LimitSetAcquisition
from .limit_set import BaseLimitSet


class LimitPool(Typed):
    """Private wrapper for load-balanced selection across multiple LimitSets.

    LimitPool is designed for scenarios with multiple independent resource pools,
    such as multi-account/multi-region API access. Each LimitSet can have different
    configs (e.g., account ID, region) that are exposed via acquisition.config.

    Architecture:
        - Lives privately in each worker (NOT shared)
        - Wraps multiple shared LimitSets
        - Selects LimitSet using load balancing (no synchronization)
        - Delegates acquire() to selected LimitSet

    Load Balancing:
        - **Random**: Random selection (stateless, zero overhead)
        - **RoundRobin**: Circular with offset (worker_index based)

    Thread-Safety:
        LimitPool itself does NOT need thread-safety since each worker has its
        own private instance. The LimitSets within the pool are shared and
        provide their own thread-safety.

    Attributes:
        limit_sets: List of LimitSet instances to select from
        load_balancing: Algorithm (LoadBalancingAlgorithm enum)
        worker_index: Starting offset for round-robin (0 for single workers)

    Example:
        Basic usage with two regions::

            from concurry import LimitSet, LimitPool, LoadBalancingAlgorithm

            pool = LimitPool(
                limit_sets=[limitset_us, limitset_eu],
                load_balancing=LoadBalancingAlgorithm.RoundRobin,
                worker_index=0
            )

            # Acquire from selected LimitSet
            with pool.acquire(requested={"tokens": 100}) as acq:
                region = acq.config["region"]
                result = call_api(region)
                acq.update(usage={"tokens": result.tokens})

        Worker integration::

            class APIWorker(Worker):
                def call_api(self, prompt: str):
                    # self.limits is a LimitPool
                    with self.limits.acquire(requested={"tokens": 100}) as acq:
                        endpoint = acq.config["endpoint"]
                        result = make_request(endpoint, prompt)
                        acq.update(usage={"tokens": result.tokens})
                        return result

            # Create workers with LimitPool
            pool = APIWorker.options(
                mode="thread",
                max_workers=10,
                limits=[limitset1, limitset2, limitset3]  # Creates LimitPool
            ).init()

    See Also:
        - LimitSet: Thread-safe limit set for atomic multi-limit acquisition
        - LoadBalancingAlgorithm: Enum of supported algorithms
        - User Guide: docs/user-guide/limits.md#limitpool
    """

    # Public immutable attributes
    limit_sets: List[BaseLimitSet]
    load_balancing: LoadBalancingAlgorithm = LoadBalancingAlgorithm.RoundRobin
    worker_index: int = 0

    # Private mutable attributes
    _balancer: Any = PrivateAttr()

    def post_initialize(self) -> NoReturn:
        """Initialize private attributes after Typed validation.

        Creates the appropriate load balancer based on the load_balancing algorithm
        and worker_index offset.

        Raises:
            ValueError: If limit_sets is empty
        """
        if len(self.limit_sets) == 0:
            raise ValueError("LimitPool requires at least one LimitSet")

        # Create appropriate load balancer
        if self.load_balancing == LoadBalancingAlgorithm.Random:
            balancer = RandomBalancer()
        elif self.load_balancing == LoadBalancingAlgorithm.RoundRobin:
            # Use RoundRobinBalancer with offset support
            balancer = RoundRobinBalancer(offset=self.worker_index)
        else:
            raise ValueError(
                f"Unsupported load balancing algorithm for LimitPool: {self.load_balancing}. "
                f"Supported: Random, RoundRobin"
            )

        # Store balancer in private attribute
        object.__setattr__(self, "_balancer", balancer)

    def acquire(
        self, requested: Optional[Dict[str, int]] = None, timeout: Optional[float] = None
    ) -> LimitSetAcquisition:
        """Acquire from a selected LimitSet.

        Selects a LimitSet using load balancing, then delegates acquisition.
        The returned acquisition will have .config from the selected LimitSet.

        Args:
            requested: Dict mapping limit keys to requested amounts.
                If None or empty, acquires all limits with defaults.
            timeout: Maximum time to wait for acquisition in seconds.
                If None, blocks indefinitely.

        Returns:
            LimitSetAcquisition with config from selected LimitSet

        Raises:
            TimeoutError: If acquisition times out
            ValueError: If requested amounts are invalid

        Example:
            Acquire with specific amounts::

                with pool.acquire(requested={"tokens": 100, "connections": 2}) as acq:
                    # acq.config contains selected LimitSet's config
                    region = acq.config.get("region", "unknown")
                    result = call_api(region)
                    acq.update(usage={"tokens": result.tokens})

            Acquire with defaults::

                with pool.acquire() as acq:
                    # CallLimit and ResourceLimit use defaults
                    result = operation()
        """
        # Select a LimitSet using load balancing
        selected_limitset = self._select_limit_set()
        # Delegate to selected LimitSet
        return selected_limitset.acquire(requested=requested, timeout=timeout)

    def try_acquire(self, requested: Optional[Dict[str, int]] = None) -> LimitSetAcquisition:
        """Try to acquire from a selected LimitSet without blocking.

        Selects a LimitSet using load balancing, then attempts non-blocking acquisition.
        Returns immediately with successful=False if limits cannot be acquired.

        Args:
            requested: Dict mapping limit keys to requested amounts.
                If None or empty, acquires all limits with defaults.

        Returns:
            LimitSetAcquisition with successful attribute indicating success

        Example:
            Try acquire with fallback::

                acq = pool.try_acquire(requested={"tokens": 100})
                if acq.successful:
                    with acq:
                        result = expensive_operation()
                        acq.update(usage={"tokens": result.tokens})
                else:
                    result = use_cached_result()
        """
        # Select a LimitSet using load balancing
        selected_limitset = self._select_limit_set()
        # Delegate to selected LimitSet
        return selected_limitset.try_acquire(requested=requested)

    def _select_limit_set(self) -> BaseLimitSet:
        """Select a LimitSet using configured load balancing algorithm.

        Returns:
            Selected BaseLimitSet instance
        """
        # Use balancer to select index
        index = self._balancer.select_worker(num_workers=len(self.limit_sets))
        return self.limit_sets[index]

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for the LimitPool and its constituent LimitSets.

        Returns:
            Dictionary containing:
                - num_limit_sets: Number of LimitSets in the pool
                - load_balancing: Load balancing algorithm
                - worker_index: Worker's starting offset
                - balancer_stats: Statistics from the load balancer
                - limit_sets: List of stats dicts, one per LimitSet

        Example:
            Get and display stats::

                stats = pool.get_stats()
                print(f"LimitSets: {stats['num_limit_sets']}")
                print(f"Algorithm: {stats['load_balancing']}")
                print(f"Balancer: {stats['balancer_stats']}")

                for i, ls_stats in enumerate(stats['limit_sets']):
                    print(f"LimitSet {i}: {ls_stats}")
        """
        return {
            "num_limit_sets": len(self.limit_sets),
            "load_balancing": self.load_balancing.value,
            "worker_index": self.worker_index,
            "balancer_stats": self._balancer.get_stats(),
            "limit_sets": [ls.get_stats() for ls in self.limit_sets],
        }

    def __getitem__(self, index: int) -> BaseLimitSet:
        """Get LimitSet by integer index.

        Note: String key access is NOT supported because different LimitSets
        in the pool may have different limit keys. To access a Limit by key,
        first get the LimitSet by index, then access the Limit:

            limit = pool[0]["tokens"]  # Get "tokens" limit from first LimitSet
            limit = pool.limit_sets[0]["tokens"]  # Equivalent, more explicit

        Args:
            index: Integer index of the LimitSet (0-based)

        Returns:
            BaseLimitSet at the specified index

        Raises:
            IndexError: If index is out of range
            TypeError: If index is not an integer

        Example:
            Access LimitSet by index::

                pool = LimitPool(limit_sets=[ls1, ls2, ls3])

                # Access first LimitSet
                first_limitset = pool[0]
                stats = first_limitset.get_stats()

                # Access Limit within first LimitSet
                token_limit = pool[0]["tokens"]
                capacity = token_limit.capacity
        """
        if not isinstance(index, int):
            raise TypeError(
                f"LimitPool indices must be integers, not {type(index).__name__}. "
                f"String key access is not supported because LimitSets may have different keys. "
                f"Use pool[i]['key'] to access a Limit from a specific LimitSet."
            )

        return self.limit_sets[index]

    def __getstate__(self) -> Dict[str, Any]:
        """Custom pickle support - exclude non-serializable balancer.

        The balancer contains threading.Lock which cannot be pickled. We store
        only the configuration needed to recreate it on unpickling.

        Returns:
            Dict of serializable state
        """
        state = self.__dict__.copy()
        # Remove the _balancer which contains locks
        state.pop("_balancer", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        """Custom unpickle support - recreate balancer from configuration.

        Recreates the balancer based on load_balancing algorithm and worker_index.

        Args:
            state: Pickled state dict
        """
        self.__dict__.update(state)

        # Recreate balancer
        if self.load_balancing == LoadBalancingAlgorithm.Random:
            balancer = RandomBalancer()
        elif self.load_balancing == LoadBalancingAlgorithm.RoundRobin:
            balancer = RoundRobinBalancer(offset=self.worker_index)
        else:
            raise ValueError(f"Unknown load balancing algorithm: {self.load_balancing}")

        object.__setattr__(self, "_balancer", balancer)
