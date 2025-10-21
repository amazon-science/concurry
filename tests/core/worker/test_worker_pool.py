"""Tests for worker pools."""

import time

import pytest

from concurry import CallLimit, Worker


# Ray initialization is handled by conftest.py initialize_ray fixture
# pool_mode fixture is provided by conftest.py
class SimpleWorker(Worker):
    """Simple worker for testing."""

    def __init__(self, multiplier: int = 1):
        self.multiplier = multiplier
        self.call_count = 0

    def compute(self, x: int) -> int:
        """Simple computation method."""
        self.call_count += 1
        return x * self.multiplier

    def slow_compute(self, x: int) -> int:
        """Slow computation for testing load balancing."""
        time.sleep(0.1)
        return x * self.multiplier

    def get_count(self) -> int:
        """Get number of calls."""
        return self.call_count


class TestWorkerPoolCreation:
    """Tests for worker pool creation."""

    def test_single_worker_no_pool(self):
        """Test that single worker doesn't create pool.

        This test:
        1. Creates a single sync worker (no max_workers specified)
        2. Verifies it behaves as a regular WorkerProxy, not a pool
        3. Makes a compute call and verifies result (10)
        4. Stops the worker
        """
        worker = SimpleWorker.options(mode="sync").init(multiplier=2)

        # Should be a regular WorkerProxy, not a pool
        # WorkerProxy has get_pool_stats method, but pool has it too
        # Just verify it behaves like a single worker
        result = worker.compute(5).result()
        assert result == 10

        worker.stop()

    def test_pool_with_max_workers(self, pool_mode):
        """Test creating pool with max_workers.

        This test:
        1. Creates a worker pool with 3 workers (thread/process/ray mode)
        2. Verifies the pool has get_pool_stats method
        3. Checks pool stats (total_workers=3, max_workers=3, on_demand=False)
        4. Makes a compute call and verifies result (10)
        5. Stops the pool
        """
        options = {"mode": pool_mode, "max_workers": 3}
        if pool_mode == "ray":
            options["actor_options"] = {"num_cpus": 0.1}

        pool = SimpleWorker.options(**options).init(multiplier=2)

        # Should be a pool
        assert hasattr(pool, "get_pool_stats")

        stats = pool.get_pool_stats()
        assert stats["total_workers"] == 3
        assert stats["max_workers"] == 3
        assert stats["on_demand"] is False

        result = pool.compute(5).result()
        assert result == 10

        pool.stop()

    def test_pool_with_on_demand(self, pool_mode):
        """Test creating on-demand pool.

        This test:
        1. Creates an on-demand worker pool (creates workers per task)
        2. Verifies the pool has get_pool_stats method
        3. Checks pool stats (on_demand=True, total_workers=0 initially)
        4. Makes a compute call (creates temporary worker)
        5. Verifies result (10)
        6. Stops the pool
        """
        pool = SimpleWorker.options(mode=pool_mode, on_demand=True).init(multiplier=2)

        # Should be a pool
        assert hasattr(pool, "get_pool_stats")

        stats = pool.get_pool_stats()
        assert stats["on_demand"] is True
        assert stats["total_workers"] == 0  # No persistent workers

        # Make a call
        future = pool.compute(5)
        result = future.result()
        assert result == 10

        pool.stop()

    def test_sync_rejects_max_workers_greater_than_one(self):
        """Test that sync mode rejects max_workers > 1.

        This test:
        1. Attempts to create a sync worker with max_workers=2
        2. Verifies this raises ValueError (sync only supports single worker)
        """
        with pytest.raises(ValueError, match="max_workers must be 1 for Sync mode"):
            SimpleWorker.options(mode="sync", max_workers=2).init()

    def test_asyncio_rejects_max_workers_greater_than_one(self):
        """Test that asyncio mode rejects max_workers > 1.

        This test:
        1. Attempts to create an asyncio worker with max_workers=2
        2. Verifies this raises ValueError (asyncio only supports single worker)
        """
        with pytest.raises(ValueError, match="max_workers must be 1 for Asyncio mode"):
            SimpleWorker.options(mode="asyncio", max_workers=2).init()

    def test_sync_rejects_on_demand(self):
        """Test that sync mode rejects on_demand.

        This test:
        1. Attempts to create a sync worker with on_demand=True
        2. Verifies this raises ValueError (sync doesn't support on-demand)
        """
        with pytest.raises(ValueError, match="on_demand mode is not supported for Sync"):
            SimpleWorker.options(mode="sync", on_demand=True).init()


class TestWorkerPoolDispatch:
    """Tests for worker pool dispatch and load balancing."""

    def test_round_robin_dispatch(self, pool_mode):
        """Test that round-robin dispatches to workers evenly.

        This test:
        1. Creates a pool with 3 workers and round-robin load balancing
        2. Submits 9 tasks (will be evenly distributed: 3 tasks per worker)
        3. Waits for all results and verifies they're correct
        4. Checks load balancer stats (algorithm=RoundRobin, total_dispatched=9)
        5. Stops the pool
        """
        options = {"mode": pool_mode, "max_workers": 3, "load_balancing": "round_robin"}
        if pool_mode == "ray":
            options["actor_options"] = {"num_cpus": 0.1}

        pool = SimpleWorker.options(**options).init(multiplier=2)

        # Make 9 calls
        futures = [pool.compute(i) for i in range(9)]
        results = [f.result() for f in futures]

        # Results should be correct
        assert results == [i * 2 for i in range(9)]

        # Check load balancer stats
        stats = pool.get_pool_stats()
        assert stats["load_balancer"]["algorithm"] == "RoundRobin"
        assert stats["load_balancer"]["total_dispatched"] == 9

        pool.stop()

    def test_pool_blocking_mode(self, pool_mode):
        """Test pool in blocking mode.

        This test:
        1. Creates a pool with 3 workers in blocking mode
        2. Calls compute method
        3. Verifies result is returned directly as int (not Future)
        4. Verifies result value is correct (15)
        5. Stops the pool
        """
        options = {"mode": pool_mode, "max_workers": 3, "blocking": True}
        if pool_mode == "ray":
            options["actor_options"] = {"num_cpus": 0.1}

        pool = SimpleWorker.options(**options).init(multiplier=3)

        # Should return result directly, not future
        result = pool.compute(5)
        assert isinstance(result, int)
        assert result == 15

        pool.stop()

    def test_pool_stops_all_workers(self, pool_mode):
        """Test that stopping pool stops all workers.

        This test:
        1. Creates a pool with 3 workers
        2. Makes a compute call and waits for result
        3. Stops the pool
        4. Verifies pool stats show stopped=True
        5. Attempts to make another call and verifies it raises RuntimeError
        """
        options = {"mode": pool_mode, "max_workers": 3}
        if pool_mode == "ray":
            options["actor_options"] = {"num_cpus": 0.1}

        pool = SimpleWorker.options(**options).init(multiplier=2)

        # Make some calls
        future = pool.compute(5)
        future.result()

        # Stop the pool
        pool.stop()

        # Pool should be stopped
        stats = pool.get_pool_stats()
        assert stats["stopped"] is True

        # Further calls should raise error
        with pytest.raises(RuntimeError, match="Worker pool is stopped"):
            pool.compute(10)


class TestWorkerPoolLoadBalancing:
    """Tests for load balancing in worker pools."""

    def test_least_active_load_balancing(self, pool_mode):
        """Test least active load balancing.

        This test:
        1. Creates a pool with 3 workers and least-active load balancing
        2. Submits 5 tasks (routed to workers with fewest active tasks)
        3. Waits for all results and verifies they're correct
        4. Checks load balancer stats (algorithm=LeastActiveLoad)
        5. Stops the pool
        """
        options = {"mode": pool_mode, "max_workers": 3, "load_balancing": "active"}
        if pool_mode == "ray":
            options["actor_options"] = {"num_cpus": 0.1}

        pool = SimpleWorker.options(**options).init(multiplier=2)

        # Submit tasks
        futures = [pool.compute(i) for i in range(5)]
        results = [f.result() for f in futures]

        assert results == [i * 2 for i in range(5)]

        stats = pool.get_pool_stats()
        assert stats["load_balancer"]["algorithm"] == "LeastActiveLoad"

        pool.stop()

    def test_least_total_load_balancing(self, pool_mode):
        """Test least total load balancing.

        This test:
        1. Creates a pool with 3 workers and least-total load balancing
        2. Submits 5 tasks (routed to workers with fewest total completed tasks)
        3. Waits for all results and verifies they're correct
        4. Checks load balancer stats (algorithm=LeastTotalLoad)
        5. Stops the pool
        """
        options = {"mode": pool_mode, "max_workers": 3, "load_balancing": "total"}
        if pool_mode == "ray":
            options["actor_options"] = {"num_cpus": 0.1}

        pool = SimpleWorker.options(**options).init(multiplier=2)

        # Submit tasks
        futures = [pool.compute(i) for i in range(5)]
        results = [f.result() for f in futures]

        assert results == [i * 2 for i in range(5)]

        stats = pool.get_pool_stats()
        assert stats["load_balancer"]["algorithm"] == "LeastTotalLoad"

        pool.stop()

    def test_random_load_balancing(self, pool_mode):
        """Test random load balancing.

        This test:
        1. Creates a pool with 3 workers and random load balancing
        2. Submits 10 tasks (randomly distributed across workers)
        3. Waits for all results and verifies they're correct
        4. Checks load balancer stats (algorithm=Random)
        5. Stops the pool
        """
        options = {"mode": pool_mode, "max_workers": 3, "load_balancing": "random"}
        if pool_mode == "ray":
            options["actor_options"] = {"num_cpus": 0.1}

        pool = SimpleWorker.options(**options).init(multiplier=2)

        # Submit tasks
        futures = [pool.compute(i) for i in range(10)]
        results = [f.result() for f in futures]

        assert results == [i * 2 for i in range(10)]

        stats = pool.get_pool_stats()
        assert stats["load_balancer"]["algorithm"] == "Random"

        pool.stop()


class TestSharedLimitState:
    """Tests for shared limit state across pool workers.

    This tests the fix for the bug where MultiprocessSharedLimitSet and
    RaySharedLimitSet were not sharing state across workers in a pool.
    """

    def test_shared_limits_enforced_across_pool(self, pool_mode):
        """Test that limits are shared across all workers in the pool.

        This test verifies the fix for a critical bug where limits were NOT shared.

        This test:
        1. Creates a Counter worker with limits.acquire() in increment method
        2. Creates a pool with 4 workers and CallLimit of 20 calls/sec
        3. Submits 100 calls (should take ~5 seconds with shared limit)
        4. Measures elapsed time
        5. Verifies time > 3.5s (with bug, would be ~1s as each worker had its own limit)
        6. Stops the pool

        Background: With the bug, each worker had a separate 20-capacity limit (80 total).
        With the fix, all workers share a single 20-capacity limit.
        """

        # Initialize ray if needed
        # Ray is initialized by conftest.py initialize_ray fixture
        class Counter(Worker):
            def __init__(self, count: int = 0):
                self.count = count

            def increment(self, count: int):
                with self.limits.acquire():
                    self.count += count
                    return self.count

        # Create pool with 4 workers and a CallLimit of 20 calls/sec
        # For Ray, use fractional CPUs to avoid resource exhaustion
        options = {
            "mode": pool_mode,
            "max_workers": 4,
            "limits": [CallLimit(window_seconds=1.0, capacity=20)],
        }
        if pool_mode == "ray":
            options["actor_options"] = {"num_cpus": 0.1}

        pool = Counter.options(**options).init(0)

        # Make 100 calls
        # With shared limits: 100 calls / 20 capacity = 5 windows * 1 sec = ~5 seconds
        # With per-worker limits (BUG): 100 calls / (4*20) capacity = ~1.25 windows = ~1.25 seconds
        start_time = time.time()

        futures = [pool.increment(1) for _ in range(100)]
        for f in futures:
            f.result()

        elapsed = time.time() - start_time

        # Should take at least 4 seconds (allowing some tolerance)
        # With the bug, it would take ~1 second
        assert elapsed > 3.5, f"Expected >3.5s but took {elapsed:.2f}s - limits may not be shared!"

        pool.stop()

    def test_single_worker_with_limits_baseline(self, pool_mode):
        """Baseline test: single worker with limits should enforce limit correctly.

        This test:
        1. Creates a Counter worker with limits.acquire() in increment method
        2. Creates a single worker with CallLimit of 20 calls/sec
        3. Submits 100 calls (should take ~5 seconds)
        4. Measures elapsed time
        5. Verifies time > 3.5s (confirms limit is enforced)
        6. Stops the worker

        This serves as a baseline to verify that limits work correctly for single workers.
        """

        # Initialize ray if needed
        # Ray is initialized by conftest.py initialize_ray fixture
        class Counter(Worker):
            def __init__(self, count: int = 0):
                self.count = count

            def increment(self, count: int):
                with self.limits.acquire():
                    self.count += count
                    return self.count

        # Single worker with 20 calls/sec limit
        # For Ray, use fractional CPUs to avoid resource exhaustion
        options = {
            "mode": pool_mode,
            "max_workers": 1,
            "limits": [CallLimit(window_seconds=1.0, capacity=20)],
        }
        if pool_mode == "ray":
            options["actor_options"] = {"num_cpus": 0.1}

        worker = Counter.options(**options).init(0)

        # Make 100 calls - should take ~5 seconds
        start_time = time.time()

        futures = [worker.increment(1) for _ in range(100)]
        for f in futures:
            f.result()

        elapsed = time.time() - start_time

        # Should take at least 4 seconds
        assert elapsed > 3.5, f"Expected >3.5s but took {elapsed:.2f}s"

        worker.stop()


class TestOnDemandWorkerPool:
    """Tests for on-demand worker pools."""

    def test_on_demand_creates_and_destroys_workers(self, pool_mode):
        """Test that on-demand mode creates and destroys workers.

        This test:
        1. Creates an on-demand pool (no persistent workers)
        2. Checks initial stats (on_demand_active=0, no workers yet)
        3. Makes a compute call (creates a temporary worker)
        4. Waits for result and verifies it's correct (10)
        5. Waits briefly for worker cleanup
        6. Checks final stats (on_demand_active=0, worker cleaned up)
        7. Stops the pool
        """
        pool = SimpleWorker.options(mode=pool_mode, on_demand=True).init(multiplier=2)

        stats_before = pool.get_pool_stats()
        assert stats_before["on_demand_active"] == 0

        # Make a call (should create worker)
        future = pool.compute(5)

        # Worker should be created (might still be active or cleaned up quickly)
        # Just verify the result is correct
        result = future.result()
        assert result == 10

        # After result is retrieved, worker should eventually be cleaned up
        time.sleep(0.2)
        stats_after = pool.get_pool_stats()
        # Worker should be cleaned up by now
        assert stats_after["on_demand_active"] == 0

        pool.stop()

    def test_on_demand_uses_random_load_balancing_by_default(self, pool_mode):
        """Test that on-demand mode defaults to random load balancing.

        This test:
        1. Creates an on-demand pool (no load_balancing specified)
        2. Checks pool stats
        3. Verifies load_balancer algorithm defaults to Random
        4. Stops the pool
        """
        pool = SimpleWorker.options(mode=pool_mode, on_demand=True).init(multiplier=2)

        stats = pool.get_pool_stats()
        assert stats["load_balancer"]["algorithm"] == "Random"

        pool.stop()
