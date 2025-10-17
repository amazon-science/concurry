"""Tests for load balancing algorithms."""

import pytest

from concurry.core.config import LoadBalancingAlgorithm
from concurry.core.worker.load_balancing import (
    BaseLoadBalancer,
    LeastActiveLoadBalancer,
    LeastTotalLoadBalancer,
    LoadBalancer,
    RandomBalancer,
    RoundRobinBalancer,
)

# Pool mode fixture is provided by conftest.py


class TestRoundRobinBalancer:
    """Tests for round-robin load balancing."""

    def test_round_robin_selection(self):
        """Test that round-robin distributes requests evenly."""
        balancer = RoundRobinBalancer(algorithm=LoadBalancingAlgorithm.RoundRobin)

        # Should cycle through workers 0, 1, 2, 0, 1, 2, ...
        num_workers = 3
        selections = [balancer.select_worker(num_workers) for _ in range(9)]

        assert selections == [0, 1, 2, 0, 1, 2, 0, 1, 2]

    def test_round_robin_with_single_worker(self):
        """Test round-robin with single worker."""
        balancer = RoundRobinBalancer(algorithm=LoadBalancingAlgorithm.RoundRobin)

        # Should always select worker 0
        selections = [balancer.select_worker(1) for _ in range(5)]
        assert selections == [0, 0, 0, 0, 0]

    def test_round_robin_stats(self):
        """Test round-robin statistics."""
        balancer = RoundRobinBalancer(algorithm=LoadBalancingAlgorithm.RoundRobin)

        for i in range(10):
            balancer.select_worker(3)

        stats = balancer.get_stats()
        assert stats["algorithm"] == "RoundRobin"
        assert stats["total_dispatched"] == 10

    def test_round_robin_invalid_num_workers(self):
        """Test round-robin with invalid num_workers."""
        balancer = RoundRobinBalancer(algorithm=LoadBalancingAlgorithm.RoundRobin)

        with pytest.raises(ValueError, match="num_workers must be positive"):
            balancer.select_worker(0)

        with pytest.raises(ValueError, match="num_workers must be positive"):
            balancer.select_worker(-1)


class TestLeastActiveLoadBalancer:
    """Tests for least active load balancing."""

    def test_least_active_selection(self):
        """Test that least active selects worker with fewest active calls."""
        balancer = LeastActiveLoadBalancer(algorithm=LoadBalancingAlgorithm.LeastActiveLoad)

        # All workers start with 0 active calls
        selected = balancer.select_worker(3)
        assert selected == 0  # First worker selected

        # Record start on worker 0
        balancer.record_start(0)

        # Next selection should go to worker 1 (has 0 active)
        selected = balancer.select_worker(3)
        assert selected == 1

        # Record start on worker 1
        balancer.record_start(1)

        # Next selection should go to worker 2 (has 0 active)
        selected = balancer.select_worker(3)
        assert selected == 2

        # Complete worker 0's call
        balancer.record_complete(0)

        # Next selection should go to worker 0 (now has 0 active again)
        selected = balancer.select_worker(3)
        assert selected == 0

    def test_least_active_stats(self):
        """Test least active statistics."""
        balancer = LeastActiveLoadBalancer(algorithm=LoadBalancingAlgorithm.LeastActiveLoad)

        balancer.select_worker(3)
        balancer.record_start(0)
        balancer.record_start(1)
        balancer.record_start(1)

        stats = balancer.get_stats()
        assert stats["algorithm"] == "LeastActiveLoad"
        assert stats["active_calls"] == {0: 1, 1: 2, 2: 0}
        assert stats["total_active"] == 3

    def test_least_active_record_complete_nonexistent(self):
        """Test completing call on nonexistent worker."""
        balancer = LeastActiveLoadBalancer(algorithm=LoadBalancingAlgorithm.LeastActiveLoad)

        # Recording complete on nonexistent worker should not raise error
        balancer.record_complete(0)
        stats = balancer.get_stats()
        assert len(stats["active_calls"]) == 0


class TestLeastTotalLoadBalancer:
    """Tests for least total load balancing."""

    def test_least_total_selection(self):
        """Test that least total selects worker with fewest total calls."""
        balancer = LeastTotalLoadBalancer(algorithm=LoadBalancingAlgorithm.LeastTotalLoad)

        # All workers start with 0 total calls
        selected = balancer.select_worker(3)
        assert selected == 0  # First worker selected

        # Record start on worker 0
        balancer.record_start(0)

        # Next selection should go to worker 1 (has 0 total)
        selected = balancer.select_worker(3)
        assert selected == 1

        # Record start on worker 1
        balancer.record_start(1)

        # Next selection should go to worker 2 (has 0 total)
        selected = balancer.select_worker(3)
        assert selected == 2

        # After all workers have 1 call, should go back to worker 0
        balancer.record_start(2)
        selected = balancer.select_worker(3)
        assert selected == 0

    def test_least_total_stats(self):
        """Test least total statistics."""
        balancer = LeastTotalLoadBalancer(algorithm=LoadBalancingAlgorithm.LeastTotalLoad)

        for i in range(10):
            selected = balancer.select_worker(3)
            balancer.record_start(selected)

        stats = balancer.get_stats()
        assert stats["algorithm"] == "LeastTotalLoad"
        # With round-robin-like distribution, should be roughly even
        # First worker gets 0, 3, 6, 9 (4 calls)
        # Second gets 1, 4, 7 (3 calls)
        # Third gets 2, 5, 8 (3 calls)
        assert stats["sum_total_calls"] == 10

    def test_least_total_no_reset_on_complete(self):
        """Test that completing calls doesn't reduce total count."""
        balancer = LeastTotalLoadBalancer(algorithm=LoadBalancingAlgorithm.LeastTotalLoad)

        balancer.select_worker(2)
        balancer.record_start(0)
        balancer.record_complete(0)  # Should not affect total count

        stats = balancer.get_stats()
        assert stats["total_calls"][0] == 1


class TestRandomBalancer:
    """Tests for random load balancing."""

    def test_random_selection_in_range(self):
        """Test that random selects workers within valid range."""
        balancer = RandomBalancer(algorithm=LoadBalancingAlgorithm.Random)

        num_workers = 5
        selections = [balancer.select_worker(num_workers) for _ in range(100)]

        # All selections should be in range [0, num_workers)
        assert all(0 <= s < num_workers for s in selections)

    def test_random_selection_distribution(self):
        """Test that random eventually distributes fairly."""
        balancer = RandomBalancer(algorithm=LoadBalancingAlgorithm.Random)

        num_workers = 3
        num_selections = 300
        selections = [balancer.select_worker(num_workers) for _ in range(num_selections)]

        # Count selections per worker
        counts = [selections.count(i) for i in range(num_workers)]

        # With 300 selections and 3 workers, expect roughly 100 each
        # Allow for statistical variation (should be within 50-150 each)
        for count in counts:
            assert 50 <= count <= 150

    def test_random_stats(self):
        """Test random balancer statistics."""
        balancer = RandomBalancer(algorithm=LoadBalancingAlgorithm.Random)

        for i in range(20):
            balancer.select_worker(5)

        stats = balancer.get_stats()
        assert stats["algorithm"] == "Random"
        assert stats["total_dispatched"] == 20


class TestLoadBalancerFactory:
    """Tests for LoadBalancer factory function."""

    def test_factory_creates_correct_types(self):
        """Test that factory creates correct balancer types."""
        algorithms = [
            (LoadBalancingAlgorithm.RoundRobin, RoundRobinBalancer),
            (LoadBalancingAlgorithm.LeastActiveLoad, LeastActiveLoadBalancer),
            (LoadBalancingAlgorithm.LeastTotalLoad, LeastTotalLoadBalancer),
            (LoadBalancingAlgorithm.Random, RandomBalancer),
        ]

        for algorithm, expected_cls in algorithms:
            balancer = LoadBalancer(algorithm)
            assert isinstance(balancer, expected_cls)
            assert isinstance(balancer, BaseLoadBalancer)

    def test_factory_invalid_algorithm(self):
        """Test factory with invalid algorithm."""

        # Create a fake algorithm value
        class FakeAlgorithm:
            value = "fake_algorithm"

        with pytest.raises(ValueError, match="Unknown load balancing algorithm"):
            LoadBalancer(FakeAlgorithm())


class TestThreadSafety:
    """Tests for thread-safety of load balancers."""

    def test_round_robin_thread_safety(self):
        """Test that round-robin is thread-safe."""
        import threading

        balancer = RoundRobinBalancer(algorithm=LoadBalancingAlgorithm.RoundRobin)
        num_threads = 10
        num_selections_per_thread = 100
        num_workers = 5

        results = []
        lock = threading.Lock()

        def select_workers():
            local_results = []
            for _ in range(num_selections_per_thread):
                selected = balancer.select_worker(num_workers)
                local_results.append(selected)

            with lock:
                results.extend(local_results)

        threads = [threading.Thread(target=select_workers) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All selections should be valid
        assert all(0 <= s < num_workers for s in results)
        # Total number of selections should be correct
        assert len(results) == num_threads * num_selections_per_thread

    def test_least_active_thread_safety(self):
        """Test that least active is thread-safe."""
        import threading

        balancer = LeastActiveLoadBalancer(algorithm=LoadBalancingAlgorithm.LeastActiveLoad)
        num_threads = 10
        num_workers = 5

        def worker_operations():
            for _ in range(50):
                selected = balancer.select_worker(num_workers)
                balancer.record_start(selected)
                balancer.record_complete(selected)

        threads = [threading.Thread(target=worker_operations) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        stats = balancer.get_stats()
        # All active calls should be completed (back to 0)
        assert stats["total_active"] == 0


class TestLoadBalancingIntegration:
    """Integration tests for load balancing with worker pools across execution modes."""

    @pytest.mark.parametrize("algorithm", ["round_robin", "active", "total", "random"])
    def test_load_balancing_with_worker_pools(self, pool_mode, algorithm):
        """Test that all load balancing algorithms work with worker pools across modes."""
        from concurry import Worker

        # Ray is initialized by conftest.py initialize_ray fixture
        class SimpleWorker(Worker):
            def __init__(self):
                self.call_count = 0

            def compute(self, x: int) -> int:
                self.call_count += 1
                return x * 2

        # Create pool with specified load balancing algorithm
        # For Ray, use fractional CPUs to avoid resource exhaustion
        options = {"mode": pool_mode, "max_workers": 3, "load_balancing": algorithm}
        if pool_mode == "ray":
            options["actor_options"] = {"num_cpus": 0.1}

        pool = SimpleWorker.options(**options).init()

        # Make some calls
        futures = [pool.compute(i) for i in range(10)]
        results = [f.result() for f in futures]

        # Verify results are correct
        assert results == [i * 2 for i in range(10)]

        # Verify pool stats show correct algorithm
        stats = pool.get_pool_stats()
        algorithm_names = {
            "round_robin": "RoundRobin",
            "active": "LeastActiveLoad",
            "total": "LeastTotalLoad",
            "random": "Random",
        }
        assert stats["load_balancer"]["algorithm"] == algorithm_names[algorithm]
        assert stats["load_balancer"]["total_dispatched"] == 10

        pool.stop()
