"""Tests for load balancing algorithms."""

import pytest

from concurry.core.algorithms.load_balancing import (
    BaseLoadBalancer,
    LeastActiveLoadBalancer,
    LeastTotalLoadBalancer,
    LoadBalancer,
    RandomBalancer,
    RoundRobinBalancer,
)
from concurry.core.constants import LoadBalancingAlgorithm

# Pool mode fixture is provided by conftest.py


class TestRoundRobinBalancer:
    """Tests for round-robin load balancing."""

    def test_round_robin_selection(self):
        """Test that round-robin distributes requests evenly."""
        balancer = RoundRobinBalancer.of()

        # Should cycle through workers 0, 1, 2, 0, 1, 2, ...
        num_workers = 3
        selections = [balancer.select_worker(num_workers) for _ in range(9)]

        assert selections == [0, 1, 2, 0, 1, 2, 0, 1, 2]

    def test_round_robin_with_single_worker(self):
        """Test round-robin with single worker."""
        balancer = RoundRobinBalancer.of()

        # Should always select worker 0
        selections = [balancer.select_worker(1) for _ in range(5)]
        assert selections == [0, 0, 0, 0, 0]

    def test_round_robin_stats(self):
        """Test round-robin statistics."""
        balancer = RoundRobinBalancer.of()

        for i in range(10):
            balancer.select_worker(3)

        stats = balancer.get_stats()
        assert stats["algorithm"] == "RoundRobin"
        assert stats["offset"] == 0
        assert stats["total_dispatched"] == 10

    def test_round_robin_invalid_num_workers(self):
        """Test round-robin with invalid num_workers."""
        balancer = RoundRobinBalancer.of()

        with pytest.raises(ValueError, match="num_workers must be positive"):
            balancer.select_worker(0)

        with pytest.raises(ValueError, match="num_workers must be positive"):
            balancer.select_worker(-1)

    def test_round_robin_with_offset(self):
        """Test round-robin with different starting offsets."""
        # Balancer with offset 0
        balancer0 = RoundRobinBalancer.of(offset=0)
        # Balancer with offset 1
        balancer1 = RoundRobinBalancer.of(offset=1)
        # Balancer with offset 2
        balancer2 = RoundRobinBalancer.of(offset=2)

        num_workers = 3

        # Each balancer should start at a different position
        selections0 = [balancer0.select_worker(num_workers) for _ in range(6)]
        selections1 = [balancer1.select_worker(num_workers) for _ in range(6)]
        selections2 = [balancer2.select_worker(num_workers) for _ in range(6)]

        # Balancer 0 starts at 0: 0, 1, 2, 0, 1, 2
        assert selections0 == [0, 1, 2, 0, 1, 2]
        # Balancer 1 starts at 1: 1, 2, 0, 1, 2, 0
        assert selections1 == [1, 2, 0, 1, 2, 0]
        # Balancer 2 starts at 2: 2, 0, 1, 2, 0, 1
        assert selections2 == [2, 0, 1, 2, 0, 1]

    def test_round_robin_offset_stats(self):
        """Test that stats correctly show offset and total_dispatched."""
        balancer = RoundRobinBalancer.of(offset=5)

        # Make 10 selections
        for _ in range(10):
            balancer.select_worker(3)

        stats = balancer.get_stats()
        assert stats["algorithm"] == "RoundRobin"
        assert stats["offset"] == 5
        assert stats["total_dispatched"] == 10  # Should be calls made, not including offset

    def test_round_robin_large_offset(self):
        """Test round-robin with large offset wraps correctly."""
        balancer = RoundRobinBalancer.of(offset=100)
        num_workers = 3

        # With offset 100 and 3 workers:
        # First selection: (100) % 3 = 1
        # Second selection: (101) % 3 = 2
        # Third selection: (102) % 3 = 0
        selections = [balancer.select_worker(num_workers) for _ in range(6)]
        assert selections == [1, 2, 0, 1, 2, 0]


class TestLeastActiveLoadBalancer:
    """Tests for least active load balancing."""

    def test_least_active_selection(self):
        """Test that least active selects worker with fewest active calls."""
        balancer = LeastActiveLoadBalancer.of()

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
        balancer = LeastActiveLoadBalancer.of()

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
        balancer = LeastActiveLoadBalancer.of()

        # Recording complete on nonexistent worker should not raise error
        balancer.record_complete(0)
        stats = balancer.get_stats()
        assert len(stats["active_calls"]) == 0


class TestLeastTotalLoadBalancer:
    """Tests for least total load balancing."""

    def test_least_total_selection(self):
        """Test that least total selects worker with fewest total calls."""
        balancer = LeastTotalLoadBalancer.of()

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
        balancer = LeastTotalLoadBalancer.of()

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
        balancer = LeastTotalLoadBalancer.of()

        balancer.select_worker(2)
        balancer.record_start(0)
        balancer.record_complete(0)  # Should not affect total count

        stats = balancer.get_stats()
        assert stats["total_calls"][0] == 1


class TestRandomBalancer:
    """Tests for random load balancing."""

    def test_random_selection_in_range(self):
        """Test that random selects workers within valid range."""
        balancer = RandomBalancer.of()

        num_workers = 5
        selections = [balancer.select_worker(num_workers) for _ in range(100)]

        # All selections should be in range [0, num_workers)
        assert all(0 <= s < num_workers for s in selections)

    def test_random_selection_distribution(self):
        """Test that random eventually distributes fairly."""
        balancer = RandomBalancer.of()

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
        balancer = RandomBalancer.of()

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

        with pytest.raises(KeyError):
            LoadBalancer(FakeAlgorithm())


class TestThreadSafety:
    """Tests for thread-safety of load balancers."""

    def test_round_robin_thread_safety(self):
        """Test that round-robin is thread-safe."""
        import threading

        balancer = RoundRobinBalancer()
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

        balancer = LeastActiveLoadBalancer.of()
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
