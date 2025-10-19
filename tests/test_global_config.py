"""Tests for global configuration system."""

import pytest

from concurry import Worker, global_config, temp_config
from concurry.core.constants import ExecutionMode, LoadBalancingAlgorithm


class SimpleWorker(Worker):
    """Simple worker for testing global_config."""

    def __init__(self, value: int = 0):
        self.value = value

    def process(self, x: int) -> int:
        """Process a value."""
        return self.value + x


class TestGlobalConfig:
    """Test global configuration system."""

    def test_config_is_mutable(self):
        """Test that global_config can be mutated."""
        # Get initial value
        initial_thread_max = global_config.thread.max_queued_tasks

        # Modify global_config
        global_config.thread.max_queued_tasks = 500

        # Verify change
        assert global_config.thread.max_queued_tasks == 500

        # Reset to original
        global_config.thread.max_queued_tasks = initial_thread_max

    def test_config_provides_defaults(self):
        """Test that global_config provides defaults for worker creation."""
        # Thread mode should use global_config defaults
        worker = SimpleWorker.options(mode="thread").init(value=10)
        assert worker.max_queued_tasks == global_config.thread.max_queued_tasks
        worker.stop()

        # Ray mode should use global_config defaults
        worker = SimpleWorker.options(mode="ray").init(value=10)
        assert worker.max_queued_tasks == global_config.ray.max_queued_tasks
        worker.stop()

    def test_config_defaults_can_be_overridden(self):
        """Test that explicitly passed values override global_config defaults."""
        worker = SimpleWorker.options(mode="thread", max_queued_tasks=999).init(value=10)
        assert worker.max_queued_tasks == 999
        worker.stop()

    def test_config_reset_to_defaults(self):
        """Test resetting global_config to defaults."""
        # Modify global_config
        global_config.thread.max_queued_tasks = 999
        global_config.ray.max_queued_tasks = 888

        # Reset
        global_config.reset_to_defaults()

        # Verify reset
        assert global_config.thread.max_queued_tasks == 1000
        assert global_config.ray.max_queued_tasks == 3

    def test_config_per_mode_defaults(self):
        """Test that each mode has correct defaults."""
        # Sync
        assert global_config.sync.max_workers == 1
        assert global_config.sync.max_queued_tasks is None

        # Asyncio
        assert global_config.asyncio.max_workers == 1
        assert global_config.asyncio.max_queued_tasks is None

        # Thread
        assert global_config.thread.max_workers == 30
        assert global_config.thread.max_queued_tasks == 1000

        # Process
        assert global_config.process.max_workers == 4
        assert global_config.process.max_queued_tasks == 100

        # Ray
        assert global_config.ray.max_workers == 0
        assert global_config.ray.max_queued_tasks == 3

    def test_config_get_defaults_method(self):
        """Test get_defaults() method."""
        thread_defaults = global_config.get_defaults(ExecutionMode.Threads)
        assert thread_defaults.max_workers == 30
        assert thread_defaults.max_queued_tasks == 1000
        assert thread_defaults.load_balancing == LoadBalancingAlgorithm.RoundRobin
        assert thread_defaults.load_balancing_on_demand == LoadBalancingAlgorithm.Random

    def test_config_load_balancing_defaults(self):
        """Test load balancing defaults from global_config."""
        # Regular pool uses round-robin by default
        worker = SimpleWorker.options(mode="thread").init(value=10)
        # Can't easily test this without exposing internal state, but we can verify creation works
        worker.stop()

        # On-demand pool uses random by default
        pool = SimpleWorker.options(mode="thread", on_demand=True).init(value=10)
        pool.stop()

    def test_custom_config_persists_across_workers(self):
        """Test that custom global_config persists across multiple worker creations."""
        # Set custom value
        global_config.thread.max_queued_tasks = 777

        # Create multiple workers
        w1 = SimpleWorker.options(mode="thread").init(value=1)
        w2 = SimpleWorker.options(mode="thread").init(value=2)
        w3 = SimpleWorker.options(mode="thread").init(value=3)

        # All should use the custom default
        assert w1.max_queued_tasks == 777
        assert w2.max_queued_tasks == 777
        assert w3.max_queued_tasks == 777

        w1.stop()
        w2.stop()
        w3.stop()

        # Reset for other tests
        global_config.reset_to_defaults()


class TestTempConfig:
    """Test temporary configuration context manager."""

    def test_temp_config_basic(self):
        """Test basic temporary config override."""
        # Get initial value
        initial_value = global_config.thread.max_queued_tasks

        # Create worker with default
        worker1 = SimpleWorker.options(mode="thread").init(value=1)
        assert worker1.max_queued_tasks == initial_value
        worker1.stop()

        # Use temp_config to override
        with temp_config(thread_max_queued_tasks=50):
            worker2 = SimpleWorker.options(mode="thread").init(value=2)
            assert worker2.max_queued_tasks == 50
            worker2.stop()

        # After context, should be back to default
        worker3 = SimpleWorker.options(mode="thread").init(value=3)
        assert worker3.max_queued_tasks == initial_value
        worker3.stop()

    def test_temp_config_multiple_modes(self):
        """Test temporary config with multiple modes."""
        initial_thread = global_config.thread.max_queued_tasks
        initial_ray = global_config.ray.max_queued_tasks

        with temp_config(thread_max_queued_tasks=100, ray_max_queued_tasks=20):
            # Thread worker uses override
            worker1 = SimpleWorker.options(mode="thread").init(value=1)
            assert worker1.max_queued_tasks == 100
            worker1.stop()

            # Ray worker uses override
            worker2 = SimpleWorker.options(mode="ray").init(value=2)
            assert worker2.max_queued_tasks == 20
            worker2.stop()

        # Both should be restored
        worker3 = SimpleWorker.options(mode="thread").init(value=3)
        assert worker3.max_queued_tasks == initial_thread
        worker3.stop()

        worker4 = SimpleWorker.options(mode="ray").init(value=4)
        assert worker4.max_queued_tasks == initial_ray
        worker4.stop()

    def test_temp_config_nested(self):
        """Test nested temporary config contexts."""
        initial_value = global_config.thread.max_queued_tasks

        with temp_config(thread_max_queued_tasks=100):
            worker1 = SimpleWorker.options(mode="thread").init(value=1)
            assert worker1.max_queued_tasks == 100

            # Nested override
            with temp_config(thread_max_queued_tasks=50):
                worker2 = SimpleWorker.options(mode="thread").init(value=2)
                assert worker2.max_queued_tasks == 50
                worker2.stop()

            # Back to outer context
            worker3 = SimpleWorker.options(mode="thread").init(value=3)
            assert worker3.max_queued_tasks == 100
            worker3.stop()

            worker1.stop()

        # Back to original default
        worker4 = SimpleWorker.options(mode="thread").init(value=4)
        assert worker4.max_queued_tasks == initial_value
        worker4.stop()

    def test_temp_config_exception_handling(self):
        """Test that temp_config restores on exception."""
        initial_value = global_config.thread.max_queued_tasks

        try:
            with temp_config(thread_max_queued_tasks=77):
                worker = SimpleWorker.options(mode="thread").init(value=1)
                assert worker.max_queued_tasks == 77
                worker.stop()
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Config should be restored despite exception
        assert global_config.thread.max_queued_tasks == initial_value

    def test_temp_config_max_workers(self):
        """Test temporary config for max_workers."""
        initial_value = global_config.thread.max_workers

        with temp_config(thread_max_workers=50):
            # When creating a pool without specifying max_workers, it should use the override
            assert global_config.thread.max_workers == 50

        # Restored
        assert global_config.thread.max_workers == initial_value

    def test_temp_config_load_balancing(self):
        """Test temporary config for load balancing."""
        initial_value = global_config.thread.load_balancing

        with temp_config(thread_load_balancing=LoadBalancingAlgorithm.LeastActiveLoad):
            assert global_config.thread.load_balancing == LoadBalancingAlgorithm.LeastActiveLoad

        # Restored
        assert global_config.thread.load_balancing == initial_value

    def test_temp_config_invalid_key(self):
        """Test that invalid keys raise errors."""
        with pytest.raises(ValueError, match="Invalid mode"):
            with temp_config(invalid_key=100):
                pass

    def test_temp_config_invalid_mode(self):
        """Test that invalid modes raise errors."""
        with pytest.raises(ValueError, match="Invalid mode"):
            with temp_config(invalid_mode_max_queued_tasks=100):
                pass

    def test_temp_config_invalid_attribute(self):
        """Test that invalid attributes raise errors."""
        with pytest.raises(ValueError, match="Invalid attribute"):
            with temp_config(thread_invalid_attribute=100):
                pass

    def test_temp_config_explicit_override_wins(self):
        """Test that explicit Worker.options() values override temp_config."""
        with temp_config(thread_max_queued_tasks=50):
            # Explicit value should override temp_config
            worker = SimpleWorker.options(mode="thread", max_queued_tasks=999).init(value=1)
            assert worker.max_queued_tasks == 999
            worker.stop()
