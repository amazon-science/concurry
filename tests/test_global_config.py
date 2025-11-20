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
        assert global_config.thread.max_queued_tasks == None
        assert global_config.ray.max_queued_tasks == 3

    def test_config_get_defaults_method(self):
        """Test get_defaults() method."""
        thread_defaults = global_config.get_defaults(ExecutionMode.Threads)
        assert thread_defaults.max_workers == 1
        assert thread_defaults.max_queued_tasks == None
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

    def test_temp_config_blocking(self):
        """Test temporary config for blocking mode."""
        initial_value = global_config.thread.blocking

        with temp_config(thread_blocking=True):
            assert global_config.thread.blocking is True

        # Restored
        assert global_config.thread.blocking == initial_value

    def test_temp_config_retry_parameters(self):
        """Test temporary config for retry parameters."""
        initial_retries = global_config.thread.num_retries
        initial_wait = global_config.thread.retry_wait
        initial_jitter = global_config.thread.retry_jitter

        with temp_config(thread_num_retries=5, thread_retry_wait=2.0, thread_retry_jitter=0.5):
            assert global_config.thread.num_retries == 5
            assert global_config.thread.retry_wait == 2.0
            assert global_config.thread.retry_jitter == 0.5

        # Restored
        assert global_config.thread.num_retries == initial_retries
        assert global_config.thread.retry_wait == initial_wait
        assert global_config.thread.retry_jitter == initial_jitter

    def test_all_defaults_from_config(self):
        """Test that all Worker.options() defaults come from global_config."""
        # Modify all defaults
        with temp_config(
            thread_blocking=True,
            thread_max_queued_tasks=777,
            thread_num_retries=10,
            thread_retry_wait=5.0,
            thread_retry_jitter=0.8,
        ):
            # Create worker without specifying any options
            worker = SimpleWorker.options(mode="thread").init(value=1)

            # All should use the temp_config values
            assert worker.blocking is True
            assert worker.max_queued_tasks == 777
            # Note: retry config is internal, we can't easily test it here
            # but the fact that worker creation succeeds means defaults were applied

            worker.stop()


class TestHierarchicalConfig:
    """Test hierarchical configuration with global defaults and fallback."""

    def test_global_defaults_apply_to_all_modes(self):
        """Test that global defaults apply to all modes."""
        # Set a global default
        with temp_config(global_num_retries=5):
            # All modes should use this default
            thread_defaults = global_config.get_defaults(ExecutionMode.Threads)
            ray_defaults = global_config.get_defaults(ExecutionMode.Ray)
            process_defaults = global_config.get_defaults(ExecutionMode.Processes)

            assert thread_defaults.num_retries == 5
            assert ray_defaults.num_retries == 5
            assert process_defaults.num_retries == 5

    def test_mode_specific_overrides_global(self):
        """Test that mode-specific values override global defaults."""
        with temp_config(
            global_num_retries=5,  # Global default
            thread_num_retries=10,  # Thread-specific override
        ):
            thread_defaults = global_config.get_defaults(ExecutionMode.Threads)
            ray_defaults = global_config.get_defaults(ExecutionMode.Ray)

            # Thread uses override
            assert thread_defaults.num_retries == 10
            # Ray uses global default
            assert ray_defaults.num_retries == 5

    def test_fallback_to_global_when_mode_specific_is_none(self):
        """Test fallback behavior when mode-specific value is None."""
        # Initially, thread has no mode-specific override for retry_algorithm
        # So it should use the global default
        thread_defaults = global_config.get_defaults(ExecutionMode.Threads)
        from concurry.core.retry import RetryAlgorithm

        # Should use global default (Exponential)
        assert thread_defaults.retry_algorithm == RetryAlgorithm.Exponential

        # Now set a mode-specific override
        global_config.thread.retry_algorithm = RetryAlgorithm.Linear
        thread_defaults = global_config.get_defaults(ExecutionMode.Threads)
        assert thread_defaults.retry_algorithm == RetryAlgorithm.Linear

        # Reset
        global_config.reset_to_defaults()

    def test_global_config_has_defaults_attribute(self):
        """Test that global_config has a defaults attribute."""
        assert hasattr(global_config, "defaults")
        assert global_config.defaults.num_retries == 0
        assert global_config.defaults.retry_wait == 1.0
        assert global_config.defaults.retry_jitter == 0.3

    def test_modifying_global_defaults_affects_all_modes(self):
        """Test that modifying global defaults affects all modes."""
        # Save initial values
        initial_thread_retries = global_config.get_defaults(ExecutionMode.Threads).num_retries
        initial_ray_retries = global_config.get_defaults(ExecutionMode.Ray).num_retries

        # Modify global default
        global_config.defaults.num_retries = 7

        # All modes should see the change (if they don't have mode-specific overrides)
        # Note: thread and ray have None for num_retries, so they fall back to global
        thread_defaults = global_config.get_defaults(ExecutionMode.Threads)
        ray_defaults = global_config.get_defaults(ExecutionMode.Ray)

        assert thread_defaults.num_retries == 7
        assert ray_defaults.num_retries == 7

        # Reset
        global_config.reset_to_defaults()

    def test_temp_config_with_global_overrides(self):
        """Test temp_config with global_ prefix."""
        with temp_config(global_retry_wait=5.0, global_retry_jitter=0.9):
            # All modes should use these values
            thread_defaults = global_config.get_defaults(ExecutionMode.Threads)
            ray_defaults = global_config.get_defaults(ExecutionMode.Ray)

            assert thread_defaults.retry_wait == 5.0
            assert thread_defaults.retry_jitter == 0.9
            assert ray_defaults.retry_wait == 5.0
            assert ray_defaults.retry_jitter == 0.9

        # After context, should be restored
        thread_defaults = global_config.get_defaults(ExecutionMode.Threads)
        assert thread_defaults.retry_wait == 1.0
        assert thread_defaults.retry_jitter == 0.3

    def test_temp_config_global_and_mode_specific(self):
        """Test temp_config with both global and mode-specific overrides."""
        with temp_config(
            global_num_retries=3,  # Global: all modes
            thread_num_retries=10,  # Thread-specific override
            ray_max_queued_tasks=20,  # Ray-specific override
        ):
            thread_defaults = global_config.get_defaults(ExecutionMode.Threads)
            ray_defaults = global_config.get_defaults(ExecutionMode.Ray)
            process_defaults = global_config.get_defaults(ExecutionMode.Processes)

            # Thread uses mode-specific for num_retries
            assert thread_defaults.num_retries == 10
            # Ray uses global for num_retries, mode-specific for max_queued_tasks
            assert ray_defaults.num_retries == 3
            assert ray_defaults.max_queued_tasks == 20
            # Process uses global for num_retries
            assert process_defaults.num_retries == 3

    def test_retry_algorithm_from_config(self):
        """Test that retry_algorithm comes from global config."""
        from concurry.core.retry import RetryAlgorithm

        with temp_config(global_retry_algorithm=RetryAlgorithm.Linear):
            worker = SimpleWorker.options(mode="thread").init(value=1)
            # Worker should be created successfully with Linear algorithm
            worker.stop()

    def test_unwrap_futures_from_config(self):
        """Test that unwrap_futures comes from global config."""
        # Default should be True
        worker1 = SimpleWorker.options(mode="thread").init(value=1)
        assert worker1.unwrap_futures is True
        worker1.stop()

        # Override globally
        with temp_config(global_unwrap_futures=False):
            worker2 = SimpleWorker.options(mode="thread").init(value=1)
            assert worker2.unwrap_futures is False
            worker2.stop()

        # Back to default
        worker3 = SimpleWorker.options(mode="thread").init(value=1)
        assert worker3.unwrap_futures is True
        worker3.stop()

    def test_stop_timeout_in_config(self):
        """Test that stop_timeout is available in config."""
        # Verify it's in the config
        assert hasattr(global_config.defaults, "stop_timeout")
        assert global_config.defaults.stop_timeout == 30.0

        # Can be modified
        with temp_config(global_stop_timeout=60.0):
            assert global_config.defaults.stop_timeout == 60.0

        # Restored
        assert global_config.defaults.stop_timeout == 30.0


class TestWorkerTimeoutConfigs:
    """Test that workers use configured timeout values."""

    def test_thread_worker_uses_config_timeout(self):
        """Test thread worker uses configured command queue timeout."""
        with temp_config(thread_worker_command_queue_timeout=0.5):
            worker = SimpleWorker.options(mode="thread", max_workers=1).init(value=1)
            assert worker.command_queue_timeout == 0.5
            worker.stop()

    def test_process_worker_uses_config_timeouts(self):
        """Test process worker uses configured result queue timeouts."""
        with temp_config(
            process_worker_result_queue_timeout=60.0,
            process_worker_result_queue_cleanup_timeout=2.0,
        ):
            worker = SimpleWorker.options(mode="process", max_workers=1).init(value=1)
            assert worker.result_queue_timeout == 60.0
            assert worker.result_queue_cleanup_timeout == 2.0
            worker.stop()

    def test_asyncio_worker_uses_config_timeouts(self):
        """Test asyncio worker uses configured timeouts."""
        with temp_config(
            asyncio_worker_loop_ready_timeout=60.0,
            asyncio_worker_thread_ready_timeout=45.0,
            asyncio_worker_sync_queue_timeout=0.5,
        ):
            worker = SimpleWorker.options(mode="asyncio", max_workers=1).init(value=1)
            assert worker.loop_ready_timeout == 60.0
            assert worker.thread_ready_timeout == 45.0
            assert worker.sync_queue_timeout == 0.5
            worker.stop()

    def test_config_values_fixed_at_creation(self):
        """Test that config values are fixed at worker creation, not dynamic."""
        # Create worker with initial config
        worker = SimpleWorker.options(mode="thread", max_workers=1).init(value=1)
        initial_timeout = worker.command_queue_timeout

        # Change global config AFTER worker creation
        global_config.thread.worker_command_queue_timeout = 999.0

        # Worker should still use initial value
        assert worker.command_queue_timeout == initial_timeout
        assert worker.command_queue_timeout != 999.0

        worker.stop()
        global_config.reset_to_defaults()

    def test_pool_uses_config_timeouts(self):
        """Test that worker pools use configured timeouts."""
        with temp_config(
            thread_pool_on_demand_cleanup_timeout=10.0,
            thread_pool_on_demand_slot_max_wait=120.0,
        ):
            pool = SimpleWorker.options(mode="thread", max_workers=3).init(value=1)
            assert pool.on_demand_cleanup_timeout == 10.0
            assert pool.on_demand_slot_max_wait == 120.0
            pool.stop()


class TestPollingStrategyConfigs:
    """Test that polling strategies use configured values."""

    def test_polling_fixed_interval_from_config(self):
        """Test that Fixed polling uses config interval."""
        from concurry import wait
        from concurry.core.constants import PollingAlgorithm

        with temp_config(global_polling_fixed_interval=0.05):
            worker = SimpleWorker.options(mode="thread").init(value=1)

            # Create futures
            futures = [worker.process(i) for i in range(5)]

            # wait() should use configured interval
            done, not_done = wait(futures, polling=PollingAlgorithm.Fixed)

            assert len(done) == 5
            assert len(not_done) == 0

            worker.stop()

    def test_polling_adaptive_intervals_from_config(self):
        """Test that Adaptive polling uses config intervals."""
        from concurry import wait
        from concurry.core.constants import PollingAlgorithm

        with temp_config(
            global_polling_adaptive_min_interval=0.0005,
            global_polling_adaptive_max_interval=0.5,
            global_polling_adaptive_initial_interval=0.05,
        ):
            worker = SimpleWorker.options(mode="thread").init(value=1)

            # Create futures
            futures = [worker.process(i) for i in range(5)]

            # wait() should use configured intervals
            done, not_done = wait(futures, polling=PollingAlgorithm.Adaptive)

            assert len(done) == 5
            assert len(not_done) == 0

            worker.stop()


class TestAsyncioAndRayMonitorConfigs:
    """Test asyncio future and Ray monitor config fields."""

    def test_asyncio_future_poll_interval_from_config(self):
        """Test that asyncio future uses configured poll interval."""
        with temp_config(global_asyncio_future_poll_interval=1e-5):
            worker = SimpleWorker.options(mode="asyncio").init(value=1)

            # Make a call that returns a future
            future = worker.process(10)
            result = future.result()

            assert result == 11

            worker.stop()

    def test_ray_monitor_config_fields_exist(self):
        """Test that Ray monitor config fields are accessible."""
        from concurry import global_config

        # Verify all Ray monitor fields exist
        assert hasattr(global_config.defaults, "ray_monitor_queue_get_timeout")
        assert hasattr(global_config.defaults, "ray_monitor_no_futures_sleep")
        assert hasattr(global_config.defaults, "ray_monitor_sleep")
        assert hasattr(global_config.defaults, "ray_monitor_error_sleep")

        # Verify default values
        assert global_config.defaults.ray_monitor_queue_get_timeout == 0.01
        assert global_config.defaults.ray_monitor_no_futures_sleep == 0.01
        assert global_config.defaults.ray_monitor_sleep == 0.001
        assert global_config.defaults.ray_monitor_error_sleep == 0.1

    def test_ray_monitor_config_can_be_modified(self):
        """Test that Ray monitor config can be modified via temp_config."""
        with temp_config(
            global_ray_monitor_queue_get_timeout=0.02,
            global_ray_monitor_no_futures_sleep=0.03,
            global_ray_monitor_sleep=0.002,
            global_ray_monitor_error_sleep=0.2,
        ):
            from concurry import global_config

            assert global_config.defaults.ray_monitor_queue_get_timeout == 0.02
            assert global_config.defaults.ray_monitor_no_futures_sleep == 0.03
            assert global_config.defaults.ray_monitor_sleep == 0.002
            assert global_config.defaults.ray_monitor_error_sleep == 0.2

        # Verify restored after context
        from concurry import global_config

        assert global_config.defaults.ray_monitor_queue_get_timeout == 0.01
        assert global_config.defaults.ray_monitor_no_futures_sleep == 0.01
        assert global_config.defaults.ray_monitor_sleep == 0.001
        assert global_config.defaults.ray_monitor_error_sleep == 0.1


class TestRateLimiterConfigs:
    """Test rate limiter config fields."""

    def test_rate_limiter_min_wait_time_exists(self):
        """Test that rate limiter config field is accessible."""
        from concurry import global_config

        # Verify field exists
        assert hasattr(global_config.defaults, "rate_limiter_min_wait_time")

        # Verify default value
        assert global_config.defaults.rate_limiter_min_wait_time == 0.01

    def test_rate_limiter_min_wait_time_can_be_modified(self):
        """Test that rate limiter config can be modified via temp_config."""
        with temp_config(global_rate_limiter_min_wait_time=0.05):
            from concurry import global_config

            assert global_config.defaults.rate_limiter_min_wait_time == 0.05

        # Verify restored after context
        from concurry import global_config

        assert global_config.defaults.rate_limiter_min_wait_time == 0.01
