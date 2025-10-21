"""Tests for polling algorithms."""

import pytest

from concurry import global_config
from concurry.core.algorithms import Poller
from concurry.core.constants import PollingAlgorithm


class TestFixedPollingStrategy:
    """Tests for FixedPollingStrategy."""

    def test_initialization_defaults(self):
        """Test default initialization."""
        defaults = global_config.defaults
        strategy = Poller(PollingAlgorithm.Fixed, interval=defaults.polling_fixed_interval)
        assert strategy.interval == defaults.polling_fixed_interval

    def test_initialization_custom(self):
        """Test custom initialization."""
        strategy = Poller(PollingAlgorithm.Fixed, interval=0.05)
        assert strategy.interval == 0.05

    def test_get_next_interval(self):
        """Test get_next_interval returns fixed value."""
        strategy = Poller(PollingAlgorithm.Fixed, interval=0.02)
        assert strategy.get_next_interval() == 0.02
        assert strategy.get_next_interval() == 0.02  # Always same

    def test_record_completion_no_change(self):
        """Test that completion doesn't change interval."""
        strategy = Poller(PollingAlgorithm.Fixed, interval=0.03)
        initial = strategy.get_next_interval()
        strategy.record_completion()
        assert strategy.get_next_interval() == initial

    def test_record_no_completion_no_change(self):
        """Test that no completion doesn't change interval."""
        strategy = Poller(PollingAlgorithm.Fixed, interval=0.03)
        initial = strategy.get_next_interval()
        strategy.record_no_completion()
        assert strategy.get_next_interval() == initial

    def test_reset_no_effect(self):
        """Test that reset has no effect on fixed strategy."""
        strategy = Poller(PollingAlgorithm.Fixed, interval=0.04)
        initial = strategy.get_next_interval()
        strategy.reset()
        assert strategy.get_next_interval() == initial


class TestAdaptivePollingStrategy:
    """Tests for AdaptivePollingStrategy."""

    def test_initialization_defaults(self):
        """Test default initialization."""
        defaults = global_config.defaults
        strategy = Poller(
            PollingAlgorithm.Adaptive,
            min_interval=defaults.polling_adaptive_min_interval,
            max_interval=defaults.polling_adaptive_max_interval,
            current_interval=defaults.polling_adaptive_initial_interval,
        )
        assert strategy.min_interval == defaults.polling_adaptive_min_interval
        assert strategy.max_interval == defaults.polling_adaptive_max_interval
        assert strategy.current_interval == defaults.polling_adaptive_initial_interval
        assert strategy.speedup_factor == 0.7
        assert strategy.slowdown_factor == 1.3
        assert strategy.consecutive_empty == 0

    def test_record_completion_speeds_up(self):
        """Test that completion speeds up polling."""
        strategy = Poller(
            PollingAlgorithm.Adaptive, min_interval=0.001, max_interval=0.1, current_interval=0.01
        )
        initial = strategy.get_next_interval()
        strategy.record_completion()
        new = strategy.get_next_interval()
        assert new < initial
        assert strategy.consecutive_empty == 0

    def test_record_completion_respects_min(self):
        """Test that speedup respects minimum interval."""
        strategy = Poller(
            PollingAlgorithm.Adaptive, min_interval=0.005, max_interval=0.1, current_interval=0.006
        )
        for _ in range(10):
            strategy.record_completion()
        assert strategy.get_next_interval() >= strategy.min_interval

    def test_record_no_completion_slows_down(self):
        """Test that no completion slows down polling after 3 empty checks."""
        strategy = Poller(
            PollingAlgorithm.Adaptive, min_interval=0.001, max_interval=0.1, current_interval=0.01
        )
        initial = strategy.get_next_interval()

        # First two empty checks - no change
        strategy.record_no_completion()
        assert strategy.get_next_interval() == initial
        strategy.record_no_completion()
        assert strategy.get_next_interval() == initial

        # Third empty check - should slow down
        strategy.record_no_completion()
        new = strategy.get_next_interval()
        assert new > initial

    def test_record_no_completion_respects_max(self):
        """Test that slowdown respects maximum interval."""
        strategy = Poller(
            PollingAlgorithm.Adaptive, min_interval=0.001, max_interval=0.05, current_interval=0.04
        )
        for _ in range(20):  # Many empty checks
            strategy.record_no_completion()
        assert strategy.get_next_interval() <= strategy.max_interval

    def test_completion_resets_consecutive_empty(self):
        """Test that completion resets consecutive empty counter."""
        defaults = global_config.defaults
        strategy = Poller(
            PollingAlgorithm.Adaptive,
            min_interval=defaults.polling_adaptive_min_interval,
            max_interval=defaults.polling_adaptive_max_interval,
            current_interval=defaults.polling_adaptive_initial_interval,
        )
        strategy.record_no_completion()
        strategy.record_no_completion()
        assert strategy.consecutive_empty == 2

        strategy.record_completion()
        assert strategy.consecutive_empty == 0

    def test_reset(self):
        """Test reset returns to initial state."""
        defaults = global_config.defaults
        strategy = Poller(
            PollingAlgorithm.Adaptive,
            min_interval=defaults.polling_adaptive_min_interval,
            max_interval=defaults.polling_adaptive_max_interval,
            current_interval=defaults.polling_adaptive_initial_interval,
        )
        # Modify state
        for _ in range(5):
            strategy.record_completion()
        for _ in range(5):
            strategy.record_no_completion()

        # Reset
        strategy.reset()
        assert strategy.current_interval == defaults.polling_adaptive_min_interval
        assert strategy.consecutive_empty == 0


class TestExponentialPollingStrategy:
    """Tests for ExponentialPollingStrategy."""

    def test_initialization_defaults(self):
        """Test default initialization."""
        defaults = global_config.defaults
        strategy = Poller(
            PollingAlgorithm.Exponential,
            initial_interval=defaults.polling_exponential_initial_interval,
            max_interval=defaults.polling_exponential_max_interval,
            current_interval=defaults.polling_exponential_initial_interval,
        )
        assert strategy.initial_interval == defaults.polling_exponential_initial_interval
        assert strategy.max_interval == defaults.polling_exponential_max_interval
        assert strategy.multiplier == 2.0
        assert strategy.current_interval == defaults.polling_exponential_initial_interval

    def test_record_completion_resets(self):
        """Test that completion resets to initial interval."""
        strategy = Poller(
            PollingAlgorithm.Exponential, initial_interval=0.001, max_interval=0.5, current_interval=0.001
        )
        # Increase interval first
        strategy.record_no_completion()
        strategy.record_no_completion()
        assert strategy.get_next_interval() > strategy.initial_interval

        # Completion should reset
        strategy.record_completion()
        assert strategy.get_next_interval() == strategy.initial_interval

    def test_record_no_completion_exponential_growth(self):
        """Test exponential growth of interval."""
        strategy = Poller(
            PollingAlgorithm.Exponential,
            initial_interval=0.001,
            max_interval=0.5,
            current_interval=0.001,
            multiplier=2.0,
        )
        intervals = [strategy.get_next_interval()]

        for _ in range(5):
            strategy.record_no_completion()
            intervals.append(strategy.get_next_interval())

        # Each interval should be double the previous (until max)
        for i in range(len(intervals) - 1):
            if intervals[i + 1] < strategy.max_interval:
                assert abs(intervals[i + 1] - intervals[i] * strategy.multiplier) < 1e-9

    def test_record_no_completion_respects_max(self):
        """Test that growth respects maximum interval."""
        strategy = Poller(
            PollingAlgorithm.Exponential, initial_interval=0.001, max_interval=0.1, current_interval=0.001
        )
        for _ in range(20):  # Many increases
            strategy.record_no_completion()
        assert strategy.get_next_interval() <= strategy.max_interval

    def test_reset(self):
        """Test reset returns to initial interval."""
        defaults = global_config.defaults
        strategy = Poller(
            PollingAlgorithm.Exponential,
            initial_interval=defaults.polling_exponential_initial_interval,
            max_interval=defaults.polling_exponential_max_interval,
            current_interval=defaults.polling_exponential_initial_interval,
        )
        for _ in range(5):
            strategy.record_no_completion()

        strategy.reset()
        assert strategy.current_interval == strategy.initial_interval


class TestProgressivePollingStrategy:
    """Tests for ProgressivePollingStrategy."""

    def test_initialization_defaults(self):
        """Test default initialization."""
        defaults = global_config.defaults
        min_int = defaults.polling_progressive_min_interval
        max_int = defaults.polling_progressive_max_interval
        intervals = (min_int, min_int * 5, min_int * 10, min_int * 50, max_int)
        strategy = Poller(PollingAlgorithm.Progressive, intervals=intervals)
        assert strategy.intervals == intervals
        assert strategy.current_index == 0
        assert strategy.checks_at_level == 0
        assert strategy.checks_before_increase == 5

    def test_record_completion_resets_to_fastest(self):
        """Test that completion resets to fastest level."""
        defaults = global_config.defaults
        min_int = defaults.polling_progressive_min_interval
        max_int = defaults.polling_progressive_max_interval
        intervals = (min_int, min_int * 5, min_int * 10, min_int * 50, max_int)
        strategy = Poller(PollingAlgorithm.Progressive, intervals=intervals)
        # Progress to higher level
        for _ in range(10):
            strategy.record_no_completion()
        assert strategy.current_index > 0

        # Completion should reset
        strategy.record_completion()
        assert strategy.current_index == 0
        assert strategy.checks_at_level == 0

    def test_record_no_completion_progresses_levels(self):
        """Test progression through levels."""
        defaults = global_config.defaults
        min_int = defaults.polling_progressive_min_interval
        max_int = defaults.polling_progressive_max_interval
        intervals = (min_int, min_int * 5, min_int * 10, min_int * 50, max_int)
        strategy = Poller(PollingAlgorithm.Progressive, intervals=intervals, checks_before_increase=3)
        levels_visited = [strategy.current_index]

        for _ in range(15):  # Enough to progress through several levels
            strategy.record_no_completion()
            levels_visited.append(strategy.current_index)

        # Should have progressed through levels
        assert max(levels_visited) > 0
        # Should not exceed max level
        assert max(levels_visited) <= len(strategy.intervals) - 1

    def test_record_no_completion_stays_at_max(self):
        """Test that we stay at max level after reaching it."""
        defaults = global_config.defaults
        min_int = defaults.polling_progressive_min_interval
        max_int = defaults.polling_progressive_max_interval
        intervals = (min_int, min_int * 5, min_int * 10, min_int * 50, max_int)
        strategy = Poller(PollingAlgorithm.Progressive, intervals=intervals)
        max_level = len(strategy.intervals) - 1

        # Progress to max level
        for _ in range(100):  # Many checks
            strategy.record_no_completion()

        assert strategy.current_index == max_level

    def test_get_next_interval_returns_correct_level(self):
        """Test that interval matches current level."""
        defaults = global_config.defaults
        min_int = defaults.polling_progressive_min_interval
        max_int = defaults.polling_progressive_max_interval
        intervals = (min_int, min_int * 5, min_int * 10, min_int * 50, max_int)
        strategy = Poller(PollingAlgorithm.Progressive, intervals=intervals)
        for i in range(len(strategy.intervals)):
            strategy.current_index = i
            assert strategy.get_next_interval() == strategy.intervals[i]

    def test_reset(self):
        """Test reset returns to fastest level."""
        defaults = global_config.defaults
        min_int = defaults.polling_progressive_min_interval
        max_int = defaults.polling_progressive_max_interval
        intervals = (min_int, min_int * 5, min_int * 10, min_int * 50, max_int)
        strategy = Poller(PollingAlgorithm.Progressive, intervals=intervals)
        for _ in range(10):
            strategy.record_no_completion()

        strategy.reset()
        assert strategy.current_index == 0
        assert strategy.checks_at_level == 0


class TestCreatePollingStrategy:
    """Tests for Poller factory function."""

    def test_create_fixed_enum(self):
        """Test creating fixed strategy with enum."""
        strategy = Poller(PollingAlgorithm.Fixed)
        # Verify it has required methods
        assert hasattr(strategy, "get_next_interval")
        assert hasattr(strategy, "record_completion")
        assert hasattr(strategy, "record_no_completion")
        assert hasattr(strategy, "reset")

    def test_create_fixed_string(self):
        """Test creating fixed strategy with string."""
        strategy = Poller("fixed")
        assert hasattr(strategy, "get_next_interval")

    def test_create_adaptive_enum(self):
        """Test creating adaptive strategy with enum."""
        strategy = Poller(PollingAlgorithm.Adaptive)
        assert hasattr(strategy, "get_next_interval")
        assert hasattr(strategy, "min_interval")

    def test_create_adaptive_string(self):
        """Test creating adaptive strategy with string."""
        strategy = Poller("adaptive")
        assert hasattr(strategy, "get_next_interval")

    def test_create_exponential_enum(self):
        """Test creating exponential strategy with enum."""
        strategy = Poller(PollingAlgorithm.Exponential)
        assert hasattr(strategy, "get_next_interval")
        assert hasattr(strategy, "initial_interval")

    def test_create_exponential_string(self):
        """Test creating exponential strategy with string."""
        strategy = Poller("exponential")
        assert hasattr(strategy, "get_next_interval")

    def test_create_progressive_enum(self):
        """Test creating progressive strategy with enum."""
        strategy = Poller(PollingAlgorithm.Progressive)
        assert hasattr(strategy, "get_next_interval")
        assert hasattr(strategy, "intervals")

    def test_create_progressive_string(self):
        """Test creating progressive strategy with string."""
        strategy = Poller("progressive")
        assert hasattr(strategy, "get_next_interval")

    def test_create_with_kwargs(self):
        """Test creating strategy with custom parameters."""
        strategy = Poller(PollingAlgorithm.Fixed, interval=0.123)
        assert hasattr(strategy, "interval")
        assert strategy.interval == 0.123

    def test_create_adaptive_with_custom_params(self):
        """Test creating adaptive with custom parameters."""
        strategy = Poller(
            PollingAlgorithm.Adaptive, min_interval=0.0001, max_interval=0.5, speedup_factor=0.5
        )
        assert strategy.min_interval == 0.0001
        assert strategy.max_interval == 0.5
        assert strategy.speedup_factor == 0.5

    def test_create_invalid_algorithm(self):
        """Test error on invalid algorithm."""
        # This should fail since the value is not registered in Registry
        with pytest.raises(KeyError):
            Poller("invalid_algorithm")


class TestPollingBehavior:
    """Integration tests for polling behavior."""

    def test_adaptive_converges(self):
        """Test that adaptive polling converges to optimal behavior."""
        defaults = global_config.defaults
        strategy = Poller(
            PollingAlgorithm.Adaptive,
            min_interval=defaults.polling_adaptive_min_interval,
            max_interval=defaults.polling_adaptive_max_interval,
            current_interval=defaults.polling_adaptive_initial_interval,
        )
        initial = strategy.get_next_interval()

        # Simulate rapid completions - should speed up
        for _ in range(5):
            strategy.record_completion()

        fast = strategy.get_next_interval()
        assert fast < initial

        # Simulate no activity - should slow down
        for _ in range(10):
            strategy.record_no_completion()

        slow = strategy.get_next_interval()
        assert slow > fast

    def test_exponential_backoff_pattern(self):
        """Test exponential backoff follows expected pattern."""
        strategy = Poller(
            PollingAlgorithm.Exponential,
            initial_interval=0.001,
            max_interval=1.0,
            current_interval=0.001,
            multiplier=2.0,
        )

        intervals = []
        for _ in range(10):
            intervals.append(strategy.get_next_interval())
            strategy.record_no_completion()

        # Check that intervals are growing
        for i in range(len(intervals) - 1):
            assert intervals[i + 1] >= intervals[i]

    def test_progressive_level_progression(self):
        """Test progressive strategy progresses through levels correctly."""
        defaults = global_config.defaults
        min_int = defaults.polling_progressive_min_interval
        max_int = defaults.polling_progressive_max_interval
        intervals = (min_int, min_int * 5, min_int * 10, min_int * 50, max_int)
        strategy = Poller(PollingAlgorithm.Progressive, intervals=intervals, checks_before_increase=2)

        levels = []
        for _ in range(12):  # 6 levels of 2 checks each
            levels.append(strategy.current_index)
            strategy.record_no_completion()

        # Should have progressed through multiple levels
        unique_levels = set(levels)
        assert len(unique_levels) > 1
