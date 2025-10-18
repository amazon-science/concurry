"""Tests for polling algorithms."""

import pytest

from concurry.core.config import PollingAlgorithm
from concurry.core.polling import (
    AdaptivePollingStrategy,
    ExponentialPollingStrategy,
    FixedPollingStrategy,
    ProgressivePollingStrategy,
    create_polling_strategy,
)


class TestFixedPollingStrategy:
    """Tests for FixedPollingStrategy."""

    def test_initialization_defaults(self):
        """Test default initialization."""
        strategy = FixedPollingStrategy()
        assert strategy.interval == 0.01

    def test_initialization_custom(self):
        """Test custom initialization."""
        strategy = FixedPollingStrategy(interval=0.05)
        assert strategy.interval == 0.05

    def test_get_next_interval(self):
        """Test get_next_interval returns fixed value."""
        strategy = FixedPollingStrategy(interval=0.02)
        assert strategy.get_next_interval() == 0.02
        assert strategy.get_next_interval() == 0.02  # Always same

    def test_record_completion_no_change(self):
        """Test that completion doesn't change interval."""
        strategy = FixedPollingStrategy(interval=0.03)
        initial = strategy.get_next_interval()
        strategy.record_completion()
        assert strategy.get_next_interval() == initial

    def test_record_no_completion_no_change(self):
        """Test that no completion doesn't change interval."""
        strategy = FixedPollingStrategy(interval=0.03)
        initial = strategy.get_next_interval()
        strategy.record_no_completion()
        assert strategy.get_next_interval() == initial

    def test_reset_no_effect(self):
        """Test that reset has no effect on fixed strategy."""
        strategy = FixedPollingStrategy(interval=0.04)
        initial = strategy.get_next_interval()
        strategy.reset()
        assert strategy.get_next_interval() == initial


class TestAdaptivePollingStrategy:
    """Tests for AdaptivePollingStrategy."""

    def test_initialization_defaults(self):
        """Test default initialization."""
        strategy = AdaptivePollingStrategy()
        assert strategy.min_interval == 0.001
        assert strategy.max_interval == 0.1
        assert strategy.current_interval == 0.01
        assert strategy.speedup_factor == 0.7
        assert strategy.slowdown_factor == 1.3
        assert strategy.consecutive_empty == 0

    def test_record_completion_speeds_up(self):
        """Test that completion speeds up polling."""
        strategy = AdaptivePollingStrategy(current_interval=0.01)
        initial = strategy.get_next_interval()
        strategy.record_completion()
        new = strategy.get_next_interval()
        assert new < initial
        assert strategy.consecutive_empty == 0

    def test_record_completion_respects_min(self):
        """Test that speedup respects minimum interval."""
        strategy = AdaptivePollingStrategy(min_interval=0.005, current_interval=0.006)
        for _ in range(10):
            strategy.record_completion()
        assert strategy.get_next_interval() >= strategy.min_interval

    def test_record_no_completion_slows_down(self):
        """Test that no completion slows down polling after 3 empty checks."""
        strategy = AdaptivePollingStrategy(current_interval=0.01)
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
        strategy = AdaptivePollingStrategy(max_interval=0.05, current_interval=0.04)
        for _ in range(20):  # Many empty checks
            strategy.record_no_completion()
        assert strategy.get_next_interval() <= strategy.max_interval

    def test_completion_resets_consecutive_empty(self):
        """Test that completion resets consecutive empty counter."""
        strategy = AdaptivePollingStrategy()
        strategy.record_no_completion()
        strategy.record_no_completion()
        assert strategy.consecutive_empty == 2

        strategy.record_completion()
        assert strategy.consecutive_empty == 0

    def test_reset(self):
        """Test reset returns to initial state."""
        strategy = AdaptivePollingStrategy()
        # Modify state
        for _ in range(5):
            strategy.record_completion()
        for _ in range(5):
            strategy.record_no_completion()

        # Reset
        strategy.reset()
        assert strategy.current_interval == 0.01
        assert strategy.consecutive_empty == 0


class TestExponentialPollingStrategy:
    """Tests for ExponentialPollingStrategy."""

    def test_initialization_defaults(self):
        """Test default initialization."""
        strategy = ExponentialPollingStrategy()
        assert strategy.initial_interval == 0.001
        assert strategy.max_interval == 0.5
        assert strategy.multiplier == 2.0
        assert strategy.current_interval == 0.001

    def test_record_completion_resets(self):
        """Test that completion resets to initial interval."""
        strategy = ExponentialPollingStrategy(initial_interval=0.001)
        # Increase interval first
        strategy.record_no_completion()
        strategy.record_no_completion()
        assert strategy.get_next_interval() > strategy.initial_interval

        # Completion should reset
        strategy.record_completion()
        assert strategy.get_next_interval() == strategy.initial_interval

    def test_record_no_completion_exponential_growth(self):
        """Test exponential growth of interval."""
        strategy = ExponentialPollingStrategy(initial_interval=0.001, multiplier=2.0)
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
        strategy = ExponentialPollingStrategy(max_interval=0.1)
        for _ in range(20):  # Many increases
            strategy.record_no_completion()
        assert strategy.get_next_interval() <= strategy.max_interval

    def test_reset(self):
        """Test reset returns to initial interval."""
        strategy = ExponentialPollingStrategy()
        for _ in range(5):
            strategy.record_no_completion()

        strategy.reset()
        assert strategy.current_interval == strategy.initial_interval


class TestProgressivePollingStrategy:
    """Tests for ProgressivePollingStrategy."""

    def test_initialization_defaults(self):
        """Test default initialization."""
        strategy = ProgressivePollingStrategy()
        assert strategy.intervals == (0.001, 0.005, 0.01, 0.05, 0.1)
        assert strategy.current_index == 0
        assert strategy.checks_at_level == 0
        assert strategy.checks_before_increase == 5

    def test_record_completion_resets_to_fastest(self):
        """Test that completion resets to fastest level."""
        strategy = ProgressivePollingStrategy()
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
        strategy = ProgressivePollingStrategy(checks_before_increase=3)
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
        strategy = ProgressivePollingStrategy()
        max_level = len(strategy.intervals) - 1

        # Progress to max level
        for _ in range(100):  # Many checks
            strategy.record_no_completion()

        assert strategy.current_index == max_level

    def test_get_next_interval_returns_correct_level(self):
        """Test that interval matches current level."""
        strategy = ProgressivePollingStrategy()
        for i in range(len(strategy.intervals)):
            strategy.current_index = i
            assert strategy.get_next_interval() == strategy.intervals[i]

    def test_reset(self):
        """Test reset returns to fastest level."""
        strategy = ProgressivePollingStrategy()
        for _ in range(10):
            strategy.record_no_completion()

        strategy.reset()
        assert strategy.current_index == 0
        assert strategy.checks_at_level == 0


class TestCreatePollingStrategy:
    """Tests for create_polling_strategy factory function."""

    def test_create_fixed_enum(self):
        """Test creating fixed strategy with enum."""
        strategy = create_polling_strategy(PollingAlgorithm.Fixed)
        assert isinstance(strategy, FixedPollingStrategy)

    def test_create_fixed_string(self):
        """Test creating fixed strategy with string."""
        strategy = create_polling_strategy("fixed")
        assert isinstance(strategy, FixedPollingStrategy)

    def test_create_adaptive_enum(self):
        """Test creating adaptive strategy with enum."""
        strategy = create_polling_strategy(PollingAlgorithm.Adaptive)
        assert isinstance(strategy, AdaptivePollingStrategy)

    def test_create_adaptive_string(self):
        """Test creating adaptive strategy with string."""
        strategy = create_polling_strategy("adaptive")
        assert isinstance(strategy, AdaptivePollingStrategy)

    def test_create_exponential_enum(self):
        """Test creating exponential strategy with enum."""
        strategy = create_polling_strategy(PollingAlgorithm.Exponential)
        assert isinstance(strategy, ExponentialPollingStrategy)

    def test_create_exponential_string(self):
        """Test creating exponential strategy with string."""
        strategy = create_polling_strategy("exponential")
        assert isinstance(strategy, ExponentialPollingStrategy)

    def test_create_progressive_enum(self):
        """Test creating progressive strategy with enum."""
        strategy = create_polling_strategy(PollingAlgorithm.Progressive)
        assert isinstance(strategy, ProgressivePollingStrategy)

    def test_create_progressive_string(self):
        """Test creating progressive strategy with string."""
        strategy = create_polling_strategy("progressive")
        assert isinstance(strategy, ProgressivePollingStrategy)

    def test_create_with_kwargs(self):
        """Test creating strategy with custom parameters."""
        strategy = create_polling_strategy(PollingAlgorithm.Fixed, interval=0.123)
        assert isinstance(strategy, FixedPollingStrategy)
        assert strategy.interval == 0.123

    def test_create_adaptive_with_custom_params(self):
        """Test creating adaptive with custom parameters."""
        strategy = create_polling_strategy(
            PollingAlgorithm.Adaptive, min_interval=0.0001, max_interval=0.5, speedup_factor=0.5
        )
        assert isinstance(strategy, AdaptivePollingStrategy)
        assert strategy.min_interval == 0.0001
        assert strategy.max_interval == 0.5
        assert strategy.speedup_factor == 0.5

    def test_create_invalid_algorithm(self):
        """Test error on invalid algorithm."""
        # This should fail since the value is out of enum range
        with pytest.raises(ValueError):
            create_polling_strategy("invalid_algorithm")


class TestPollingBehavior:
    """Integration tests for polling behavior."""

    def test_adaptive_converges(self):
        """Test that adaptive polling converges to optimal behavior."""
        strategy = AdaptivePollingStrategy()
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
        strategy = ExponentialPollingStrategy(initial_interval=0.001, max_interval=1.0, multiplier=2.0)

        intervals = []
        for _ in range(10):
            intervals.append(strategy.get_next_interval())
            strategy.record_no_completion()

        # Check that intervals are growing
        for i in range(len(intervals) - 1):
            assert intervals[i + 1] >= intervals[i]

    def test_progressive_level_progression(self):
        """Test progressive strategy progresses through levels correctly."""
        strategy = ProgressivePollingStrategy(checks_before_increase=2)

        levels = []
        for _ in range(12):  # 6 levels of 2 checks each
            levels.append(strategy.current_index)
            strategy.record_no_completion()

        # Should have progressed through multiple levels
        unique_levels = set(levels)
        assert len(unique_levels) > 1
