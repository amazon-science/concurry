"""Polling algorithms for efficient future completion checking.

This module provides various polling strategies that balance responsiveness
against CPU usage when checking the completion status of futures.
"""

from typing import Protocol, Union

from morphic import MutableTyped

from .config import PollingAlgorithm


class PollingStrategy(Protocol):
    """Protocol defining the interface for polling strategies."""

    def get_next_interval(self) -> float:
        """Get the next polling interval in seconds."""
        ...

    def record_completion(self) -> None:
        """Record that futures completed in this check."""
        ...

    def record_no_completion(self) -> None:
        """Record that no futures completed in this check."""
        ...

    def reset(self) -> None:
        """Reset strategy to initial state."""
        ...


class FixedPollingStrategy(MutableTyped):
    """Fixed interval polling - constant wait time between checks.

    This strategy uses a constant polling interval regardless of whether
    futures are completing. Simple and predictable, but may check too
    frequently (wasting CPU) or too slowly (adding latency).

    Best for:
        - Predictable workloads with known completion times
        - Testing and benchmarking
        - When you want complete control over polling frequency

    Attributes:
        interval: Polling interval in seconds (default: 0.01 = 10ms)

    Example:
        ```python
        # Check every 50ms
        strategy = FixedPollingStrategy(interval=0.05)
        ```
    """

    interval: float = 0.01  # 10ms default

    def get_next_interval(self) -> float:
        """Get the next polling interval."""
        return self.interval

    def record_completion(self) -> None:
        """No adaptation in fixed strategy."""
        pass

    def record_no_completion(self) -> None:
        """No adaptation in fixed strategy."""
        pass

    def reset(self) -> None:
        """No state to reset in fixed strategy."""
        pass


class AdaptivePollingStrategy(MutableTyped):
    """Adaptive polling that adjusts based on completion rate.

    This strategy dynamically adjusts the polling interval based on whether
    futures are completing. When futures complete, it speeds up (checks more
    frequently). When nothing completes, it slows down (saves CPU).

    Algorithm:
        - On completion: interval = max(min_interval, interval * speedup_factor)
        - On no completion (after 3 empty checks): interval = min(max_interval, interval * slowdown_factor)

    Best for:
        - Variable workloads (recommended default)
        - Unknown completion patterns
        - Large batches of futures with varying completion times
        - Minimizing both latency and CPU usage

    Attributes:
        min_interval: Minimum polling interval (default: 0.001 = 1ms)
        max_interval: Maximum polling interval (default: 0.1 = 100ms)
        current_interval: Current polling interval (default: 0.01 = 10ms)
        speedup_factor: Multiplier when futures complete (default: 0.7 = 30% faster)
        slowdown_factor: Multiplier when idle (default: 1.3 = 30% slower)
        consecutive_empty: Number of consecutive empty checks

    Example:
        ```python
        # More aggressive adaptation
        strategy = AdaptivePollingStrategy(
            min_interval=0.0001,  # 0.1ms min
            max_interval=0.2,     # 200ms max
            speedup_factor=0.5,   # 50% faster on completion
            slowdown_factor=1.5   # 50% slower when idle
        )
        ```
    """

    min_interval: float = 0.001  # 1ms minimum
    max_interval: float = 0.1  # 100ms maximum
    current_interval: float = 0.01  # Start at 10ms
    speedup_factor: float = 0.7  # Speed up by 30% on completions
    slowdown_factor: float = 1.3  # Slow down by 30% on no completions
    consecutive_empty: int = 0  # Track empty checks

    def get_next_interval(self) -> float:
        """Get the current polling interval."""
        return self.current_interval

    def record_completion(self) -> None:
        """Speed up - futures are completing, check more frequently."""
        self.current_interval = max(self.min_interval, self.current_interval * self.speedup_factor)
        self.consecutive_empty = 0

    def record_no_completion(self) -> None:
        """Slow down after consecutive empty checks to save CPU."""
        self.consecutive_empty += 1
        if self.consecutive_empty >= 3:  # After 3 empty checks
            self.current_interval = min(self.max_interval, self.current_interval * self.slowdown_factor)

    def reset(self) -> None:
        """Reset to initial state."""
        self.current_interval = 0.01
        self.consecutive_empty = 0


class ExponentialPollingStrategy(MutableTyped):
    """Exponential backoff polling.

    This strategy starts with a fast polling interval and exponentially
    increases it when nothing completes. Resets to fast polling on any
    completion.

    Algorithm:
        - On completion: Reset to initial_interval
        - On no completion: interval = min(max_interval, interval * multiplier)

    Best for:
        - Long-running operations with sporadic completion
        - Minimizing CPU usage when futures take a long time
        - Operations where latency on the first completion is critical

    Attributes:
        initial_interval: Starting interval (default: 0.001 = 1ms)
        max_interval: Maximum interval cap (default: 0.5 = 500ms)
        multiplier: Growth factor per empty check (default: 2.0 = double)
        current_interval: Current interval

    Example:
        ```python
        # Slower growth, higher max
        strategy = ExponentialPollingStrategy(
            initial_interval=0.01,  # 10ms start
            max_interval=2.0,       # 2 second max
            multiplier=1.5          # 50% growth
        )
        ```
    """

    initial_interval: float = 0.001  # Start at 1ms
    max_interval: float = 0.5  # Cap at 500ms
    multiplier: float = 2.0  # Double each time
    current_interval: float = 0.001

    def get_next_interval(self) -> float:
        """Get the current polling interval."""
        return self.current_interval

    def record_completion(self) -> None:
        """Reset to fast polling on completion."""
        self.current_interval = self.initial_interval

    def record_no_completion(self) -> None:
        """Exponentially increase interval."""
        self.current_interval = min(self.max_interval, self.current_interval * self.multiplier)

    def reset(self) -> None:
        """Reset to initial interval."""
        self.current_interval = self.initial_interval


class ProgressivePollingStrategy(MutableTyped):
    """Progressive backoff with fixed interval levels.

    This strategy progresses through predefined polling intervals, staying
    at each level for a fixed number of checks before moving to the next.
    Resets to the fastest level on any completion.

    Algorithm:
        - On completion: Reset to fastest level (index 0)
        - On no completion: Progress to next level after N checks at current level

    Best for:
        - Workloads with predictable phases
        - When you want explicit control over polling levels
        - Balancing between adaptive and fixed strategies

    Attributes:
        intervals: Tuple of interval levels (default: 1ms, 5ms, 10ms, 50ms, 100ms)
        current_index: Current level index
        checks_at_level: Number of checks performed at current level
        checks_before_increase: Checks before progressing to next level (default: 5)

    Example:
        ```python
        # Custom interval levels
        strategy = ProgressivePollingStrategy(
            intervals=(0.001, 0.01, 0.05, 0.1, 0.5, 1.0),
            checks_before_increase=10  # Stay longer at each level
        )
        ```
    """

    intervals: tuple = (0.001, 0.005, 0.01, 0.05, 0.1)  # Progressive steps
    current_index: int = 0
    checks_at_level: int = 0
    checks_before_increase: int = 5  # Stay at each level for N checks

    def get_next_interval(self) -> float:
        """Get the current interval based on level."""
        return self.intervals[self.current_index]

    def record_completion(self) -> None:
        """Reset to fastest level on completion."""
        self.current_index = 0
        self.checks_at_level = 0

    def record_no_completion(self) -> None:
        """Progress to next level after N checks."""
        self.checks_at_level += 1
        if self.checks_at_level >= self.checks_before_increase:
            self.current_index = min(len(self.intervals) - 1, self.current_index + 1)
            self.checks_at_level = 0

    def reset(self) -> None:
        """Reset to fastest level."""
        self.current_index = 0
        self.checks_at_level = 0


def create_polling_strategy(
    algorithm: Union[PollingAlgorithm, str] = PollingAlgorithm.Adaptive, **kwargs
) -> PollingStrategy:
    """Create a polling strategy instance.

    Args:
        algorithm: Polling algorithm to use (enum or string name)
        **kwargs: Additional arguments passed to strategy constructor

    Returns:
        PollingStrategy instance

    Raises:
        ValueError: If algorithm is unknown

    Example:
        ```python
        # Using enum
        strategy = create_polling_strategy(PollingAlgorithm.Adaptive)

        # Using string
        strategy = create_polling_strategy("exponential")

        # With custom parameters
        strategy = create_polling_strategy(
            "adaptive",
            min_interval=0.0001,
            max_interval=0.5
        )
        ```
    """
    # Convert string to enum if needed
    if isinstance(algorithm, str):
        algorithm = PollingAlgorithm(algorithm)

    if algorithm == PollingAlgorithm.Fixed:
        return FixedPollingStrategy(**kwargs)
    elif algorithm == PollingAlgorithm.Adaptive:
        return AdaptivePollingStrategy(**kwargs)
    elif algorithm == PollingAlgorithm.Exponential:
        return ExponentialPollingStrategy(**kwargs)
    elif algorithm == PollingAlgorithm.Progressive:
        return ProgressivePollingStrategy(**kwargs)
    else:
        raise ValueError(f"Unknown polling algorithm: {algorithm}")
