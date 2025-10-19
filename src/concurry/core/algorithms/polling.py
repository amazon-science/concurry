"""Polling algorithms for efficient future completion checking."""

from abc import ABC, abstractmethod

from morphic import MutableTyped, Registry
from pydantic import ConfigDict

from ..constants import PollingAlgorithm


class BasePollingStrategy(Registry, MutableTyped, ABC):
    """Base class for polling strategies using Registry pattern.

    All polling strategies inherit from this class and are automatically
    registered for factory-based creation.
    """

    model_config = ConfigDict(extra="ignore")

    @abstractmethod
    def get_next_interval(self) -> float:
        """Get the next polling interval in seconds."""
        pass

    @abstractmethod
    def record_completion(self) -> None:
        """Record that futures completed in this check."""
        pass

    @abstractmethod
    def record_no_completion(self) -> None:
        """Record that no futures completed in this check."""
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset strategy to initial state."""
        pass


class FixedPollingStrategy(BasePollingStrategy):
    """Fixed interval polling - constant wait time between checks.

    This strategy uses a constant polling interval regardless of whether
    futures are completing. Simple and predictable, but may check too
    frequently (wasting CPU) or too slowly (adding latency).

    Best for:
        - Predictable workloads with known completion times
        - Testing and benchmarking
        - When you want complete control over polling frequency

    Attributes:
        interval: Polling interval in seconds. When used via wait() or gather(),
            the value is taken from global_config.defaults.polling_fixed_interval

    Example:
        ```python
        # Check every 50ms
        strategy = FixedPollingStrategy.of(interval=0.05)
        ```
    """

    aliases = ["fixed", PollingAlgorithm.Fixed]

    interval: float

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


class AdaptivePollingStrategy(BasePollingStrategy):
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
        min_interval: Minimum polling interval. When used via wait() or gather(),
            the value is taken from global_config.defaults.polling_adaptive_min_interval
        max_interval: Maximum polling interval. When used via wait() or gather(),
            the value is taken from global_config.defaults.polling_adaptive_max_interval
        current_interval: Current polling interval. When used via wait() or gather(),
            the initial value is taken from global_config.defaults.polling_adaptive_initial_interval
        speedup_factor: Multiplier when futures complete (0.7 = 30% faster)
        slowdown_factor: Multiplier when idle (1.3 = 30% slower)
        consecutive_empty: Number of consecutive empty checks

    Example:
        ```python
        # More aggressive adaptation
        strategy = AdaptivePollingStrategy.of(
            min_interval=0.0001,  # 0.1ms min
            max_interval=0.2,     # 200ms max
            speedup_factor=0.5,   # 50% faster on completion
            slowdown_factor=1.5   # 50% slower when idle
        )
        ```
    """

    aliases = ["adaptive", PollingAlgorithm.Adaptive]

    min_interval: float
    max_interval: float
    current_interval: float
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
        # Reset to min_interval since we don't store the original initial_interval
        self.current_interval = self.min_interval
        self.consecutive_empty = 0


class ExponentialPollingStrategy(BasePollingStrategy):
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
        initial_interval: Starting interval. When used via wait() or gather(),
            the value is taken from global_config.defaults.polling_exponential_initial_interval
        max_interval: Maximum interval cap. When used via wait() or gather(),
            the value is taken from global_config.defaults.polling_exponential_max_interval
        multiplier: Growth factor per empty check (2.0 = double)
        current_interval: Current interval

    Example:
        ```python
        # Slower growth, higher max
        strategy = ExponentialPollingStrategy.of(
            initial_interval=0.01,  # 10ms start
            max_interval=2.0,       # 2 second max
            multiplier=1.5          # 50% growth
        )
        ```
    """

    aliases = ["exponential", PollingAlgorithm.Exponential]

    initial_interval: float
    max_interval: float
    multiplier: float = 2.0  # Double each time
    current_interval: float

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


class ProgressivePollingStrategy(BasePollingStrategy):
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
        intervals: Tuple of interval levels (e.g., 1ms, 5ms, 10ms, 50ms, 100ms).
            When used via wait() or gather(), this is generated from
            global_config.defaults.polling_progressive_min_interval and
            global_config.defaults.polling_progressive_max_interval
        current_index: Current level index
        checks_at_level: Number of checks performed at current level
        checks_before_increase: Checks before progressing to next level (5)

    Example:
        ```python
        # Custom interval levels
        strategy = ProgressivePollingStrategy.of(
            intervals=(0.001, 0.01, 0.05, 0.1, 0.5, 1.0),
            checks_before_increase=10  # Stay longer at each level
        )
        ```
    """

    aliases = ["progressive", PollingAlgorithm.Progressive]

    intervals: tuple
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


def Poller(algorithm: PollingAlgorithm, **kwargs) -> BasePollingStrategy:
    """Create a polling strategy instance using Registry pattern.

    Args:
        algorithm: Polling algorithm to use (enum or string name)
        **kwargs: Additional arguments passed to strategy constructor

    Returns:
        BasePollingStrategy instance

    Raises:
        ValueError: If algorithm is unknown

    Example:
        ```python
        # Using enum
        strategy = Poller(PollingAlgorithm.Adaptive)

        # Using string
        strategy = Poller("exponential")

        # With custom parameters
        strategy = Poller(
            "adaptive",
            min_interval=0.0001,
            max_interval=0.5
        )
        ```
    """
    from ...config import global_config

    # Fill in defaults from config if not provided
    local_config = global_config.clone()
    defaults = local_config.defaults

    if algorithm in (PollingAlgorithm.Fixed, "fixed", "fixed_polling"):
        if "interval" not in kwargs:
            kwargs["interval"] = defaults.polling_fixed_interval
    elif algorithm in (PollingAlgorithm.Adaptive, "adaptive", "adaptive_polling"):
        if "min_interval" not in kwargs:
            kwargs["min_interval"] = defaults.polling_adaptive_min_interval
        if "max_interval" not in kwargs:
            kwargs["max_interval"] = defaults.polling_adaptive_max_interval
        if "current_interval" not in kwargs:
            kwargs["current_interval"] = defaults.polling_adaptive_initial_interval
    elif algorithm in (PollingAlgorithm.Exponential, "exponential", "exponential_polling"):
        if "initial_interval" not in kwargs:
            kwargs["initial_interval"] = defaults.polling_exponential_initial_interval
        if "max_interval" not in kwargs:
            kwargs["max_interval"] = defaults.polling_exponential_max_interval
        if "current_interval" not in kwargs:
            kwargs["current_interval"] = defaults.polling_exponential_initial_interval
    elif algorithm in (PollingAlgorithm.Progressive, "progressive", "progressive_polling"):
        if "intervals" not in kwargs:
            min_int = defaults.polling_progressive_min_interval
            max_int = defaults.polling_progressive_max_interval
            kwargs["intervals"] = (min_int, min_int * 5, min_int * 10, min_int * 50, max_int)

    return BasePollingStrategy.of(algorithm, **kwargs)
