"""Polling algorithms for efficient future completion checking."""

from abc import ABC, abstractmethod
from typing import Union

from morphic import MutableTyped, Registry
from pydantic import ConfigDict

from ...utils import _NO_ARG, _NO_ARG_TYPE
from ..constants import PollingAlgorithm


class _BasePollingStrategy(Registry, MutableTyped, ABC):
    """Base class for polling strategies using Registry pattern.

    **PRIVATE CLASS**: Do not use directly. Use the Poller() factory function instead.

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


class _FixedPollingStrategy(_BasePollingStrategy):
    """Fixed interval polling - constant wait time between checks.

    **PRIVATE CLASS**: Do not use directly. Use the Poller() factory function instead.

    This strategy uses a constant polling interval regardless of whether
    futures are completing. Simple and predictable, but may check too
    frequently (wasting CPU) or too slowly (adding latency).

    Best for:
        - Predictable workloads with known completion times
        - Testing and benchmarking
        - When you want complete control over polling frequency

    Attributes:
        interval: Polling interval in seconds. Defaults to
            global_config.defaults.polling_fixed_interval

    Example:
        ```python
        # Use Poller() factory instead
        strategy = Poller(PollingAlgorithm.Fixed, interval=0.05)
        ```
    """

    aliases = ["fixed", PollingAlgorithm.Fixed]

    interval: Union[float, _NO_ARG_TYPE] = _NO_ARG

    def post_initialize(self) -> None:
        """Apply defaults from global config."""
        if self.interval is _NO_ARG:
            from ...config import global_config

            local_config = global_config.clone()
            object.__setattr__(self, "interval", local_config.defaults.polling_fixed_interval)

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


class _AdaptivePollingStrategy(_BasePollingStrategy):
    """Adaptive polling that adjusts based on completion rate.

    **PRIVATE CLASS**: Do not use directly. Use the Poller() factory function instead.

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
        min_interval: Minimum polling interval. Defaults to
            global_config.defaults.polling_adaptive_min_interval
        max_interval: Maximum polling interval. Defaults to
            global_config.defaults.polling_adaptive_max_interval
        current_interval: Current polling interval. Defaults to
            global_config.defaults.polling_adaptive_initial_interval
        speedup_factor: Multiplier when futures complete (0.7 = 30% faster)
        slowdown_factor: Multiplier when idle (1.3 = 30% slower)
        consecutive_empty: Number of consecutive empty checks

    Example:
        ```python
        # Use Poller() factory instead
        strategy = Poller(
            PollingAlgorithm.Adaptive,
            min_interval=0.0001,  # 0.1ms min
            max_interval=0.2,     # 200ms max
            speedup_factor=0.5,   # 50% faster on completion
            slowdown_factor=1.5   # 50% slower when idle
        )
        ```
    """

    aliases = ["adaptive", PollingAlgorithm.Adaptive]

    min_interval: Union[float, _NO_ARG_TYPE] = _NO_ARG
    max_interval: Union[float, _NO_ARG_TYPE] = _NO_ARG
    current_interval: Union[float, _NO_ARG_TYPE] = _NO_ARG
    speedup_factor: Union[float, _NO_ARG_TYPE] = _NO_ARG
    slowdown_factor: Union[float, _NO_ARG_TYPE] = _NO_ARG
    consecutive_empty_threshold: Union[int, _NO_ARG_TYPE] = _NO_ARG
    consecutive_empty: int = 0  # Track empty checks (internal state, not configurable)

    def post_initialize(self) -> None:
        """Apply defaults from global config."""
        from ...config import global_config

        local_config = global_config.clone()
        defaults = local_config.defaults

        if self.min_interval is _NO_ARG:
            object.__setattr__(self, "min_interval", defaults.polling_adaptive_min_interval)
        if self.max_interval is _NO_ARG:
            object.__setattr__(self, "max_interval", defaults.polling_adaptive_max_interval)
        if self.current_interval is _NO_ARG:
            object.__setattr__(self, "current_interval", defaults.polling_adaptive_initial_interval)
        if self.speedup_factor is _NO_ARG:
            object.__setattr__(self, "speedup_factor", defaults.polling_adaptive_speedup_factor)
        if self.slowdown_factor is _NO_ARG:
            object.__setattr__(self, "slowdown_factor", defaults.polling_adaptive_slowdown_factor)
        if self.consecutive_empty_threshold is _NO_ARG:
            object.__setattr__(
                self, "consecutive_empty_threshold", defaults.polling_adaptive_consecutive_empty_threshold
            )

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
        if self.consecutive_empty >= self.consecutive_empty_threshold:
            self.current_interval = min(self.max_interval, self.current_interval * self.slowdown_factor)

    def reset(self) -> None:
        """Reset to initial state."""
        # Reset to min_interval since we don't store the original initial_interval
        self.current_interval = self.min_interval
        self.consecutive_empty = 0


class _ExponentialPollingStrategy(_BasePollingStrategy):
    """Exponential backoff polling.

    **PRIVATE CLASS**: Do not use directly. Use the Poller() factory function instead.

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
        initial_interval: Starting interval. Defaults to
            global_config.defaults.polling_exponential_initial_interval
        max_interval: Maximum interval cap. Defaults to
            global_config.defaults.polling_exponential_max_interval
        multiplier: Growth factor per empty check (2.0 = double)
        current_interval: Current interval

    Example:
        ```python
        # Use Poller() factory instead
        strategy = Poller(
            PollingAlgorithm.Exponential,
            initial_interval=0.01,  # 10ms start
            max_interval=2.0,       # 2 second max
            multiplier=1.5          # 50% growth
        )
        ```
    """

    aliases = ["exponential", PollingAlgorithm.Exponential]

    initial_interval: Union[float, _NO_ARG_TYPE] = _NO_ARG
    max_interval: Union[float, _NO_ARG_TYPE] = _NO_ARG
    multiplier: Union[float, _NO_ARG_TYPE] = _NO_ARG
    current_interval: Union[float, _NO_ARG_TYPE] = _NO_ARG

    def post_initialize(self) -> None:
        """Apply defaults from global config."""
        from ...config import global_config

        local_config = global_config.clone()
        defaults = local_config.defaults

        if self.initial_interval is _NO_ARG:
            object.__setattr__(self, "initial_interval", defaults.polling_exponential_initial_interval)
        if self.max_interval is _NO_ARG:
            object.__setattr__(self, "max_interval", defaults.polling_exponential_max_interval)
        if self.multiplier is _NO_ARG:
            object.__setattr__(self, "multiplier", defaults.polling_exponential_multiplier)
        if self.current_interval is _NO_ARG:
            object.__setattr__(self, "current_interval", defaults.polling_exponential_initial_interval)

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


class _ProgressivePollingStrategy(_BasePollingStrategy):
    """Progressive backoff with fixed interval levels.

    **PRIVATE CLASS**: Do not use directly. Use the Poller() factory function instead.

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
            Defaults to tuple generated from
            global_config.defaults.polling_progressive_min_interval and
            global_config.defaults.polling_progressive_max_interval
        current_index: Current level index
        checks_at_level: Number of checks performed at current level
        checks_before_increase: Checks before progressing to next level (5)

    Example:
        ```python
        # Use Poller() factory instead
        strategy = Poller(
            PollingAlgorithm.Progressive,
            intervals=(0.001, 0.01, 0.05, 0.1, 0.5, 1.0),
            checks_before_increase=10  # Stay longer at each level
        )
        ```
    """

    aliases = ["progressive", PollingAlgorithm.Progressive]

    intervals: Union[tuple, _NO_ARG_TYPE] = _NO_ARG
    current_index: int = 0
    checks_at_level: int = 0
    checks_before_increase: Union[int, _NO_ARG_TYPE] = _NO_ARG

    def post_initialize(self) -> None:
        """Apply defaults from global config."""
        from ...config import global_config

        local_config = global_config.clone()
        defaults = local_config.defaults

        if self.intervals is _NO_ARG:
            min_int = defaults.polling_progressive_min_interval
            max_int = defaults.polling_progressive_max_interval
            object.__setattr__(self, "intervals", (min_int, min_int * 5, min_int * 10, min_int * 50, max_int))

        if self.checks_before_increase is _NO_ARG:
            object.__setattr__(
                self, "checks_before_increase", defaults.polling_progressive_checks_before_increase
            )

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


def Poller(algorithm: PollingAlgorithm, **kwargs) -> _BasePollingStrategy:
    """Create a polling strategy instance using Registry pattern.

    This is the only public API for creating polling strategies. Implementation
    classes are private and should not be used directly.

    Args:
        algorithm: Polling algorithm to use (enum or string name)
        **kwargs: Additional arguments passed to strategy constructor

    Returns:
        Polling strategy instance (private implementation class)

    Raises:
        ValueError: If algorithm is unknown

    Example:
        ```python
        # Using enum with defaults from global_config
        strategy = Poller(PollingAlgorithm.Adaptive)

        # Using string
        strategy = Poller("exponential")

        # With custom parameters
        strategy = Poller(
            PollingAlgorithm.Adaptive,
            min_interval=0.0001,
            max_interval=0.5
        )
        ```
    """
    # Just delegate to .of() - defaults are handled in post_initialize()
    return _BasePollingStrategy.of(algorithm, **kwargs)
