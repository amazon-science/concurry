"""Acquisition classes for tracking limit usage.

This module provides acquisition tracking for individual limits and limit sets.
Acquisitions track both requested amounts and actual usage, ensuring proper
resource accounting and automatic release via context managers.

Architecture:
    - Acquisition: Tracks single limit acquisition (requested, used, released state)
    - LimitSetAcquisition: Manages multiple limit acquisitions atomically,
      enforces update requirements, and coordinates with LimitSet for thread-safe release

Usage:
    Acquisitions are created by LimitSet.acquire() and should be used via
    context managers for automatic resource cleanup:

    ```python
    from concurry import LimitSet, RateLimit

    limits = LimitSet(limits=[RateLimit(key="tokens", window_seconds=60, capacity=1000)])

    with limits.acquire(requested={"tokens": 100}) as acq:
        result = operation()
        acq.update(usage={"tokens": result.actual_tokens})
    # Automatically released on exit
    ```
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional, Set

if TYPE_CHECKING:
    from .limit import Limit
    from .limit_set import LimitSet


class Acquisition:
    """Represents an acquisition of a single limit with usage tracking.

    Tracks the requested amount, actual usage, and release state for a single
    limit acquisition. Used internally by LimitSetAcquisition for individual
    limit tracking.

    Attributes:
        limit: The Limit instance that was acquired
        requested: Amount that was requested
        successful: Whether the acquisition was successful
        used: Actual amount used (None until updated)

    State Machine:
        acquired -> updated (optional) -> released

    Thread-Safety:
        This class is NOT thread-safe on its own. Thread safety is provided by
        the parent LimitSet which holds locks during acquisition/release operations.

    Example:
        Typically used via LimitSetAcquisition::

            # LimitSet creates Acquisitions internally
            with limits.acquire(requested={"tokens": 100}) as limit_set_acq:
                # Access individual acquisitions if needed
                token_acq = limit_set_acq.acquisitions["tokens"]
                assert token_acq.requested == 100

                result = operation()
                limit_set_acq.update(usage={"tokens": 80})
                assert token_acq.used == 80
    """

    def __init__(self, limit: "Limit", requested: int, successful: bool = True):
        """Initialize an acquisition.

        Args:
            limit: The limit that was acquired
            requested: Amount requested
            successful: Whether the acquisition was successful
        """
        self.limit = limit
        self.requested = requested
        self.successful = successful
        self.used: Optional[int] = None
        self._released = False

    def update(self, used: int) -> None:
        """Update the actual usage amount.

        Args:
            used: Actual amount used

        Raises:
            ValueError: If used is negative or exceeds requested
            RuntimeError: If already released
        """
        if self._released:
            raise RuntimeError("Cannot update an already released acquisition")

        if used < 0:
            raise ValueError(f"Usage cannot be negative, got: {used}")

        # Validate usage doesn't exceed requested (limit-specific)
        self.limit.validate_usage(self.requested, used)

        self.used = used

    def release(self) -> None:
        """Release the acquisition.

        Note: This method is now primarily for tracking state.
        Actual release logic is handled by LimitSet.
        """
        if self._released:
            return

        self._released = True

    def __enter__(self) -> "Acquisition":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and release the acquisition."""
        self.release()


class LimitSetAcquisition:
    """Represents an atomic acquisition of multiple limits in a LimitSet.

    Manages acquisitions for all limits in a set, enforcing update requirements
    and coordinating with LimitSet for thread-safe release of resources.

    Attributes:
        limit_set: The parent LimitSet that created this acquisition
        acquisitions: Dict mapping limit keys to individual Acquisition objects
        successful: Whether all limits were successfully acquired
        config: Static configuration dict from the parent LimitSet

    Update Requirements:
        - **RateLimits**: MUST call update() to report actual usage (raises RuntimeError if missing)
        - **CallLimit**: Automatic (no update needed, always uses 1)
        - **ResourceLimit**: Automatic (no update needed, uses requested amount)

    Partial Acquisition:
        When specifying requested dict, CallLimit and ResourceLimit are automatically
        included with default of 1. Only RateLimits need explicit specification:

        ```python
        limits = LimitSet(limits=[
            CallLimit(...),
            RateLimit(key="tokens", ...),
            ResourceLimit(key="connections", ...)
        ])

        # CallLimit auto-acquired with default of 1
        with limits.acquire(requested={"tokens": 100}) as acq:
            result = operation()
            acq.update(usage={"tokens": result.tokens})
        ```

    Thread-Safety:
        Release operations are thread-safe and coordinated through the parent LimitSet
        which holds appropriate locks during release.

    Example:
        Basic usage with update::

            limits = LimitSet(limits=[
                RateLimit(key="tokens", window_seconds=60, capacity=1000)
            ])

            with limits.acquire(requested={"tokens": 100}) as acq:
                result = call_api()
                # Required for RateLimits
                acq.update(usage={"tokens": result.actual_tokens})
            # Automatically released on exit

        Mixed limits::

            limits = LimitSet(limits=[
                CallLimit(window_seconds=60, capacity=100),
                RateLimit(key="tokens", window_seconds=60, capacity=1000),
                ResourceLimit(key="connections", capacity=10)
            ])

            with limits.acquire(requested={"tokens": 500, "connections": 2}) as acq:
                # CallLimit auto-acquired
                result = operation()
                # Only update RateLimits
                acq.update(usage={"tokens": result.tokens})
                # CallLimit and ResourceLimit are automatic

    See Also:
        - LimitSet: Creates LimitSetAcquisition instances
        - Acquisition: Individual limit tracking
        - BaseLimitSet: Parent class that manages releases
    """

    def __init__(
        self,
        limit_set: "LimitSet",
        acquisitions: Dict[str, Acquisition],
        successful: bool = True,
        config: Optional[dict] = None,
    ):
        """Initialize a limit set acquisition.

        Args:
            limit_set: The parent LimitSet
            acquisitions: Mapping of limit key to acquisition
            successful: Whether all acquisitions were successful
            config: Static configuration dict from the parent LimitSet
        """
        self.limit_set = limit_set
        self.acquisitions = acquisitions
        self.successful = successful
        # Make a copy of config to prevent mutations
        self.config = dict(config) if config is not None else {}
        self._updated_keys: Set[str] = set()
        self._released = False

    def update(self, usage: Dict[str, int]) -> None:
        """Update usage for multiple limits.

        Args:
            usage: Mapping of limit key to actual usage

        Raises:
            ValueError: If key doesn't exist or usage is invalid
            RuntimeError: If already released
        """
        if self._released:
            raise RuntimeError("Cannot update an already released acquisition")

        for key, used in usage.items():
            if key not in self.acquisitions:
                raise ValueError(
                    f"Cannot update limit '{key}': not acquired in this LimitSet. "
                    f"Available keys: {list(self.acquisitions.keys())}"
                )

            # Update the individual acquisition
            self.acquisitions[key].update(used)
            self._updated_keys.add(key)

    def _validate_updates(self) -> None:
        """Validate that all required limits were updated.

        Raises:
            RuntimeError: If required limits were not updated
        """
        # Get all limits that require explicit update
        from .limit import CallLimit, ResourceLimit

        required_updates = set()
        for key, acq in self.acquisitions.items():
            limit = acq.limit
            # RateLimits (excluding CallLimit) require updates
            # ResourceLimits and CallLimits are automatic
            if not isinstance(limit, (ResourceLimit, CallLimit)):
                required_updates.add(key)

        missing_updates = required_updates - self._updated_keys

        if len(missing_updates) > 0:
            raise RuntimeError(
                f"Not all limits in the LimitSet were updated. Missing: {sorted(missing_updates)}"
            )

    def release(self) -> None:
        """Release all acquisitions."""
        if self._released:
            return

        # Validate updates before releasing
        if self.successful:
            self._validate_updates()

        # Release via LimitSet for proper thread-safety
        # Mark acquisitions as released first
        for acq in self.acquisitions.values():
            acq._released = True

        # Call LimitSet to handle actual release with locking
        self.limit_set.release_limit_set_acquisition(self)

        self._released = True

    def __enter__(self) -> "LimitSetAcquisition":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and release all acquisitions."""
        self.release()
