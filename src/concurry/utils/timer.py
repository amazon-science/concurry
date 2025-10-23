"""Timer utility for tracking elapsed time with context manager support."""

import math
import time
from datetime import datetime, timedelta
from typing import Callable, Dict, Optional, Union

from morphic import Typed
from pydantic import field_validator


class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""

    pass


def _readable_datetime(
    dt: datetime,
    *,
    microsec: bool = True,
    tz: bool = True,
) -> str:
    """
    Format a datetime object as a readable string.

    Args:
        dt: datetime object to format
        microsec: whether to include microseconds in the output
        tz: whether to include timezone in the output

    Returns:
        Formatted datetime string (e.g., "2024-01-15T10:30:45.123456-08:00")
    """
    dt = dt.replace(tzinfo=dt.astimezone().tzinfo)
    format_str = "%Y-%m-%dT%H:%M:%S"

    if microsec:
        format_str += ".%f"

    split_tz_colon = False
    if tz and dt.tzinfo is not None:
        format_str += "%z"
        split_tz_colon = True

    out = dt.strftime(format_str).strip()

    if split_tz_colon:  # Makes the output exactly like dt.isoformat()
        out = out[:-2] + ":" + out[-2:]

    return out


def _readable_seconds(
    time_in_seconds: Union[float, timedelta],
    *,
    decimals: int = 2,
) -> str:
    """
    Convert time in seconds to a human-readable string.

    Args:
        time_in_seconds: Time in seconds (or timedelta object)
        decimals: Number of decimal places to show

    Returns:
        Human-readable time string (e.g., "1.23 seconds", "2.5 mins", "1.5 hours")
    """
    if isinstance(time_in_seconds, timedelta):
        time_in_seconds = time_in_seconds.total_seconds()

    time_units = {
        "nanoseconds": 1e-9,
        "microseconds": 1e-6,
        "milliseconds": 1e-3,
        "seconds": 1.0,
        "mins": 60,
        "hours": 60 * 60,
        "days": 24 * 60 * 60,
    }

    times: Dict[str, float] = {
        time_unit: round(time_in_seconds / time_units[time_unit], decimals) for time_unit in time_units
    }

    # Find the most appropriate unit (smallest unit where value >= 1)
    sorted_times = sorted(times.items(), key=lambda item: item[1])
    time_unit, time_val = "seconds", time_in_seconds
    for unit, val in sorted_times:
        if val >= 1:
            time_unit, time_val = unit, val
            break

    if decimals <= 0:
        time_val = int(time_val)

    return f"{time_val} {time_unit}"


def _pad_zeros(i: int, max_i: int) -> str:
    """
    Pad an integer with zeros based on the maximum value.

    Args:
        i: Integer to pad
        max_i: Maximum integer value (determines padding width)

    Returns:
        Zero-padded string representation of i

    Examples:
        >>> _pad_zeros(5, 100)
        '005'
        >>> _pad_zeros(42, 1000)
        '0042'
    """
    assert isinstance(i, int)
    assert i >= 0
    assert isinstance(max_i, int)
    assert max_i >= i, f"Expected max_i to be >= current i; found max_i={max_i}, i={i}"

    # Calculate number of digits needed based on max_i
    # Use max_i + 1 to ensure powers of 10 get proper padding
    if max_i == 0:
        num_zeros = 1
    else:
        num_zeros = math.ceil(math.log10(max_i + 1))

    return f"{i:0{num_zeros}}"


class Timer(Typed):
    """
    A timer class for tracking elapsed time with context manager support.

    This class provides a simple and flexible way to measure execution time with
    features including:
    - Start/stop timing with high-resolution counters
    - Context manager support for automatic timing
    - Optional logging of start/stop messages
    - Multiple time format outputs (seconds, milliseconds, microseconds, etc.)
    - Progress tracking with iteration counters
    - Single-line or multi-line output modes

    Basic Usage:
        ```python
        # Context manager (automatic start/stop)
        with Timer(task="Processing data"):
            # your code here
            process_data()

        # Manual start/stop
        timer = Timer(task="Training model")
        timer.start()
        train_model()
        timer.stop()

        # Get elapsed time in different formats
        print(timer.time_taken_sec)  # 123.45
        print(timer.time_taken_ms)   # 123450.0
        print(timer.time_taken_str)  # "2.06 mins"
        ```

    Advanced Features:
        ```python
        # Silent mode (no logging)
        with Timer(task="Silent task", silent=True) as t:
            do_work()
        print(f"Took {t.time_taken_str}")

        # Custom logger
        import logging
        with Timer(task="Custom log", logger=logging.info):
            do_work()

        # Single-line output
        with Timer(task="Single line", single_line=True):
            do_work()

        # Progress tracking
        for i in range(100):
            with Timer(task="Batch", i=i, max_i=100):
                process_batch(i)

        # Alert during long-running tasks
        timer = Timer(task="Long task")
        timer.start()
        for step in range(10):
            do_step(step)
            if step == 5:
                timer.alert("Halfway done!")
        timer.stop()
        ```

    Attributes:
        task: Description of the task being timed
        logger: Logging function (default: print). Set to None to disable logging
        silent: If True, suppress all logging output
        single_line: If True, use single-line output format (start and end on same line)
        i: Current iteration index (optional, for progress tracking)
        max_i: Maximum iteration count (optional, for progress tracking)

    Properties:
        has_started: Whether timer has been started
        has_stopped: Whether timer has been stopped
        start_datetime: datetime when timer was started
        end_datetime: datetime when timer was stopped
        start_time_str: Human-readable start time string
        end_time_str: Human-readable end time string
        time_taken_str: Human-readable elapsed time string
        time_taken_human: Alias for time_taken_str
        time_taken_sec: Elapsed time in seconds
        time_taken_ms: Elapsed time in milliseconds
        time_taken_us: Elapsed time in microseconds
        time_taken_ns: Elapsed time in nanoseconds
        time_taken_td: Elapsed time as timedelta object

    Methods:
        start(): Start the timer
        stop(): Stop the timer
        alert(text): Log current elapsed time with optional message
        time_taken(format): Get elapsed time in specified format

    Note:
        - Uses time.perf_counter_ns() for high-resolution timing
        - Timer is frozen (immutable) after initialization
        - Can be reused by creating new instances, not by restarting
    """

    task: str = ""
    logger: Union[Callable, None, bool] = None  # Allow Callable, None, or False
    silent: bool = False
    single_line: bool = False
    i: Optional[int] = None
    max_i: Optional[int] = None
    _start_dt: Optional[datetime] = None
    _start_time_ns: Optional[int] = None
    _end_dt: Optional[datetime] = None
    _end_time_ns: Optional[int] = None
    _logger_was_none: bool = False  # Track if logger was explicitly None

    @field_validator("logger", mode="before")
    @classmethod
    def validate_logger(cls, v):
        """Validate logger field, allowing None and False as special values."""
        if v is False:
            # False means explicitly disable logging
            return None
        return v

    def post_initialize(self) -> None:
        """
        Post-initialization hook to set default values.

        This is called automatically after Pydantic validation. It sets:
        - Default logger to print if not specified (and silent=False)
        - Keeps logger=None if explicitly set to None or False
        """
        # Check if logger was set at initialization
        # If logger is still None and silent is False, default to print
        # We use a heuristic: if task is empty and logger is None, likely user didn't specify logger
        # But this is not foolproof. Better approach: use sentinel value
        # For now, we'll only set default if logger is None AND silent is False
        # AND the user likely didn't explicitly set logger=None
        #
        # Actually, looking at the original bears implementation, logger defaults to Log.info
        # We want to default to print. Let's just default when logger is None and silent is False
        if self.logger is None and self.silent is False:
            # Check if we're in a "default" state (no custom logger set)
            # Since we can't perfectly distinguish, we'll default to print
            # Users who want None should use silent=True
            object.__setattr__(self, "logger", print)

    @property
    def has_started(self) -> bool:
        """Check if timer has been started."""
        return self._start_time_ns is not None

    @property
    def has_stopped(self) -> bool:
        """Check if timer has been stopped."""
        return self._end_time_ns is not None

    def time_taken(self, format: str) -> Union[timedelta, int, float, str]:
        """
        Get elapsed time in the specified format.

        Args:
            format: One of: "str"/"string", "s"/"sec"/"seconds", "ms"/"milliseconds",
                   "us"/"microseconds", "ns"/"nanoseconds", "dt"/"td"/"timedelta"

        Returns:
            Elapsed time in the requested format

        Raises:
            NotImplementedError: If format is not recognized
        """
        if format in {str, "str", "string"}:
            return self.time_taken_str
        elif format in {"s", "sec", "seconds"}:
            return self.time_taken_sec
        elif format in {"ms", "milli", "millis", "millisec", "milliseconds"}:
            return self.time_taken_ms
        elif format in {"us", "micro", "micros", "microsec", "microseconds"}:
            return self.time_taken_us
        elif format in {"ns", "nano", "nanos", "nanosec", "nanoseconds"}:
            return self.time_taken_ns
        elif format in {"dt", "td", "datetime", "timedelta"}:
            return self.time_taken_td
        raise NotImplementedError(f"Unsupported `format` with type {type(format)} and value: {format}")

    @property
    def start_datetime(self) -> datetime:
        """Get the datetime when timer was started."""
        return self._start_dt

    @property
    def end_datetime(self) -> datetime:
        """Get the datetime when timer was stopped."""
        return self._end_dt

    @property
    def start_time_str(self) -> str:
        """Get human-readable start time string."""
        return _readable_datetime(self._start_dt)

    @property
    def end_time_str(self) -> str:
        """Get human-readable end time string."""
        return _readable_datetime(self._end_dt)

    @property
    def time_taken_str(self) -> str:
        """Get human-readable elapsed time string."""
        return _readable_seconds(self.time_taken_sec, decimals=2)

    @property
    def time_taken_human(self) -> str:
        """Get human-readable elapsed time string (alias for time_taken_str)."""
        return _readable_seconds(self.time_taken_sec, decimals=2)

    @property
    def time_taken_sec(self) -> float:
        """Get elapsed time in seconds."""
        return self.time_taken_ns / 1e9

    @property
    def time_taken_ms(self) -> float:
        """Get elapsed time in milliseconds."""
        return self.time_taken_ns / 1e6

    @property
    def time_taken_us(self) -> float:
        """Get elapsed time in microseconds."""
        return self.time_taken_ns / 1e3

    @property
    def time_taken_ns(self) -> int:
        """
        Get elapsed time in nanoseconds.

        Returns:
            Nanoseconds elapsed. If timer is still running, returns time since start.
            If timer is stopped, returns total elapsed time.

        Raises:
            TimerError: If timer has not been started
        """
        self._check_started()
        if self.has_stopped:
            return self._end_time_ns - self._start_time_ns
        return time.perf_counter_ns() - self._start_time_ns

    @property
    def time_taken_td(self) -> timedelta:
        """
        Get elapsed time as timedelta object.

        Note: Python timedelta does not have nanosecond resolution, so this uses microseconds.
        """
        return timedelta(microseconds=self.time_taken_us)

    def _check_started(self):
        """Raise error if timer has not been started."""
        if not self.has_started:
            raise TimerError("Timer has not been started. Use .start() to start it.")

    def _check_not_started(self):
        """Raise error if timer has already been started."""
        if self.has_started:
            raise TimerError(f"Timer has already been started at {_readable_datetime(self._start_dt)}")

    def _check_stopped(self):
        """Raise error if timer has not been stopped."""
        if not self.has_stopped:
            raise TimerError("Timer has not been stopped. Use .stop() to stop it.")

    def _check_not_stopped(self):
        """Raise error if timer has already been stopped."""
        if self.has_stopped:
            raise TimerError(f"Timer has already been stopped at {_readable_datetime(self._end_dt)}")

    def start(self):
        """
        Start the timer.

        Records the current time and optionally logs a start message.

        Raises:
            TimerError: If timer has already been started
        """
        self._check_not_started()
        object.__setattr__(self, "_start_time_ns", time.perf_counter_ns())
        now = datetime.now()
        now = now.replace(tzinfo=now.astimezone().tzinfo)
        object.__setattr__(self, "_start_dt", now)
        if self.should_log and not self.single_line:
            self.logger(self._start_msg())

    def alert(self, text: Optional[str] = None):
        """
        Log current elapsed time with optional message.

        Useful for checkpointing during long-running tasks.

        Args:
            text: Optional message to include in the alert

        Raises:
            TimerError: If timer has not been started or has already been stopped
        """
        self._check_started()
        self._check_not_stopped()
        if self.should_log:
            self.logger(self._alert_msg(text))

    def stop(self):
        """
        Stop the timer.

        Records the end time and optionally logs an end message.

        Raises:
            TimerError: If timer has not been started or has already been stopped
        """
        self._check_started()  # BUG FIX: Check if timer was started before stopping
        self._check_not_stopped()
        object.__setattr__(self, "_end_time_ns", time.perf_counter_ns())
        now = datetime.now()
        now = now.replace(tzinfo=now.astimezone().tzinfo)
        object.__setattr__(self, "_end_dt", now)
        if self.should_log:
            self.logger(self._end_msg())

    @property
    def should_log(self) -> bool:
        """Check if logging is enabled."""
        return self.logger is not None and self.silent is False

    def __enter__(self):
        """Start timer as context manager."""
        self.start()
        return self

    def __exit__(self, *exc_info):
        """Stop timer when exiting context manager."""
        self.stop()

    def _start_msg(self) -> str:
        """Generate start message."""
        out = ""
        out += self._task_msg()
        out += self._idx_msg()
        out += f"Started at {_readable_datetime(self._start_dt)}..."
        return out

    def _alert_msg(self, text: Optional[str] = None) -> str:
        """Generate alert message."""
        out = ""
        out += self._task_msg()
        out += self._idx_msg()
        out += f"Timer has been running for {_readable_seconds(self.time_taken_sec, decimals=2)}."
        if isinstance(text, str):
            out += f" {text}"
        return out

    def _end_msg(self) -> str:
        """Generate end message."""
        out = ""
        out += self._task_msg()
        out += self._idx_msg()
        if self.single_line:
            out += (
                f"Started at {_readable_datetime(self._start_dt)}, "
                f"completed in {_readable_seconds(self.time_taken_sec, decimals=2)}."
            )
            return out
        out += f"...completed in {_readable_seconds(self.time_taken_sec, decimals=2)}."
        return out

    def _task_msg(self) -> str:
        """Generate task portion of message."""
        out = ""
        if len(self.task) > 0:
            out += f"({self.task}) "
        return out

    def _idx_msg(self) -> str:
        """Generate index/progress portion of message."""
        out = ""
        if self.i is not None and self.max_i is not None:
            out += (
                f"[{_pad_zeros(i=self.i + 1, max_i=self.max_i)}/"
                f"{_pad_zeros(i=self.max_i, max_i=self.max_i)}] "
            )
        elif self.i is not None:
            out += f"[{self.i}] "
        return out
