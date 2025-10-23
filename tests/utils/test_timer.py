"""Comprehensive tests for Timer class."""

import io
import time
from contextlib import redirect_stdout
from datetime import datetime, timedelta

import pytest

from concurry.utils.timer import Timer, TimerError, _pad_zeros, _readable_datetime, _readable_seconds


class TestHelperFunctions:
    """Test helper functions used by Timer."""

    def test_pad_zeros_basic(self):
        """Test basic zero padding functionality."""
        assert _pad_zeros(5, 100) == "005"
        assert _pad_zeros(42, 1000) == "0042"
        assert _pad_zeros(999, 1000) == "0999"
        assert _pad_zeros(1, 10) == "01"
        assert _pad_zeros(9, 10) == "09"

    def test_pad_zeros_edge_cases(self):
        """Test edge cases for zero padding."""
        # Single digit max
        assert _pad_zeros(0, 5) == "0"
        assert _pad_zeros(5, 5) == "5"

        # Power of 10 boundaries (fixed: proper padding for powers of 10)
        assert _pad_zeros(10, 10) == "10"  # 10 needs 2 digits
        assert _pad_zeros(9, 10) == "09"  # 9 needs 2 digits
        assert _pad_zeros(100, 100) == "100"  # 100 needs 3 digits
        assert _pad_zeros(99, 100) == "099"  # 99 needs 3 digits
        assert _pad_zeros(1000, 1000) == "1000"  # 1000 needs 4 digits

        # Large numbers
        assert _pad_zeros(123456, 1000000) == "0123456"

    def test_pad_zeros_validation(self):
        """Test validation in pad_zeros."""
        with pytest.raises(AssertionError):
            _pad_zeros(10, 5)  # i > max_i

        with pytest.raises(AssertionError):
            _pad_zeros(-1, 10)  # negative i

    def test_readable_datetime_basic(self):
        """Test basic datetime formatting."""
        dt = datetime(2024, 1, 15, 10, 30, 45, 123456)
        dt = dt.replace(tzinfo=dt.astimezone().tzinfo)

        result = _readable_datetime(dt)
        assert "2024-01-15T10:30:45" in result
        assert "123456" in result  # microseconds

    def test_readable_datetime_no_microseconds(self):
        """Test datetime formatting without microseconds."""
        dt = datetime(2024, 1, 15, 10, 30, 45, 123456)
        dt = dt.replace(tzinfo=dt.astimezone().tzinfo)

        result = _readable_datetime(dt, microsec=False)
        assert "2024-01-15T10:30:45" in result
        assert "123456" not in result  # no microseconds

    def test_readable_seconds_basic(self):
        """Test basic seconds formatting."""
        # Nanoseconds
        assert "nanoseconds" in _readable_seconds(1e-9)

        # Microseconds
        assert "microseconds" in _readable_seconds(1e-6)

        # Milliseconds
        assert "milliseconds" in _readable_seconds(1e-3)

        # Seconds
        result = _readable_seconds(1.5)
        assert "1.5" in result
        assert "seconds" in result

        # Minutes
        result = _readable_seconds(90)
        assert "1.5" in result
        assert "mins" in result

        # Hours
        result = _readable_seconds(3600)
        assert "1.0" in result
        assert "hours" in result

        # Days
        result = _readable_seconds(86400)
        assert "1.0" in result
        assert "days" in result

    def test_readable_seconds_timedelta(self):
        """Test seconds formatting with timedelta input."""
        td = timedelta(seconds=90)
        result = _readable_seconds(td)
        assert "1.5" in result
        assert "mins" in result

    def test_readable_seconds_decimals(self):
        """Test decimal places in seconds formatting."""
        result = _readable_seconds(1.23456, decimals=3)
        assert "1.235" in result or "1.234" in result  # rounding

        result = _readable_seconds(1.23456, decimals=0)
        assert "1 seconds" in result


class TestTimerBasic:
    """Test basic Timer functionality."""

    def test_timer_creation_default(self):
        """Test creating Timer with default parameters."""
        timer = Timer()
        assert timer.task == ""
        assert timer.logger is not None  # Should default to print
        assert timer.silent is False
        assert timer.single_line is False
        assert timer.i is None
        assert timer.max_i is None
        assert not timer.has_started
        assert not timer.has_stopped

    def test_timer_creation_with_task(self):
        """Test creating Timer with task name."""
        timer = Timer(task="Test task")
        assert timer.task == "Test task"

    def test_timer_creation_with_logger(self):
        """Test creating Timer with custom logger."""

        def custom_logger(msg):
            pass

        timer = Timer(logger=custom_logger)
        assert timer.logger is custom_logger

    def test_timer_creation_silent(self):
        """Test creating Timer in silent mode."""
        timer = Timer(silent=True)
        assert timer.silent is True
        assert not timer.should_log

    def test_timer_creation_logger_none(self):
        """Test creating Timer with logger=None (defaults to print when silent=False)."""
        # When logger=None and silent=False, it defaults to print
        timer = Timer(logger=None)
        assert timer.logger is print  # Defaults to print
        assert timer.should_log

        # To truly disable logging, use silent=True
        timer_silent = Timer(logger=None, silent=True)
        assert not timer_silent.should_log

    def test_timer_creation_logger_false(self):
        """Test creating Timer with logger=False (disables logging)."""
        timer = Timer(logger=False, silent=False)
        # logger=False converts to None and stays None even with silent=False
        # Actually, looking at the validator, logger=False converts to None
        # But post_initialize will set it to print if silent=False
        # So this case is the same as logger=None
        assert timer.logger is print  # Defaults to print when silent=False

        # To disable logging with logger=False, also set silent=True
        timer_silent = Timer(logger=False, silent=True)
        assert timer_silent.logger is None or timer_silent.silent
        assert not timer_silent.should_log

    def test_timer_creation_single_line(self):
        """Test creating Timer with single_line mode."""
        timer = Timer(single_line=True)
        assert timer.single_line is True

    def test_timer_creation_with_index(self):
        """Test creating Timer with iteration index."""
        timer = Timer(i=5, max_i=10)
        assert timer.i == 5
        assert timer.max_i == 10


class TestTimerStartStop:
    """Test Timer start/stop functionality."""

    def test_start_basic(self):
        """Test starting timer."""
        timer = Timer(silent=True)
        timer.start()
        assert timer.has_started
        assert not timer.has_stopped
        assert timer._start_time_ns is not None
        assert timer._start_dt is not None

    def test_start_twice_raises_error(self):
        """Test that starting timer twice raises error."""
        timer = Timer(silent=True)
        timer.start()
        with pytest.raises(TimerError, match="already been started"):
            timer.start()

    def test_stop_basic(self):
        """Test stopping timer."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)
        timer.stop()
        assert timer.has_started
        assert timer.has_stopped
        assert timer._end_time_ns is not None
        assert timer._end_dt is not None

    def test_stop_twice_raises_error(self):
        """Test that stopping timer twice raises error."""
        timer = Timer(silent=True)
        timer.start()
        timer.stop()
        with pytest.raises(TimerError, match="already been stopped"):
            timer.stop()

    def test_stop_without_start_raises_error(self):
        """Test that stopping timer without starting raises error."""
        timer = Timer(silent=True)
        with pytest.raises(TimerError, match="not been started"):
            timer.stop()

    def test_start_logs_message(self):
        """Test that starting timer logs message."""
        output = io.StringIO()
        with redirect_stdout(output):
            timer = Timer(task="Test")
            timer.start()

        log_output = output.getvalue()
        assert "Test" in log_output
        assert "Started at" in log_output

    def test_stop_logs_message(self):
        """Test that stopping timer logs message."""
        output = io.StringIO()
        with redirect_stdout(output):
            timer = Timer(task="Test")
            timer.start()
            time.sleep(0.01)
            timer.stop()

        log_output = output.getvalue()
        assert "completed in" in log_output

    def test_single_line_mode(self):
        """Test single-line logging mode."""
        output = io.StringIO()
        with redirect_stdout(output):
            timer = Timer(task="Test", single_line=True)
            timer.start()
            time.sleep(0.01)
            timer.stop()

        log_output = output.getvalue()
        # In single-line mode, only stop message is logged
        assert log_output.count("\n") == 1  # Only one line
        assert "Started at" in log_output
        assert "completed in" in log_output


class TestTimerAlert:
    """Test Timer alert functionality."""

    def test_alert_basic(self):
        """Test basic alert functionality."""
        output = io.StringIO()
        with redirect_stdout(output):
            timer = Timer(task="Test")
            timer.start()
            time.sleep(0.01)
            timer.alert()

        log_output = output.getvalue()
        assert "Timer has been running for" in log_output

    def test_alert_with_message(self):
        """Test alert with custom message."""
        output = io.StringIO()
        with redirect_stdout(output):
            timer = Timer(task="Test")
            timer.start()
            time.sleep(0.01)
            timer.alert("Checkpoint reached")

        log_output = output.getvalue()
        assert "Timer has been running for" in log_output
        assert "Checkpoint reached" in log_output

    def test_alert_without_start_raises_error(self):
        """Test that alert without start raises error."""
        timer = Timer(silent=True)
        with pytest.raises(TimerError, match="not been started"):
            timer.alert()

    def test_alert_after_stop_raises_error(self):
        """Test that alert after stop raises error."""
        timer = Timer(silent=True)
        timer.start()
        timer.stop()
        with pytest.raises(TimerError, match="already been stopped"):
            timer.alert()


class TestTimerContextManager:
    """Test Timer context manager functionality."""

    def test_context_manager_basic(self):
        """Test basic context manager usage."""
        timer = Timer(silent=True)
        assert not timer.has_started

        with timer:
            assert timer.has_started
            assert not timer.has_stopped
            time.sleep(0.01)

        assert timer.has_started
        assert timer.has_stopped

    def test_context_manager_with_logging(self):
        """Test context manager with logging."""
        output = io.StringIO()
        with redirect_stdout(output):
            with Timer(task="Context test"):
                time.sleep(0.01)

        log_output = output.getvalue()
        assert "Context test" in log_output
        assert "Started at" in log_output
        assert "completed in" in log_output

    def test_context_manager_returns_timer(self):
        """Test that context manager returns timer instance."""
        with Timer(silent=True) as t:
            assert isinstance(t, Timer)
            assert t.has_started

    def test_context_manager_stops_on_exception(self):
        """Test that timer stops even when exception is raised."""
        timer = Timer(silent=True)
        try:
            with timer:
                time.sleep(0.01)
                raise ValueError("Test error")
        except ValueError:
            pass

        assert timer.has_started
        assert timer.has_stopped


class TestTimerTimeMeasurement:
    """Test Timer time measurement functionality."""

    def test_time_taken_ns_basic(self):
        """Test nanosecond time measurement."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)
        timer.stop()

        # Should be around 10 million nanoseconds (10ms)
        assert timer.time_taken_ns > 5_000_000  # at least 5ms
        assert timer.time_taken_ns < 50_000_000  # less than 50ms

    def test_time_taken_ns_without_start_raises_error(self):
        """Test that accessing time_taken_ns without start raises error."""
        timer = Timer(silent=True)
        with pytest.raises(TimerError, match="not been started"):
            _ = timer.time_taken_ns

    def test_time_taken_ns_while_running(self):
        """Test that time_taken_ns works while timer is running."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)

        # Should be able to check time while running
        elapsed1 = timer.time_taken_ns
        assert elapsed1 > 0

        time.sleep(0.01)
        elapsed2 = timer.time_taken_ns
        assert elapsed2 > elapsed1  # Time should increase

    def test_time_taken_sec(self):
        """Test second time measurement."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)
        timer.stop()

        assert timer.time_taken_sec > 0.005  # at least 5ms
        assert timer.time_taken_sec < 0.05  # less than 50ms

    def test_time_taken_ms(self):
        """Test millisecond time measurement."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)
        timer.stop()

        assert timer.time_taken_ms > 5  # at least 5ms
        assert timer.time_taken_ms < 50  # less than 50ms

    def test_time_taken_us(self):
        """Test microsecond time measurement."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)
        timer.stop()

        assert timer.time_taken_us > 5000  # at least 5000us (5ms)
        assert timer.time_taken_us < 50000  # less than 50000us (50ms)

    def test_time_taken_td(self):
        """Test timedelta time measurement."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)
        timer.stop()

        td = timer.time_taken_td
        assert isinstance(td, timedelta)
        assert td.total_seconds() > 0.005
        assert td.total_seconds() < 0.05

    def test_time_taken_str(self):
        """Test human-readable time string."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)
        timer.stop()

        time_str = timer.time_taken_str
        assert isinstance(time_str, str)
        assert "milliseconds" in time_str or "seconds" in time_str

    def test_time_taken_human(self):
        """Test human-readable time string (alias)."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)
        timer.stop()

        assert timer.time_taken_human == timer.time_taken_str

    def test_time_taken_method_string(self):
        """Test time_taken() method with string format."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)
        timer.stop()

        assert timer.time_taken("str") == timer.time_taken_str
        assert timer.time_taken("string") == timer.time_taken_str

    def test_time_taken_method_seconds(self):
        """Test time_taken() method with seconds format."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)
        timer.stop()

        assert timer.time_taken("s") == timer.time_taken_sec
        assert timer.time_taken("sec") == timer.time_taken_sec
        assert timer.time_taken("seconds") == timer.time_taken_sec

    def test_time_taken_method_milliseconds(self):
        """Test time_taken() method with milliseconds format."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)
        timer.stop()

        assert timer.time_taken("ms") == timer.time_taken_ms
        assert timer.time_taken("milliseconds") == timer.time_taken_ms

    def test_time_taken_method_microseconds(self):
        """Test time_taken() method with microseconds format."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)
        timer.stop()

        assert timer.time_taken("us") == timer.time_taken_us
        assert timer.time_taken("microseconds") == timer.time_taken_us

    def test_time_taken_method_nanoseconds(self):
        """Test time_taken() method with nanoseconds format."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)
        timer.stop()

        assert timer.time_taken("ns") == timer.time_taken_ns
        assert timer.time_taken("nanoseconds") == timer.time_taken_ns

    def test_time_taken_method_timedelta(self):
        """Test time_taken() method with timedelta format."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)
        timer.stop()

        assert timer.time_taken("td") == timer.time_taken_td
        assert timer.time_taken("timedelta") == timer.time_taken_td

    def test_time_taken_method_invalid_format(self):
        """Test time_taken() method with invalid format."""
        timer = Timer(silent=True)
        timer.start()
        timer.stop()

        with pytest.raises(NotImplementedError):
            timer.time_taken("invalid_format")


class TestTimerDatetimeProperties:
    """Test Timer datetime properties."""

    def test_start_datetime(self):
        """Test start_datetime property."""
        timer = Timer(silent=True)
        before = datetime.now().replace(tzinfo=datetime.now().astimezone().tzinfo)
        timer.start()
        after = datetime.now().replace(tzinfo=datetime.now().astimezone().tzinfo)

        assert isinstance(timer.start_datetime, datetime)
        assert before <= timer.start_datetime <= after

    def test_end_datetime(self):
        """Test end_datetime property."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.01)
        before = datetime.now().replace(tzinfo=datetime.now().astimezone().tzinfo)
        timer.stop()
        after = datetime.now().replace(tzinfo=datetime.now().astimezone().tzinfo)

        assert isinstance(timer.end_datetime, datetime)
        assert before <= timer.end_datetime <= after

    def test_start_time_str(self):
        """Test start_time_str property."""
        timer = Timer(silent=True)
        timer.start()

        time_str = timer.start_time_str
        assert isinstance(time_str, str)
        assert "T" in time_str  # ISO format includes T

    def test_end_time_str(self):
        """Test end_time_str property."""
        timer = Timer(silent=True)
        timer.start()
        timer.stop()

        time_str = timer.end_time_str
        assert isinstance(time_str, str)
        assert "T" in time_str  # ISO format includes T


class TestTimerProgressTracking:
    """Test Timer progress tracking with iteration counters."""

    def test_progress_with_index(self):
        """Test timer with iteration index."""
        output = io.StringIO()
        with redirect_stdout(output):
            timer = Timer(task="Batch", i=5, max_i=10)
            timer.start()
            timer.stop()

        log_output = output.getvalue()
        assert "Batch" in log_output
        assert "[06/10]" in log_output or "[6/10]" in log_output  # i+1 (0-indexed)

    def test_progress_multiple_batches(self):
        """Test timer with multiple batches."""
        output = io.StringIO()
        with redirect_stdout(output):
            for i in range(3):
                with Timer(task="Batch", i=i, max_i=3, single_line=True):
                    time.sleep(0.001)

        log_output = output.getvalue()
        assert "[1/3]" in log_output or "[01/03]" in log_output
        assert "[2/3]" in log_output or "[02/03]" in log_output
        assert "[3/3]" in log_output or "[03/03]" in log_output

    def test_progress_only_i(self):
        """Test timer with only iteration index (no max_i)."""
        output = io.StringIO()
        with redirect_stdout(output):
            timer = Timer(task="Batch", i=5)
            timer.start()
            timer.stop()

        log_output = output.getvalue()
        assert "[5]" in log_output


class TestTimerEdgeCases:
    """Test Timer edge cases and error conditions."""

    def test_very_short_duration(self):
        """Test timer with very short duration."""
        timer = Timer(silent=True)
        timer.start()
        # Don't sleep, just stop immediately
        timer.stop()

        # Should still measure some time (even if very small)
        assert timer.time_taken_ns >= 0
        assert timer.time_taken_sec >= 0

    def test_long_duration(self):
        """Test timer with longer duration."""
        timer = Timer(silent=True)
        timer.start()
        time.sleep(0.1)  # 100ms
        timer.stop()

        assert timer.time_taken_sec > 0.09  # at least 90ms
        assert timer.time_taken_ms > 90

    def test_empty_task_name(self):
        """Test timer with empty task name."""
        output = io.StringIO()
        with redirect_stdout(output):
            with Timer():
                time.sleep(0.001)

        log_output = output.getvalue()
        # Should not have task name in output
        assert log_output.startswith("Started at") or "Started at" in log_output

    def test_custom_logger_called(self):
        """Test that custom logger is called."""
        log_messages = []

        def custom_logger(msg):
            log_messages.append(msg)

        with Timer(task="Test", logger=custom_logger):
            time.sleep(0.001)

        assert len(log_messages) == 2  # start and stop messages
        assert "Test" in log_messages[0]
        assert "Test" in log_messages[1]

    def test_timer_immutability(self):
        """Test that Timer is immutable (frozen)."""
        timer = Timer(task="Test")

        # Should not be able to modify public attributes (raises ValidationError from Pydantic)
        from pydantic_core import ValidationError

        with pytest.raises(ValidationError):
            timer.task = "Modified"

    def test_timer_reuse_not_possible(self):
        """Test that timer cannot be reused (must create new instance)."""
        timer = Timer(silent=True)
        timer.start()
        timer.stop()

        # Cannot start again
        with pytest.raises(TimerError, match="already been started"):
            timer.start()


class TestTimerRealWorldScenarios:
    """Test Timer in real-world usage scenarios."""

    def test_nested_timers(self):
        """Test nested timer usage."""
        output = io.StringIO()
        with redirect_stdout(output):
            with Timer(task="Outer"):
                time.sleep(0.001)
                with Timer(task="Inner"):
                    time.sleep(0.001)
                time.sleep(0.001)

        log_output = output.getvalue()
        assert "Outer" in log_output
        assert "Inner" in log_output

    def test_multiple_sequential_timers(self):
        """Test multiple sequential timers."""
        timers = []
        for i in range(3):
            timer = Timer(silent=True)
            timer.start()
            time.sleep(0.001)
            timer.stop()
            timers.append(timer)

        # All timers should have measured time
        for timer in timers:
            assert timer.has_started
            assert timer.has_stopped
            assert timer.time_taken_sec > 0

    def test_timer_with_exception_handling(self):
        """Test timer with exception handling."""
        timer = Timer(silent=True)
        timer.start()

        try:
            time.sleep(0.001)
            raise ValueError("Test error")
        except ValueError:
            timer.stop()

        assert timer.has_stopped
        assert timer.time_taken_sec > 0

    def test_progress_bar_simulation(self):
        """Test simulating progress bar with timer."""
        output = io.StringIO()
        with redirect_stdout(output):
            total = 5
            for i in range(total):
                with Timer(task="Processing", i=i, max_i=total, single_line=True):
                    time.sleep(0.001)

        log_output = output.getvalue()
        # Should have output for each iteration
        assert log_output.count("Processing") == total

    def test_benchmarking_scenario(self):
        """Test using timer for benchmarking."""
        results = {}

        for algo_name in ["algo1", "algo2"]:
            timer = Timer(silent=True)
            timer.start()
            time.sleep(0.001)  # Simulate algorithm execution
            timer.stop()
            results[algo_name] = timer.time_taken_sec

        # Both should have recorded times
        assert len(results) == 2
        assert all(t > 0 for t in results.values())
