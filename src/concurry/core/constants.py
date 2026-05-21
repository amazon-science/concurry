"""Constants and enumerations for concurry."""

from morphic import AutoEnum, alias, auto

# Environment variable names for configuration
ENV_MAX_THREADS = "CONCURRY_MAX_THREADS"
ENV_MAX_PROCESSES = "CONCURRY_MAX_PROCESSES"


class ExecutionMode(AutoEnum):
    """Execution modes supported by concurry."""

    Auto = auto()  # Auto-detect best mode based on function characteristics
    Sync = alias("synchronous")  # Synchronous execution (no parallelism)
    Asyncio = alias("async", "asynchronous")  # AsyncIO execution (good for I/O)
    Threads = alias("thread")  # Thread-based execution (good for I/O bound tasks)
    Processes = alias("proc", "procs", "process")  # Process-based execution (good for CPU bound tasks)
    Ray = auto()  # Ray distributed execution (good for distributed tasks)


class LoadBalancingAlgorithm(AutoEnum):
    """Load balancing algorithms for worker pools."""

    RoundRobin = alias("rr")  # Distribute requests in round-robin fashion
    LeastActiveLoad = alias("active")  # Select worker with fewest active calls
    LeastTotalLoad = alias("total")  # Select worker with fewest total calls
    Random = alias("rand")  # Random worker selection


class RateLimitAlgorithm(AutoEnum):
    """Rate limiting algorithms."""

    TokenBucket = alias("token")
    LeakyBucket = alias("leaky")
    SlidingWindow = alias("sliding")
    FixedWindow = alias("fixed")
    GCRA = alias("Generic cell rate algorithm", "Generic cell rate")


class RateWindow(AutoEnum):
    """Named rate-limit windows. Use as a shorthand for ``window`` seconds.

    Each member maps to a fixed number of seconds. Pass a member or one of its
    aliases (singular ``"minute"``, plural ``"minutes"``, abbreviation ``"min"``,
    abbreviated plural ``"mins"``, or any ``"per_..."``-prefixed form like
    ``"per_minute"`` / ``"per_mins"``) to :class:`RateLimit` / :class:`CallLimit`
    via the ``window`` constructor field. Aliases are case-insensitive.

    Members:
        Secondly: 1s window
        Minutely: 60s window
        Hourly: 3600s window
        Daily: 86400s window (24h)
        Weekly: 604800s window (7 days)

    Why no ``Monthly``: months have varying lengths (28-31 days), so a "monthly"
    window has no single correct value in seconds. Express monthly limits
    explicitly in seconds (e.g. ``window=30 * 86400``) or in days
    (``window=Daily``, capacity = 30x your daily cap).
    """

    Secondly = alias(
        # canonical singular & plural
        "second",
        "seconds",
        # short forms
        "sec",
        "secs",
        # per_-prefixed
        "per_second",
        "per_seconds",
        "per_sec",
        "per_secs",
    )
    Minutely = alias(
        "minute",
        "minutes",
        "min",
        "mins",
        "per_minute",
        "per_minutes",
        "per_min",
        "per_mins",
    )
    Hourly = alias(
        "hour",
        "hours",
        "hr",
        "hrs",
        "per_hour",
        "per_hours",
        "per_hr",
        "per_hrs",
    )
    Daily = alias(
        "day",
        "days",
        "per_day",
        "per_days",
    )
    Weekly = alias(
        "week",
        "weeks",
        "wk",
        "wks",
        "per_week",
        "per_weeks",
        "per_wk",
        "per_wks",
    )

    def to_seconds(self) -> float:
        """Convert the rate window to seconds."""
        return {
            RateWindow.Secondly: 1.0,
            RateWindow.Minutely: 60.0,
            RateWindow.Hourly: 3600.0,
            RateWindow.Daily: 86400.0,
            RateWindow.Weekly: 604800.0,
        }[self]


class RetryAlgorithm(AutoEnum):
    """Retry backoff strategies.

    Attributes:
        Linear: Wait time increases linearly (wait * attempt)
        Exponential: Wait time doubles each attempt (wait * 2^attempt)
        Fibonacci: Wait time follows Fibonacci sequence
    """

    Linear = auto()
    Exponential = auto()
    Fibonacci = auto()


class PollingAlgorithm(AutoEnum):
    """Polling strategies for checking future completion.

    Attributes:
        Fixed: Constant polling interval (predictable, simple)
        Adaptive: Adapts based on completion rate (recommended default)
        Exponential: Exponential backoff (good for slow operations)
        Progressive: Progressive steps with fixed levels (balanced approach)
    """

    Fixed = auto()
    Adaptive = auto()
    Exponential = auto()
    Progressive = auto()


class ReturnWhen(AutoEnum):
    """Control when wait() should return.

    Attributes:
        ALL_COMPLETED: Wait until all futures are done
        FIRST_COMPLETED: Return as soon as any future completes
        FIRST_EXCEPTION: Return as soon as any future raises an exception
    """

    ALL_COMPLETED = alias("all")
    FIRST_COMPLETED = alias("first")
    FIRST_EXCEPTION = alias("exception")
