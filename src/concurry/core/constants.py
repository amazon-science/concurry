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
