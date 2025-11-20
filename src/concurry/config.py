"""Global configuration for Concurry library.

This module provides a hierarchical global configuration system with:
1. Global defaults that apply to all execution modes
2. Mode-specific overrides that take precedence when set

Users can customize defaults at both levels.
"""

from contextlib import contextmanager
from typing import Any, List, Literal, Optional

from morphic import MutableTyped
from pydantic import ConfigDict, Field, confloat, conint

from .core.constants import ExecutionMode, LoadBalancingAlgorithm, RateLimitAlgorithm
from .core.retry import RetryAlgorithm


class GlobalDefaults(MutableTyped):
    """Global default configuration that applies to all execution modes.

    These defaults are used when mode-specific overrides are not set.
    All values can be overridden at the mode level.

    Attributes:
        max_workers: Default number of workers for pools (None = single worker)
        max_queued_tasks: Default submission queue length (None = bypass queue)
        load_balancing: Default load balancing algorithm for pools
        load_balancing_on_demand: Default load balancing for on-demand pools
        blocking: Default blocking mode (False = return futures, True = return results)
        unwrap_futures: Default future unwrapping behavior (True = auto-unwrap)
        num_retries: Default number of retry attempts (0 = no retries)
        retry_on: Default exception types or filters that trigger retries ([Exception] = all exceptions)
        retry_algorithm: Default retry backoff algorithm
        retry_wait: Default minimum wait time between retries in seconds
        retry_jitter: Default jitter factor (0-1) for retry backoff
        retry_until: Default validation functions for output (None = no validation)
        stop_timeout: Default timeout for worker.stop() in seconds
        rate_limit_algorithm: Default rate limiting algorithm
        limit_pool_load_balancing: Default load balancing for LimitPool
        limit_pool_worker_index: Default worker index for LimitPool
        task_decorator_on_demand: Default on-demand worker creation for @task (True = create workers per request)
    """

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        validate_private_assignment=True,
    )

    # Worker/Pool configuration
    max_workers: Optional[conint(ge=0)] = None
    max_queued_tasks: Optional[conint(ge=0)] = None
    load_balancing: LoadBalancingAlgorithm = LoadBalancingAlgorithm.RoundRobin
    load_balancing_on_demand: LoadBalancingAlgorithm = LoadBalancingAlgorithm.Random
    on_demand: bool = False  # Default for Worker.options() on_demand parameter

    # Multiprocessing configuration (process mode only)
    mp_context: Literal["fork", "spawn", "forkserver"] = "forkserver"  # forkserver is safe with gRPC threads

    # @task decorator configuration
    task_decorator_on_demand: bool = True  # Default for @task decorator

    # Execution configuration
    blocking: bool = False
    unwrap_futures: bool = True

    # Retry configuration
    num_retries: conint(ge=0) = 0
    retry_on: List[Any] = Field(default_factory=lambda: [Exception])
    retry_algorithm: RetryAlgorithm = RetryAlgorithm.Exponential
    retry_wait: confloat(ge=0) = 1.0
    retry_jitter: confloat(ge=0, le=1) = 0.3
    retry_until: Optional[Any] = None

    # Timeout configuration
    stop_timeout: confloat(ge=0) = 30.0

    # === Rate Limiting Defaults ===
    rate_limit_algorithm: RateLimitAlgorithm = RateLimitAlgorithm.SlidingWindow

    # === Limit Pool Defaults ===
    limit_pool_load_balancing: LoadBalancingAlgorithm = LoadBalancingAlgorithm.RoundRobin
    limit_pool_worker_index: conint(ge=0) = 0

    # === Polling Strategy Defaults (Global) ===
    polling_fixed_interval: confloat(ge=0) = 0.01  # 10ms

    polling_adaptive_min_interval: confloat(ge=0) = 0.0001  # 0.1ms
    polling_adaptive_max_interval: confloat(ge=0) = 0.2  # 200ms
    polling_adaptive_initial_interval: confloat(ge=0) = 0.01  # 10ms
    polling_adaptive_speedup_factor: confloat(gt=0, le=1) = 0.7  # Speed up by 30% on completions
    polling_adaptive_slowdown_factor: confloat(ge=1) = 1.3  # Slow down by 30% on no completions
    polling_adaptive_consecutive_empty_threshold: conint(ge=1) = 3  # Checks before slowing down

    polling_exponential_initial_interval: confloat(ge=0) = 0.01  # 10ms
    polling_exponential_max_interval: confloat(ge=0) = 2.0  # 2s
    polling_exponential_multiplier: confloat(gt=1) = 2.0  # Double each time

    polling_progressive_min_interval: confloat(ge=0) = 0.0001  # 0.1ms
    polling_progressive_max_interval: confloat(ge=0) = 0.5  # 500ms
    polling_progressive_checks_before_increase: conint(ge=1) = 5  # Checks at each level

    # === Asyncio Future Polling Defaults ===
    asyncio_future_poll_interval: confloat(ge=0) = 1e-6  # 1 microsecond

    # === Async Wait/Gather Polling Defaults ===
    async_wait_poll_interval: confloat(ge=0) = 100e-6  # 100 microseconds
    async_gather_poll_interval: confloat(ge=0) = 100e-6  # 100 microseconds

    # === Ray Monitor Defaults ===
    ray_monitor_queue_get_timeout: confloat(ge=0) = 0.01  # 10ms
    ray_monitor_no_futures_sleep: confloat(ge=0) = 0.01  # 10ms
    ray_monitor_sleep: confloat(ge=0) = 0.001  # 1ms
    ray_monitor_error_sleep: confloat(ge=0) = 0.1  # 100ms

    # === Rate Limiter Defaults ===
    rate_limiter_min_wait_time: confloat(ge=0) = 0.01  # 10ms minimum wait between checks

    # === Limit Set Defaults ===
    limit_set_acquire_sleep: confloat(ge=0) = 0.01  # 10ms sleep when waiting for limits

    # === Worker Pool Defaults ===
    worker_pool_cleanup_sleep: confloat(ge=0) = 0.1  # 100ms sleep during pool cleanup

    # === Progress Bar Defaults ===
    progress_bar_ncols: conint(ge=10) = 100  # Progress bar width in characters
    progress_bar_smoothing: confloat(ge=0, le=1) = 0.15  # Display smoothing factor
    progress_bar_miniters: conint(ge=1) = 1  # Minimum iterations between updates

    def clone(self) -> "GlobalDefaults":
        """Clone the global defaults."""
        return self.__class__.model_validate(self.model_dump())


class ExecutionModeDefaults(MutableTyped):
    """Mode-specific default configuration that overrides global defaults.

    All fields are Optional. When None, the global default is used.
    When set, the mode-specific value takes precedence.

    This allows for flexible configuration:
    - Set global defaults that apply everywhere
    - Override specific modes as needed
    - Leave most modes using global defaults

    Attributes:
        max_workers: Override for number of workers for pools
        max_queued_tasks: Override for submission queue length
        load_balancing: Override for load balancing algorithm
        load_balancing_on_demand: Override for on-demand load balancing
        blocking: Override for blocking mode
        unwrap_futures: Override for future unwrapping behavior
        num_retries: Override for retry attempts
        retry_on: Override for exception types or filters that trigger retries
        retry_algorithm: Override for retry backoff algorithm
        retry_wait: Override for retry wait time
        retry_jitter: Override for retry jitter factor
        retry_until: Override for validation functions for output
        stop_timeout: Override for stop timeout
        task_decorator_on_demand: Override for on-demand worker creation for @task
    """

    # Worker/Pool configuration
    max_workers: Optional[conint(ge=0)] = None
    max_queued_tasks: Optional[conint(ge=0)] = None
    load_balancing: Optional[LoadBalancingAlgorithm] = None
    load_balancing_on_demand: Optional[LoadBalancingAlgorithm] = None
    on_demand: Optional[bool] = None

    # Multiprocessing configuration (process mode only)
    mp_context: Optional[Literal["fork", "spawn", "forkserver"]] = None

    # @task decorator configuration
    task_decorator_on_demand: Optional[bool] = None

    # Execution configuration
    blocking: Optional[bool] = None
    unwrap_futures: Optional[bool] = None

    # Retry configuration
    num_retries: Optional[conint(ge=0)] = None
    retry_on: Optional[List[Any]] = None
    retry_algorithm: Optional[RetryAlgorithm] = None
    retry_wait: Optional[confloat(ge=0)] = None
    retry_jitter: Optional[confloat(ge=0, le=1)] = None
    retry_until: Optional[Any] = None

    # Timeout configuration
    stop_timeout: Optional[confloat(ge=0)] = None

    # === Worker Internal Timeouts (Mode-Specific) ===
    # Thread mode
    worker_command_queue_timeout: Optional[confloat(ge=0)] = None  # default: 0.1s

    # Process mode
    worker_result_queue_timeout: Optional[confloat(ge=0)] = None  # default: 30s
    worker_result_queue_cleanup_timeout: Optional[confloat(ge=0)] = None  # default: 1s

    # Asyncio mode
    worker_loop_ready_timeout: Optional[confloat(ge=0)] = None  # default: 30s
    worker_thread_ready_timeout: Optional[confloat(ge=0)] = None  # default: 30s
    worker_sync_queue_timeout: Optional[confloat(ge=0)] = None  # default: 0.1s

    # Pool-specific (all modes with pools)
    pool_on_demand_cleanup_timeout: Optional[confloat(ge=0)] = None  # default: 5s
    pool_on_demand_slot_max_wait: Optional[confloat(ge=0)] = None  # default: 60s

    # === Polling Strategy Overrides (Mode-Specific) ===
    polling_fixed_interval: Optional[confloat(ge=0)] = None

    polling_adaptive_min_interval: Optional[confloat(ge=0)] = None
    polling_adaptive_max_interval: Optional[confloat(ge=0)] = None
    polling_adaptive_initial_interval: Optional[confloat(ge=0)] = None
    polling_adaptive_speedup_factor: Optional[confloat(gt=0, le=1)] = None
    polling_adaptive_slowdown_factor: Optional[confloat(ge=1)] = None
    polling_adaptive_consecutive_empty_threshold: Optional[conint(ge=1)] = None

    polling_exponential_initial_interval: Optional[confloat(ge=0)] = None
    polling_exponential_max_interval: Optional[confloat(ge=0)] = None
    polling_exponential_multiplier: Optional[confloat(gt=1)] = None

    polling_progressive_min_interval: Optional[confloat(ge=0)] = None
    polling_progressive_max_interval: Optional[confloat(ge=0)] = None
    polling_progressive_checks_before_increase: Optional[conint(ge=1)] = None

    # === Asyncio Future Polling Overrides ===
    asyncio_future_poll_interval: Optional[confloat(ge=0)] = None

    # === Ray Monitor Overrides ===
    ray_monitor_queue_get_timeout: Optional[confloat(ge=0)] = None
    ray_monitor_no_futures_sleep: Optional[confloat(ge=0)] = None
    ray_monitor_sleep: Optional[confloat(ge=0)] = None
    ray_monitor_error_sleep: Optional[confloat(ge=0)] = None

    # === Rate Limiter Overrides ===
    rate_limiter_min_wait_time: Optional[confloat(ge=0)] = None

    # === Limit Set Overrides ===
    limit_set_acquire_sleep: Optional[confloat(ge=0)] = None

    # === Worker Pool Overrides ===
    worker_pool_cleanup_sleep: Optional[confloat(ge=0)] = None

    # === Progress Bar Overrides ===
    progress_bar_ncols: Optional[conint(ge=10)] = None
    progress_bar_smoothing: Optional[confloat(ge=0, le=1)] = None
    progress_bar_miniters: Optional[conint(ge=1)] = None

    def clone(self) -> "ExecutionModeDefaults":
        """Clone the execution mode defaults."""
        return self.__class__.model_validate(self.model_dump())


_GLOBAL_DEFAULTS = GlobalDefaults()

# Mode-specific overrides (only set values that differ from global defaults)
_SYNC_DEFAULTS = ExecutionModeDefaults(
    max_workers=1,
    max_queued_tasks=None,
)

_ASYNCIO_DEFAULTS = ExecutionModeDefaults(
    max_workers=1,
    max_queued_tasks=None,
    worker_loop_ready_timeout=30.0,
    worker_thread_ready_timeout=30.0,
    worker_sync_queue_timeout=0.1,
)

_THREADS_DEFAULTS = ExecutionModeDefaults(
    max_workers=1,
    max_queued_tasks=None,
    worker_command_queue_timeout=0.1,
    pool_on_demand_cleanup_timeout=5.0,
    pool_on_demand_slot_max_wait=60.0,
)

_PROCESSES_DEFAULTS = ExecutionModeDefaults(
    max_workers=1,
    max_queued_tasks=100,
    worker_result_queue_timeout=30.0,
    worker_result_queue_cleanup_timeout=1.0,
    pool_on_demand_cleanup_timeout=5.0,
    pool_on_demand_slot_max_wait=60.0,
)

_RAY_DEFAULTS = ExecutionModeDefaults(
    max_workers=0,
    max_queued_tasks=3,
    pool_on_demand_cleanup_timeout=5.0,
    pool_on_demand_slot_max_wait=60.0,
)


class ResolvedDefaults:
    """Resolved configuration for a specific mode with fallback to global defaults.

    This class provides a view of the effective configuration for a mode,
    automatically falling back to global defaults when mode-specific values are None.

    This is NOT a MutableTyped class - it's a read-only view that dynamically
    resolves values from mode-specific and global defaults.
    """

    def __init__(self, global_defaults: GlobalDefaults, mode_defaults: ExecutionModeDefaults):
        self._global = global_defaults
        self._mode = mode_defaults

    @property
    def max_workers(self) -> Optional[conint(ge=0)]:
        return self._mode.max_workers if self._mode.max_workers is not None else self._global.max_workers

    @property
    def max_queued_tasks(self) -> Optional[conint(ge=0)]:
        return (
            self._mode.max_queued_tasks
            if self._mode.max_queued_tasks is not None
            else self._global.max_queued_tasks
        )

    @property
    def load_balancing(self) -> LoadBalancingAlgorithm:
        return (
            self._mode.load_balancing
            if self._mode.load_balancing is not None
            else self._global.load_balancing
        )

    @property
    def load_balancing_on_demand(self) -> LoadBalancingAlgorithm:
        return (
            self._mode.load_balancing_on_demand
            if self._mode.load_balancing_on_demand is not None
            else self._global.load_balancing_on_demand
        )

    @property
    def on_demand(self) -> bool:
        return self._mode.on_demand if self._mode.on_demand is not None else self._global.on_demand

    @property
    def mp_context(self) -> Literal["fork", "spawn", "forkserver"]:
        return self._mode.mp_context if self._mode.mp_context is not None else self._global.mp_context

    @property
    def task_decorator_on_demand(self) -> bool:
        return (
            self._mode.task_decorator_on_demand
            if self._mode.task_decorator_on_demand is not None
            else self._global.task_decorator_on_demand
        )

    @property
    def blocking(self) -> bool:
        return self._mode.blocking if self._mode.blocking is not None else self._global.blocking

    @property
    def unwrap_futures(self) -> bool:
        return (
            self._mode.unwrap_futures
            if self._mode.unwrap_futures is not None
            else self._global.unwrap_futures
        )

    @property
    def num_retries(self) -> conint(ge=0):
        return self._mode.num_retries if self._mode.num_retries is not None else self._global.num_retries

    @property
    def retry_algorithm(self) -> RetryAlgorithm:
        return (
            self._mode.retry_algorithm
            if self._mode.retry_algorithm is not None
            else self._global.retry_algorithm
        )

    @property
    def retry_wait(self) -> confloat(ge=0):
        return self._mode.retry_wait if self._mode.retry_wait is not None else self._global.retry_wait

    @property
    def retry_jitter(self) -> confloat(ge=0, le=1):
        return self._mode.retry_jitter if self._mode.retry_jitter is not None else self._global.retry_jitter

    @property
    def retry_on(self) -> List[Any]:
        return self._mode.retry_on if self._mode.retry_on is not None else self._global.retry_on

    @property
    def retry_until(self) -> Optional[Any]:
        return self._mode.retry_until if self._mode.retry_until is not None else self._global.retry_until

    @property
    def stop_timeout(self) -> confloat(ge=0):
        return self._mode.stop_timeout if self._mode.stop_timeout is not None else self._global.stop_timeout

    # === Worker Internal Timeouts (Mode-Specific with Fallbacks) ===
    @property
    def worker_command_queue_timeout(self) -> confloat(ge=0):
        return (
            self._mode.worker_command_queue_timeout
            if self._mode.worker_command_queue_timeout is not None
            else 0.1
        )

    @property
    def worker_result_queue_timeout(self) -> confloat(ge=0):
        return (
            self._mode.worker_result_queue_timeout
            if self._mode.worker_result_queue_timeout is not None
            else 30.0
        )

    @property
    def worker_result_queue_cleanup_timeout(self) -> confloat(ge=0):
        return (
            self._mode.worker_result_queue_cleanup_timeout
            if self._mode.worker_result_queue_cleanup_timeout is not None
            else 1.0
        )

    @property
    def worker_loop_ready_timeout(self) -> confloat(ge=0):
        return (
            self._mode.worker_loop_ready_timeout if self._mode.worker_loop_ready_timeout is not None else 30.0
        )

    @property
    def worker_thread_ready_timeout(self) -> confloat(ge=0):
        return (
            self._mode.worker_thread_ready_timeout
            if self._mode.worker_thread_ready_timeout is not None
            else 30.0
        )

    @property
    def worker_sync_queue_timeout(self) -> confloat(ge=0):
        return (
            self._mode.worker_sync_queue_timeout if self._mode.worker_sync_queue_timeout is not None else 0.1
        )

    @property
    def pool_on_demand_cleanup_timeout(self) -> confloat(ge=0):
        return (
            self._mode.pool_on_demand_cleanup_timeout
            if self._mode.pool_on_demand_cleanup_timeout is not None
            else 5.0
        )

    @property
    def pool_on_demand_slot_max_wait(self) -> confloat(ge=0):
        return (
            self._mode.pool_on_demand_slot_max_wait
            if self._mode.pool_on_demand_slot_max_wait is not None
            else 60.0
        )

    # === Polling Strategy Properties (with Fallback to Global) ===
    @property
    def polling_fixed_interval(self) -> confloat(ge=0):
        return (
            self._mode.polling_fixed_interval
            if self._mode.polling_fixed_interval is not None
            else self._global.polling_fixed_interval
        )

    @property
    def polling_adaptive_min_interval(self) -> confloat(ge=0):
        return (
            self._mode.polling_adaptive_min_interval
            if self._mode.polling_adaptive_min_interval is not None
            else self._global.polling_adaptive_min_interval
        )

    @property
    def polling_adaptive_max_interval(self) -> confloat(ge=0):
        return (
            self._mode.polling_adaptive_max_interval
            if self._mode.polling_adaptive_max_interval is not None
            else self._global.polling_adaptive_max_interval
        )

    @property
    def polling_adaptive_initial_interval(self) -> confloat(ge=0):
        return (
            self._mode.polling_adaptive_initial_interval
            if self._mode.polling_adaptive_initial_interval is not None
            else self._global.polling_adaptive_initial_interval
        )

    @property
    def polling_exponential_initial_interval(self) -> confloat(ge=0):
        return (
            self._mode.polling_exponential_initial_interval
            if self._mode.polling_exponential_initial_interval is not None
            else self._global.polling_exponential_initial_interval
        )

    @property
    def polling_exponential_max_interval(self) -> confloat(ge=0):
        return (
            self._mode.polling_exponential_max_interval
            if self._mode.polling_exponential_max_interval is not None
            else self._global.polling_exponential_max_interval
        )

    @property
    def polling_exponential_multiplier(self) -> confloat(gt=1):
        return (
            self._mode.polling_exponential_multiplier
            if self._mode.polling_exponential_multiplier is not None
            else self._global.polling_exponential_multiplier
        )

    @property
    def polling_adaptive_speedup_factor(self) -> confloat(gt=0, le=1):
        return (
            self._mode.polling_adaptive_speedup_factor
            if self._mode.polling_adaptive_speedup_factor is not None
            else self._global.polling_adaptive_speedup_factor
        )

    @property
    def polling_adaptive_slowdown_factor(self) -> confloat(ge=1):
        return (
            self._mode.polling_adaptive_slowdown_factor
            if self._mode.polling_adaptive_slowdown_factor is not None
            else self._global.polling_adaptive_slowdown_factor
        )

    @property
    def polling_adaptive_consecutive_empty_threshold(self) -> int:
        return (
            self._mode.polling_adaptive_consecutive_empty_threshold
            if self._mode.polling_adaptive_consecutive_empty_threshold is not None
            else self._global.polling_adaptive_consecutive_empty_threshold
        )

    @property
    def polling_progressive_min_interval(self) -> confloat(ge=0):
        return (
            self._mode.polling_progressive_min_interval
            if self._mode.polling_progressive_min_interval is not None
            else self._global.polling_progressive_min_interval
        )

    @property
    def polling_progressive_max_interval(self) -> confloat(ge=0):
        return (
            self._mode.polling_progressive_max_interval
            if self._mode.polling_progressive_max_interval is not None
            else self._global.polling_progressive_max_interval
        )

    @property
    def polling_progressive_checks_before_increase(self) -> int:
        return (
            self._mode.polling_progressive_checks_before_increase
            if self._mode.polling_progressive_checks_before_increase is not None
            else self._global.polling_progressive_checks_before_increase
        )

    # === Asyncio Future Polling Properties ===
    @property
    def asyncio_future_poll_interval(self) -> confloat(ge=0):
        return (
            self._mode.asyncio_future_poll_interval
            if self._mode.asyncio_future_poll_interval is not None
            else self._global.asyncio_future_poll_interval
        )

    # === Ray Monitor Properties ===
    @property
    def ray_monitor_queue_get_timeout(self) -> confloat(ge=0):
        return (
            self._mode.ray_monitor_queue_get_timeout
            if self._mode.ray_monitor_queue_get_timeout is not None
            else self._global.ray_monitor_queue_get_timeout
        )

    @property
    def ray_monitor_no_futures_sleep(self) -> confloat(ge=0):
        return (
            self._mode.ray_monitor_no_futures_sleep
            if self._mode.ray_monitor_no_futures_sleep is not None
            else self._global.ray_monitor_no_futures_sleep
        )

    @property
    def ray_monitor_sleep(self) -> confloat(ge=0):
        return (
            self._mode.ray_monitor_sleep
            if self._mode.ray_monitor_sleep is not None
            else self._global.ray_monitor_sleep
        )

    @property
    def ray_monitor_error_sleep(self) -> confloat(ge=0):
        return (
            self._mode.ray_monitor_error_sleep
            if self._mode.ray_monitor_error_sleep is not None
            else self._global.ray_monitor_error_sleep
        )

    # === Rate Limiter Properties ===
    @property
    def rate_limiter_min_wait_time(self) -> confloat(ge=0):
        return (
            self._mode.rate_limiter_min_wait_time
            if self._mode.rate_limiter_min_wait_time is not None
            else self._global.rate_limiter_min_wait_time
        )

    # === Limit Set Properties ===
    @property
    def limit_set_acquire_sleep(self) -> confloat(ge=0):
        return (
            self._mode.limit_set_acquire_sleep
            if self._mode.limit_set_acquire_sleep is not None
            else self._global.limit_set_acquire_sleep
        )

    # === Worker Pool Properties ===
    @property
    def worker_pool_cleanup_sleep(self) -> confloat(ge=0):
        return (
            self._mode.worker_pool_cleanup_sleep
            if self._mode.worker_pool_cleanup_sleep is not None
            else self._global.worker_pool_cleanup_sleep
        )

    # === Progress Bar Properties ===
    @property
    def progress_bar_ncols(self) -> int:
        return (
            self._mode.progress_bar_ncols
            if self._mode.progress_bar_ncols is not None
            else self._global.progress_bar_ncols
        )

    @property
    def progress_bar_smoothing(self) -> float:
        return (
            self._mode.progress_bar_smoothing
            if self._mode.progress_bar_smoothing is not None
            else self._global.progress_bar_smoothing
        )

    @property
    def progress_bar_miniters(self) -> int:
        return (
            self._mode.progress_bar_miniters
            if self._mode.progress_bar_miniters is not None
            else self._global.progress_bar_miniters
        )


class ConcurryConfig(MutableTyped):
    """Global configuration for Concurry library.

    This configuration is mutable and can be updated at runtime. All changes
    are validated automatically via Typed.

    The configuration has a hierarchical structure:
    1. Global defaults (apply to all modes)
    2. Mode-specific overrides (take precedence when set)

    When you call get_defaults(mode), you get a ResolvedDefaults object that
    automatically falls back to global defaults when mode-specific values are None.

    Example:
        ```python
        from concurry import global_config

        # Set global defaults (apply to all modes)
        global_config.defaults.num_retries = 3
        global_config.defaults.retry_wait = 2.0

        # Override for specific mode
        global_config.thread.max_workers = 32
        global_config.thread.max_queued_tasks = 200

        # Ray uses global retry defaults but custom queue
        global_config.ray.max_queued_tasks = 5
        # ray.num_retries will be 3 (from global)
        # ray.retry_wait will be 2.0 (from global)

        # Reset to library defaults
        global_config.reset_to_defaults()
        ```
    """

    # Global defaults (apply to all modes unless overridden)
    defaults: GlobalDefaults = _GLOBAL_DEFAULTS.clone()

    # Per-mode overrides (None values fall back to global defaults)
    sync: ExecutionModeDefaults = _SYNC_DEFAULTS.clone()
    asyncio: ExecutionModeDefaults = _ASYNCIO_DEFAULTS.clone()
    thread: ExecutionModeDefaults = _THREADS_DEFAULTS.clone()
    process: ExecutionModeDefaults = _PROCESSES_DEFAULTS.clone()
    ray: ExecutionModeDefaults = _RAY_DEFAULTS.clone()

    def get_defaults(self, mode: ExecutionMode) -> ResolvedDefaults:
        """Get resolved defaults for a specific execution mode.

        Returns a ResolvedDefaults object that automatically falls back to
        global defaults when mode-specific values are None.

        Args:
            mode: The execution mode

        Returns:
            ResolvedDefaults for the mode (with fallback to global)
        """
        if mode == ExecutionMode.Sync:
            return ResolvedDefaults(self.defaults, self.sync)
        elif mode == ExecutionMode.Asyncio:
            return ResolvedDefaults(self.defaults, self.asyncio)
        elif mode == ExecutionMode.Threads:
            return ResolvedDefaults(self.defaults, self.thread)
        elif mode == ExecutionMode.Processes:
            return ResolvedDefaults(self.defaults, self.process)
        elif mode == ExecutionMode.Ray:
            return ResolvedDefaults(self.defaults, self.ray)
        else:
            raise ValueError(f"Unknown execution mode: {mode}")

    def reset_to_defaults(self) -> None:
        """Reset all configuration to library defaults."""
        self.defaults = _GLOBAL_DEFAULTS.clone()
        self.sync = _SYNC_DEFAULTS.clone()
        self.asyncio = _ASYNCIO_DEFAULTS.clone()
        self.thread = _THREADS_DEFAULTS.clone()
        self.process = _PROCESSES_DEFAULTS.clone()
        self.ray = _RAY_DEFAULTS.clone()

    def _snapshot(self) -> dict[str, ExecutionModeDefaults]:
        """Create a snapshot of current configuration.

        Returns:
            Dictionary with global defaults and mode-specific defaults
        """
        return {
            "defaults": self.defaults.clone(),
            "sync": self.sync.clone(),
            "asyncio": self.asyncio.clone(),
            "thread": self.thread.clone(),
            "process": self.process.clone(),
            "ray": self.ray.clone(),
        }

    def _restore(self, snapshot: dict[str, Any]) -> None:
        """Restore configuration from a snapshot.

        Args:
            snapshot: Dictionary with global and mode-specific defaults
        """
        self.defaults = snapshot["defaults"]
        self.sync = snapshot["sync"]
        self.asyncio = snapshot["asyncio"]
        self.thread = snapshot["thread"]
        self.process = snapshot["process"]
        self.ray = snapshot["ray"]

    def clone(self, thread_safe: bool = False) -> "ConcurryConfig":
        """Create a thread-safe clone of the current configuration.

        Args:
            thread_safe: Whether to create a thread-safe clone.
                If True, the clone will be slow but thread-safe.
                If False, the clone will be very fast but not thread-safe.

        Returns:
            A new ConcurryConfig instance with copied defaults
        """
        if thread_safe:
            return self.__class__.model_validate(self.model_dump())  ## Slow (~100us) but thread-safe
        return self  ## Basically free, but not thread-safe


# Global configuration instance
global_config = ConcurryConfig()


@contextmanager
def temp_config(**overrides):
    """Temporarily override global configuration.

    This context manager allows you to temporarily modify the global configuration
    for the duration of a with block. All changes are automatically reverted when
    exiting the context.

    You can override:
    1. Global defaults (apply to all modes): use "global_<attribute>"
    2. Mode-specific values: use "<mode>_<attribute>"

    Args:
        **overrides: Keyword arguments specifying configuration overrides.
            Examples:
                - global_num_retries=3 (applies to all modes)
                - global_retry_wait=2.0 (applies to all modes)
                - thread_max_workers=50 (thread-specific)
                - thread_max_queued_tasks=200 (thread-specific)
                - ray_max_queued_tasks=5 (ray-specific)

    Yields:
        The global_config instance with temporary overrides applied

    Example:
        ```python
        from concurry import Worker, temp_config

        # Set global defaults for all modes
        with temp_config(global_num_retries=3, global_retry_wait=2.0):
            # All workers use these retry settings
            worker1 = MyWorker.options(mode="thread").init()
            worker2 = MyWorker.options(mode="ray").init()
            # Both have num_retries=3, retry_wait=2.0
            worker1.stop()
            worker2.stop()

        # Override specific modes
        with temp_config(thread_max_queued_tasks=50, ray_max_queued_tasks=10):
            worker3 = MyWorker.options(mode="thread").init()
            print(worker3.max_queued_tasks)  # 50
            worker3.stop()

            worker4 = MyWorker.options(mode="ray").init()
            print(worker4.max_queued_tasks)  # 10
            worker4.stop()

        # Combine global and mode-specific
        with temp_config(
            global_num_retries=5,  # All modes
            thread_max_queued_tasks=100,  # Thread only
        ):
            worker5 = MyWorker.options(mode="thread").init()
            # Uses num_retries=5 (global), max_queued_tasks=100 (thread)
            worker5.stop()
        ```

    Nested Context Managers:
        ```python
        # Nested overrides work correctly
        with temp_config(global_num_retries=3):
            with temp_config(thread_num_retries=5):
                worker = MyWorker.options(mode="thread").init()
                # Uses thread-specific value: 5
                worker.stop()
        ```
    """
    # Create snapshot of current config
    snapshot = global_config._snapshot()

    try:
        # Apply overrides
        for key, value in overrides.items():
            # Parse key like "thread_max_queued_tasks" or "global_num_retries"
            parts = key.split("_", 1)
            if len(parts) != 2:
                raise ValueError(
                    f"Invalid override key: '{key}'. "
                    f"Expected format: '<mode>_<attribute>' or 'global_<attribute>'"
                )

            mode_name, attr_name = parts

            # Get the target defaults object
            local_config = global_config.clone()
            if mode_name == "global":
                target_defaults = local_config.defaults
            elif mode_name == "sync":
                target_defaults = local_config.sync
            elif mode_name == "asyncio":
                target_defaults = local_config.asyncio
            elif mode_name == "thread":
                target_defaults = local_config.thread
            elif mode_name == "process":
                target_defaults = local_config.process
            elif mode_name == "ray":
                target_defaults = local_config.ray
            else:
                raise ValueError(
                    f"Invalid mode in override key: '{mode_name}'. "
                    f"Valid modes: global, sync, asyncio, thread, process, ray"
                )

            # Set the attribute
            if not hasattr(target_defaults, attr_name):
                raise ValueError(
                    f"Invalid attribute in override key: '{attr_name}'. "
                    f"Valid attributes: max_workers, max_queued_tasks, load_balancing, "
                    f"load_balancing_on_demand, on_demand, blocking, unwrap_futures, num_retries, "
                    f"retry_on, retry_algorithm, retry_wait, retry_jitter, retry_until, stop_timeout, "
                    f"rate_limit_algorithm, limit_pool_load_balancing, limit_pool_worker_index, "
                    f"task_decorator_on_demand, "
                    f"worker_command_queue_timeout, worker_result_queue_timeout, "
                    f"worker_result_queue_cleanup_timeout, worker_loop_ready_timeout, "
                    f"worker_thread_ready_timeout, worker_sync_queue_timeout, "
                    f"pool_on_demand_cleanup_timeout, pool_on_demand_slot_max_wait, "
                    f"polling_fixed_interval, polling_adaptive_min_interval, "
                    f"polling_adaptive_max_interval, polling_adaptive_initial_interval, "
                    f"polling_exponential_initial_interval, polling_exponential_max_interval, "
                    f"polling_progressive_min_interval, polling_progressive_max_interval, "
                    f"asyncio_future_poll_interval, ray_monitor_queue_get_timeout, "
                    f"ray_monitor_no_futures_sleep, ray_monitor_sleep, ray_monitor_error_sleep, "
                    f"rate_limiter_min_wait_time, limit_set_acquire_sleep, worker_pool_cleanup_sleep, "
                    f"progress_bar_ncols, progress_bar_smoothing, progress_bar_miniters"
                )

            setattr(target_defaults, attr_name, value)

        # Yield control to the with block
        yield global_config

    finally:
        # Restore original config
        global_config._restore(snapshot)
