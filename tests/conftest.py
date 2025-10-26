"""Shared pytest fixtures and configuration for all concurry tests.

This module provides common fixtures that are automatically available to all test files:
- worker_mode: Parametrized fixture for testing across all worker modes
- cleanup_all: Session-level fixture for cleaning up Ray and multiprocessing resources

Pytest Configuration:
- Default timeout: 60 seconds per test (configurable via --timeout)
- Timeout method: 'thread' for better compatibility with Ray and multiprocessing
- Full stack traces on timeout for debugging
"""

import gc
import multiprocessing
import os
import subprocess
import sys
import time

import morphic
import pytest

import concurry
from concurry.utils import _IS_RAY_INSTALLED

# =============================================================================
# Configuration Constants
# =============================================================================

# Ray server configuration
RAY_SERVER_PORT = 6379
RAY_CLIENT_PORT = 10001
RAY_NUM_CPUS = 4
RAY_TEMP_DIR = "/tmp/ray_test_server/"

# Timing configuration (seconds)
RAY_STARTUP_WAIT = 5
RAY_SHUTDOWN_WAIT = 2
RAY_RESTART_WAIT = 3
CLEANUP_WAIT = 0.5

# Default restart interval for batched Ray restarts
DEFAULT_RAY_RESTART_INTERVAL = 5

# =============================================================================
# Helper Functions
# =============================================================================


def is_ray_client_mode_enabled() -> bool:
    """Check if Ray client mode is enabled via environment variable."""
    return os.environ.get("DISABLE_RAY_CLIENT_MODE", "0") != "1"


def get_ray_restart_interval() -> int:
    """Get the configured Ray restart interval from environment variable."""
    return int(os.environ.get("RAY_RESTART_INTERVAL", str(DEFAULT_RAY_RESTART_INTERVAL)))


def setup_tests_module_path() -> None:
    """Add tests module parent directory to sys.path for Ray serialization."""
    tests_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if tests_parent_dir not in sys.path:
        sys.path.insert(0, tests_parent_dir)


def set_max_file_descriptors() -> None:
    """Set file descriptor limit to maximum available."""
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        print(f"Current FD limits: soft={soft}, hard={hard}")
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
            print(f"Increased FD limit to {hard}")
        except Exception:
            print(f"Could not increase FD limit (already at {soft})")
    except Exception as e:
        print(f"Warning: Could not check/set FD limits: {e}")


def stop_ray_server(timeout: int = 10) -> None:
    """Stop any running Ray server."""
    try:
        subprocess.run(
            ["ray", "stop", "--force"],
            timeout=timeout,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(RAY_SHUTDOWN_WAIT)
    except Exception:
        pass


def start_ray_server() -> subprocess.Popen:
    """Start a Ray server process and return the Popen object."""
    return subprocess.Popen(
        [
            "ray",
            "start",
            "--head",
            f"--port={RAY_SERVER_PORT}",
            f"--ray-client-server-port={RAY_CLIENT_PORT}",
            f"--num-cpus={RAY_NUM_CPUS}",
            f"--temp-dir={RAY_TEMP_DIR}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def connect_ray_client() -> None:
    """Connect to Ray server as a client with proper runtime environment."""
    import ray

    import tests

    ray.init(
        address=f"ray://127.0.0.1:{RAY_CLIENT_PORT}",
        ignore_reinit_error=True,
        runtime_env={"py_modules": [concurry, morphic, tests]},
    )


def initialize_ray_standard() -> None:
    """Initialize Ray in standard (non-client) mode."""
    import ray

    import tests

    ray.init(
        ignore_reinit_error=True,
        num_cpus=RAY_NUM_CPUS,
        runtime_env={"py_modules": [concurry, morphic, tests]},
    )


def cleanup_multiprocessing_children(timeout: float = 0.5) -> None:
    """Terminate any lingering multiprocessing child processes."""
    try:
        active_children = multiprocessing.active_children()
        if len(active_children) > 0:
            for child in active_children:
                try:
                    child.terminate()
                    child.join(timeout=timeout)
                except Exception:
                    pass
    except Exception:
        pass


# =============================================================================
# Pytest Configuration Hooks
# =============================================================================


def pytest_configure(config):
    """Configure pytest with default timeout and other settings.

    This hook runs before test collection. It sets up:
    - Default timeout per test (if not overridden by CLI)
    - Thread-based timeout method (works better with Ray/multiprocessing)
    - Full traceback display on timeout

    Note: Timeouts are non-fatal by default - tests continue after timeout.
    Use -x flag to stop on first timeout/failure.
    """
    # Set default timeout if not specified via command line
    if config.option.timeout is None:
        config.option.timeout = 120  # 120 seconds default

    # Use 'thread' timeout method for better compatibility
    # (works with Ray actors and multiprocessing)
    if not hasattr(config.option, "timeout_method") or config.option.timeout_method is None:
        config.option.timeout_method = "thread"


def pytest_addoption(parser):
    """Add custom command-line options for concurry tests.

    This allows users to override the default timeout:
        pytest --timeout=120  # 2 minute timeout
        pytest --timeout=0    # Disable timeout

    Use -x to stop on first failure (including timeouts):
        pytest --timeout=60 -x  # Stop on first timeout/failure
    """
    # The pytest-timeout plugin already adds --timeout option,
    # but we ensure it's available and document it
    pass


# Test modes available for all tests
WORKER_MODES = ["sync", "thread", "process", "asyncio"]

if _IS_RAY_INSTALLED:
    WORKER_MODES.append("ray")

# Pool modes (subset of worker modes that support pooling)
POOL_MODES = ["thread", "process"]

if _IS_RAY_INSTALLED:
    POOL_MODES.append("ray")


@pytest.fixture(scope="session", autouse=True)
def initialize_ray():
    """Session-level fixture to initialize Ray once if available.

    This fixture runs automatically before all tests. If Ray is installed,
    it initializes the Ray cluster with the correct runtime environment.

    Ray Client Mode Testing:
    ------------------------
    Ray client mode is ENABLED BY DEFAULT to match real-world deployment scenarios
    where users connect to a remote Ray cluster while also using process mode for
    local multiprocessing tasks.

    To disable client mode (use standard Ray), set DISABLE_RAY_CLIENT_MODE=1:
        DISABLE_RAY_CLIENT_MODE=1 pytest tests/

    **Multiprocessing Compatibility:**
    Process mode now uses 'forkserver' as the default multiprocessing context instead
    of 'fork'. This provides:
    - **Safety**: No corruption from forking active gRPC threads (Ray client mode)
    - **Speed**: ~200ms startup vs. 10-20s for 'spawn'
    - **Compatibility**: Safe to use Ray client + process workers concurrently

    Both workers and MultiprocessSharedLimitSet Manager use the same context (forkserver
    by default). This is required for Manager proxy pickling to work correctly across
    process boundaries. Together, these changes allow safe concurrent use of:
    - Ray client for distributed compute
    - Process mode workers for local multiprocessing
    - Shared limits across process workers

    **Resource Management:**
    To prevent "too many open files" errors when running large test suites,
    the Ray server is automatically restarted between test modules (files).
    This ensures file descriptors are released and prevents resource exhaustion.

    **Requirements for client mode:**
    - grpcio package: pip install "ray[client]"
    - Working Ray installation with client server support

    If client mode connection fails, tests will fail with a clear error message.
    """
    if not _IS_RAY_INSTALLED:
        yield
        return

    import ray

    # Setup: Clean slate
    stop_ray_server(timeout=5)
    setup_tests_module_path()

    # Determine mode
    use_client_mode = is_ray_client_mode_enabled()

    if not ray.is_initialized():
        if use_client_mode:
            # Check for grpcio dependency
            try:
                import grpc  # noqa: F401
            except ImportError:
                raise RuntimeError(
                    "⚠  Ray client mode requested but grpcio is not installed\n"
                    "   Install with: pip install 'ray[client]'"
                )

            # Start Ray server and connect as client
            try:
                print("\n" + "=" * 70)
                print("Starting Ray server for client mode testing...")
                set_max_file_descriptors()
                print("=" * 70)

                start_ray_server()
                time.sleep(RAY_STARTUP_WAIT)

                print("Connecting to Ray server in client mode...")
                connect_ray_client()
                print("✓ Connected to Ray server in client mode")
                print("=" * 70 + "\n")

            except Exception as e:
                stop_ray_server(timeout=5)
                raise RuntimeError(f"⚠  Failed to connect to Ray server in client mode: {e}")
        else:
            # Standard Ray initialization
            initialize_ray_standard()

    yield

    # Cleanup: Shutdown Ray
    try:
        if ray.is_initialized():
            ray.shutdown()
            time.sleep(CLEANUP_WAIT)
    except Exception:
        pass

    # Stop Ray server
    try:
        print("\n" + "=" * 70)
        print("Stopping Ray server...")
        stop_ray_server(timeout=10)
        print("✓ Ray server stopped")
        print("=" * 70 + "\n")
    except Exception as e:
        print(f"Note: Error stopping Ray server: {e}")


# Track module count for batched Ray restarts
_module_counter = {"count": 0}


@pytest.fixture(scope="module", autouse=True)
def cleanup_between_modules(request):
    """Module-level fixture to clean up resources between test modules.

    PERFORMANCE-OPTIMIZED: Instead of restarting Ray after EVERY module (slow),
    we restart only every N modules to balance performance with resource cleanup.

    Configuration (via environment variables):
    - RAY_RESTART_INTERVAL: Number of modules between restarts (default: 5)
      Set to 1 for restart after every module (slow but safest)
      Set to 10+ for faster tests (may hit FD limits on large suites)

    Example:
        RAY_RESTART_INTERVAL=3 pytest tests/  # Restart every 3 modules
        RAY_RESTART_INTERVAL=1 pytest tests/  # Restart every module (slowest)

    Each restart takes ~6 seconds, so:
    - Interval=1: ~180s overhead for 30 modules (SLOW)
    - Interval=5: ~36s overhead for 30 modules (BALANCED) ✓
    - Interval=10: ~18s overhead for 30 modules (FAST, may hit FD limits)
    """
    yield  # Let the module's tests run

    # Increment module counter
    _module_counter["count"] += 1

    # Lightweight cleanup after every module
    gc.collect()
    cleanup_multiprocessing_children()

    # Heavy cleanup: Restart Ray periodically
    if not _IS_RAY_INSTALLED:
        return

    import ray

    use_client_mode = is_ray_client_mode_enabled()
    restart_interval = get_ray_restart_interval()

    should_restart = (
        use_client_mode and ray.is_initialized() and (_module_counter["count"] % restart_interval == 0)
    )

    if should_restart:
        try:
            print("\n" + "=" * 70)
            print(f"Completed {_module_counter['count']} modules - Restarting Ray...")
            print("=" * 70)

            # Shutdown Ray client
            ray.shutdown()
            gc.collect()
            time.sleep(1)

            # Stop and restart Ray server
            stop_ray_server(timeout=10)
            start_ray_server()
            time.sleep(RAY_RESTART_WAIT)

            # Reconnect as client
            connect_ray_client()

            print("✓ Ray restarted successfully")
            print("=" * 70 + "\n")

        except Exception as e:
            print(f"Warning: Ray restart failed: {e}")
            stop_ray_server(timeout=5)


@pytest.fixture(params=WORKER_MODES)
def worker_mode(request):
    """Fixture providing different worker modes.

    This fixture is automatically parametrized across all supported worker modes.
    If Ray is installed, it will be included in the test modes.

    The Ray cluster is initialized by the initialize_ray fixture, so this
    fixture just yields the mode name.

    Args:
        request: pytest request object containing the parameter

    Yields:
        str: The worker mode name ("sync", "thread", "process", "asyncio", or "ray")
    """
    yield request.param


@pytest.fixture(params=POOL_MODES)
def pool_mode(request):
    """Fixture providing different pool modes.

    This fixture is automatically parametrized across pool-supporting modes.
    Pool modes are modes that support max_workers > 1 (thread, process, and ray if installed).

    The Ray cluster is initialized by the initialize_ray fixture, so this
    fixture just yields the mode name.

    Args:
        request: pytest request object containing the parameter

    Yields:
        str: The pool mode name ("thread", "process", or "ray")
    """
    yield request.param


@pytest.fixture(scope="session", autouse=True)
def cleanup_all():
    """Session-level fixture to ensure all resources are cleaned up after tests.

    This fixture automatically runs after all tests in a session complete.
    It ensures proper cleanup of:
    - Multiprocessing worker processes
    - Python garbage collection

    Note: Ray cleanup is handled by the initialize_ray fixture to ensure
    proper ordering (Ray must be shut down after this fixture runs).

    The fixture runs at session scope, meaning:
    - When running full test suite: Cleans up once at the very end
    - When running single file: Cleans up after that file's tests complete
    - When running specific tests: Cleans up after those tests complete
    """
    yield

    # Force garbage collection
    gc.collect()
    time.sleep(0.2)

    # Terminate any active multiprocessing children
    cleanup_multiprocessing_children(timeout=1.0)

    # Final garbage collection
    gc.collect()
    time.sleep(0.2)
