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

# Disable Ray's UV runtime-env hook BEFORE importing ray. Recent Ray versions
# (~2.55+) deepcopy any `runtime_env` dict passed to `ray.init`; our test
# conftest passes `{"py_modules": [concurry, morphic, tests]}` (live module
# objects), and `copy.deepcopy(module_obj)` raises
# `TypeError: cannot pickle 'module' object`. The hook only matters when the
# driver is launched via `uv run` (it auto-propagates the uv environment to
# workers). We aren't using `uv run` to launch pytest, so the hook is
# unnecessary. The env var is read at module import time inside Ray, so we
# must set it BEFORE the first `import ray`. Use setdefault so a user can
# still opt back in by exporting `RAY_ENABLE_UV_RUN_RUNTIME_ENV=1`.
os.environ.setdefault("RAY_ENABLE_UV_RUN_RUNTIME_ENV", "0")

import morphic
import pytest

import concurry
from concurry.core.constants import ExecutionMode
from concurry.utils import _IS_RAY_INSTALLED

# =============================================================================
# Configuration Constants
# =============================================================================

# Ray server configuration
RAY_SERVER_PORT = 6379
RAY_CLIENT_PORT = 10001
RAY_NUM_CPUS = 2 if os.environ.get("CI") == "true" else 4
RAY_TEMP_DIR = "/tmp/ray_test_server/"

# Timing configuration (seconds)
RAY_STARTUP_WAIT = 5
RAY_SHUTDOWN_WAIT = 2
RAY_RESTART_WAIT = 3
CLEANUP_WAIT = 0.5

# How often to restart the Ray server between test modules (files).
# After every N modules, the Ray client is disconnected, the server process is killed
# and restarted, and a fresh client connection is established. This cleans up leaked
# gRPC connections, actor state, and file descriptors that accumulate across tests.
# Each restart takes ~6-8 seconds (shutdown + startup + reconnect).
# With ~25 test modules in the suite:
#   Interval=1: restart every module  → ~150-200s overhead (most restarts, highest failure risk)
#   Interval=3: restart every 3       → ~50-70s overhead
#   Interval=5: restart every 5       → ~30-40s overhead
#   Interval=10: restart every 10     → ~15-20s overhead
#   Interval=0: never restart         → 0s overhead (requires high ulimit)
# On CI, restarts are disabled because ulimit is raised in the workflow.
# Restarts are the #1 cause of "Ray is not initialized" failures on CI.
DEFAULT_RAY_RESTART_INTERVAL = 0 if os.environ.get("CI") == "true" else 5

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
    cmd = [
        "ray",
        "start",
        "--head",
        f"--port={RAY_SERVER_PORT}",
        f"--ray-client-server-port={RAY_CLIENT_PORT}",
        f"--num-cpus={RAY_NUM_CPUS}",
        f"--temp-dir={RAY_TEMP_DIR}",
    ]
    if os.environ.get("CI") == "true":
        cmd.append("--object-store-memory=209715200")  # 200 MB; CI runners have ~7 GB total
    return subprocess.Popen(
        cmd,
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


def wait_for_ray_client_port(timeout: float = 30.0) -> bool:
    """Wait until the Ray client port is accepting TCP connections.

    This is more reliable than a fixed sleep — it confirms the gRPC server
    is actually listening before we attempt ray.init().

    Args:
        timeout: Maximum seconds to wait.

    Returns:
        True if port is ready, False if timeout reached.
    """
    import socket

    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1.0)
                s.connect(("127.0.0.1", RAY_CLIENT_PORT))
                return True
        except (ConnectionRefusedError, OSError, socket.timeout):
            time.sleep(0.5)
    return False


def start_ray_server_and_wait(timeout: float = 30.0) -> None:
    """Start Ray server and wait until the client port is accepting connections.

    Combines start_ray_server() with wait_for_ray_client_port() for a
    deterministic startup sequence.

    Raises:
        RuntimeError: If the server doesn't become ready within timeout.
    """
    start_ray_server()
    if not wait_for_ray_client_port(timeout=timeout):
        raise RuntimeError(
            f"Ray server did not start accepting connections on port {RAY_CLIENT_PORT} within {timeout}s"
        )


def connect_ray_client_with_retry(max_attempts: int = 3, wait_between: float = 3.0) -> None:
    """Connect to Ray server with retries, clearing stale client state between attempts.

    ray.init() in client mode can leave partial state on failure, and
    ignore_reinit_error=True doesn't work properly with Ray Client
    (see ray-project/ray#24888). So we call ray.shutdown() before each
    retry to ensure a clean slate.

    Args:
        max_attempts: Maximum number of connection attempts.
        wait_between: Seconds to wait between attempts.

    Raises:
        The last exception if all attempts fail.
    """
    import ray

    last_error = None
    for attempt in range(max_attempts):
        try:
            # Clear any stale client state from a previous failed connection.
            # ray.shutdown() is safe to call even when not connected.
            if attempt > 0:
                try:
                    ray.shutdown()
                except Exception:
                    pass
                time.sleep(wait_between)

            connect_ray_client()
            return  # Success
        except Exception as e:
            last_error = e
            if attempt < max_attempts - 1:
                print(f"  Ray client connect attempt {attempt + 1}/{max_attempts} failed: {e}")

    raise last_error  # type: ignore[misc]


def initialize_ray_standard() -> None:
    """Initialize Ray in standard (non-client) mode."""
    import ray

    import tests

    init_kwargs = dict(
        ignore_reinit_error=True,
        num_cpus=RAY_NUM_CPUS,
        runtime_env={"py_modules": [concurry, morphic, tests]},
    )
    if os.environ.get("CI") == "true":
        init_kwargs.update(
            num_cpus=2,
            object_store_memory=200 * 1024 * 1024,  # 200 MB
            _system_config={"object_store_memory": 200 * 1024 * 1024},
        )
    ray.init(**init_kwargs)


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
    # Set default timeout if not specified via command line.
    # Use getattr because pytest-timeout's options may not be registered yet
    # depending on plugin load order.
    if getattr(config.option, "timeout", None) is None:
        config.option.timeout = 120  # 120 seconds default

    # Use 'thread' timeout method for better compatibility
    # (works with Ray actors and multiprocessing)
    if getattr(config.option, "timeout_method", None) is None:
        config.option.timeout_method = "thread"


def pytest_addoption(parser):
    """Add custom command-line options for concurry tests.

    Execution mode filtering (accepts any ExecutionMode alias):
        pytest --execution-modes=thread,process
        pytest --execution-modes=sync
        pytest --execution-modes=proc,threads   # aliases work too

    Timeout configuration:
        pytest --timeout=120  # 2 minute timeout
        pytest --timeout=0    # Disable timeout

    Use -x to stop on first failure (including timeouts):
        pytest --timeout=60 -x  # Stop on first timeout/failure
    """
    parser.addoption(
        "--execution-modes",
        action="store",
        default=None,
        help="Comma-separated list of execution modes to test. "
        "Accepts any ExecutionMode alias (e.g. 'thread', 'threads', 'process', 'proc', "
        "'sync', 'synchronous', 'async', 'asyncio', 'ray'). "
        "The worker_mode fixture uses the full list; the pool_mode fixture uses "
        "only the subset that supports pooling. "
        "Default: all installed modes.",
    )


# ---------------------------------------------------------------------------
# ExecutionMode → canonical test string mapping
# ---------------------------------------------------------------------------

_EXECUTION_MODE_TO_TEST_STR: dict = {
    ExecutionMode.Sync: "sync",
    ExecutionMode.Threads: "thread",
    ExecutionMode.Processes: "process",
    ExecutionMode.Asyncio: "asyncio",
    ExecutionMode.Ray: "ray",
}

_ALL_EXECUTION_MODES: list = [
    ExecutionMode.Sync,
    ExecutionMode.Threads,
    ExecutionMode.Processes,
    ExecutionMode.Asyncio,
]
if _IS_RAY_INSTALLED:
    _ALL_EXECUTION_MODES.append(ExecutionMode.Ray)

_POOL_EXECUTION_MODES: list = [
    ExecutionMode.Threads,
    ExecutionMode.Processes,
]
if _IS_RAY_INSTALLED:
    _POOL_EXECUTION_MODES.append(ExecutionMode.Ray)

WORKER_MODES: list = [_EXECUTION_MODE_TO_TEST_STR[m] for m in _ALL_EXECUTION_MODES]
POOL_MODES: list = [_EXECUTION_MODE_TO_TEST_STR[m] for m in _POOL_EXECUTION_MODES]


def _parse_execution_modes(raw: str) -> list:
    """Parse a comma-separated mode string, validate via ExecutionMode, return test strings."""
    tokens = [t.strip() for t in raw.split(",") if len(t.strip()) > 0]
    test_strings = []
    for token in tokens:
        try:
            mode = ExecutionMode(token)
        except (KeyError, ValueError):
            raise pytest.UsageError(
                f"Invalid execution mode: '{token}'. "
                f"Accepted values (and their aliases): "
                + ", ".join(
                    f"{_EXECUTION_MODE_TO_TEST_STR[m]} ({m.name})" for m in _EXECUTION_MODE_TO_TEST_STR
                )
            )
        if mode == ExecutionMode.Auto:
            raise pytest.UsageError(
                f"'auto' is not a valid test execution mode. Choose from: {', '.join(WORKER_MODES)}"
            )
        test_str = _EXECUTION_MODE_TO_TEST_STR.get(mode)
        if test_str is None:
            raise pytest.UsageError(f"ExecutionMode.{mode.name} is not mapped to a test string.")
        if mode not in _ALL_EXECUTION_MODES:
            raise pytest.UsageError(
                f"Execution mode '{token}' (ExecutionMode.{mode.name}) is not available. "
                f"Ray is {'installed' if _IS_RAY_INSTALLED else 'NOT installed'}."
            )
        if test_str not in test_strings:
            test_strings.append(test_str)
    return test_strings


def _already_parametrized(metafunc, name: str) -> bool:
    """Check if a fixture is already parametrized via @pytest.mark.parametrize."""
    for marker in metafunc.definition.iter_markers("parametrize"):
        args = marker.args
        if len(args) > 0:
            argnames = args[0]
            if isinstance(argnames, str):
                names = [n.strip() for n in argnames.split(",")]
            else:
                names = list(argnames)
            if name in names:
                return True
    return False


def pytest_generate_tests(metafunc):
    """Dynamically parametrize worker_mode / pool_mode fixtures.

    Reads --execution-modes from the CLI (if provided) and narrows the
    parametrization accordingly:
      - worker_mode: uses the requested modes directly.
      - pool_mode: uses the intersection of requested modes with POOL_MODES.

    When the CLI option is not given, all installed modes are used.

    Tests that already apply @pytest.mark.parametrize("worker_mode", ...)
    or @pytest.mark.parametrize("pool_mode", ...) are left alone to avoid
    "duplicate parametrization" errors.
    """
    raw = metafunc.config.getoption("execution_modes")
    requested = _parse_execution_modes(raw) if raw is not None else None

    if "worker_mode" in metafunc.fixturenames and not _already_parametrized(metafunc, "worker_mode"):
        modes = requested if requested is not None else list(WORKER_MODES)
        metafunc.parametrize("worker_mode", modes)

    if "pool_mode" in metafunc.fixturenames and not _already_parametrized(metafunc, "pool_mode"):
        if requested is not None:
            modes = [m for m in requested if m in POOL_MODES]
            if len(modes) == 0:
                pytest.skip(
                    f"--execution-modes={raw} contains no pool-capable modes "
                    f"(need one of: {', '.join(POOL_MODES)})"
                )
        else:
            modes = list(POOL_MODES)
        metafunc.parametrize("pool_mode", modes)


def _is_ray_mode_requested(config) -> bool:
    """Check if 'ray' is among the requested execution modes (or if no filter was given)."""
    raw = config.getoption("execution_modes")
    if raw is None:
        return True  # No filter → all modes including ray
    requested = _parse_execution_modes(raw)
    return "ray" in requested


@pytest.fixture
def requires_ray_mode(request):
    """Skip the test if Ray is not installed or not in --execution-modes.

    Use this fixture in tests that hardcode mode="ray" instead of using the
    worker_mode / pool_mode fixtures.

    Example:
        def test_ray_specific_feature(self, requires_ray_mode):
            w = MyWorker.options(mode="ray").init()
            ...
    """
    if not _IS_RAY_INSTALLED:
        pytest.skip("Ray is not installed")
    if not _is_ray_mode_requested(request.config):
        pytest.skip("Ray mode not included in --execution-modes")
    import ray

    if not ray.is_initialized():
        pytest.fail("Ray is not initialized (server may have failed to restart)")


@pytest.fixture(scope="session", autouse=True)
def initialize_ray(request):
    """Session-level fixture to initialize Ray once if available and requested.

    This fixture runs automatically before all tests. If Ray is installed
    and "ray" is included in --execution-modes (or no filter is given),
    it initializes the Ray cluster with the correct runtime environment.
    When --execution-modes omits "ray", the cluster is never started.

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
    if not _IS_RAY_INSTALLED or not _is_ray_mode_requested(request.config):
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

                start_ray_server_and_wait(timeout=30)

                print("Connecting to Ray server in client mode...")
                connect_ray_client_with_retry(max_attempts=3, wait_between=3.0)
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


@pytest.fixture(autouse=True)
def cleanup_after_each_test():
    """Per-test fixture to release Ray actor handles via garbage collection.

    Large test modules (e.g., test_pydantic_integration.py) can have 50+ Ray tests.
    Without per-test cleanup, leaked Python references to actor handles accumulate.
    gc.collect() releases dead references, allowing Ray to reclaim resources.

    Note: We intentionally do NOT call ray.kill() here because killing actors causes
    Ray worker processes to die and respawn without the runtime_env (py_modules),
    leading to ModuleNotFoundError on the next test.

    Safe for non-ray modes: exits immediately if Ray is not initialized.
    """
    yield

    if not _IS_RAY_INSTALLED:
        return
    try:
        import ray

        if ray.is_initialized():
            gc.collect()
    except Exception:
        pass


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
    if not _IS_RAY_INSTALLED or not _is_ray_mode_requested(request.config):
        return

    import ray

    use_client_mode = is_ray_client_mode_enabled()
    restart_interval = get_ray_restart_interval()

    should_restart = (
        restart_interval > 0
        and use_client_mode
        and ray.is_initialized()
        and (_module_counter["count"] % restart_interval == 0)
    )

    if should_restart:
        try:
            print("\n" + "=" * 70)
            print(f"Completed {_module_counter['count']} modules - Restarting Ray...")
            print("=" * 70)

            # Shutdown Ray client and clean up
            ray.shutdown()
            gc.collect()
            time.sleep(1)

            # Stop old server, start new one, wait for port to be ready
            stop_ray_server(timeout=10)
            start_ray_server_and_wait(timeout=30)

            # Reconnect with retry (clears stale client state between attempts)
            connect_ray_client_with_retry(max_attempts=3, wait_between=3.0)

            print("✓ Ray restarted successfully")
            print("=" * 70 + "\n")

        except Exception as e:
            print(f"WARNING: Ray restart failed: {e}")
            print("Subsequent Ray tests will fail until Ray is re-initialized.")
            stop_ray_server(timeout=5)


@pytest.fixture
def worker_mode(request):
    """Fixture providing different worker modes.

    This fixture is dynamically parametrized by pytest_generate_tests across
    all supported worker modes. Override via CLI:

        pytest --execution-modes=thread,process  # Only test these modes

    If Ray is installed, it will be included in the test modes by default.

    Args:
        request: pytest request object containing the parameter

    Yields:
        str: The worker mode name ("sync", "thread", "process", "asyncio", or "ray")
    """
    mode = request.param
    if mode == "ray":
        import ray

        if not ray.is_initialized():
            pytest.fail("Ray is not initialized (server may have failed to restart)")
    yield mode


@pytest.fixture
def pool_mode(request):
    """Fixture providing different pool modes.

    This fixture is dynamically parametrized by pytest_generate_tests using the
    pool-capable subset of --execution-modes. Override via CLI:

        pytest --execution-modes=thread  # Only test thread pool mode

    Pool modes are modes that support max_workers > 1 (thread, process, and ray if installed).

    Args:
        request: pytest request object containing the parameter

    Yields:
        str: The pool mode name ("thread", "process", or "ray")
    """
    mode = request.param
    if mode == "ray":
        import ray

        if not ray.is_initialized():
            pytest.fail("Ray is not initialized (server may have failed to restart)")
    yield mode


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
