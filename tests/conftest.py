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
import time

import morphic
import pytest

import concurry
from concurry.utils import _IS_RAY_INSTALLED

# =============================================================================
# Pytest Configuration Hooks
# =============================================================================


def pytest_configure(config):
    """Configure pytest with default timeout and other settings.

    This hook runs before test collection. It sets up:
    - Default 60-second timeout per test (if not overridden by CLI)
    - Thread-based timeout method (works better with Ray/multiprocessing)
    - Full traceback display on timeout

    Note: Timeouts are non-fatal by default - tests continue after timeout.
    Use -x flag to stop on first timeout/failure.
    """
    # Set default timeout if not specified via command line
    if config.option.timeout is None:
        config.option.timeout = 60  # 60 seconds default

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
    This ensures Ray is ready for any test that needs it, without requiring
    manual initialization in each test file.
    """
    if _IS_RAY_INSTALLED:
        import ray

        if not ray.is_initialized():
            ray.init(
                ignore_reinit_error=True,
                num_cpus=4,
                runtime_env={"py_modules": [concurry, morphic]},
            )

    yield


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
    - Ray cluster resources
    - Python garbage collection

    The fixture runs at session scope, meaning:
    - When running full test suite: Cleans up once at the very end
    - When running single file: Cleans up after that file's tests complete
    - When running specific tests: Cleans up after those tests complete
    """
    yield

    # Force cleanup of any remaining processes
    # Force garbage collection to clean up any remaining workers
    gc.collect()

    # Give a brief moment for cleanup to complete
    time.sleep(0.2)

    # Terminate any active multiprocessing children
    try:
        active_children = multiprocessing.active_children()
        for child in active_children:
            try:
                child.terminate()
                child.join(timeout=1.0)
            except Exception:
                pass
    except Exception:
        pass

    # Shutdown Ray after all tests complete
    if _IS_RAY_INSTALLED:
        try:
            import ray

            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass  # Ignore shutdown errors

    # Force another garbage collection after Ray shutdown
    gc.collect()
    time.sleep(0.2)
