import os
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from concurry.utils.frameworks import _IS_RAY_INSTALLED, RayContext, ray_context
from concurry.utils.progress import ProgressBar

# Ray initialization and cleanup are handled by tests/conftest.py

# Skip all progress tests in CI environment (they produce massive output and are slow)
pytestmark = pytest.mark.skipif(
    os.environ.get("CI") == "true", reason="Progress bar tests skipped in CI due to massive output"
)


def test_basic_progress_bar():
    """Test basic progress bar functionality"""
    pbar = ProgressBar(total=100, desc="Test")
    for i in range(100):
        pbar.update(1)
        time.sleep(0.005)
    pbar.success()


def test_progress_bar_with_range():
    """Test progress bar with range iterator"""
    for i in ProgressBar(range(300), total=500, desc="Testing", color="MAGENTA"):
        time.sleep(0.005)


def test_progress_bar_with_list():
    """Test progress bar with list iterator"""
    for i in ProgressBar(list(range(300)), desc="Testing", style="std"):
        time.sleep(0.005)


def test_progress_bar_with_thread_pool():
    """Test progress bar with ThreadPoolExecutor"""

    def g(position: int):
        for i in ProgressBar(list(range(300)), desc="Testing", style="std", position=position):
            time.sleep(0.005)
        return position

    executor = ThreadPoolExecutor(max_workers=10)
    results = [x.result() for x in [executor.submit(g, i) for i in range(10)]]
    executor.shutdown(wait=True)
    assert results == list(range(10))


@pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray is not installed")
def test_progress_bar_with_ray():
    """Test progress bar with Ray"""
    import ray

    @ray.remote(num_cpus=0.1)
    def f(position: int):
        for i in ProgressBar(list(range(300)), desc="Testing", style="ray", position=position):
            time.sleep(0.005)
        return f"{ray_context()} {position}"

    results = ray.get([f.remote(i) for i in range(10)])
    assert results == [f"{RayContext.Task} {i}" for i in range(10)]


def test_progress_bar_with_multiple_threads():
    """Test multiple progress bars running in parallel using ThreadPoolExecutor"""

    def g(position: int):
        for i in ProgressBar(list(range(300)), desc="Testing", position=position):
            time.sleep(0.005)
        return position

    executor = ThreadPoolExecutor(max_workers=10)
    results = [x.result() for x in [executor.submit(g, i) for i in range(10)]]
    executor.shutdown(wait=True)
    assert results == list(range(10))


@pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray is not installed")
def test_progress_bar_with_ray_parallel():
    """Test multiple progress bars running in parallel using Ray"""
    import ray

    @ray.remote(num_cpus=0.1)
    def f(position: int):
        for i in ProgressBar(list(range(300)), desc="Testing", style="ray", position=position):
            time.sleep(0.005)
        return f"{ray_context()} {position}"

    results = ray.get([f.remote(i) for i in range(10)])
    assert results == [f"{RayContext.Task} {i}" for i in range(10)]


def test_progress_bar_styles():
    """Test different progress bar styles"""
    # Test std style
    for i in ProgressBar(list(range(100)), desc="Testing std", style="std"):
        time.sleep(0.005)

    # Test ray style only if ray is installed
    if _IS_RAY_INSTALLED:
        for i in ProgressBar(list(range(100)), desc="Testing ray", style="ray"):
            time.sleep(0.005)


def test_progress_bar_colors():
    """Test different progress bar colors"""
    for i in ProgressBar(list(range(100)), desc="Testing magenta", color="MAGENTA"):
        time.sleep(0.005)

    for i in ProgressBar(list(range(100)), desc="Testing default color"):
        time.sleep(0.005)


def test_progress_bar_positions():
    """Test progress bar with different positions"""
    for i in ProgressBar(list(range(100)), desc="Testing position 0", position=0):
        time.sleep(0.005)

    for i in ProgressBar(list(range(100)), desc="Testing position 1", position=1):
        time.sleep(0.005)


def test_progress_bar_failure():
    """Test progress bar failed method"""
    pbar = ProgressBar(total=100, desc="Test failure")
    for i in range(100):
        pbar.update(1)
        time.sleep(0.005)
    pbar.failure()


def test_progress_bar_stop():
    """Test progress bar stop method"""
    pbar = ProgressBar(total=100, desc="Test stop")
    for i in range(100):
        pbar.update(1)
        if i == 60:
            pbar.stop()
        time.sleep(0.005)


def test_progress_bar_with_different_totals():
    """Test progress bar with different total values"""
    # Test with total less than actual iterations
    for i in ProgressBar(range(300), total=120, desc="Testing total < iterations"):
        time.sleep(0.005)

    # Test with total equal to actual iterations
    for i in ProgressBar(range(100), total=100, desc="Testing total = iterations"):
        time.sleep(0.005)

    # Test with total greater than actual iterations
    for i in ProgressBar(range(50), total=100, desc="Testing total > iterations"):
        time.sleep(0.005)


def test_progress_bar_with_exception():
    """Test progress bar behavior when an exception occurs"""
    with pytest.raises(ValueError):
        for i in ProgressBar(range(100), desc="Test exception"):
            if i == 50:
                raise ValueError("Test exception")
            time.sleep(0.005)


def test_progress_bar_with_exception_in_thread():
    """Test progress bar behavior when an exception occurs in a thread"""

    def g(position: int):
        for i in ProgressBar(list(range(300)), desc="Testing exception", position=position):
            if i == 150:
                raise ValueError(f"Test exception in thread {position}")
            time.sleep(0.005)
        return position

    executor = ThreadPoolExecutor(max_workers=2)
    futures = [executor.submit(g, i) for i in range(2)]

    with pytest.raises(ValueError):
        for future in futures:
            future.result()

    executor.shutdown(wait=True)


@pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray is not installed")
def test_progress_bar_with_exception_in_ray():
    """Test progress bar behavior when an exception occurs in Ray task"""
    import ray

    @ray.remote(num_cpus=0.1)
    def f(position: int):
        for i in ProgressBar(list(range(300)), desc="Testing exception", style="ray", position=position):
            if i == 160:
                raise ValueError(f"Test exception in Ray task {position}")
            time.sleep(0.005)
        return position

    with pytest.raises(ValueError):
        ray.get([f.remote(i) for i in range(2)])


def test_progress_bar_with_exception_in_iterator():
    """Test progress bar behavior when an exception occurs in the iterator"""

    class FailingIterator:
        def __init__(self, n):
            self.n = n
            self.i = 0

        def __iter__(self):
            return self

        def __next__(self):
            if self.i >= self.n:
                raise StopIteration
            if self.i == 60:
                raise ValueError("Test exception in iterator")
            self.i += 1
            return self.i

    with pytest.raises(ValueError):
        for i in ProgressBar(FailingIterator(100), desc="Test iterator exception"):
            time.sleep(0.005)
