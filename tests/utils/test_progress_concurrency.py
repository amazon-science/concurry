"""Test ProgressBar in multithreaded and multiprocessing contexts.

These tests catch issues that can occur when ProgressBar is used in concurrent contexts:
1. Pickling errors (multiprocessing)
2. Lock/threading issues
3. Display corruption
4. Position conflicts
"""

import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Process, Queue

import pytest

from concurry.utils.frameworks import _IS_RAY_INSTALLED
from concurry.utils.progress import ProgressBar

# Skip all progress tests in CI environment (they produce massive output and are slow)
pytestmark = pytest.mark.skipif(
    os.environ.get("CI") == "true", reason="Progress bar tests skipped in CI due to massive output"
)


# ==============================================================================
# Module-level worker functions for multiprocessing tests
# (These must be at module level to be picklable)
# ==============================================================================


def _mp_worker_with_progress(worker_id: int) -> int:
    """Worker function that creates its own ProgressBar (for multiprocessing)."""
    pbar = ProgressBar(total=30, desc=f"Process-{worker_id}", position=worker_id, style="std")
    for i in range(30):
        time.sleep(0.02)
        pbar.update(1)
    pbar.success()
    return worker_id


def _mp_worker_with_exception(worker_id: int) -> int:
    """Worker that raises an exception midway (for multiprocessing)."""
    pbar = ProgressBar(total=30, desc=f"Process-{worker_id}", position=worker_id, style="std")
    try:
        for i in range(30):
            if i == 15:
                raise ValueError(f"Test exception in process {worker_id}")
            time.sleep(0.02)
            pbar.update(1)
        pbar.success()
    except Exception as e:
        pbar.failure(f"Failed: {e}")
        raise
    return worker_id


def _mp_worker_process(queue: Queue, worker_id: int) -> None:
    """Worker for multiprocessing.Process that creates ProgressBar and reports completion."""
    try:
        pbar = ProgressBar(total=30, desc=f"Process-{worker_id}", position=worker_id, style="std")
        for i in range(30):
            time.sleep(0.02)
            pbar.update(1)
        pbar.success()
        queue.put(("success", worker_id))
    except Exception as e:
        queue.put(("error", str(e)))


def _mp_worker_with_zero_items(worker_id: int) -> int:
    """Worker that processes zero items (for multiprocessing)."""
    pbar = ProgressBar(total=0, desc=f"Process-{worker_id}", style="std")
    pbar.success()
    return worker_id


def _mp_worker_with_large_miniters(worker_id: int) -> int:
    """Worker that uses large miniters for performance (for multiprocessing)."""
    pbar = ProgressBar(
        total=1000,
        desc=f"Process-{worker_id}",
        position=worker_id,
        style="std",
        miniters=100,  # Only update display every 100 iterations
    )
    for i in range(1000):
        pbar.update(1)
        # Very fast loop - miniters prevents display spam

    pbar.success()
    return worker_id


class TestProgressBarMultithreading:
    """Test ProgressBar in multithreaded contexts."""

    def test_progress_bar_in_thread_pool_executor(self):
        """Test ProgressBar works correctly in ThreadPoolExecutor.

        This test creates multiple threads, each with its own ProgressBar,
        and verifies that they can run concurrently without errors.
        """

        def worker_with_progress(worker_id: int) -> int:
            """Worker function that uses a ProgressBar."""
            pbar = ProgressBar(total=50, desc=f"Thread-{worker_id}", position=worker_id, style="std")
            for i in range(50):
                time.sleep(0.01)
                pbar.update(1)
            pbar.success()
            return worker_id

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(worker_with_progress, i) for i in range(4)]
            results = [f.result(timeout=30.0) for f in futures]

        assert results == list(range(4))

    def test_progress_bar_shared_in_threads(self):
        """Test that a single ProgressBar can be safely updated from multiple threads.

        This test creates one ProgressBar and updates it from multiple threads
        to verify thread-safety of the update mechanism.
        """

        def worker_with_shared_progress(pbar: ProgressBar, count: int) -> None:
            """Worker that updates a shared ProgressBar."""
            for i in range(count):
                time.sleep(0.01)
                pbar.update(1)

        total_items = 100
        items_per_worker = 25
        num_workers = 4

        pbar = ProgressBar(total=total_items, desc="Shared Progress", style="std")

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [
                executor.submit(worker_with_shared_progress, pbar, items_per_worker)
                for _ in range(num_workers)
            ]
            for f in futures:
                f.result(timeout=30.0)

        pbar.success()

    def test_progress_bar_exception_in_thread(self):
        """Test ProgressBar properly handles exceptions in threads."""

        def worker_with_exception(worker_id: int) -> int:
            """Worker that raises an exception midway."""
            pbar = ProgressBar(total=50, desc=f"Thread-{worker_id}", position=worker_id, style="std")
            try:
                for i in range(50):
                    if i == 25:
                        raise ValueError(f"Test exception in thread {worker_id}")
                    time.sleep(0.01)
                    pbar.update(1)
                pbar.success()
            except Exception as e:
                pbar.failure(f"Failed: {e}")
                raise
            return worker_id

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(worker_with_exception, i) for i in range(2)]

            with pytest.raises(ValueError, match="Test exception in thread"):
                for f in futures:
                    f.result(timeout=30.0)


class TestProgressBarMultiprocessing:
    """Test ProgressBar in multiprocessing contexts."""

    def test_progress_bar_in_process_pool_executor(self):
        """Test ProgressBar works correctly in ProcessPoolExecutor.

        This test creates multiple processes, each with its own ProgressBar,
        and verifies that they can run without pickling errors.

        NOTE: ProgressBar objects themselves should NOT be passed between processes
        as they contain unpicklable lock objects. Each process should create its own.
        """
        with ProcessPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(_mp_worker_with_progress, i) for i in range(3)]
            results = [f.result(timeout=30.0) for f in futures]

        assert results == list(range(3))

    def test_progress_bar_cannot_be_pickled(self):
        """Test that ProgressBar raises appropriate error when pickled.

        This test verifies that attempting to pass a ProgressBar object
        to a subprocess results in a clear error about pickling.
        """
        import pickle

        pbar = ProgressBar(total=100, desc="Test", style="std")

        # ProgressBar contains tqdm which has locks that cannot be pickled
        with pytest.raises((TypeError, AttributeError, pickle.PicklingError)):
            pickle.dumps(pbar)

    def test_progress_bar_in_multiprocessing_process(self):
        """Test ProgressBar works in multiprocessing.Process.

        This test uses multiprocessing.Process directly (not pool)
        to verify ProgressBar works in subprocess.
        """
        queue = Queue()
        processes = []

        for i in range(3):
            p = Process(target=_mp_worker_process, args=(queue, i))
            p.start()
            processes.append(p)

        # Wait for all processes
        for p in processes:
            p.join(timeout=30.0)

        # Collect results
        results = []
        while len(results) < 3:
            status, data = queue.get(timeout=5.0)
            if status == "error":
                pytest.fail(f"Process failed with error: {data}")
            results.append(data)

        assert sorted(results) == list(range(3))

    def test_progress_bar_exception_in_process(self):
        """Test ProgressBar properly handles exceptions in subprocesses."""
        with ProcessPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(_mp_worker_with_exception, i) for i in range(2)]

            with pytest.raises(ValueError, match="Test exception in process"):
                for f in futures:
                    f.result(timeout=30.0)


class TestProgressBarRayConcurrency:
    """Test ProgressBar in Ray distributed contexts."""

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray is not installed")
    def test_progress_bar_in_ray_tasks(self):
        """Test ProgressBar works correctly in Ray tasks.

        This test creates multiple Ray tasks, each with its own ProgressBar,
        and verifies that they work in a distributed context.
        """
        import ray

        @ray.remote
        def worker_with_progress(worker_id: int) -> int:
            """Ray task that uses ProgressBar."""
            pbar = ProgressBar(total=30, desc=f"Ray-Task-{worker_id}", position=worker_id, style="ray")
            for i in range(30):
                time.sleep(0.02)
                pbar.update(1)
            pbar.success()
            return worker_id

        futures = [worker_with_progress.remote(i) for i in range(3)]
        results = ray.get(futures, timeout=30.0)

        assert sorted(results) == list(range(3))

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray is not installed")
    def test_progress_bar_in_ray_actor(self):
        """Test ProgressBar works correctly in Ray actors.

        This test creates a Ray actor that uses ProgressBar for its operations.
        """
        import ray

        @ray.remote
        class WorkerActor:
            def __init__(self, actor_id: int):
                self.actor_id = actor_id

            def work_with_progress(self, num_items: int) -> int:
                """Actor method that uses ProgressBar."""
                pbar = ProgressBar(total=num_items, desc=f"Ray-Actor-{self.actor_id}", style="ray")
                for i in range(num_items):
                    time.sleep(0.02)
                    pbar.update(1)
                pbar.success()
                return self.actor_id

        actors = [WorkerActor.remote(i) for i in range(3)]
        futures = [actor.work_with_progress.remote(30) for actor in actors]
        results = ray.get(futures, timeout=30.0)

        assert sorted(results) == list(range(3))

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray is not installed")
    def test_progress_bar_exception_in_ray_task(self):
        """Test ProgressBar properly handles exceptions in Ray tasks."""
        import ray

        @ray.remote
        def worker_with_exception(worker_id: int) -> int:
            """Ray task that raises an exception midway."""
            pbar = ProgressBar(total=30, desc=f"Ray-Task-{worker_id}", position=worker_id, style="ray")
            try:
                for i in range(30):
                    if i == 15:
                        raise ValueError(f"Test exception in Ray task {worker_id}")
                    time.sleep(0.02)
                    pbar.update(1)
                pbar.success()
            except Exception as e:
                pbar.failure(f"Failed: {e}")
                raise
            return worker_id

        futures = [worker_with_exception.remote(i) for i in range(2)]

        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(futures, timeout=30.0)


class TestProgressBarWithConcurryWorkers:
    """Test ProgressBar integration with concurry Workers."""

    def test_progress_bar_with_task_worker_map(self, worker_mode):
        """Test ProgressBar works with TaskWorker.map() across all modes.

        This test verifies that the ProgressBar integration in TaskWorker.map()
        works correctly for all execution modes (sync, thread, process, asyncio, ray).
        """
        from concurry import TaskWorker

        def square(x: int) -> int:
            time.sleep(0.01)
            return x * x

        if worker_mode == "thread":
            worker = TaskWorker.options(mode=worker_mode, max_workers=30).init(fn=square)
        elif worker_mode == "process":
            worker = TaskWorker.options(mode=worker_mode, max_workers=4).init(fn=square)
        elif worker_mode == "ray":
            worker = TaskWorker.options(mode=worker_mode, max_workers=0).init(fn=square)
        else:
            worker = TaskWorker.options(mode=worker_mode).init(fn=square)

        # Use progress bar with map
        results = list(worker.map(range(20), progress=True))

        assert results == [x * x for x in range(20)]

        worker.stop()

    def test_progress_bar_with_task_worker_map_custom_config(self, worker_mode):
        """Test ProgressBar with custom configuration in TaskWorker.map().

        This test verifies that custom ProgressBar configurations work
        correctly across all execution modes.
        """
        from concurry import TaskWorker

        def double(x: int) -> int:
            time.sleep(0.01)
            return x * 2

        if worker_mode == "thread":
            worker = TaskWorker.options(mode=worker_mode, max_workers=30).init(fn=double)
        elif worker_mode == "process":
            worker = TaskWorker.options(mode=worker_mode, max_workers=4).init(fn=double)
        elif worker_mode == "ray":
            worker = TaskWorker.options(mode=worker_mode, max_workers=0).init(fn=double)
        else:
            worker = TaskWorker.options(mode=worker_mode).init(fn=double)

        # Custom progress configuration
        progress_config = {
            "desc": "Doubling",
            "unit": "num",
            "color": "#9c27b0",  # Purple
            "style": "std",
        }

        results = list(worker.map(range(15), progress=progress_config))

        assert results == [x * 2 for x in range(15)]

        worker.stop()

    def test_progress_bar_with_pool_workers(self, pool_mode):
        """Test ProgressBar with worker pools (thread, process, ray).

        This test verifies that ProgressBar works correctly when using
        multiple workers in a pool, where tasks are distributed across workers.
        """
        from concurry import TaskWorker

        def process_item(x: int) -> int:
            time.sleep(0.02)
            return x * 3

        worker = TaskWorker.options(mode=pool_mode, max_workers=3).init(fn=process_item)

        # Progress bar should work even with pool distribution
        results = list(worker.map(range(30), progress=True))

        assert results == [x * 3 for x in range(30)]

        worker.stop()

    def test_progress_bar_with_exception_in_worker_map(self, worker_mode):
        """Test ProgressBar handles exceptions during TaskWorker.map().

        This test verifies that when an exception occurs during map execution,
        the ProgressBar is properly cleaned up.
        """
        from concurry import TaskWorker

        def failing_function(x: int) -> int:
            if x == 10:
                raise ValueError(f"Test exception at x={x}")
            return x * 2

        if worker_mode == "thread":
            worker = TaskWorker.options(mode=worker_mode, max_workers=30).init(fn=failing_function)
        elif worker_mode == "process":
            worker = TaskWorker.options(mode=worker_mode, max_workers=4).init(fn=failing_function)
        elif worker_mode == "ray":
            worker = TaskWorker.options(mode=worker_mode, max_workers=0).init(fn=failing_function)
        else:
            worker = TaskWorker.options(mode=worker_mode).init(fn=failing_function)

        with pytest.raises(ValueError, match="Test exception at x=10"):
            # This should fail when processing x=10
            list(worker.map(range(20), progress=True))

        worker.stop()


class TestProgressBarEdgeCases:
    """Test edge cases and boundary conditions for ProgressBar in concurrent contexts."""

    def test_progress_bar_with_very_fast_updates_in_threads(self):
        """Test ProgressBar handles very fast updates from multiple threads.

        This test verifies that when updates come very quickly from multiple threads,
        the progress bar doesn't break or corrupt display.
        """

        def fast_worker(pbar: ProgressBar, count: int) -> None:
            """Worker that updates progress very quickly."""
            for i in range(count):
                pbar.update(1)
                # No sleep - updates as fast as possible

        total_items = 1000
        items_per_worker = 250
        num_workers = 4

        pbar = ProgressBar(total=total_items, desc="Fast Updates", style="std", miniters=10)

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(fast_worker, pbar, items_per_worker) for _ in range(num_workers)]
            for f in futures:
                f.result(timeout=10.0)

        pbar.success()

    def test_progress_bar_with_zero_items_in_process(self):
        """Test ProgressBar handles zero items in subprocess."""
        with ProcessPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(_mp_worker_with_zero_items, i) for i in range(2)]
            results = [f.result(timeout=5.0) for f in futures]

        assert results == list(range(2))

    def test_progress_bar_with_dynamic_total_in_threads(self):
        """Test ProgressBar with dynamically changing total in threads."""

        def worker_with_dynamic_total(worker_id: int) -> int:
            """Worker that changes total midway."""
            pbar = ProgressBar(total=50, desc=f"Thread-{worker_id}", position=worker_id, style="std")
            for i in range(50):
                if i == 25:
                    # Discover more work!
                    pbar.set_total(75)
                time.sleep(0.01)
                pbar.update(1)

            # Process remaining items
            for i in range(25):
                time.sleep(0.01)
                pbar.update(1)

            pbar.success()
            return worker_id

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(worker_with_dynamic_total, i) for i in range(2)]
            results = [f.result(timeout=30.0) for f in futures]

        assert results == list(range(2))

    def test_progress_bar_with_large_miniters_in_process(self):
        """Test ProgressBar with large miniters setting in subprocess.

        This test verifies that the miniters buffering mechanism works
        correctly in subprocess contexts.
        """
        with ProcessPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(_mp_worker_with_large_miniters, i) for i in range(2)]
            results = [f.result(timeout=30.0) for f in futures]

        assert results == list(range(2))
