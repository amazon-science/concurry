"""Synchronization primitives for concurry.

This module provides efficient wait() and gather() functions for managing
multiple futures, following concurrent.futures patterns with enhanced features:
- Adaptive polling algorithms
- Flexible progress tracking
- Nested structure support
- Iterator mode for streaming results
"""

import time
from concurrent.futures import TimeoutError
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Set, Tuple, Union

from morphic.structs import map_collection

from ..utils.frameworks import _IS_RAY_INSTALLED
from ..utils.progress import ProgressBar
from .future import BaseFuture, wrap_future
from .polling import PollingAlgorithm, create_polling_strategy

# Constants matching concurrent.futures.wait
ALL_COMPLETED = "ALL_COMPLETED"
FIRST_COMPLETED = "FIRST_COMPLETED"
FIRST_EXCEPTION = "FIRST_EXCEPTION"

# Import Ray if available
if _IS_RAY_INSTALLED:
    import ray


# ======================== Helper Functions ========================


def _check_futures_batch(futures_to_check: Iterable[BaseFuture]) -> Set[BaseFuture]:
    """Efficiently check which futures are done.

    For Ray futures: Uses single ray.wait() call for batch checking (efficient IPC)
    For others: Checks .done() individually

    Args:
        futures_to_check: Iterable of BaseFuture instances to check

    Returns:
        Set of completed futures from input
    """
    if len(list(futures_to_check)) == 0:
        return set()

    futures_list = list(futures_to_check)
    completed = set()

    # Optimize Ray futures with batch checking
    if _IS_RAY_INSTALLED:
        # Separate Ray futures from others
        ray_futures = []
        ray_future_map = {}  # Map ObjectRef to BaseFuture wrapper

        for fut in futures_list:
            if hasattr(fut, "_object_ref"):  # RayFuture has _object_ref attribute
                ray_futures.append(fut._object_ref)
                ray_future_map[id(fut._object_ref)] = fut

        # Batch check Ray futures if any exist
        if len(ray_futures) > 0:
            ready, not_ready = ray.wait(ray_futures, num_returns=len(ray_futures), timeout=0)
            for ref in ready:
                if id(ref) in ray_future_map:
                    completed.add(ray_future_map[id(ref)])

    # Check all non-Ray futures individually (and Ray futures not in batch)
    for fut in futures_list:
        if fut not in completed:  # Skip already completed Ray futures
            if fut.done():
                completed.add(fut)

    return completed


def _wrap_all_futures(obj: Any, recurse: bool = False) -> Any:
    """Wrap all future-like objects using wrap_future().

    Args:
        obj: Object or structure containing futures
        recurse: If True, recursively process nested structures

    Returns:
        Structure with all futures wrapped as BaseFuture

    Example:
        >>> futures = [asyncio_future, ray_object_ref, value]
        >>> wrapped = _wrap_all_futures(futures)
        >>> all(isinstance(f, BaseFuture) for f in wrapped)
        True
    """
    if recurse:
        return map_collection(obj, wrap_future, recurse=True)
    else:
        # For non-recursive, only wrap if it's a collection at top level
        if isinstance(obj, (list, tuple, set)):
            collection_type = type(obj)
            return collection_type(wrap_future(item) for item in obj)
        elif isinstance(obj, dict):
            return {k: wrap_future(v) for k, v in obj.items()}
        else:
            return wrap_future(obj)


def _create_progress_tracker(
    progress: Union[bool, Dict, Callable, None], total: Optional[int], desc: str
) -> Optional[Union[ProgressBar, Callable]]:
    """Create progress tracker from progress parameter.

    Args:
        progress: Progress configuration
            - None/False: No progress tracking
            - True: Auto-create ProgressBar with miniters=total/100
            - Dict: Configuration for ProgressBar
            - Callable: Progress callback function
        total: Total number of items
        desc: Description for progress bar

    Returns:
        ProgressBar instance, callable, or None

    Example:
        >>> # Auto progress bar
        >>> pbar = _create_progress_tracker(True, 1000, "Waiting")
        >>> # Custom progress bar
        >>> pbar = _create_progress_tracker({"unit": "task"}, 100, "Processing")
        >>> # Callback function
        >>> callback = _create_progress_tracker(lambda c, t, e: print(f"{c}/{t}"), 10, "")
    """
    if progress is None or progress is False:
        return None

    if callable(progress):
        return progress

    if progress is True:
        # Auto-create with miniters=total/100
        miniters = max(1, total // 100) if total is not None else 1
        return ProgressBar(total=total, desc=desc, miniters=miniters)

    if isinstance(progress, dict):
        # Apply defaults, let user dict override
        config = {"total": total, "desc": desc}
        if total is not None:
            config["miniters"] = max(1, total // 100)
        config.update(progress)  # User settings override defaults
        return ProgressBar(**config)

    raise ValueError(f"Invalid progress type: {type(progress)}. Must be bool, dict, callable, or None")


def _update_progress(
    tracker: Union[ProgressBar, Callable, None],
    completed: int,
    total: int,
    elapsed: float,
) -> None:
    """Update progress tracker.

    Args:
        tracker: ProgressBar, callable, or None
        completed: Number of completed items
        total: Total number of items
        elapsed: Elapsed time in seconds
    """
    if tracker is None:
        return

    if isinstance(tracker, ProgressBar):
        # Update progress bar to current completed count
        current_n = getattr(tracker.pbar, "n", 0)
        delta = completed - current_n
        if delta > 0:
            tracker.update(delta)
    elif callable(tracker):
        # Call progress callback with (completed, total, elapsed)
        tracker(completed, total, elapsed)


# ======================== wait() Function ========================


def wait(
    fs: Union[List, Tuple, Set, Dict, Any],
    *futs,
    timeout: Optional[float] = None,
    return_when: str = ALL_COMPLETED,
    polling: Union[PollingAlgorithm, str] = PollingAlgorithm.Adaptive,
    progress: Union[bool, Dict, Callable, None] = None,
    recurse: bool = False,
) -> Tuple[Set[BaseFuture], Set[BaseFuture]]:
    """Wait for futures to complete and return done and not_done sets.

    Follows concurrent.futures.wait() API:
    https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.wait

    This function efficiently waits for futures by:
    - Using adaptive polling to balance responsiveness and CPU usage
    - Batch-checking Ray futures with single ray.wait() call
    - Supporting progress tracking (progress bar or callback)
    - Handling nested structures with recurse parameter

    Args:
        fs: Primary argument - can be:
            - List/tuple/set of futures (most common)
            - Dict with futures as values (keys preserved)
            - Single future
        *futs: Additional futures (only if fs is not a structure)
        timeout: Maximum time to wait in seconds (None = indefinite)
        return_when: Condition for returning. Options:
            - ALL_COMPLETED: Wait until all futures are done (default)
            - FIRST_COMPLETED: Return when any future completes
            - FIRST_EXCEPTION: Return when any future raises exception
        polling: Polling algorithm for checking completion
            - PollingAlgorithm enum value or string name
            - Default: Adaptive (adjusts based on completion rate)
        progress: Progress tracking configuration
            - None/False: No progress
            - True: Auto ProgressBar with miniters=len/100
            - Dict: ProgressBar configuration
            - Callable: Progress callback(completed, total, elapsed)
        recurse: If True, recursively wrap nested futures in structures

    Returns:
        Tuple of (done, not_done) sets containing BaseFuture instances

    Raises:
        TimeoutError: If timeout expires before return_when condition met
        ValueError: If return_when is invalid or if fs is a structure and *futs provided

    Example:
        Basic usage (most common):
            ```python
            from concurry import wait, ALL_COMPLETED, FIRST_COMPLETED

            # Wait for list of futures (most common)
            futures = [worker.task(i) for i in range(10)]
            done, not_done = wait(futures, timeout=30)

            # Wait for dict of futures (preserves keys)
            futures_dict = {"task1": f1, "task2": f2}
            done, not_done = wait(futures_dict)

            # Return when first completes
            done, not_done = wait(futures, return_when=FIRST_COMPLETED)
            ```

        Multiple individual futures:
            ```python
            # Pass multiple futures directly (less common)
            done, not_done = wait(future1, future2, future3)
            ```

        With progress tracking:
            ```python
            # Auto progress bar
            done, not_done = wait(futures, progress=True)

            # Custom progress bar
            done, not_done = wait(
                futures,
                progress={"desc": "Waiting", "unit": "task", "color": "#ff0000"}
            )

            # Callback function
            def progress_callback(completed, total, elapsed):
                print(f"Progress: {completed}/{total} ({elapsed:.1f}s)")

            done, not_done = wait(futures, progress=progress_callback)
            ```

        With custom polling:
            ```python
            # Use exponential backoff
            done, not_done = wait(futures, polling="exponential")

            # Fixed interval polling
            done, not_done = wait(futures, polling="fixed")
            ```

        With nested structures:
            ```python
            # Nested futures
            nested = [[future1, future2], [future3, future4]]
            done, not_done = wait(nested, recurse=True)
            ```
    """
    # Validate return_when
    valid_conditions = {ALL_COMPLETED, FIRST_COMPLETED, FIRST_EXCEPTION}
    if return_when not in valid_conditions:
        raise ValueError(f"Invalid return_when: {return_when}. Must be one of: {', '.join(valid_conditions)}")

    # Validate usage: can't mix structure and *futs
    if len(futs) > 0 and isinstance(fs, (list, tuple, set, dict)):
        raise ValueError(
            "Cannot provide both a structure (list/tuple/set/dict) as first argument "
            "and additional futures via *futs. Either pass a structure, or pass individual futures."
        )

    # Build futures list based on input pattern
    if len(futs) > 0:
        # Multiple individual futures: wait(f1, f2, f3)
        futures_list = [fs] + list(futs)
        futures_list = [wrap_future(f) for f in futures_list]
    elif isinstance(fs, dict):
        # Dict of futures - extract values only, keys are not futures
        futures_list = [wrap_future(v) for v in fs.values()]
    elif isinstance(fs, (list, tuple, set)):
        # Structure of futures
        if recurse:
            wrapped = _wrap_all_futures(fs, recurse=True)
            # Flatten to get all futures
            all_futures = []

            def collect_futures(obj):
                if isinstance(obj, BaseFuture):
                    all_futures.append(obj)
                return obj

            map_collection(wrapped, collect_futures, recurse=True)
            futures_list = all_futures
        else:
            futures_list = [wrap_future(f) for f in fs]
    else:
        # Single future
        futures_list = [wrap_future(fs)]

    # Initialize sets
    done: Set[BaseFuture] = set()
    not_done: Set[BaseFuture] = set(futures_list)

    # Early return if empty
    if len(futures_list) == 0:
        return done, not_done

    # Create polling strategy
    if isinstance(polling, str):
        polling = PollingAlgorithm(polling)
    strategy = create_polling_strategy(polling)

    # Create progress tracker
    total = len(futures_list)
    tracker = _create_progress_tracker(progress, total, "Waiting")
    start_time = time.time()

    # Initial check for already-done futures
    initial_done = _check_futures_batch(not_done)
    done.update(initial_done)
    not_done.difference_update(initial_done)
    _update_progress(tracker, len(done), total, time.time() - start_time)

    # Check if we can return early
    if return_when == FIRST_COMPLETED and len(done) > 0:
        if isinstance(tracker, ProgressBar):
            tracker.success()
        return done, not_done

    if return_when == FIRST_EXCEPTION:
        for fut in done:
            try:
                if fut.exception(timeout=0) is not None:
                    if isinstance(tracker, ProgressBar):
                        tracker.success()
                    return done, not_done
            except Exception:
                pass

    if return_when == ALL_COMPLETED and len(not_done) == 0:
        if isinstance(tracker, ProgressBar):
            tracker.success()
        return done, not_done

    # Main polling loop
    while True:
        # Check timeout
        elapsed = time.time() - start_time
        if timeout is not None and elapsed >= timeout:
            if isinstance(tracker, ProgressBar):
                tracker.stop("Timeout")
            raise TimeoutError(
                f"wait() timed out after {elapsed:.2f}s. "
                f"Completed {len(done)}/{total} futures. "
                f"return_when={return_when}"
            )

        # Check batch of not_done futures
        newly_done = _check_futures_batch(not_done)

        if len(newly_done) > 0:
            # Futures completed
            done.update(newly_done)
            not_done.difference_update(newly_done)
            strategy.record_completion()
            _update_progress(tracker, len(done), total, elapsed)

            # Check return conditions
            if return_when == FIRST_COMPLETED:
                if isinstance(tracker, ProgressBar):
                    tracker.success()
                return done, not_done

            if return_when == FIRST_EXCEPTION:
                for fut in newly_done:
                    try:
                        if fut.exception(timeout=0) is not None:
                            if isinstance(tracker, ProgressBar):
                                tracker.success()
                            return done, not_done
                    except Exception:
                        pass

            if return_when == ALL_COMPLETED and len(not_done) == 0:
                if isinstance(tracker, ProgressBar):
                    tracker.success()
                return done, not_done
        else:
            # No completions
            strategy.record_no_completion()

        # Sleep before next check
        interval = strategy.get_next_interval()
        time.sleep(interval)


# ======================== gather() Function ========================


def gather(
    fs: Union[List, Tuple, Set, Dict, Any],
    *futs,
    return_exceptions: bool = False,
    iter: bool = False,
    timeout: Optional[float] = None,
    polling: Union[PollingAlgorithm, str] = PollingAlgorithm.Adaptive,
    progress: Union[bool, Dict, Callable, None] = None,
    recurse: bool = False,
) -> Union[List[Any], Dict[Any, Any], Iterator[Tuple[int, Any]]]:
    """Gather results from multiple futures.

    Similar to asyncio.gather() but works with all future types and provides
    additional features like progress tracking, adaptive polling, and iterator mode.

    Args:
        fs: Primary argument - can be:
            - List/tuple/set of futures (most common) -> returns list
            - Dict with futures as values -> returns dict with same keys
            - Single future -> returns list with one element
        *futs: Additional futures (only if fs is not a structure)
        return_exceptions: If True, return exceptions as results instead of raising
        iter: If True, return generator yielding results as they complete
        timeout: Maximum time to wait for all results (None = indefinite)
        polling: Polling algorithm for checking completion
        progress: Progress tracking configuration
            - None/False: No progress
            - True: Auto ProgressBar with miniters=len/100
            - Dict: ProgressBar configuration
            - Callable: Progress callback(completed, total, elapsed)
        recurse: If True, recursively gather nested futures in structures

    Returns:
        If iter=False and fs is list/tuple: List of results in same order
        If iter=False and fs is dict: Dict with same keys and gathered values
        If iter=True: Generator yielding (index/key, result) as futures complete

    Raises:
        Exception: Any exception from futures (if return_exceptions=False)
        TimeoutError: If timeout expires before all futures complete
        ValueError: If fs is a structure and *futs is provided

    Example:
        Blocking mode (most common):
            ```python
            from concurry import gather

            # Gather list of futures (most common)
            futures = [worker.task(i) for i in range(10)]
            results = gather(futures)  # Returns list

            # Gather dict of futures (preserves keys)
            futures_dict = {"task1": f1, "task2": f2}
            results = gather(futures_dict)  # Returns {"task1": r1, "task2": r2}

            # With exception handling
            results = gather(futures, return_exceptions=True)
            # May contain Exception objects
            ```

        Multiple individual futures (less common):
            ```python
            # Pass multiple futures directly
            results = gather(future1, future2, future3)
            ```

        Iterator mode:
            ```python
            # Process as they arrive (out of order)
            for idx, result in gather(futures, iter=True):
                print(f"Future {idx} completed: {result}")

            # With dict
            futures_dict = {"task1": f1, "task2": f2}
            for key, result in gather(futures_dict, iter=True):
                print(f"{key} completed: {result}")
            ```

        With progress and timeout:
            ```python
            results = gather(
                futures,
                timeout=60,
                progress={"desc": "Gathering", "unit": "result"}
            )
            ```

        Nested structure handling:
            ```python
            # Gather nested futures
            nested_futures = [[f1, f2], [f3, f4]]
            results = gather(nested_futures, recurse=True)
            # Returns: [[r1, r2], [r3, r4]]
            ```
    """
    # Validate usage: can't mix structure and *futs
    if len(futs) > 0 and isinstance(fs, (list, tuple, set, dict)):
        raise ValueError(
            "Cannot provide both a structure (list/tuple/set/dict) as first argument "
            "and additional futures via *futs. Either pass a structure, or pass individual futures."
        )

    # Build args tuple for backend functions
    if len(futs) > 0:
        # Multiple individual futures: gather(f1, f2, f3)
        args = (fs,) + futs
        is_dict_input = False
    else:
        # Single structure or single future
        args = (fs,)
        is_dict_input = isinstance(fs, dict)

    # Delegate to appropriate backend
    if iter:
        return _gather_iter_backend(
            args, return_exceptions, timeout, polling, progress, recurse, is_dict_input
        )
    else:
        return _gather_blocking_backend(
            args, return_exceptions, timeout, polling, progress, recurse, is_dict_input
        )


def _gather_blocking_backend(
    fs: tuple,
    return_exceptions: bool,
    timeout: Optional[float],
    polling: Union[PollingAlgorithm, str],
    progress: Union[bool, Dict, Callable, None],
    recurse: bool,
    is_dict_input: bool,
) -> Union[List[Any], Dict[Any, Any]]:
    """Backend for gather with iter=False. Waits for all futures and returns ordered results.

    Args:
        fs: Tuple of futures (or tuple with single structure)
        return_exceptions: If True, return exceptions as results
        timeout: Maximum wait time
        polling: Polling algorithm
        progress: Progress tracking config
        recurse: Process nested structures
        is_dict_input: If True, first element of fs is a dict, preserve keys

    Returns:
        List of results in same order as input, or dict with keys preserved
    """
    # Handle empty input
    if len(fs) == 0:
        return []

    # Handle dict input specially to preserve keys
    if is_dict_input and len(fs) == 1:
        futures_dict = fs[0]
        if len(futures_dict) == 0:
            return {}

        # Extract keys and futures
        keys = list(futures_dict.keys())
        futures_list = [wrap_future(v) for v in futures_dict.values()]

        # Wait for all futures
        done, not_done = wait(
            futures_list, timeout=timeout, return_when=ALL_COMPLETED, polling=polling, progress=progress
        )

        if len(not_done) > 0:
            raise TimeoutError(f"gather() timed out. {len(not_done)}/{len(futures_list)} futures incomplete")

        # Collect results preserving keys
        results_dict = {}
        for key, fut in zip(keys, futures_list):
            try:
                result = fut.result(timeout=0)  # Should be immediate since done
                results_dict[key] = result
            except Exception as e:
                if return_exceptions:
                    results_dict[key] = e
                else:
                    raise

        return results_dict

    # Handle list/tuple/set input (single structure as first arg)
    if len(fs) == 1 and isinstance(fs[0], (list, tuple, set)):
        # Unwrap the structure
        futures_list = [wrap_future(f) for f in fs[0]]
        total = len(futures_list)

        # Wait for all futures
        done, not_done = wait(
            futures_list, timeout=timeout, return_when=ALL_COMPLETED, polling=polling, progress=progress
        )

        if len(not_done) > 0:
            raise TimeoutError(f"gather() timed out. {len(not_done)}/{total} futures incomplete")

        # Collect results in order
        results = []
        for fut in futures_list:
            try:
                result = fut.result(timeout=0)  # Should be immediate since done
                results.append(result)
            except Exception as e:
                if return_exceptions:
                    results.append(e)
                else:
                    raise

        return results

    # Wrap all futures
    if recurse:
        wrapped = _wrap_all_futures(fs, recurse=True)
        # For recurse mode, we need to gather recursively through the structure
        # This is complex, so for now we'll gather all futures then reconstruct

        def get_result_or_exception(fut):
            if isinstance(fut, BaseFuture):
                try:
                    return fut.result(timeout=0 if fut.done() else timeout)
                except Exception as e:
                    if return_exceptions:
                        return e
                    else:
                        raise
            return fut

        # Wait for all futures first
        all_futures = []

        def collect_futures(obj):
            if isinstance(obj, BaseFuture):
                all_futures.append(obj)
            return obj

        map_collection(wrapped, collect_futures, recurse=True)

        if len(all_futures) > 0:
            # Wait for all to complete
            done, not_done = wait(
                all_futures, timeout=timeout, return_when=ALL_COMPLETED, polling=polling, progress=progress
            )

            if len(not_done) > 0:
                raise TimeoutError(
                    f"gather() timed out. {len(not_done)}/{len(all_futures)} futures incomplete"
                )

        # Now get all results recursively
        results = map_collection(wrapped, get_result_or_exception, recurse=True)
        # If single item in tuple, return as list
        if len(fs) == 1:
            return [results]
        return list(results) if isinstance(results, tuple) else [results]

    else:
        # Non-recursive mode
        futures_list = [wrap_future(f) for f in fs]
        total = len(futures_list)

        # Wait for all futures
        done, not_done = wait(
            futures_list, timeout=timeout, return_when=ALL_COMPLETED, polling=polling, progress=progress
        )

        if len(not_done) > 0:
            raise TimeoutError(f"gather() timed out. {len(not_done)}/{total} futures incomplete")

        # Collect results in order
        results = []
        for fut in futures_list:
            try:
                result = fut.result(timeout=0)  # Should be immediate since done
                results.append(result)
            except Exception as e:
                if return_exceptions:
                    results.append(e)
                else:
                    raise

        return results


def _gather_iter_backend(
    fs: tuple,
    return_exceptions: bool,
    timeout: Optional[float],
    polling: Union[PollingAlgorithm, str],
    progress: Union[bool, Dict, Callable, None],
    recurse: bool,
    is_dict_input: bool,
) -> Iterator[Tuple[Union[int, Any], Any]]:
    """Backend generator for gather with iter=True. Yields (index/key, result) as futures complete.

    Args:
        fs: Tuple of futures (or tuple with single structure)
        return_exceptions: If True, yield exceptions as results
        timeout: Maximum wait time
        polling: Polling algorithm
        progress: Progress tracking config
        recurse: Process nested structures
        is_dict_input: If True, first element of fs is a dict, yield keys instead of indices

    Yields:
        (index/key, result) tuples in completion order
    """
    # Handle empty input
    if len(fs) == 0:
        return

    # Handle dict input specially to yield keys
    if is_dict_input and len(fs) == 1:
        futures_dict = fs[0]
        if len(futures_dict) == 0:
            return

        # Extract keys and futures, maintaining mapping
        keys_list = list(futures_dict.keys())
        futures_list = [wrap_future(v) for v in futures_dict.values()]
        future_to_key = {id(fut): key for fut, key in zip(futures_list, keys_list)}
    # Handle list/tuple/set input (single structure as first arg)
    elif len(fs) == 1 and isinstance(fs[0], (list, tuple, set)):
        # Unwrap the structure
        futures_list = [wrap_future(f) for f in fs[0]]
        future_to_key = {id(fut): i for i, fut in enumerate(futures_list)}
    else:
        # Wrap all futures
        if recurse:
            wrapped = _wrap_all_futures(fs, recurse=True)
            all_futures = []
            future_to_path = {}  # Map future to its path in structure

            def collect_with_path(obj, path=[]):
                if isinstance(obj, BaseFuture):
                    all_futures.append(obj)
                    future_to_path[id(obj)] = path.copy()
                return obj

            # Collect all futures with their paths
            def recursive_collect(obj, path=[]):
                if isinstance(obj, BaseFuture):
                    collect_with_path(obj, path)
                elif isinstance(obj, (list, tuple)):
                    for i, item in enumerate(obj):
                        recursive_collect(item, path + [i])
                elif isinstance(obj, dict):
                    for k, v in obj.items():
                        recursive_collect(v, path + [k])
                return obj

            recursive_collect(wrapped)

            futures_list = all_futures
            future_to_key = future_to_path
        else:
            futures_list = [wrap_future(f) for f in fs]
            future_to_key = {id(fut): i for i, fut in enumerate(futures_list)}

    total = len(futures_list)
    if total == 0:
        return

    # Create polling strategy
    if isinstance(polling, str):
        polling = PollingAlgorithm(polling)
    strategy = create_polling_strategy(polling)

    # Create progress tracker
    tracker = _create_progress_tracker(progress, total, "Gathering")
    start_time = time.time()

    # Track yielded futures
    pending = set(futures_list)
    yielded_count = 0

    # Main loop
    while len(pending) > 0:
        # Check timeout
        elapsed = time.time() - start_time
        if timeout is not None and elapsed >= timeout:
            if isinstance(tracker, ProgressBar):
                tracker.stop("Timeout")
            raise TimeoutError(
                f"gather(iter=True) timed out after {elapsed:.2f}s. Yielded {yielded_count}/{total} results"
            )

        # Check batch
        newly_done = _check_futures_batch(pending)

        if len(newly_done) > 0:
            # Yield completed futures
            for fut in newly_done:
                # Get the index or key for this future
                key_or_index = future_to_key[id(fut)]
                # For non-recurse non-dict, it's just an index
                # For dict, it's the actual key
                # For recurse, it could be a path (list)
                if isinstance(key_or_index, list) and len(key_or_index) == 1:
                    key_or_index = key_or_index[0]

                try:
                    result = fut.result(timeout=0)
                    yield (key_or_index, result)
                except Exception as e:
                    if return_exceptions:
                        yield (key_or_index, e)
                    else:
                        if isinstance(tracker, ProgressBar):
                            tracker.failure()
                        raise

                yielded_count += 1

            pending.difference_update(newly_done)
            strategy.record_completion()
            _update_progress(tracker, yielded_count, total, elapsed)
        else:
            strategy.record_no_completion()

        # Sleep before next check
        if len(pending) > 0:
            interval = strategy.get_next_interval()
            time.sleep(interval)

    # All done
    if isinstance(tracker, ProgressBar):
        tracker.success()
