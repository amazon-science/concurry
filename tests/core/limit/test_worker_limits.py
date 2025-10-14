"""Tests for Worker integration with Limits."""

import morphic
import pytest

import concurry
from concurry import (
    CallLimit,
    LimitSet,
    RateLimit,
    RateLimiterAlgorithm,
    ResourceLimit,
    Worker,
)
from concurry.utils import _IS_RAY_INSTALLED

# Parametrize modes to test
WORKER_MODES = ["sync", "thread", "process", "asyncio"]

# Add Ray if it's installed
if _IS_RAY_INSTALLED:
    WORKER_MODES.append("ray")


@pytest.fixture(params=WORKER_MODES)
def worker_mode(request):
    """Fixture providing different worker modes."""
    # Initialize Ray if needed
    if request.param == "ray":
        import ray

        if not ray.is_initialized():
            ray.init(
                ignore_reinit_error=True,
                num_cpus=4,
                runtime_env={"py_modules": [concurry, morphic]},
            )

    yield request.param


class TestWorkerLimits:
    """Test Worker integration with Limits.

    Note: Ray workers with limits are skipped because LimitSet contains threading
    primitives (locks, semaphores) that cannot be pickled for Ray serialization.
    This is a known limitation of Ray's serialization system.
    """

    def test_worker_with_limits(self, worker_mode):
        """Test that worker can access limits."""
        # Pass list of Limits - each worker will create its own private LimitSet
        limits = [
            RateLimit(
                key="tokens", window_seconds=1, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=100
            ),
            ResourceLimit(key="connections", capacity=5),
        ]

        class TestWorker(Worker):
            def __init__(self):
                self.results = []

            def process(self, value: int) -> int:
                # Access limits
                assert self.limits is not None
                return value * 2

        worker = TestWorker.options(mode=worker_mode, limits=limits).init()
        result = worker.process(10).result()
        assert result == 20
        worker.stop()

    def test_worker_using_limits(self, worker_mode):
        """Test worker actually using limits."""
        # Pass list of Limits - each worker will create its own private LimitSet
        limits = [
            RateLimit(
                key="tokens", window_seconds=1, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=100
            ),
        ]

        class TokenWorker(Worker):
            def __init__(self):
                pass

            def process(self, tokens_needed: int) -> str:
                with self.limits.acquire(requested={"tokens": tokens_needed}) as acq:
                    # Simulate work
                    actual_used = tokens_needed - 5  # Use slightly less
                    acq.update(usage={"tokens": actual_used})
                    return f"Used {actual_used} tokens"

        worker = TokenWorker.options(mode=worker_mode, limits=limits).init()
        result = worker.process(50).result()
        assert "Used 45 tokens" == result
        worker.stop()

    def test_worker_with_resource_limits(self, worker_mode):
        """Test worker using resource limits."""
        # Pass list of Limits - each worker will create its own private LimitSet
        limits = [ResourceLimit(key="connections", capacity=2)]

        class DBWorker(Worker):
            def __init__(self):
                self.conn_count = 0

            def query(self) -> str:
                with self.limits.acquire(requested={"connections": 1}):
                    self.conn_count += 1
                    # Simulate DB query
                    return "Query result"

        worker = DBWorker.options(mode=worker_mode, limits=limits).init()

        # Should succeed
        result1 = worker.query().result()
        assert result1 == "Query result"

        # Should succeed
        result2 = worker.query().result()
        assert result2 == "Query result"

        worker.stop()

    def test_worker_with_mixed_limits(self, worker_mode):
        """Test worker using mixed limit types."""
        # Pass list of Limits - each worker will create its own private LimitSet
        limits = [
            CallLimit(window_seconds=60, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=100),
            RateLimit(
                key="input_tokens",
                window_seconds=1,
                algorithm=RateLimiterAlgorithm.TokenBucket,
                capacity=1000,
            ),
            RateLimit(
                key="output_tokens",
                window_seconds=1,
                algorithm=RateLimiterAlgorithm.TokenBucket,
                capacity=500,
            ),
            ResourceLimit(key="db_connections", capacity=2),
        ]

        class LLMWorker(Worker):
            def __init__(self, model: str):
                self.model = model

            def process(self, prompt: str) -> str:
                # Acquire resource first
                with self.limits.acquire(requested={"db_connections": 1}):
                    # Then acquire rate limits
                    with self.limits.acquire(requested={"input_tokens": 100, "output_tokens": 50}):
                        # Update with actual usage
                        result = f"Processed: {prompt}"
                        # Assume we calculated actual usage
                        actual_input = 80
                        actual_output = 40

                        # This should raise error because we're outside the inner context
                        # Actually, we need to update inside the context
                        return result

        worker = LLMWorker.options(mode=worker_mode, limits=limits).init("test-model")

        # This will fail because we're not updating inside the context
        with pytest.raises(RuntimeError):
            worker.process("test prompt").result()

        worker.stop()

    def test_worker_with_nested_limit_acquisition(self, worker_mode):
        """Test worker with properly nested limit acquisition."""
        # Pass list of Limits - each worker will create its own private LimitSet
        limits = [
            RateLimit(
                key="tokens", window_seconds=1, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=1000
            ),
            ResourceLimit(key="connections", capacity=2),
        ]

        class ProperWorker(Worker):
            def __init__(self):
                pass

            def process(self, value: int) -> str:
                # Proper nesting
                with self.limits.acquire(requested={"connections": 1}) as res_acq:
                    with self.limits.acquire(requested={"tokens": 100}) as rate_acq:
                        # Do work
                        rate_acq.update(usage={"tokens": 80})
                        return f"Processed {value}"

        worker = ProperWorker.options(mode=worker_mode, limits=limits).init()
        result = worker.process(42).result()
        assert result == "Processed 42"
        worker.stop()

    def test_worker_without_limits(self, worker_mode):
        """Test that worker works without limits."""

        class SimpleWorker(Worker):
            def __init__(self):
                pass

            def process(self, value: int) -> int:
                return value * 2

        worker = SimpleWorker.options(mode=worker_mode).init()
        result = worker.process(10).result()
        assert result == 20
        worker.stop()

    def test_worker_get_limit_by_key(self, worker_mode):
        """Test worker accessing individual limits by key."""
        limits = [
            CallLimit(window_seconds=60, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=100),
            RateLimit(
                key="tokens", window_seconds=1, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=1000
            ),
            ResourceLimit(key="connections", capacity=5),
        ]

        class InspectorWorker(Worker):
            def __init__(self):
                pass

            def get_limit_capacity(self, limit_key: str) -> int:
                """Get the capacity of a specific limit."""
                limit = self.limits[limit_key]
                return limit.capacity

            def get_limit_stats(self, limit_key: str) -> dict:
                """Get statistics for a specific limit."""
                limit = self.limits[limit_key]
                return limit.get_stats()

            def check_all_limits(self) -> dict:
                """Check capacities of all configured limits."""
                return {
                    "call_count_capacity": self.limits["call_count"].capacity,
                    "tokens_capacity": self.limits["tokens"].capacity,
                    "connections_capacity": self.limits["connections"].capacity,
                }

        worker = InspectorWorker.options(mode=worker_mode, limits=limits).init()

        # Test accessing CallLimit (special "call_count" key)
        call_capacity = worker.get_limit_capacity("call_count").result()
        assert call_capacity == 100

        # Test accessing RateLimit
        token_capacity = worker.get_limit_capacity("tokens").result()
        assert token_capacity == 1000

        # Test accessing ResourceLimit
        conn_capacity = worker.get_limit_capacity("connections").result()
        assert conn_capacity == 5

        # Test getting stats for a limit
        token_stats = worker.get_limit_stats("tokens").result()
        assert token_stats["key"] == "tokens"
        assert token_stats["capacity"] == 1000

        # Test checking all limits at once
        all_capacities = worker.check_all_limits().result()
        assert all_capacities["call_count_capacity"] == 100
        assert all_capacities["tokens_capacity"] == 1000
        assert all_capacities["connections_capacity"] == 5

        worker.stop()


class TestWorkerSharedLimits:
    """Test Worker integration with shared LimitSets."""

    def test_worker_with_shared_limits_list_conversion(self):
        """Test that passing list of Limits creates private LimitSet."""
        limits_list = [
            RateLimit(
                key="tokens", window_seconds=1, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=100
            ),
            ResourceLimit(key="connections", capacity=5),
        ]

        class TestWorker(Worker):
            def process(self) -> bool:
                # Should have limits set
                return self.limits is not None

        # Passing list should create private LimitSet (no warning for sync mode)
        worker = TestWorker.options(mode="sync", limits=limits_list).init()
        result = worker.process().result()
        assert result is True
        worker.stop()

    def test_worker_with_shared_limitset_thread(self):
        """Test workers sharing a LimitSet in thread mode."""
        shared_limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens", window_seconds=1, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=100
                )
            ],
            shared=True,
            mode="thread",
        )

        class TestWorker(Worker):
            def process(self) -> str:
                with self.limits.acquire(requested={"tokens": 10}) as acq:
                    acq.update(usage={"tokens": 10})
                    return "done"

        # Create two workers sharing the same LimitSet
        worker1 = TestWorker.options(mode="thread", limits=shared_limits).init()
        worker2 = TestWorker.options(mode="thread", limits=shared_limits).init()

        # Both should be able to use the shared limits
        result1 = worker1.process().result()
        result2 = worker2.process().result()

        assert result1 == "done"
        assert result2 == "done"

        worker1.stop()
        worker2.stop()

    def test_worker_with_shared_limitset_process(self):
        """Test workers sharing a LimitSet in process mode."""
        shared_limits = LimitSet(
            limits=[ResourceLimit(key="connections", capacity=2)], shared=True, mode="process"
        )

        class TestWorker(Worker):
            def process(self) -> str:
                with self.limits.acquire(requested={"connections": 1}):
                    return "done"

        # Create two process workers sharing the same LimitSet
        worker1 = TestWorker.options(mode="process", limits=shared_limits).init()
        worker2 = TestWorker.options(mode="process", limits=shared_limits).init()

        result1 = worker1.process().result()
        result2 = worker2.process().result()

        assert result1 == "done"
        assert result2 == "done"

        worker1.stop()
        worker2.stop()

    def test_worker_incompatible_limitset_mode(self):
        """Test that incompatible LimitSet mode raises error."""
        # Create thread-mode shared LimitSet
        thread_limits = LimitSet(
            limits=[
                RateLimit(
                    key="tokens", window_seconds=1, algorithm=RateLimiterAlgorithm.TokenBucket, capacity=100
                )
            ],
            shared=True,
            mode="thread",
        )

        class TestWorker(Worker):
            def process(self) -> str:
                return "done"

        # Should raise error when trying to use with process worker
        with pytest.raises(ValueError, match="not compatible"):
            TestWorker.options(mode="process", limits=thread_limits).init()
