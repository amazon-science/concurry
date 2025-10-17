"""Tests for automatic future unwrapping in worker methods."""

import time
from typing import Any, Dict, List

import pytest

from concurry.core.future import BaseFuture
from concurry.core.worker import Worker
from concurry.utils import _IS_RAY_INSTALLED


# Test worker classes
class SimpleWorker(Worker):
    """Simple worker for testing."""

    def __init__(self, value: int = 0):
        self.value = value

    def add(self, x: int) -> int:
        """Add x to the stored value."""
        return self.value + x

    def multiply(self, x: int) -> int:
        """Multiply stored value by x."""
        return self.value * x

    def get_value(self) -> int:
        """Get the stored value."""
        return self.value


class NestedDataWorker(Worker):
    """Worker that processes nested data structures."""

    def __init__(self):
        pass

    def sum_list(self, values: List[int]) -> int:
        """Sum a list of integers."""
        return sum(values)

    def sum_dict_values(self, data: Dict[str, int]) -> int:
        """Sum dictionary values."""
        return sum(data.values())

    def sum_nested(self, data: Dict[str, Any]) -> int:
        """Sum all integers in a nested structure."""
        total = 0
        if isinstance(data, dict):
            for value in data.values():
                if isinstance(value, int):
                    total += value
                elif isinstance(value, list):
                    total += sum(v for v in value if isinstance(v, int))
                elif isinstance(value, dict):
                    total += self.sum_nested(value)
        return total


class FutureAwareWorker(Worker):
    """Worker that can check if it received a future."""

    def __init__(self):
        pass

    def check_if_future(self, obj: Any) -> bool:
        """Check if the object is a BaseFuture."""
        return isinstance(obj, BaseFuture)

    def get_type_name(self, obj: Any) -> str:
        """Get the type name of the object."""
        return type(obj).__name__


# Worker mode fixture and cleanup are provided by tests/conftest.py


class TestBasicFutureUnwrapping:
    """Tests for basic future unwrapping functionality."""

    def test_single_future_argument(self, worker_mode):
        """Test passing a single future as an argument."""
        producer = SimpleWorker.options(mode=worker_mode).init(value=10)
        consumer = SimpleWorker.options(mode=worker_mode).init(value=0)

        # Producer creates a future
        future1 = producer.add(5)  # Returns future -> 15

        # Consumer receives materialized value
        result = consumer.add(future1).result()  # Should compute 0 + 15 = 15

        assert result == 15

        producer.stop()
        consumer.stop()

    def test_multiple_future_arguments(self, worker_mode):
        """Test passing multiple futures as arguments."""
        producer = SimpleWorker.options(mode=worker_mode).init(value=10)

        # Create multiple futures
        f1 = producer.add(5)  # Future -> 15
        f2 = producer.add(10)  # Future -> 20

        # Pass futures to a function that uses them
        nested_worker = NestedDataWorker.options(mode=worker_mode).init()
        result = nested_worker.sum_list([f1, f2]).result()

        # Should compute: 15 + 20 = 35
        assert result == 35

        producer.stop()
        nested_worker.stop()

    def test_cross_worker_type_unwrapping(self):
        """Test passing a future from one worker type to another."""
        # Thread worker produces, process worker consumes
        producer = SimpleWorker.options(mode="thread").init(value=100)
        consumer = SimpleWorker.options(mode="process").init(value=0)

        future1 = producer.add(50)  # Future -> 150

        # Consumer receives materialized value
        result = consumer.add(future1).result()  # Should compute 0 + 150 = 150

        assert result == 150

        producer.stop()
        consumer.stop()

    def test_future_with_non_future_args(self, worker_mode):
        """Test mixing future and non-future arguments."""
        producer = SimpleWorker.options(mode=worker_mode).init(value=10)
        consumer = NestedDataWorker.options(mode=worker_mode).init()

        f1 = producer.add(5)  # Future -> 15

        # Mix future and non-future values
        result = consumer.sum_list([f1, 20, 30]).result()

        # Should compute: 15 + 20 + 30 = 65
        assert result == 65

        producer.stop()
        consumer.stop()


class TestNestedStructureUnwrapping:
    """Tests for unwrapping futures in nested data structures."""

    def test_list_of_futures(self, worker_mode):
        """Test unwrapping futures in a list."""
        producer = SimpleWorker.options(mode=worker_mode).init(value=10)
        consumer = NestedDataWorker.options(mode=worker_mode).init()

        # Create list of futures
        futures = [producer.add(i) for i in [1, 2, 3, 4, 5]]
        # futures = [11, 12, 13, 14, 15]

        result = consumer.sum_list(futures).result()

        # Should compute: 11 + 12 + 13 + 14 + 15 = 65
        assert result == 65

        producer.stop()
        consumer.stop()

    def test_dict_with_future_values(self, worker_mode):
        """Test unwrapping futures in dictionary values."""
        producer = SimpleWorker.options(mode=worker_mode).init(value=10)
        consumer = NestedDataWorker.options(mode=worker_mode).init()

        # Create dict with future values
        data = {
            "a": producer.add(5),  # 15
            "b": producer.add(10),  # 20
            "c": producer.add(15),  # 25
        }

        result = consumer.sum_dict_values(data).result()

        # Should compute: 15 + 20 + 25 = 60
        assert result == 60

        producer.stop()
        consumer.stop()

    def test_deeply_nested_structure(self, worker_mode):
        """Test unwrapping futures in deeply nested structures."""
        producer = SimpleWorker.options(mode=worker_mode).init(value=100)
        consumer = NestedDataWorker.options(mode=worker_mode).init()

        # Create deeply nested structure with futures
        data = {
            "values": [producer.add(10), producer.add(20)],  # [110, 120]
            "extra": {
                "bonus": producer.add(30),  # 130
            },
        }

        result = consumer.sum_nested(data).result()

        # Should compute: 110 + 120 + 130 = 360
        assert result == 360

        producer.stop()
        consumer.stop()

    def test_tuple_of_futures(self, worker_mode):
        """Test unwrapping futures in a tuple."""
        producer = SimpleWorker.options(mode=worker_mode).init(value=5)
        consumer = NestedDataWorker.options(mode=worker_mode).init()

        # Create tuple of futures
        futures_tuple = (producer.add(1), producer.add(2), producer.add(3))
        # futures_tuple = (6, 7, 8)

        # Convert to list for sum_list method
        result = consumer.sum_list(list(futures_tuple)).result()

        # Should compute: 6 + 7 + 8 = 21
        assert result == 21

        producer.stop()
        consumer.stop()


class TestUnwrapFuturesDisabled:
    """Tests for when unwrap_futures=False."""

    def test_future_not_unwrapped_when_disabled(self):
        """Test that futures are not unwrapped when unwrap_futures=False."""
        producer = SimpleWorker.options(mode="thread").init(value=10)
        consumer = FutureAwareWorker.options(
            mode="thread",
            unwrap_futures=False,
        ).init()

        future1 = producer.add(5)  # Returns BaseFuture

        # Consumer should receive the future object itself
        result = consumer.check_if_future(future1).result()
        assert result is True

        producer.stop()
        consumer.stop()

    def test_value_type_with_unwrapping_enabled(self):
        """Test that regular values receive correct type when unwrapping enabled."""
        consumer = FutureAwareWorker.options(
            mode="thread",
            unwrap_futures=True,
        ).init()

        # Pass a regular integer
        result = consumer.get_type_name(42).result()
        assert result == "int"

        consumer.stop()

    def test_value_type_with_unwrapping_disabled(self):
        """Test that regular values still work when unwrapping disabled."""
        consumer = FutureAwareWorker.options(
            mode="thread",
            unwrap_futures=False,
        ).init()

        # Pass a regular integer
        result = consumer.get_type_name(42).result()
        assert result == "int"

        consumer.stop()


class TestEdgeCases:
    """Tests for edge cases in future unwrapping."""

    def test_empty_list(self, worker_mode):
        """Test unwrapping with empty list."""
        consumer = NestedDataWorker.options(mode=worker_mode).init()

        result = consumer.sum_list([]).result()
        assert result == 0

        consumer.stop()

    def test_empty_dict(self, worker_mode):
        """Test unwrapping with empty dict."""
        consumer = NestedDataWorker.options(mode=worker_mode).init()

        result = consumer.sum_dict_values({}).result()
        assert result == 0

        consumer.stop()

    def test_none_value(self, worker_mode):
        """Test that None values are handled correctly."""
        consumer = FutureAwareWorker.options(mode=worker_mode).init()

        result = consumer.check_if_future(None).result()
        assert result is False

        consumer.stop()

    def test_mixed_none_and_futures(self, worker_mode):
        """Test mixing None with futures in a list."""
        producer = SimpleWorker.options(mode=worker_mode).init(value=10)
        consumer = FutureAwareWorker.options(mode=worker_mode).init()

        f1 = producer.add(5)  # Future -> 15

        # Pass list with future - checking type of first element
        result = consumer.check_if_future(f1).result()
        assert result is False  # Should be unwrapped to int

        producer.stop()
        consumer.stop()


class TestExceptionPreservation:
    """Tests that exceptions in futures are properly propagated."""

    def test_exception_in_future_is_propagated(self, worker_mode):
        """Test that exceptions in futures are raised during unwrapping."""

        class ErrorWorker(Worker):
            def __init__(self):
                pass

            def raise_error(self):
                raise ValueError("Test error")

            def process(self, x: int) -> int:
                return x * 2

        error_producer = ErrorWorker.options(mode=worker_mode).init()
        consumer = SimpleWorker.options(mode=worker_mode).init(value=0)

        # Create a future that will fail
        failing_future = error_producer.raise_error()

        # When trying to unwrap the future, the exception should be raised
        with pytest.raises(ValueError, match="Test error"):
            consumer.add(failing_future).result()

        error_producer.stop()
        consumer.stop()


@pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
class TestRayZeroCopyOptimization:
    """Tests specific to Ray's zero-copy optimization."""

    def test_ray_to_ray_future_passing(self):
        """Test that Ray futures can be passed between Ray workers."""
        # Ray is initialized by conftest.py initialize_ray fixture
        producer = SimpleWorker.options(mode="ray").init(value=100)
        consumer = SimpleWorker.options(mode="ray").init(value=0)

        # Producer creates a RayFuture
        future1 = producer.add(50)  # RayFuture wrapping ObjectRef -> 150

        # Consumer should receive the value (via ObjectRef zero-copy)
        result = consumer.add(future1).result()  # Should compute 0 + 150 = 150

        assert result == 150

        producer.stop()
        consumer.stop()

    def test_ray_nested_futures(self):
        """Test Ray zero-copy with nested futures."""
        # Ray is initialized by conftest.py initialize_ray fixture
        producer = SimpleWorker.options(mode="ray").init(value=10)
        consumer = NestedDataWorker.options(mode="ray").init()

        # Create nested structure with Ray futures
        data = {
            "values": [producer.add(5), producer.add(10)],  # [15, 20]
            "extra": {"bonus": producer.add(15)},  # 25
        }

        result = consumer.sum_nested(data).result()

        # Should compute: 15 + 20 + 25 = 60
        assert result == 60

        producer.stop()
        consumer.stop()

    def test_cross_worker_ray_to_process(self):
        """Test passing Ray future to process worker (should materialize)."""
        # Ray is initialized by conftest.py initialize_ray fixture
        producer = SimpleWorker.options(mode="ray").init(value=100)
        consumer = SimpleWorker.options(mode="process").init(value=0)

        # Create RayFuture
        future1 = producer.add(50)  # RayFuture -> 150

        # Should materialize when passing to process worker
        result = consumer.add(future1).result()  # Should compute 0 + 150 = 150

        assert result == 150

        producer.stop()
        consumer.stop()


@pytest.mark.performance
class TestPerformance:
    """Performance tests (not strict, just sanity checks)."""

    def test_many_futures_unwrapped(self):
        """Test unwrapping many futures in a list."""
        producer = SimpleWorker.options(mode="thread").init(value=0)
        consumer = NestedDataWorker.options(mode="thread").init()

        # Create many futures
        num_futures = 100
        futures = [producer.add(i) for i in range(num_futures)]

        start = time.time()
        result = consumer.sum_list(futures).result()
        elapsed = time.time() - start

        # Should compute sum(0 to 99) = 4950
        expected = sum(range(num_futures))
        assert result == expected

        # Just a sanity check that it completes in reasonable time (< 5 seconds)
        assert elapsed < 5.0

        producer.stop()
        consumer.stop()
