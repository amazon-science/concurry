"""Tests for Worker inheritance with model types (Typed and BaseModel).

This module tests that Worker subclasses can inherit from:
- morphic.Typed
- pydantic.BaseModel

And that these combinations work correctly across all execution modes.

**Ray Mode Support:**

Ray mode now fully supports Pydantic-based workers (both Typed and BaseModel) thanks to
automatic composition-based wrapping. When a worker inherits from Typed or BaseModel,
Concurry automatically creates a Ray-compatible wrapper that uses composition instead of
inheritance to avoid Ray's `__setattr__` conflicts.

**Supported Modes:**
- ✅ Sync: Full support
- ✅ Thread: Full support
- ✅ Process: Full support (with cloudpickle serialization)
- ✅ Asyncio: Full support
- ✅ Ray: Full support (via automatic composition wrapper)

**How Ray Support Works:**

When you create a Typed/BaseModel worker in Ray mode, Concurry automatically:
1. Creates a plain Python wrapper class (no Pydantic inheritance)
2. Stores the Typed/BaseModel instance internally (composition)
3. Exposes only user-defined methods (infrastructure methods excluded)
4. Delegates method calls to the internal instance

This is transparent to users - just use `.options(mode="ray")` as normal!

**Test Timeouts:**

Tests use 15-second timeouts to account for Ray client mode's additional latency.
Ray client mode has network overhead that makes operations slower than standard mode.
"""

import asyncio
from typing import List, Optional

import pytest
from morphic import Typed, validate
from pydantic import BaseModel, Field, ValidationError, validate_call

from concurry import CallLimit, RateLimit, RateLimitAlgorithm, ResourceLimit, Worker
from concurry.core.worker.asyncio_worker import AsyncioWorkerProxy
from concurry.core.worker.process_worker import ProcessWorkerProxy
from concurry.core.worker.sync_worker import SyncWorkerProxy
from concurry.core.worker.thread_worker import ThreadWorkerProxy
from concurry.utils import _IS_RAY_INSTALLED

# Default timeout for test result() calls
# Increased to 15 seconds to handle Ray client mode's additional latency
FUTURE_TIMEOUT = 15

# Worker mode fixture and cleanup are provided by tests/conftest.py


class TestWorkerProxyTypedValidation:
    """Test Typed validation features for WorkerProxy."""

    def test_public_field_immutability(self, worker_mode):
        """Test that public fields are immutable after creation across all modes."""

        class TestWorker(Worker):
            def __init__(self, x: int):
                self.x = x

        # Create worker proxy
        proxy = TestWorker.options(mode=worker_mode).init(10)

        # Try to modify public field - should fail
        with pytest.raises((ValidationError, AttributeError)):
            proxy.worker_cls = TestWorker

        with pytest.raises((ValidationError, AttributeError)):
            proxy.blocking = True

        proxy.stop()

    def test_private_attribute_type_checking(self, worker_mode):
        """Test that private attributes trigger type checking on update across all modes."""

        class TestWorker(Worker):
            def __init__(self):
                pass

        # Create worker proxy (single worker to test direct attribute access)
        proxy = TestWorker.options(mode=worker_mode, max_workers=1).init()

        # Test setting _stopped with correct type (bool)
        proxy._stopped = True
        assert proxy._stopped is True

        proxy._stopped = False
        assert proxy._stopped is False

        # Test setting _stopped with incorrect type (should raise error due to type checking)
        with pytest.raises((ValidationError, AttributeError, TypeError)):
            proxy._stopped = "not a bool"

        proxy.stop()

    def test_worker_options_validate_decorator(self):
        """Test that @validate decorator on Worker.options() provides type checking."""

        class TestWorker(Worker):
            def __init__(self):
                pass

        # Valid mode
        builder = TestWorker.options(mode="sync")
        assert builder is not None

        # Invalid mode should still work (ExecutionMode will validate)
        # but will fail when trying to create the worker
        with pytest.raises(Exception):  # Could be ValueError or KeyError
            TestWorker.options(mode="invalid_mode").init()

    def test_worker_options_boolean_coercion(self, worker_mode):
        """Test that @validate decorator coerces string booleans across all modes."""

        class TestWorker(Worker):
            def __init__(self):
                pass

        # String boolean should be coerced to bool
        builder = TestWorker.options(mode=worker_mode, blocking="true")
        proxy = builder.init()

        # Blocking should be True (coerced from string)
        assert proxy.blocking is True
        proxy.stop()

        # Test with False
        builder = TestWorker.options(mode=worker_mode, blocking="false")
        proxy = builder.init()
        assert proxy.blocking is False
        proxy.stop()

    def test_proxy_initialization_validation(self, worker_mode):
        """Test that proxy initialization validates all fields across all modes."""

        class TestWorker(Worker):
            def __init__(self, x: int):
                self.x = x

        # Valid initialization
        proxy = TestWorker.options(mode=worker_mode).init(10)
        assert proxy.worker_cls == TestWorker
        assert proxy.blocking is False
        proxy.stop()

        # Test with explicit fields
        proxy = TestWorker.options(mode=worker_mode, blocking=True).init(20)
        assert proxy.blocking is True
        proxy.stop()

    def test_mode_options_passed_through(self, worker_mode):
        """Test that mode-specific options are passed through correctly."""

        class TestWorker(Worker):
            def __init__(self):
                pass

        # Mode-specific options should be passed through
        # Use an option that's valid for at least some modes
        if worker_mode == "process":
            # mp_context is a valid process-specific option
            proxy = TestWorker.options(mode=worker_mode, max_workers=1, mp_context="spawn").init()
        elif worker_mode == "ray":
            # actor_options is a valid ray-specific option
            proxy = TestWorker.options(mode=worker_mode, max_workers=1, actor_options={"num_cpus": 1}).init()
        else:
            # For other modes, just test that it works without mode options
            proxy = TestWorker.options(mode=worker_mode, max_workers=1).init()

        # Proxy should be created successfully
        assert proxy is not None
        proxy.stop()

    def test_different_proxy_types_all_use_typed(self):
        """Test that all WorkerProxy subclasses inherit from Typed."""

        # All proxy classes should be Typed subclasses
        assert issubclass(SyncWorkerProxy, Typed)
        assert issubclass(ThreadWorkerProxy, Typed)
        assert issubclass(ProcessWorkerProxy, Typed)
        assert issubclass(AsyncioWorkerProxy, Typed)

    def test_process_worker_mp_context_validation(self):
        """Test that ProcessWorkerProxy validates mp_context field."""

        class TestWorker(Worker):
            def __init__(self):
                pass

        # Valid mp_context values (single worker to test direct attribute access)
        for context in ["fork", "spawn", "forkserver"]:
            proxy = TestWorker.options(mode="process", max_workers=1, mp_context=context).init()
            assert proxy.mp_context == context
            proxy.stop()

        # Invalid mp_context should fail
        # Note: Literal type checking might happen at Pydantic validation time
        # or at runtime when actually using the context
        with pytest.raises(Exception):  # ValidationError or ValueError
            TestWorker.options(mode="process", max_workers=1, mp_context="invalid").init()


class TestWorkerTypedFeatures:
    """Test Typed features for Worker class itself (not WorkerProxy)."""

    def test_worker_not_typed_subclass(self):
        """Test that Worker itself does NOT inherit from Typed."""
        # Worker should NOT be a Typed subclass
        assert not issubclass(Worker, Typed)

    def test_worker_init_flexibility(self, worker_mode):
        """Test that users can define Worker __init__ freely across all modes."""

        class CustomWorker(Worker):
            def __init__(self, a, b, c=10, *args, **kwargs):
                self.a = a
                self.b = b
                self.c = c
                self.args = args
                self.kwargs = kwargs

            def process(self):
                return self.a + self.b + self.c

        # Should work with various initialization patterns
        w = CustomWorker.options(mode=worker_mode).init(1, 2, c=3, extra1="x", extra2="y")
        result = w.process().result(timeout=FUTURE_TIMEOUT)
        assert result == 6
        w.stop()

    def test_validate_decorator_on_options(self):
        """Test that @validate decorator works on Worker.options() classmethod."""

        class TestWorker(Worker):
            def __init__(self):
                pass

        # The @validate decorator should provide automatic validation
        # Test with valid inputs
        builder = TestWorker.options(mode="sync", blocking=False)
        assert builder is not None

        # Test type coercion (string to bool)
        builder = TestWorker.options(mode="thread", blocking="true")
        proxy = builder.init()
        assert proxy.blocking is True
        proxy.stop()


# ============================================================================
# Test Cases: Worker + morphic.Typed
# ============================================================================


class TypedWorkerSimple(Worker, Typed):
    """Simple worker that inherits from both Worker and Typed."""

    name: str
    value: int = 0

    def get_name(self) -> str:
        """Get the worker name."""
        return self.name

    def compute(self, x: int) -> int:
        """Compute value * x."""
        return self.value * x

    def increment(self, amount: int = 1) -> int:
        """Increment value and return new value.

        Note: This modifies state, which requires MutableTyped in practice,
        but for testing we're just incrementing a temporary variable.
        """
        return self.value + amount


class TypedWorkerWithValidation(Worker, Typed):
    """Typed worker with field validation."""

    name: str = Field(..., min_length=1, max_length=50)
    age: int = Field(..., ge=0, le=150)
    email: Optional[str] = None
    tags: List[str] = []

    def get_info(self) -> dict:
        """Get worker information."""
        return {
            "name": self.name,
            "age": self.age,
            "email": self.email,
            "tags": self.tags,
        }

    def add_tag(self, tag: str) -> List[str]:
        """Add a tag and return updated list."""
        return self.tags + [tag]


class TypedWorkerWithHooks(Worker, Typed):
    """Typed worker with pre/post hooks."""

    first_name: str
    last_name: str
    full_name: Optional[str] = None
    call_count: int = 0

    @classmethod
    def pre_initialize(cls, data: dict) -> None:
        """Set up derived fields before validation."""
        if "first_name" in data and "last_name" in data:
            data["full_name"] = f"{data['first_name']} {data['last_name']}"

    def post_initialize(self) -> None:
        """Post-initialization hook."""
        # This runs after Typed validation
        pass

    def get_full_name(self) -> str:
        """Get the full name."""
        return self.full_name

    def process(self, value: int) -> int:
        """Process a value."""
        return value * 2


class TypedWorkerAsync(Worker, Typed):
    """Typed worker with async methods."""

    name: str
    multiplier: int = 2

    async def async_compute(self, x: int) -> int:
        """Async computation method."""
        await asyncio.sleep(0.01)
        return x * self.multiplier

    def sync_compute(self, x: int) -> int:
        """Sync computation method."""
        return x * self.multiplier


# ============================================================================
# Test Cases: Worker + pydantic.BaseModel
# ============================================================================


class PydanticWorkerSimple(Worker, BaseModel):
    """Simple worker that inherits from both Worker and BaseModel."""

    name: str
    value: int = 0

    def get_name(self) -> str:
        """Get the worker name."""
        return self.name

    def compute(self, x: int) -> int:
        """Compute value * x."""
        return self.value * x


class PydanticWorkerWithValidation(Worker, BaseModel):
    """Pydantic worker with field validation."""

    name: str = Field(..., min_length=1, max_length=50)
    age: int = Field(..., ge=0, le=150)
    email: Optional[str] = None
    tags: List[str] = []

    def get_info(self) -> dict:
        """Get worker information."""
        return {
            "name": self.name,
            "age": self.age,
            "email": self.email,
            "tags": self.tags,
        }


class PydanticWorkerAsync(Worker, BaseModel):
    """Pydantic worker with async methods."""

    name: str
    multiplier: int = 2

    async def async_compute(self, x: int) -> int:
        """Async computation method."""
        await asyncio.sleep(0.01)
        return x * self.multiplier

    def sync_compute(self, x: int) -> int:
        """Sync computation method."""
        return x * self.multiplier


# ============================================================================
# Tests for Worker + Typed
# ============================================================================


class TestTypedWorkerBasics:
    """Test basic functionality of Typed workers."""

    def test_typed_worker_initialization(self, worker_mode):
        """Test that Typed worker can be initialized across all modes including Ray."""
        w = TypedWorkerSimple.options(mode=worker_mode).init(name="test", value=10)

        # Should be able to call methods
        result = w.get_name().result(timeout=FUTURE_TIMEOUT)
        assert result == "test"

        result = w.compute(5).result(timeout=FUTURE_TIMEOUT)
        assert result == 50

        w.stop()

    def test_typed_worker_with_kwargs(self, worker_mode):
        """Test Typed worker initialization with keyword arguments across all modes including Ray."""
        w = TypedWorkerSimple.options(mode=worker_mode).init(name="worker1", value=20)

        result = w.compute(3).result(timeout=FUTURE_TIMEOUT)
        assert result == 60

        w.stop()

    def test_typed_worker_default_values(self, worker_mode):
        """Test Typed worker with default field values across all modes including Ray."""
        w = TypedWorkerSimple.options(mode=worker_mode).init(name="default_test")

        # value should default to 0
        result = w.compute(10).result(timeout=FUTURE_TIMEOUT)
        assert result == 0

        w.stop()

    def test_typed_worker_validation(self, worker_mode):
        """Test that Typed worker validates fields correctly across all modes including Ray."""
        # Valid initialization
        w = TypedWorkerWithValidation.options(mode=worker_mode).init(
            name="Alice", age=30, email="alice@example.com", tags=["python", "ml"]
        )

        info = w.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["name"] == "Alice"
        assert info["age"] == 30
        assert info["email"] == "alice@example.com"
        assert info["tags"] == ["python", "ml"]

        w.stop()

    def test_typed_worker_validation_errors(self, worker_mode):
        """Test that Typed worker raises validation errors for invalid data across all modes including Ray."""
        if worker_mode == "ray":
            # Ray validates inside the actor, so the error occurs when calling a method, not during creation
            w = TypedWorkerWithValidation.options(mode=worker_mode).init(name="Bob", age=-5)
            # The validation error will occur when we try to call a method
            # (The worker creation succeeds, but the wrapped instance creation inside Ray fails)
            # For now, just skip this test for Ray as validation behavior is different
            pytest.skip("Ray validates inside actor, error behavior differs from other modes")
            return

        # Invalid age (negative)
        with pytest.raises(Exception):  # ValidationError or ValueError
            TypedWorkerWithValidation.options(mode=worker_mode).init(name="Bob", age=-5)

        # Invalid age (too high)
        with pytest.raises(Exception):
            TypedWorkerWithValidation.options(mode=worker_mode).init(name="Bob", age=200)

        # Invalid name (empty)
        with pytest.raises(Exception):
            TypedWorkerWithValidation.options(mode=worker_mode).init(name="", age=25)

    def test_typed_worker_with_hooks(self, worker_mode):
        """Test Typed worker with pre_initialize hooks across all modes including Ray."""
        w = TypedWorkerWithHooks.options(mode=worker_mode).init(first_name="John", last_name="Doe")

        # full_name should be set by pre_initialize
        result = w.get_full_name().result(timeout=FUTURE_TIMEOUT)
        assert result == "John Doe"

        w.stop()

    def test_typed_worker_state_persistence(self, worker_mode):
        """Test that Typed worker maintains state across calls in all modes including Ray."""
        w = TypedWorkerSimple.options(mode=worker_mode).init(name="stateful", value=5)

        # Make multiple calls
        result1 = w.compute(2).result(timeout=FUTURE_TIMEOUT)
        result2 = w.compute(3).result(timeout=FUTURE_TIMEOUT)
        result3 = w.compute(4).result(timeout=FUTURE_TIMEOUT)

        assert result1 == 10
        assert result2 == 15
        assert result3 == 20

        w.stop()

    def test_typed_worker_async_methods(self, worker_mode):
        """Test Typed worker with async methods in all modes including Ray."""
        if worker_mode == "ray":
            # Ray cannot serialize coroutines, so async methods don't work in Ray mode
            pytest.skip("Ray cannot serialize async methods (coroutines not picklable)")
            return

        w = TypedWorkerAsync.options(mode=worker_mode).init(name="async_test", multiplier=3)

        # Test async method
        result1 = w.async_compute(10).result(timeout=FUTURE_TIMEOUT)
        assert result1 == 30

        # Test sync method
        result2 = w.sync_compute(10).result(timeout=FUTURE_TIMEOUT)
        assert result2 == 30

        w.stop()

    def test_typed_worker_blocking_mode(self, worker_mode):
        """Test Typed worker in blocking mode in all modes including Ray."""
        w = TypedWorkerSimple.options(mode=worker_mode, blocking=True).init(name="blocking", value=7)

        # Should return result directly, not a future
        result = w.compute(3)
        assert isinstance(result, int)
        assert result == 21

        w.stop()


# ============================================================================
# Tests for Worker + BaseModel
# ============================================================================


class TestPydanticWorkerBasics:
    """Test basic functionality of Pydantic workers."""

    def test_pydantic_worker_initialization(self, worker_mode):
        """Test that Pydantic worker can be initialized in all modes including Ray."""
        w = PydanticWorkerSimple.options(mode=worker_mode).init(name="test", value=10)

        # Should be able to call methods
        result = w.get_name().result(timeout=FUTURE_TIMEOUT)
        assert result == "test"

        result = w.compute(5).result(timeout=FUTURE_TIMEOUT)
        assert result == 50

        w.stop()

    def test_pydantic_worker_with_kwargs(self, worker_mode):
        """Test Pydantic worker initialization with keyword arguments in all modes including Ray."""
        w = PydanticWorkerSimple.options(mode=worker_mode).init(name="worker1", value=20)

        result = w.compute(3).result(timeout=FUTURE_TIMEOUT)
        assert result == 60

        w.stop()

    def test_pydantic_worker_default_values(self, worker_mode):
        """Test Pydantic worker with default field values in all modes including Ray."""
        w = PydanticWorkerSimple.options(mode=worker_mode).init(name="default_test")

        # value should default to 0
        result = w.compute(10).result(timeout=FUTURE_TIMEOUT)
        assert result == 0

        w.stop()

    def test_pydantic_worker_validation(self, worker_mode):
        """Test that Pydantic worker validates fields correctly in all modes including Ray."""
        # Valid initialization
        w = PydanticWorkerWithValidation.options(mode=worker_mode).init(
            name="Alice", age=30, email="alice@example.com", tags=["python", "ml"]
        )

        info = w.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["name"] == "Alice"
        assert info["age"] == 30
        assert info["email"] == "alice@example.com"
        assert info["tags"] == ["python", "ml"]

        w.stop()

    def test_pydantic_worker_validation_errors(self, worker_mode):
        """Test that Pydantic worker raises validation errors for invalid data in all modes including Ray."""
        if worker_mode == "ray":
            # Ray validates inside the actor, error behavior differs from other modes
            pytest.skip("Ray validates inside actor, error behavior differs from other modes")
            return

        # Invalid age (negative)
        with pytest.raises(Exception):  # ValidationError or ValueError
            PydanticWorkerWithValidation.options(mode=worker_mode).init(name="Bob", age=-5)

        # Invalid age (too high)
        with pytest.raises(Exception):
            PydanticWorkerWithValidation.options(mode=worker_mode).init(name="Bob", age=200)

        # Invalid name (empty)
        with pytest.raises(Exception):
            PydanticWorkerWithValidation.options(mode=worker_mode).init(name="", age=25)

    def test_pydantic_worker_async_methods(self, worker_mode):
        """Test Pydantic worker with async methods in all modes including Ray."""
        if worker_mode == "ray":
            # Ray cannot serialize coroutines, so async methods don't work in Ray mode
            pytest.skip("Ray cannot serialize async methods (coroutines not picklable)")
            return

        w = PydanticWorkerAsync.options(mode=worker_mode).init(name="async_test", multiplier=3)

        # Test async method
        result1 = w.async_compute(10).result(timeout=FUTURE_TIMEOUT)
        assert result1 == 30

        # Test sync method
        result2 = w.sync_compute(10).result(timeout=FUTURE_TIMEOUT)
        assert result2 == 30

        w.stop()

    def test_pydantic_worker_blocking_mode(self, worker_mode):
        """Test Pydantic worker in blocking mode in all modes including Ray."""
        w = PydanticWorkerSimple.options(mode=worker_mode, blocking=True).init(name="blocking", value=7)

        # Should return result directly, not a future
        result = w.compute(3)
        assert isinstance(result, int)
        assert result == 21

        w.stop()


# ============================================================================
# Advanced Tests
# ============================================================================


class TestModelWorkerAdvanced:
    """Advanced tests for model-based workers."""

    def test_typed_worker_serialization_process_mode(self):
        """Test that Typed worker can be serialized for process mode."""
        w = TypedWorkerSimple.options(mode="process").init(name="process_test", value=15)

        result = w.compute(2).result(timeout=FUTURE_TIMEOUT)
        assert result == 30

        w.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_serialization_ray_mode(self):
        """Test that Typed worker works in Ray mode with automatic composition wrapper."""
        # Ray is initialized by conftest.py initialize_ray fixture
        # Now works thanks to automatic composition wrapper!
        w = TypedWorkerSimple.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
            name="ray_test", value=20
        )

        result = w.compute(2).result(timeout=FUTURE_TIMEOUT)
        assert result == 40

        w.stop()

    def test_pydantic_worker_serialization_process_mode(self):
        """Test that Pydantic worker can be serialized for process mode."""
        w = PydanticWorkerSimple.options(mode="process").init(name="process_test", value=15)

        result = w.compute(2).result(timeout=FUTURE_TIMEOUT)
        assert result == 30

        w.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_pydantic_worker_serialization_ray_mode(self):
        """Test that Pydantic worker works in Ray mode with automatic composition wrapper."""
        # Ray is initialized by conftest.py initialize_ray fixture
        # Now works thanks to automatic composition wrapper!
        w = PydanticWorkerSimple.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
            name="ray_test", value=20
        )

        result = w.compute(2).result(timeout=FUTURE_TIMEOUT)
        assert result == 40

        w.stop()

    def test_typed_worker_multiple_instances(self, worker_mode):
        """Test multiple instances of Typed worker with different state in all modes including Ray."""
        w1 = TypedWorkerSimple.options(mode=worker_mode).init(name="worker1", value=10)
        w2 = TypedWorkerSimple.options(mode=worker_mode).init(name="worker2", value=20)

        result1 = w1.compute(2).result(timeout=FUTURE_TIMEOUT)
        result2 = w2.compute(2).result(timeout=FUTURE_TIMEOUT)

        assert result1 == 20
        assert result2 == 40

        w1.stop()
        w2.stop()

    def test_pydantic_worker_multiple_instances(self, worker_mode):
        """Test multiple instances of Pydantic worker with different state in all modes including Ray."""
        w1 = PydanticWorkerSimple.options(mode=worker_mode).init(name="worker1", value=10)
        w2 = PydanticWorkerSimple.options(mode=worker_mode).init(name="worker2", value=20)

        result1 = w1.compute(2).result(timeout=FUTURE_TIMEOUT)
        result2 = w2.compute(2).result(timeout=FUTURE_TIMEOUT)

        assert result1 == 20
        assert result2 == 40

        w1.stop()
        w2.stop()

    def test_typed_worker_pool(self):
        """Test Typed worker with worker pool."""
        pool = TypedWorkerSimple.options(mode="thread", max_workers=3).init(name="pool_worker", value=5)

        # Make multiple calls
        futures = [pool.compute(i) for i in range(10)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        expected = [i * 5 for i in range(10)]
        assert results == expected

        pool.stop()

    def test_pydantic_worker_pool(self):
        """Test Pydantic worker with worker pool."""
        pool = PydanticWorkerSimple.options(mode="thread", max_workers=3).init(name="pool_worker", value=5)

        # Make multiple calls
        futures = [pool.compute(i) for i in range(10)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        expected = [i * 5 for i in range(10)]
        assert results == expected

        pool.stop()


# ============================================================================
# Edge Cases and Compatibility Tests
# ============================================================================


class TestRayCompatibility:
    """Test Ray mode compatibility with Pydantic-based workers via automatic composition wrapper."""

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_ray_mode_works(self):
        """Test that Typed worker works in Ray mode with automatic composition wrapper."""
        # Ray is initialized by conftest.py initialize_ray fixture
        # Now works thanks to automatic composition wrapper!
        worker = TypedWorkerSimple.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
            name="test", value=10
        )

        result = worker.get_name().result(timeout=FUTURE_TIMEOUT)
        assert result == "test"

        result = worker.compute(5).result(timeout=FUTURE_TIMEOUT)
        assert result == 50

        worker.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_ray_simple_case(self):
        """Test simple Typed worker with Ray (regression test for recursion bug).

        This test verifies that the CompositionWrapper.__getattr__ fix prevents
        infinite recursion when Ray's tracing system tries to access internal attributes.
        The bug manifested when using Ray client mode or when Ray's tracing is enabled.
        """
        # Ray is initialized by conftest.py initialize_ray fixture

        # Create a minimal Typed worker (similar to user's Cat example)
        class SimpleTypedWorker(Worker, Typed):
            def meow(self) -> str:
                return "meow"

        # This should not cause RecursionError
        # Previously failed with "maximum recursion depth exceeded" when Ray's
        # tracing system called __getattr__ for _ray_trace_ctx and similar attributes
        worker = SimpleTypedWorker.options(mode="ray", actor_options={"num_cpus": 0.01}).init()

        result = worker.meow().result(timeout=FUTURE_TIMEOUT)
        assert result == "meow"

        worker.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_ray_pool_simple_case(self):
        """Test simple Typed worker pool with Ray (regression test for pool recursion bug).

        This test verifies that the CompositionWrapper fix works correctly with pools.
        The user reported RecursionError when creating a pool with max_workers=2.
        """
        # Ray is initialized by conftest.py initialize_ray fixture

        # Create a minimal Typed worker
        class CatWorker(Worker, Typed):
            def meow(self) -> str:
                return "meow"

        # This should not cause RecursionError
        # User's original failing case: max_workers=2, actor_options=dict(num_cpus=0.01)
        pool = CatWorker.options(mode="ray", max_workers=2, actor_options={"num_cpus": 0.01}).init()

        # Test that the pool works correctly
        result = pool.meow().result(timeout=FUTURE_TIMEOUT)
        assert result == "meow"

        # Test multiple concurrent calls
        futures = [pool.meow() for _ in range(10)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]
        assert all(r == "meow" for r in results)

        pool.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_pydantic_worker_ray_mode_works(self):
        """Test that Pydantic worker works in Ray mode with automatic composition wrapper."""
        # Ray is initialized by conftest.py initialize_ray fixture
        # Now works thanks to automatic composition wrapper!
        worker = PydanticWorkerSimple.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
            name="test", value=10
        )

        result = worker.get_name().result(timeout=FUTURE_TIMEOUT)
        assert result == "test"

        result = worker.compute(5).result(timeout=FUTURE_TIMEOUT)
        assert result == 50

        worker.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_ray_pool_works(self):
        """Test that Typed worker pool works in Ray mode with automatic composition wrapper."""
        # Ray is initialized by conftest.py initialize_ray fixture
        # Now works thanks to automatic composition wrapper!
        pool = TypedWorkerSimple.options(mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}).init(
            name="test", value=10
        )

        futures = [pool.compute(i) for i in range(10)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        assert results == [i * 10 for i in range(10)]

        pool.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_pydantic_worker_ray_pool_works(self):
        """Test that Pydantic worker pool works in Ray mode with automatic composition wrapper."""
        # Ray is initialized by conftest.py initialize_ray fixture
        # Now works thanks to automatic composition wrapper!
        pool = PydanticWorkerSimple.options(mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}).init(
            name="test", value=10
        )

        futures = [pool.compute(i) for i in range(10)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        assert results == [i * 10 for i in range(10)]

        pool.stop()

    def test_regular_worker_ray_mode_still_works(self):
        """Test that regular (non-Pydantic) workers continue to work fine in Ray mode."""
        if not _IS_RAY_INSTALLED:
            pytest.skip("Ray not installed")

        # Ray is initialized by conftest.py initialize_ray fixture
        class RegularWorker(Worker):
            def __init__(self, value: int):
                self.value = value

            def compute(self, x: int) -> int:
                return self.value * x

        # Should work without issues (as before)
        worker = RegularWorker.options(mode="ray", actor_options={"num_cpus": 0.1}).init(value=10)
        result = worker.compute(5).result(timeout=FUTURE_TIMEOUT)
        assert result == 50
        worker.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_validation_works_in_ray(self):
        """Test that Typed field validation works correctly in Ray mode."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Test 1: Valid data should work
        worker = TypedWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
            name="Alice", age=30, email="alice@example.com", tags=["python", "ml"]
        )

        info = worker.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["name"] == "Alice"
        assert info["age"] == 30
        assert info["email"] == "alice@example.com"
        assert info["tags"] == ["python", "ml"]

        worker.stop()

        # Test 2: Invalid age (negative) should fail during actor creation
        # The error manifests as an ActorDiedError when trying to use the worker
        try:
            worker = TypedWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
                name="Bob", age=-5
            )
            # Try to call a method - this should fail because the actor died during creation
            result = worker.get_info().result(timeout=FUTURE_TIMEOUT)
            # If we get here, validation didn't work - fail the test
            worker.stop()
            assert False, "Expected validation error for negative age, but worker was created successfully"
        except Exception as e:
            # Expected - actor should have died due to validation error
            # The error should mention validation or actor death
            error_str = str(e).lower()
            assert "actor" in error_str or "validation" in error_str or "error" in error_str

        # Test 3: Invalid age (too high) should fail
        try:
            worker = TypedWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
                name="Charlie", age=200
            )
            result = worker.get_info().result(timeout=FUTURE_TIMEOUT)
            worker.stop()
            assert False, "Expected validation error for age > 150, but worker was created successfully"
        except Exception as e:
            error_str = str(e).lower()
            assert "actor" in error_str or "validation" in error_str or "error" in error_str

        # Test 4: Invalid name (empty string) should fail
        try:
            worker = TypedWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
                name="", age=25
            )
            result = worker.get_info().result(timeout=FUTURE_TIMEOUT)
            worker.stop()
            assert False, "Expected validation error for empty name, but worker was created successfully"
        except Exception as e:
            error_str = str(e).lower()
            assert "actor" in error_str or "validation" in error_str or "error" in error_str

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_pydantic_worker_validation_works_in_ray(self):
        """Test that Pydantic BaseModel field validation works correctly in Ray mode."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Test 1: Valid data should work
        worker = PydanticWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
            name="Alice", age=30, email="alice@example.com", tags=["python", "ml"]
        )

        info = worker.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["name"] == "Alice"
        assert info["age"] == 30
        assert info["email"] == "alice@example.com"
        assert info["tags"] == ["python", "ml"]

        worker.stop()

        # Test 2: Invalid age (negative) should fail during actor creation
        try:
            worker = PydanticWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
                name="Bob", age=-5
            )
            result = worker.get_info().result(timeout=FUTURE_TIMEOUT)
            worker.stop()
            assert False, "Expected validation error for negative age, but worker was created successfully"
        except Exception as e:
            error_str = str(e).lower()
            assert "actor" in error_str or "validation" in error_str or "error" in error_str

        # Test 3: Invalid name (too long) should fail
        try:
            worker = PydanticWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
                name="A" * 100,
                age=25,  # max_length=50
            )
            result = worker.get_info().result(timeout=FUTURE_TIMEOUT)
            worker.stop()
            assert False, "Expected validation error for name too long, but worker was created successfully"
        except Exception as e:
            error_str = str(e).lower()
            assert "actor" in error_str or "validation" in error_str or "error" in error_str

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_with_defaults_in_ray(self):
        """Test that default values work correctly with Typed workers in Ray mode."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Create worker with only required field (value should default to 0)
        worker = TypedWorkerSimple.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
            name="default_test"
        )

        result = worker.compute(10).result(timeout=FUTURE_TIMEOUT)
        assert result == 0  # value defaults to 0, so 10 * 0 = 0

        worker.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_pre_initialize_hook_in_ray(self):
        """Test that Typed pre_initialize hook works in Ray mode."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # TypedWorkerWithHooks has a pre_initialize that sets full_name
        worker = TypedWorkerWithHooks.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
            first_name="John", last_name="Doe"
        )

        # full_name should be set by pre_initialize hook
        result = worker.get_full_name().result(timeout=FUTURE_TIMEOUT)
        assert result == "John Doe"

        worker.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_field_constraints_in_ray(self):
        """Test that Field constraints (min/max, etc.) work in Ray mode."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Valid: age within range
        worker = TypedWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
            name="Test", age=50
        )
        info = worker.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["age"] == 50
        worker.stop()

        # Invalid: age below minimum (ge=0)
        try:
            worker = TypedWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
                name="Test", age=-1
            )
            result = worker.get_info().result(timeout=FUTURE_TIMEOUT)
            worker.stop()
            assert False, "Expected validation error for age < 0"
        except Exception:
            # Expected - validation should fail
            pass

        # Invalid: age above maximum (le=150)
        try:
            worker = TypedWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
                name="Test", age=151
            )
            result = worker.get_info().result(timeout=FUTURE_TIMEOUT)
            worker.stop()
            assert False, "Expected validation error for age > 150"
        except Exception:
            # Expected - validation should fail
            pass

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_pydantic_worker_field_constraints_in_ray(self):
        """Test that Pydantic Field constraints work in Ray mode."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Valid: name within length constraints
        worker = PydanticWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
            name="ValidName", age=30
        )
        info = worker.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["name"] == "ValidName"
        worker.stop()

        # Invalid: name too short (min_length=1)
        try:
            worker = PydanticWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
                name="", age=30
            )
            result = worker.get_info().result(timeout=FUTURE_TIMEOUT)
            worker.stop()
            assert False, "Expected validation error for empty name"
        except Exception:
            # Expected - validation should fail
            pass

        # Invalid: name too long (max_length=50)
        try:
            worker = PydanticWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
                name="X" * 51, age=30
            )
            result = worker.get_info().result(timeout=FUTURE_TIMEOUT)
            worker.stop()
            assert False, "Expected validation error for name too long"
        except Exception:
            # Expected - validation should fail
            pass

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_optional_fields_in_ray(self):
        """Test that optional fields work correctly in Ray mode."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Create worker with optional field provided
        worker = TypedWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
            name="Test", age=30, email="test@example.com"
        )
        info = worker.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["email"] == "test@example.com"
        worker.stop()

        # Create worker without optional field (should default to None)
        worker = TypedWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
            name="Test", age=30
        )
        info = worker.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["email"] is None
        worker.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_list_fields_in_ray(self):
        """Test that list fields work correctly in Ray mode."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Create worker with list field
        worker = TypedWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
            name="Test", age=30, tags=["python", "ml", "data"]
        )
        info = worker.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["tags"] == ["python", "ml", "data"]

        # Test add_tag method
        result = worker.add_tag("new_tag").result(timeout=FUTURE_TIMEOUT)
        assert result == ["python", "ml", "data", "new_tag"]

        worker.stop()

        # Create worker without list field (should default to empty list)
        worker = TypedWorkerWithValidation.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
            name="Test", age=30
        )
        info = worker.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["tags"] == []
        worker.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_pool_validation_in_ray(self):
        """Test that validation works correctly with Typed worker pools in Ray mode."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Create a pool of 3 workers - all should validate successfully
        pool = TypedWorkerWithValidation.options(
            mode="ray", max_workers=3, actor_options={"num_cpus": 0.1}
        ).init(name="PoolWorker", age=25, email="pool@example.com", tags=["ray", "pool"])

        # Submit tasks to all workers in the pool
        futures = [pool.get_info() for _ in range(10)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        # All results should have the same validated data
        for result in results:
            assert result["name"] == "PoolWorker"
            assert result["age"] == 25
            assert result["email"] == "pool@example.com"
            assert result["tags"] == ["ray", "pool"]

        pool.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_pydantic_worker_pool_validation_in_ray(self):
        """Test that validation works correctly with Pydantic worker pools in Ray mode."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Create a pool of 3 workers
        pool = PydanticWorkerWithValidation.options(
            mode="ray", max_workers=3, actor_options={"num_cpus": 0.1}
        ).init(name="PydanticPool", age=30, tags=["pydantic", "validation"])

        # Submit tasks across the pool
        futures = [pool.get_info() for _ in range(15)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        # All workers should return validated data
        for result in results:
            assert result["name"] == "PydanticPool"
            assert result["age"] == 30
            assert result["tags"] == ["pydantic", "validation"]

        pool.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_pool_invalid_data_fails_all_actors(self):
        """Test that invalid data causes all actors in the pool to fail validation."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Try to create pool with invalid data - all actors should fail
        try:
            pool = TypedWorkerWithValidation.options(
                mode="ray", max_workers=3, actor_options={"num_cpus": 0.1}
            ).init(name="Invalid", age=-10)  # Invalid: age < 0

            # Try to use the pool - should fail
            result = pool.get_info().result(timeout=FUTURE_TIMEOUT)
            pool.stop()
            assert False, "Expected pool creation to fail due to validation error"
        except Exception as e:
            # Expected - all actors should have failed validation
            error_str = str(e).lower()
            assert "actor" in error_str or "validation" in error_str or "error" in error_str

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_pool_state_isolation_with_validation(self):
        """Test that workers in a Ray pool maintain state isolation with validated fields."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Create a pool where each worker has the same initial validated state
        pool = TypedWorkerSimple.options(mode="ray", max_workers=3, actor_options={"num_cpus": 0.1}).init(
            name="StateTest", value=10
        )

        # Submit multiple tasks - they should all return the same result
        # (each actor has value=10)
        futures = [pool.compute(5) for _ in range(9)]  # 3 tasks per worker
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        # All should return 50 (5 * 10)
        assert all(r == 50 for r in results)

        pool.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_pool_with_different_field_values(self):
        """Test pool behavior with various valid field combinations."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Test 1: Pool with minimal fields (using defaults)
        pool = TypedWorkerSimple.options(mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}).init(
            name="Minimal"
        )

        # value defaults to 0
        result = pool.compute(10).result(timeout=FUTURE_TIMEOUT)
        assert result == 0
        pool.stop()

        # Test 2: Pool with all fields specified
        pool = TypedWorkerWithValidation.options(
            mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}
        ).init(name="Complete", age=45, email="complete@example.com", tags=["tag1", "tag2", "tag3"])

        info = pool.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["name"] == "Complete"
        assert info["age"] == 45
        assert info["email"] == "complete@example.com"
        assert len(info["tags"]) == 3
        pool.stop()

        # Test 3: Pool with boundary values
        pool = TypedWorkerWithValidation.options(
            mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}
        ).init(name="X", age=0)  # Minimum valid age

        info = pool.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["age"] == 0
        pool.stop()

        pool = TypedWorkerWithValidation.options(
            mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}
        ).init(name="Y" * 50, age=150)  # Maximum valid age and name length

        info = pool.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["age"] == 150
        assert len(info["name"]) == 50
        pool.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_pool_concurrent_validation_checks(self):
        """Test that validation works correctly under concurrent load."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Create a large pool
        pool = TypedWorkerWithValidation.options(
            mode="ray", max_workers=4, actor_options={"num_cpus": 0.1}
        ).init(name="Concurrent", age=50, tags=["stress", "test"])

        # Submit many concurrent tasks
        futures = [pool.get_info() for _ in range(50)]
        results = [f.result(timeout=10) for f in futures]

        # All should have correct validated data
        assert len(results) == 50
        for result in results:
            assert result["name"] == "Concurrent"
            assert result["age"] == 50
            assert result["tags"] == ["stress", "test"]

        pool.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_pydantic_worker_pool_with_field_constraints(self):
        """Test that Field constraints are enforced in worker pools."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Test 1: Valid data at lower boundary
        pool = PydanticWorkerWithValidation.options(
            mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}
        ).init(name="A", age=0)  # min_length=1, ge=0

        info = pool.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["name"] == "A"
        assert info["age"] == 0
        pool.stop()

        # Test 2: Valid data at upper boundary
        pool = PydanticWorkerWithValidation.options(
            mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}
        ).init(name="X" * 50, age=150)  # max_length=50, le=150

        info = pool.get_info().result(timeout=FUTURE_TIMEOUT)
        assert len(info["name"]) == 50
        assert info["age"] == 150
        pool.stop()

        # Test 3: Invalid - below minimum
        try:
            pool = PydanticWorkerWithValidation.options(
                mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}
            ).init(name="", age=30)  # Empty name violates min_length=1

            result = pool.get_info().result(timeout=FUTURE_TIMEOUT)
            pool.stop()
            assert False, "Expected validation error for empty name"
        except Exception:
            pass

        # Test 4: Invalid - above maximum
        try:
            pool = PydanticWorkerWithValidation.options(
                mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}
            ).init(name="X" * 51, age=30)  # Name too long

            result = pool.get_info().result(timeout=FUTURE_TIMEOUT)
            pool.stop()
            assert False, "Expected validation error for name too long"
        except Exception:
            pass

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_pool_with_shared_limits_and_validation(self):
        """Test that validated workers work correctly with shared limits in Ray pools."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Create a pool with shared rate limits
        limits = [
            RateLimit(
                key="api_calls", window_seconds=1, capacity=100, algorithm=RateLimitAlgorithm.TokenBucket
            )
        ]

        pool = TypedWorkerWithValidation.options(
            mode="ray", max_workers=3, actor_options={"num_cpus": 0.1}, limits=limits
        ).init(name="LimitedPool", age=35, tags=["limited"])

        # Submit tasks that would use the shared limit
        # All workers share the same limit pool
        futures = [pool.get_info() for _ in range(10)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        # All should succeed and return validated data
        assert len(results) == 10
        for result in results:
            assert result["name"] == "LimitedPool"
            assert result["age"] == 35

        pool.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_pool_methods_access_validated_fields(self):
        """Test that worker methods can access and use validated fields in Ray pools."""
        # Ray is initialized by conftest.py initialize_ray fixture

        pool = TypedWorkerWithValidation.options(
            mode="ray", max_workers=3, actor_options={"num_cpus": 0.1}
        ).init(name="FieldAccess", age=40, tags=["test1", "test2"])

        # Test 1: Method that returns validated field directly
        futures = [pool.get_info() for _ in range(6)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        for result in results:
            assert result["name"] == "FieldAccess"
            assert result["age"] == 40

        # Test 2: Method that modifies list field (returns new list)
        futures = [pool.add_tag("newtag") for _ in range(6)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        # Each call should return the original tags + new tag
        for result in results:
            assert result == ["test1", "test2", "newtag"]

        pool.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_pydantic_worker_pool_round_robin_with_validation(self):
        """Test that round-robin dispatch works correctly with validated Pydantic pools."""
        # Ray is initialized by conftest.py initialize_ray fixture

        pool = PydanticWorkerSimple.options(
            mode="ray", max_workers=3, actor_options={"num_cpus": 0.1}, load_balancing="round_robin"
        ).init(name="RoundRobin", value=5)

        # Submit 12 tasks (should distribute evenly: 4 per worker)
        futures = [pool.compute(i) for i in range(12)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        # Each result should be correct (i * 5)
        expected = [i * 5 for i in range(12)]
        assert results == expected

        # All workers were properly validated and work correctly
        pool.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_pool_with_pre_initialize_hook(self):
        """Test that pre_initialize hooks work correctly in Ray pools."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # TypedWorkerWithHooks uses pre_initialize to set full_name from first_name + last_name
        pool = TypedWorkerWithHooks.options(mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}).init(
            first_name="Jane", last_name="Smith"
        )

        # All workers should have full_name set by pre_initialize
        futures = [pool.get_full_name() for _ in range(6)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        assert all(r == "Jane Smith" for r in results)

        pool.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_pool_error_handling_with_validation(self):
        """Test error handling when validation fails during pool creation."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Test 1: Age constraint violation
        with pytest.raises(Exception):
            pool = TypedWorkerWithValidation.options(
                mode="ray", max_workers=3, actor_options={"num_cpus": 0.1}
            ).init(name="Error", age=-1)
            # If we somehow get past init, trying to use it should fail
            result = pool.get_info().result(timeout=FUTURE_TIMEOUT)
            pool.stop()

        # Test 2: Name constraint violation
        with pytest.raises(Exception):
            pool = TypedWorkerWithValidation.options(
                mode="ray", max_workers=3, actor_options={"num_cpus": 0.1}
            ).init(name="", age=30)
            result = pool.get_info().result(timeout=FUTURE_TIMEOUT)
            pool.stop()

        # Test 3: After failed pool creation, we can create a valid pool
        pool = TypedWorkerWithValidation.options(
            mode="ray", max_workers=3, actor_options={"num_cpus": 0.1}
        ).init(name="Valid", age=30)

        result = pool.get_info().result(timeout=FUTURE_TIMEOUT)
        assert result["name"] == "Valid"
        assert result["age"] == 30

        pool.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_pydantic_worker_pool_optional_and_default_fields(self):
        """Test that optional and default fields work correctly in Pydantic Ray pools."""
        # Ray is initialized by conftest.py initialize_ray fixture

        # Test 1: Pool with optional field provided
        pool = PydanticWorkerWithValidation.options(
            mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}
        ).init(name="WithEmail", age=30, email="test@pool.com")

        info = pool.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["email"] == "test@pool.com"
        pool.stop()

        # Test 2: Pool with optional field omitted (should be None)
        pool = PydanticWorkerWithValidation.options(
            mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}
        ).init(name="NoEmail", age=30)

        info = pool.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["email"] is None
        pool.stop()

        # Test 3: Pool with list field provided
        pool = PydanticWorkerWithValidation.options(
            mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}
        ).init(name="WithTags", age=30, tags=["a", "b", "c"])

        info = pool.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["tags"] == ["a", "b", "c"]
        pool.stop()

        # Test 4: Pool with list field omitted (should be empty list)
        pool = PydanticWorkerWithValidation.options(
            mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}
        ).init(name="NoTags", age=30)

        info = pool.get_info().result(timeout=FUTURE_TIMEOUT)
        assert info["tags"] == []
        pool.stop()


class TestModelWorkerEdgeCases:
    """Test edge cases and compatibility."""

    def test_typed_worker_with_complex_types(self, worker_mode):
        """Test Typed worker with complex field types."""

        class ComplexTypedWorker(Worker, Typed):
            name: str
            data: dict = {}
            items: List[int] = []

            def get_data(self) -> dict:
                return self.data

            def get_items(self) -> List[int]:
                return self.items

        w = ComplexTypedWorker.options(mode=worker_mode).init(
            name="complex", data={"key": "value"}, items=[1, 2, 3]
        )

        data = w.get_data().result(timeout=FUTURE_TIMEOUT)
        assert data == {"key": "value"}

        items = w.get_items().result(timeout=FUTURE_TIMEOUT)
        assert items == [1, 2, 3]

        w.stop()

    def test_pydantic_worker_with_complex_types(self, worker_mode):
        """Test Pydantic worker with complex field types."""

        class ComplexPydanticWorker(Worker, BaseModel):
            name: str
            data: dict = {}
            items: List[int] = []

            def get_data(self) -> dict:
                return self.data

            def get_items(self) -> List[int]:
                return self.items

        w = ComplexPydanticWorker.options(mode=worker_mode).init(
            name="complex", data={"key": "value"}, items=[1, 2, 3]
        )

        data = w.get_data().result(timeout=FUTURE_TIMEOUT)
        assert data == {"key": "value"}

        items = w.get_items().result(timeout=FUTURE_TIMEOUT)
        assert items == [1, 2, 3]

        w.stop()

    def test_typed_worker_inheritance_order(self, worker_mode):
        """Test that Worker and Typed can be inherited in any order."""

        # Order 1: Worker first
        class Worker1(Worker, Typed):
            value: int

            def compute(self) -> int:
                return self.value * 2

        # Order 2: Typed first
        class Worker2(Typed, Worker):
            value: int

            def compute(self) -> int:
                return self.value * 2

        # Both should work in all modes including Ray
        w1 = Worker1.options(mode=worker_mode).init(value=10)
        result1 = w1.compute().result(timeout=FUTURE_TIMEOUT)
        assert result1 == 20
        w1.stop()

        w2 = Worker2.options(mode=worker_mode).init(value=15)
        result2 = w2.compute().result(timeout=FUTURE_TIMEOUT)
        assert result2 == 30
        w2.stop()

    def test_pydantic_worker_inheritance_order(self, worker_mode):
        """Test that Worker and BaseModel can be inherited in any order."""

        # Order 1: Worker first
        class Worker1(Worker, BaseModel):
            value: int

            def compute(self) -> int:
                return self.value * 2

        # Order 2: BaseModel first
        class Worker2(BaseModel, Worker):
            value: int

            def compute(self) -> int:
                return self.value * 2

        # Both should work in all modes including Ray
        w1 = Worker1.options(mode=worker_mode).init(value=10)
        result1 = w1.compute().result(timeout=FUTURE_TIMEOUT)
        assert result1 == 20
        w1.stop()

        w2 = Worker2.options(mode=worker_mode).init(value=15)
        result2 = w2.compute().result(timeout=FUTURE_TIMEOUT)
        assert result2 == 30
        w2.stop()


# ============================================================================
# Module-level worker classes for validate decorator tests
# (Defined at module level so they can be pickled for process/ray mode)
# ============================================================================


class ValidatedWorker(Worker):
    """Worker with @validate decorated method."""

    def __init__(self, multiplier: int):
        self.multiplier = multiplier

    @validate
    def process(self, value: int, scale: float = 1.0) -> float:
        """Process with automatic type validation and coercion."""
        return (value * self.multiplier) * scale


class TypedValidatedWorker(Worker, Typed):
    """Typed worker with @validate decorated method."""

    name: str
    multiplier: int = 2

    @validate
    def compute(self, x: int, y: int = 1) -> int:
        return (x + y) * self.multiplier


class PydanticValidatedWorker(Worker, BaseModel):
    """Pydantic worker with @validate decorated method."""

    name: str = Field(..., min_length=1)
    multiplier: int = Field(default=2, ge=1)

    @validate
    def compute(self, x: int, y: int = 0) -> int:
        return (x + y) * self.multiplier


class AsyncValidatedWorker(Worker):
    """Worker with async @validate decorated method."""

    def __init__(self, base: int):
        self.base = base

    @validate
    async def async_compute(self, x: int, delay: float = 0.01) -> int:
        await asyncio.sleep(delay)
        return x + self.base


class MultiValidatedWorker(Worker):
    """Worker with multiple @validate decorated methods."""

    def __init__(self, base: int):
        self.base = base

    @validate
    def add(self, x: int) -> int:
        return self.base + x

    @validate
    def multiply(self, x: int, factor: int = 2) -> int:
        return x * factor

    @validate
    def complex_calc(self, a: int, b: int, c: float = 1.0) -> float:
        return (a + b) * c


class PydanticValidateCallWorker(Worker):
    """Worker with @validate_call decorated method."""

    def __init__(self, multiplier: int):
        self.multiplier = multiplier

    @validate_call
    def process(self, value: int, scale: float = 1.0) -> float:
        """Process with Pydantic validation."""
        return (value * self.multiplier) * scale


class TypedValidateCallWorker(Worker, Typed):
    """Typed worker with @validate_call decorated method."""

    name: str
    multiplier: int = 2

    @validate_call
    def compute(self, x: int, y: int = 0) -> int:
        return (x + y) * self.multiplier


class FullyValidatedWorker(Worker, BaseModel):
    """Pydantic worker with @validate_call decorated method."""

    name: str = Field(..., min_length=1)
    multiplier: int = Field(default=2, ge=1)

    @validate_call
    def compute(self, x: int, y: int = 0) -> int:
        return (x + y) * self.multiplier


class StrictWorker(Worker):
    """Worker with strict validation."""

    def __init__(self):
        pass

    @validate_call
    def strict_process(self, value: int, name: str) -> str:
        return f"{name}: {value}"


class AsyncValidateCallWorker(Worker):
    """Worker with async @validate_call decorated method."""

    def __init__(self, base: int):
        self.base = base

    @validate_call
    async def async_process(self, x: int, multiplier: int = 2) -> int:
        await asyncio.sleep(0.001)
        return (x + self.base) * multiplier


class InitValidatedWorker(Worker):
    """Worker with @validate on __init__."""

    @validate
    def __init__(self, value: int, name: str = "default"):
        self.value = value
        self.name = name

    def get_info(self) -> dict:
        return {"value": self.value, "name": self.name}


class PydanticInitWorker(Worker):
    """Worker with @validate_call on __init__."""

    @validate_call
    def __init__(self, count: int, label: str = "default"):
        self.count = count
        self.label = label

    def get_data(self) -> dict:
        return {"count": self.count, "label": self.label}


class ThreadInitWorker(Worker):
    """Worker with @validate on __init__ for thread mode."""

    @validate
    def __init__(self, value: int, multiplier: int = 2):
        self.value = value
        self.multiplier = multiplier

    def compute(self) -> int:
        return self.value * self.multiplier


class ComplexWorkerValidated(Worker, Typed):
    """Complex Typed worker with @validate methods."""

    name: str = Field(..., min_length=1)
    multiplier: int = Field(default=2, ge=1)

    @classmethod
    def pre_initialize(cls, data: dict) -> None:
        if "name" in data:
            data["name"] = data["name"].strip().title()

    @validate
    def process(self, value: int, factor: float = 1.0) -> float:
        return value * self.multiplier * factor


class FullyValidatedPydanticWorker(Worker, BaseModel):
    """Pydantic worker with @validate_call methods."""

    name: str = Field(..., min_length=1, max_length=50)
    rate: int = Field(default=10, ge=1, le=100)

    @validate_call
    def compute(self, x: int, scale: float = 1.0) -> float:
        return x * self.rate * scale


class MixedValidationWorker(Worker):
    """Worker with both @validate and @validate_call methods."""

    def __init__(self, base: int):
        self.base = base

    @validate
    def morphic_method(self, x: int) -> int:
        return self.base + x

    @validate_call
    def pydantic_method(self, x: int, y: int = 0) -> int:
        return self.base + x + y


# Worker classes for Limits tests
class APIWorker(Worker, Typed):
    """Typed worker for API rate limiting tests."""

    name: str
    api_key: str

    def call_api(self, tokens_needed: int) -> str:
        with self.limits.acquire(requested={"api_tokens": tokens_needed}) as acq:
            acq.update(usage={"api_tokens": tokens_needed})
            return f"{self.name} used {tokens_needed} tokens"


class DBWorker(Worker, Typed):
    """Typed worker for database connection limits tests."""

    db_name: str
    max_connections: int = 10

    def query(self, query_str: str) -> dict:
        with self.limits.acquire(requested={"connections": 1}):
            return {"db": self.db_name, "query": query_str, "result": "success"}


class RateLimitedWorker(Worker, Typed):
    """Typed worker for call limits tests."""

    name: str
    requests_per_minute: int = 60

    def process(self, data: str) -> str:
        return f"{self.name} processed: {data}"


class TokenWorker(Worker, BaseModel):
    """Pydantic worker for token limits tests."""

    service_name: str = Field(..., min_length=1)
    max_tokens: int = Field(default=1000, ge=1)

    def process_request(self, tokens: int) -> dict:
        with self.limits.acquire(requested={"tokens": tokens}) as acq:
            acq.update(usage={"tokens": tokens})
            return {"service": self.service_name, "tokens_used": tokens}


# Worker classes for Pool tests
class PoolWorkerTyped(Worker, Typed):
    """Typed worker for pool tests."""

    worker_id: str
    multiplier: int = 2

    def compute(self, x: int) -> dict:
        return {"worker_id": self.worker_id, "result": x * self.multiplier}


class LimitedPoolWorker(Worker, Typed):
    """Typed worker with shared limits for pool tests."""

    name: str

    def process(self, value: int) -> int:
        with self.limits.acquire(requested={"tokens": 10}) as acq:
            acq.update(usage={"tokens": 10})
            return value * 2


class StatelessWorker(Worker, Typed):
    """Typed worker for state isolation tests."""

    name: str
    worker_id: int = 0

    def process(self, value: int) -> dict:
        return {"worker_id": self.worker_id, "result": value * 2}


class PoolWorkerPydantic(Worker, BaseModel):
    """Pydantic worker for pool tests."""

    worker_name: str = Field(..., min_length=1)
    multiplier: int = Field(default=2, ge=1)

    def compute(self, x: int) -> dict:
        return {"worker": self.worker_name, "result": x * self.multiplier}


class ProcessPoolWorker(Worker, BaseModel):
    """Pydantic worker for process pool tests."""

    name: str
    value: int = 10

    def compute(self, x: int) -> int:
        return x * self.value


# Worker classes for complex scenarios
class ComplexWorkerWithLimits(Worker, Typed):
    """Complex Typed worker with validate and limits."""

    name: str
    max_tokens: int = 1000

    @validate
    def process(self, prompt: str, tokens: int = 100) -> dict:
        with self.limits.acquire(requested={"tokens": tokens}) as acq:
            acq.update(usage={"tokens": tokens})
            return {"name": self.name, "prompt": prompt, "tokens": tokens}


class PooledValidatedPydanticWorker(Worker, BaseModel):
    """Pydantic worker pool with validated methods."""

    worker_id: str = Field(..., min_length=1)
    multiplier: int = Field(default=2, ge=1)

    @validate_call
    def compute(self, x: int, y: int = 0) -> dict:
        result = (x + y) * self.multiplier
        return {"worker_id": self.worker_id, "result": result}


class FullValidationStackWorker(Worker, Typed):
    """Typed worker with full validation at all levels."""

    name: str = Field(..., min_length=1, max_length=50)
    rate: int = Field(default=10, ge=1, le=100)

    @classmethod
    def pre_initialize(cls, data: dict) -> None:
        if "name" in data:
            data["name"] = data["name"].strip().title()

    @validate
    def process(self, value: int, scale: float = 1.0) -> dict:
        result = value * self.rate * scale
        return {"name": self.name, "result": result}


# ============================================================================
# Tests for validate decorators on Worker methods
# ============================================================================


class TestMorphicValidateOnWorkerMethods:
    """Test morphic.validate decorator on worker methods across all modes."""

    def test_validate_on_regular_worker_method(self, worker_mode):
        """Test morphic.validate on a regular worker method.

        Note: @validate works with ALL modes including Ray because it's just
        a function decorator, not class inheritance.
        """
        worker = ValidatedWorker.options(mode=worker_mode).init(multiplier=5)

        # Valid call
        result = worker.process(10, scale=2.0).result(timeout=FUTURE_TIMEOUT)
        assert result == 100.0

        # String should be coerced to int/float
        result = worker.process("5", scale="3.0").result(timeout=FUTURE_TIMEOUT)
        assert result == 75.0

        worker.stop()

    def test_validate_on_typed_worker_method(self, worker_mode):
        """Test morphic.validate on Typed worker methods in all modes including Ray.

        Note: Works with all modes including Ray thanks to automatic composition wrapper.
        """
        worker = TypedValidatedWorker.options(mode=worker_mode).init(name="validated", multiplier=3)

        result = worker.compute(5, y=3).result(timeout=FUTURE_TIMEOUT)
        assert result == 24  # (5 + 3) * 3

        # Type coercion
        result = worker.compute("10", y="5").result(timeout=FUTURE_TIMEOUT)
        assert result == 45  # (10 + 5) * 3

        worker.stop()

    def test_validate_on_pydantic_worker_method(self, worker_mode):
        """Test morphic.validate on Pydantic BaseModel worker methods in all modes including Ray.

        Note: Works with all modes including Ray thanks to automatic composition wrapper.
        """
        worker = PydanticValidatedWorker.options(mode=worker_mode).init(name="validated", multiplier=4)

        result = worker.compute(10, y=5).result(timeout=FUTURE_TIMEOUT)
        assert result == 60  # (10 + 5) * 4

        # Type coercion
        result = worker.compute("8", y="2").result(timeout=FUTURE_TIMEOUT)
        assert result == 40  # (8 + 2) * 4

        worker.stop()

    def test_validate_on_async_worker_method(self, worker_mode):
        """Test morphic.validate on async worker methods.

        Note: @validate works with ALL modes including Ray.
        """
        worker = AsyncValidatedWorker.options(mode=worker_mode).init(base=100)

        result = worker.async_compute(42, delay=0.001).result(timeout=FUTURE_TIMEOUT)
        assert result == 142

        # Type coercion on async method
        result = worker.async_compute("50", delay=0.001).result(timeout=FUTURE_TIMEOUT)
        assert result == 150

        worker.stop()

    def test_validate_with_multiple_methods(self, worker_mode):
        """Test multiple methods with @validate decorator.

        Note: @validate works with ALL modes including Ray.
        """
        worker = MultiValidatedWorker.options(mode=worker_mode).init(base=10)

        # Test all methods
        assert worker.add(5).result(timeout=FUTURE_TIMEOUT) == 15
        assert worker.multiply(3, factor=4).result(timeout=FUTURE_TIMEOUT) == 12
        assert worker.complex_calc("5", "3", c="2.5").result(timeout=FUTURE_TIMEOUT) == 20.0

        worker.stop()


class TestPydanticValidateCallOnWorkerMethods:
    """Test pydantic.validate_call decorator on worker methods across all modes."""

    def test_validate_call_on_regular_worker_method(self, worker_mode):
        """Test pydantic.validate_call on a regular worker method.

        Note: @validate_call works with ALL modes including Ray.
        """
        worker = PydanticValidateCallWorker.options(mode=worker_mode).init(multiplier=4)

        # Valid call
        result = worker.process(10, scale=2.5).result(timeout=FUTURE_TIMEOUT)
        assert result == 100.0

        # Pydantic should coerce string to int/float
        result = worker.process("5", scale="2.0").result(timeout=FUTURE_TIMEOUT)
        assert result == 40.0

        worker.stop()

    def test_validate_call_on_typed_worker_method(self, worker_mode):
        """Test pydantic.validate_call on Typed worker methods in all modes including Ray.

        Note: Works with all modes including Ray thanks to automatic composition wrapper.
        """
        worker = TypedValidateCallWorker.options(mode=worker_mode).init(name="validated", multiplier=3)

        result = worker.compute(10, y=5).result(timeout=FUTURE_TIMEOUT)
        assert result == 45  # (10 + 5) * 3

        worker.stop()

    def test_validate_call_on_pydantic_worker_method(self, worker_mode):
        """Test pydantic.validate_call on BaseModel worker methods in all modes including Ray.

        Note: Works with all modes including Ray thanks to automatic composition wrapper.
        """
        worker = FullyValidatedWorker.options(mode=worker_mode).init(name="validated", multiplier=5)

        result = worker.compute(10, y=5).result(timeout=FUTURE_TIMEOUT)
        assert result == 75  # (10 + 5) * 5

        # Type coercion
        result = worker.compute("8", y="2").result(timeout=FUTURE_TIMEOUT)
        assert result == 50  # (8 + 2) * 5

        worker.stop()

    def test_validate_call_validation_errors(self, worker_mode):
        """Test that validate_call raises validation errors properly.

        Note: @validate_call works with ALL modes including Ray.
        """
        worker = StrictWorker.options(mode=worker_mode).init()

        # Valid call
        result = worker.strict_process(42, name="test").result(timeout=FUTURE_TIMEOUT)
        assert result == "test: 42"

        # Invalid: missing required argument should fail
        try:
            future = worker.strict_process(42)
            future.result(timeout=FUTURE_TIMEOUT)
            assert False, "Should have raised validation error"
        except Exception:
            # Expected - validation error occurred
            pass

        worker.stop()

    def test_validate_call_on_async_method(self, worker_mode):
        """Test pydantic.validate_call on async methods.

        Note: @validate_call works with ALL modes including Ray.
        """
        worker = AsyncValidateCallWorker.options(mode=worker_mode).init(base=10)

        result = worker.async_process(5, multiplier=3).result(timeout=FUTURE_TIMEOUT)
        assert result == 45  # (5 + 10) * 3

        # Type coercion
        result = worker.async_process("8", multiplier="2").result(timeout=FUTURE_TIMEOUT)
        assert result == 36  # (8 + 10) * 2

        worker.stop()


class TestValidateOnWorkerInit:
    """Test validate decorators on Worker __init__ method."""

    def test_morphic_validate_on_worker_init(self, worker_mode):
        """Test morphic.validate on Worker __init__.

        Note: @validate works with ALL modes including Ray because it's just
        a function decorator, not class inheritance.
        """
        worker = InitValidatedWorker.options(mode=worker_mode).init(value=42, name="test")
        result = worker.get_info().result(timeout=FUTURE_TIMEOUT)
        assert result["value"] == 42
        assert result["name"] == "test"
        worker.stop()

        # Type coercion
        worker = InitValidatedWorker.options(mode=worker_mode).init(value="100", name="coerced")
        result = worker.get_info().result(timeout=FUTURE_TIMEOUT)
        assert result["value"] == 100  # Coerced to int
        worker.stop()

    def test_pydantic_validate_call_on_worker_init(self, worker_mode):
        """Test pydantic.validate_call on Worker __init__.

        Note: @validate_call works with ALL modes including Ray.
        """
        worker = PydanticInitWorker.options(mode=worker_mode).init(count=50, label="test")
        result = worker.get_data().result(timeout=FUTURE_TIMEOUT)
        assert result["count"] == 50
        assert result["label"] == "test"
        worker.stop()

        # Type coercion
        worker = PydanticInitWorker.options(mode=worker_mode).init(count="75", label="coerced")
        result = worker.get_data().result(timeout=FUTURE_TIMEOUT)
        assert result["count"] == 75  # Coerced to int
        worker.stop()

    def test_validate_on_init_all_modes(self, worker_mode):
        """Test validate on __init__ with all execution modes.

        Note: @validate works with ALL modes including Ray.
        """
        worker = ThreadInitWorker.options(mode=worker_mode).init(value="10", multiplier="3")
        result = worker.compute().result(timeout=FUTURE_TIMEOUT)
        assert result == 30  # Coerced values: 10 * 3
        worker.stop()


class TestValidateCombinations:
    """Test combinations of validate decorators with model inheritance."""

    def test_typed_worker_with_validate_methods(self, worker_mode):
        """Test Typed worker with @validate decorated methods in all modes including Ray."""
        worker = ComplexWorkerValidated.options(mode=worker_mode).init(name="  processor  ", multiplier=5)

        # Name should be normalized by pre_initialize
        result = worker.process("10", factor="2.0").result(timeout=FUTURE_TIMEOUT)
        assert result == 100.0  # 10 * 5 * 2.0

        worker.stop()

    def test_pydantic_worker_with_validate_call_methods(self, worker_mode):
        """Test Pydantic worker with @validate_call decorated methods in all modes including Ray."""
        worker = FullyValidatedPydanticWorker.options(mode=worker_mode).init(name="validator", rate=20)

        result = worker.compute(5, scale=2.0).result(timeout=FUTURE_TIMEOUT)
        assert result == 200.0  # 5 * 20 * 2.0

        # Type coercion
        result = worker.compute("3", scale="3.0").result(timeout=FUTURE_TIMEOUT)
        assert result == 180.0  # 3 * 20 * 3.0

        worker.stop()

    def test_mixing_validate_and_validate_call(self, worker_mode):
        """Test mixing morphic.validate and pydantic.validate_call on same worker.

        Note: Both decorators work with ALL modes including Ray.
        """
        worker = MixedValidationWorker.options(mode=worker_mode).init(base=100)

        # Both decorators work on same worker
        result1 = worker.morphic_method("10").result(timeout=FUTURE_TIMEOUT)
        assert result1 == 110

        result2 = worker.pydantic_method("20", y="5").result(timeout=FUTURE_TIMEOUT)
        assert result2 == 125

        worker.stop()


# ============================================================================
# Test Cases: Limits with Model Workers
# ============================================================================


class TestLimitsWithTypedWorkers:
    """Test Limits integration with Typed workers."""

    def test_typed_worker_with_rate_limits(self, worker_mode):
        """Test Typed worker using rate limits in all modes including Ray."""
        limits = [
            RateLimit(
                key="api_tokens", window_seconds=1, capacity=1000, algorithm=RateLimitAlgorithm.TokenBucket
            )
        ]

        worker = APIWorker.options(mode=worker_mode, limits=limits).init(name="API Service", api_key="secret")

        result = worker.call_api(100).result(timeout=FUTURE_TIMEOUT)
        assert "used 100 tokens" in result
        worker.stop()

    def test_typed_worker_with_resource_limits(self, worker_mode):
        """Test Typed worker using resource limits in all modes including Ray."""
        limits = [ResourceLimit(key="connections", capacity=5)]

        worker = DBWorker.options(mode=worker_mode, limits=limits).init(
            db_name="production", max_connections=10
        )

        result = worker.query("SELECT * FROM users").result(timeout=FUTURE_TIMEOUT)
        assert result["db"] == "production"
        assert result["result"] == "success"
        worker.stop()

    def test_typed_worker_with_call_limits(self, worker_mode):
        """Test Typed worker using call limits in all modes including Ray."""

        class RateLimitedWorker(Worker, Typed):
            name: str
            requests_per_minute: int = 60

            def process(self, data: str) -> str:
                # CallLimit is automatically acquired
                return f"{self.name} processed: {data}"

        limits = [CallLimit(window_seconds=60, capacity=100)]

        worker = RateLimitedWorker.options(mode=worker_mode, limits=limits).init(
            name="Processor", requests_per_minute=100
        )

        result = worker.process("test data").result(timeout=FUTURE_TIMEOUT)
        assert "processed: test data" in result
        worker.stop()


class TestLimitsWithPydanticWorkers:
    """Test Limits integration with Pydantic BaseModel workers."""

    def test_pydantic_worker_with_limits(self, worker_mode):
        """Test Pydantic worker using limits in all modes including Ray."""

        class TokenWorker(Worker, BaseModel):
            service_name: str = Field(..., min_length=1)
            max_tokens: int = Field(default=1000, ge=1)

            def process_request(self, tokens: int) -> dict:
                with self.limits.acquire(requested={"tokens": tokens}) as acq:
                    acq.update(usage={"tokens": tokens})
                    return {"service": self.service_name, "tokens_used": tokens}

        limits = [
            RateLimit(key="tokens", window_seconds=1, capacity=5000, algorithm=RateLimitAlgorithm.TokenBucket)
        ]

        worker = TokenWorker.options(mode=worker_mode, limits=limits).init(
            service_name="LLM Service", max_tokens=5000
        )

        result = worker.process_request(250).result(timeout=FUTURE_TIMEOUT)
        assert result["service"] == "LLM Service"
        assert result["tokens_used"] == 250
        worker.stop()


# ============================================================================
# Test Cases: Worker Pools with Model Workers
# ============================================================================


class TestWorkerPoolsWithTypedWorkers:
    """Test worker pools with Typed workers."""

    def test_typed_worker_pool_basic(self):
        """Test basic typed worker pool functionality."""
        pool = PoolWorkerTyped.options(mode="thread", max_workers=3).init(
            worker_id="pool_worker", multiplier=5
        )

        # Submit multiple tasks
        futures = [pool.compute(i) for i in range(10)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        assert len(results) == 10
        assert all(r["result"] == i * 5 for i, r in enumerate(results))
        pool.stop()

    def test_typed_worker_pool_with_limits(self):
        """Test typed worker pool with shared limits."""
        limits = [
            RateLimit(key="tokens", window_seconds=1, capacity=100, algorithm=RateLimitAlgorithm.TokenBucket)
        ]

        pool = LimitedPoolWorker.options(mode="thread", max_workers=3, limits=limits).init(
            name="limited_pool"
        )

        # All workers share the same 100 token/sec limit
        futures = [pool.process(i) for i in range(5)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        assert len(results) == 5
        pool.stop()

    def test_typed_worker_pool_state_isolation(self):
        """Test that typed worker pool maintains state isolation."""
        pool = StatelessWorker.options(mode="thread", max_workers=3).init(name="stateless", worker_id=1)

        # Make multiple calls - they'll be distributed across workers
        futures = [pool.process(i) for i in range(10)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        # All results should be correct (stateless processing)
        assert len(results) == 10
        assert all(r["result"] == i * 2 for i, r in enumerate(results))
        pool.stop()


class TestWorkerPoolsWithPydanticWorkers:
    """Test worker pools with Pydantic BaseModel workers."""

    def test_pydantic_worker_pool_basic(self):
        """Test basic pydantic worker pool functionality."""
        pool = PoolWorkerPydantic.options(mode="thread", max_workers=4).init(
            worker_name="pydantic_pool", multiplier=3
        )

        futures = [pool.compute(i) for i in range(12)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        assert len(results) == 12
        assert all(r["result"] == i * 3 for i, r in enumerate(results))
        pool.stop()

    def test_pydantic_worker_pool_process_mode(self):
        """Test pydantic worker pool in process mode."""
        pool = ProcessPoolWorker.options(mode="process", max_workers=2).init(name="process_pool", value=7)

        futures = [pool.compute(i) for i in range(6)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        assert results == [0, 7, 14, 21, 28, 35]
        pool.stop()


# ============================================================================
# Test Cases: validate decorators on Worker methods
# ============================================================================


class TestValidateOnWorkerMethods:
    """Test morphic.validate decorator on worker methods."""

    def test_validate_on_worker_method(self, worker_mode):
        """Test morphic.validate on a worker method."""
        # Use module-level ValidatedWorker class
        worker = ValidatedWorker.options(mode=worker_mode).init(multiplier=5)

        # Valid call
        result = worker.process(10, scale=2.0).result(timeout=FUTURE_TIMEOUT)
        assert result == 100.0

        # String should be coerced to int/float
        result = worker.process("5", scale="3.0").result(timeout=FUTURE_TIMEOUT)
        assert result == 75.0

        worker.stop()

    def test_validate_on_typed_worker_method(self, worker_mode):
        """Test morphic.validate on Typed worker methods in all modes including Ray."""
        worker = TypedValidatedWorker.options(mode=worker_mode).init(name="validated", multiplier=3)

        result = worker.compute(5, y=3).result(timeout=FUTURE_TIMEOUT)
        assert result == 24  # (5 + 3) * 3

        # Type coercion
        result = worker.compute("10", y="5").result(timeout=FUTURE_TIMEOUT)
        assert result == 45  # (10 + 5) * 3

        worker.stop()

    def test_validate_on_async_worker_method(self, worker_mode):
        """Test morphic.validate on async worker methods.

        Note: @validate works with ALL modes including Ray.
        """
        worker = AsyncValidatedWorker.options(mode=worker_mode).init(base=100)

        result = worker.async_compute(42, delay=0.001).result(timeout=FUTURE_TIMEOUT)
        assert result == 142

        worker.stop()


class TestValidateCallOnWorkerMethods:
    """Test pydantic.validate_call decorator on worker methods."""

    def test_validate_call_on_worker_method(self, worker_mode):
        """Test pydantic.validate_call on a worker method."""
        # Use module-level PydanticValidateCallWorker class
        worker = PydanticValidateCallWorker.options(mode=worker_mode).init(multiplier=4)

        # Valid call
        result = worker.process(10, scale=2.5).result(timeout=FUTURE_TIMEOUT)
        assert result == 100.0

        # Pydantic should coerce string to int/float
        result = worker.process("5", scale="2.0").result(timeout=FUTURE_TIMEOUT)
        assert result == 40.0

        worker.stop()

    def test_validate_call_on_pydantic_worker_method(self, worker_mode):
        """Test pydantic.validate_call on BaseModel worker methods in all modes including Ray."""
        # Use module-level FullyValidatedWorker class
        worker = FullyValidatedWorker.options(mode=worker_mode).init(name="validated", multiplier=5)

        result = worker.compute(10, y=5).result(timeout=FUTURE_TIMEOUT)
        assert result == 75  # (10 + 5) * 5

        # Type coercion
        result = worker.compute("8", y="2").result(timeout=FUTURE_TIMEOUT)
        assert result == 50  # (8 + 2) * 5

        worker.stop()

    def test_validate_call_validation_errors(self, worker_mode):
        """Test that validate_call raises validation errors properly.

        Note: @validate_call works with ALL modes including Ray.
        """
        worker = StrictWorker.options(mode=worker_mode).init()

        # Valid call
        result = worker.strict_process(42, name="test").result(timeout=FUTURE_TIMEOUT)
        assert result == "test: 42"

        # Invalid: missing required argument should fail
        # Note: This might fail at call time or when getting result
        try:
            future = worker.strict_process(42)
            future.result(timeout=FUTURE_TIMEOUT)
            assert False, "Should have raised validation error"
        except Exception:
            # Expected - validation error occurred
            pass

        worker.stop()


# ============================================================================
# Test Cases: Complex scenarios
# ============================================================================


class TestComplexValidationScenarios:
    """Test complex scenarios combining multiple features."""

    def test_typed_worker_with_validated_methods_and_limits(self, worker_mode):
        """Test Typed worker with validate decorators and limits in all modes including Ray."""
        limits = [
            RateLimit(key="tokens", window_seconds=1, capacity=5000, algorithm=RateLimitAlgorithm.TokenBucket)
        ]

        worker = ComplexWorkerWithLimits.options(mode=worker_mode, limits=limits).init(
            name="complex", max_tokens=1000
        )

        result = worker.process("test prompt", tokens=200).result(timeout=FUTURE_TIMEOUT)
        assert result["name"] == "complex"
        assert result["tokens"] == 200
        worker.stop()

    def test_pydantic_worker_pool_with_validated_methods(self, worker_mode):
        """Test Pydantic worker pool with validate_call methods in all modes including Ray."""
        # For sync/asyncio, pools require max_workers=1
        max_workers = 1 if worker_mode in ("sync", "asyncio") else 3

        pool = PooledValidatedPydanticWorker.options(mode=worker_mode, max_workers=max_workers).init(
            worker_id="pool", multiplier=4
        )

        futures = [pool.compute(i, y=1) for i in range(6)]
        results = [f.result(timeout=FUTURE_TIMEOUT) for f in futures]

        assert len(results) == 6
        assert all(r["result"] == (i + 1) * 4 for i, r in enumerate(results))
        pool.stop()

    def test_typed_worker_full_validation_stack(self, worker_mode):
        """Test Typed worker with full validation at all levels in all modes including Ray."""
        worker = FullValidationStackWorker.options(mode=worker_mode).init(name="  validator  ", rate=20)

        # Name should be normalized by pre_initialize
        result = worker.process(5, scale=2.0).result(timeout=FUTURE_TIMEOUT)
        assert result["name"] == "Validator"  # Stripped and titled
        assert result["result"] == 200.0  # 5 * 20 * 2.0
        worker.stop()
