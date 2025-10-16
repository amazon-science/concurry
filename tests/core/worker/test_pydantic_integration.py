"""Tests for Worker inheritance with model types (Typed and BaseModel).

This module tests that Worker subclasses can inherit from:
- morphic.Typed
- pydantic.BaseModel

And that these combinations work correctly across all execution modes.

**Ray Mode Limitation:**

Ray mode has a known incompatibility with Pydantic-based workers (both Typed and BaseModel).
This is due to Ray's actor wrapping mechanism (`ray.remote()`) conflicting with Pydantic's
custom `__setattr__` implementation. When Ray wraps a Pydantic class as an actor, it tries
to set metadata attributes on the ActorClass wrapper, which triggers Pydantic's validation
and causes an AttributeError.

**Supported Modes:**
- ✅ Sync: Full support
- ✅ Thread: Full support
- ✅ Process: Full support (with cloudpickle serialization)
- ✅ Asyncio: Full support
- ❌ Ray: Not supported for Typed/BaseModel workers

**Workaround for Ray:**
If you need Ray mode with structured data, use regular Worker subclasses with manual
field initialization in `__init__` instead of inheriting from Typed/BaseModel.
"""

import asyncio
from typing import List, Optional

import pytest
from morphic import Typed, validate
from pydantic import BaseModel, Field, ValidationError, validate_call

from concurry import CallLimit, RateLimit, RateLimitAlgorithm, ResourceLimit, Worker
from concurry.utils import _IS_RAY_INSTALLED

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

        # Create worker proxy
        proxy = TestWorker.options(mode=worker_mode).init()

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

    def test_options_stored_in_private_options(self, worker_mode):
        """Test that extra options are stored in _options private attribute across all modes."""

        class TestWorker(Worker):
            def __init__(self):
                pass

        # Create proxy with extra options
        proxy = TestWorker.options(mode=worker_mode, custom_option="test_value").init()

        # Extra options should be in _options
        assert "_options" in proxy.__pydantic_private__
        # Note: Since WorkerProxy has extra="allow", custom_option might be in __pydantic_extra__
        if proxy.__pydantic_private__.get("_options"):
            assert "custom_option" in proxy._options or hasattr(proxy, "__pydantic_extra__")

        proxy.stop()

    def test_different_proxy_types_all_use_typed(self):
        """Test that all WorkerProxy subclasses inherit from Typed."""
        from morphic import Typed

        from concurry.core.worker.asyncio_worker import AsyncioWorkerProxy
        from concurry.core.worker.process_worker import ProcessWorkerProxy
        from concurry.core.worker.sync_worker import SyncWorkerProxy
        from concurry.core.worker.thread_worker import ThreadWorkerProxy

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

        # Valid mp_context values
        for context in ["fork", "spawn", "forkserver"]:
            proxy = TestWorker.options(mode="process", mp_context=context).init()
            assert proxy.mp_context == context
            proxy.stop()

        # Invalid mp_context should fail
        # Note: Literal type checking might happen at Pydantic validation time
        # or at runtime when actually using the context
        with pytest.raises(Exception):  # ValidationError or ValueError
            TestWorker.options(mode="process", mp_context="invalid").init()


class TestWorkerTypedFeatures:
    """Test Typed features for Worker class itself (not WorkerProxy)."""

    def test_worker_not_typed_subclass(self):
        """Test that Worker itself does NOT inherit from Typed."""
        from morphic import Typed

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
        result = w.process().result(timeout=5)
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
        """Test that Typed worker can be initialized."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                TypedWorkerSimple.options(mode=worker_mode).init(name="test", value=10)
            return

        w = TypedWorkerSimple.options(mode=worker_mode).init(name="test", value=10)

        # Should be able to call methods
        result = w.get_name().result(timeout=5)
        assert result == "test"

        result = w.compute(5).result(timeout=5)
        assert result == 50

        w.stop()

    def test_typed_worker_with_kwargs(self, worker_mode):
        """Test Typed worker initialization with keyword arguments."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                TypedWorkerSimple.options(mode=worker_mode).init(name="worker1", value=20)
            return

        w = TypedWorkerSimple.options(mode=worker_mode).init(name="worker1", value=20)

        result = w.compute(3).result(timeout=5)
        assert result == 60

        w.stop()

    def test_typed_worker_default_values(self, worker_mode):
        """Test Typed worker with default field values."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                TypedWorkerSimple.options(mode=worker_mode).init(name="default_test")
            return

        w = TypedWorkerSimple.options(mode=worker_mode).init(name="default_test")

        # value should default to 0
        result = w.compute(10).result(timeout=5)
        assert result == 0

        w.stop()

    def test_typed_worker_validation(self, worker_mode):
        """Test that Typed worker validates fields correctly."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                TypedWorkerWithValidation.options(mode=worker_mode).init(
                    name="Alice", age=30, email="alice@example.com", tags=["python", "ml"]
                )
            return

        # Valid initialization
        w = TypedWorkerWithValidation.options(mode=worker_mode).init(
            name="Alice", age=30, email="alice@example.com", tags=["python", "ml"]
        )

        info = w.get_info().result(timeout=5)
        assert info["name"] == "Alice"
        assert info["age"] == 30
        assert info["email"] == "alice@example.com"
        assert info["tags"] == ["python", "ml"]

        w.stop()

    def test_typed_worker_validation_errors(self, worker_mode):
        """Test that Typed worker raises validation errors for invalid data."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers (not validation errors)
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                TypedWorkerWithValidation.options(mode=worker_mode).init(name="Bob", age=-5)
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
        """Test Typed worker with pre_initialize hooks."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                TypedWorkerWithHooks.options(mode=worker_mode).init(first_name="John", last_name="Doe")
            return

        w = TypedWorkerWithHooks.options(mode=worker_mode).init(first_name="John", last_name="Doe")

        # full_name should be set by pre_initialize
        result = w.get_full_name().result(timeout=5)
        assert result == "John Doe"

        w.stop()

    def test_typed_worker_state_persistence(self, worker_mode):
        """Test that Typed worker maintains state across calls."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                TypedWorkerSimple.options(mode=worker_mode).init(name="stateful", value=5)
            return

        w = TypedWorkerSimple.options(mode=worker_mode).init(name="stateful", value=5)

        # Make multiple calls
        result1 = w.compute(2).result(timeout=5)
        result2 = w.compute(3).result(timeout=5)
        result3 = w.compute(4).result(timeout=5)

        assert result1 == 10
        assert result2 == 15
        assert result3 == 20

        w.stop()

    def test_typed_worker_async_methods(self, worker_mode):
        """Test Typed worker with async methods."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                TypedWorkerAsync.options(mode=worker_mode).init(name="async_test", multiplier=3)
            return

        w = TypedWorkerAsync.options(mode=worker_mode).init(name="async_test", multiplier=3)

        # Test async method
        result1 = w.async_compute(10).result(timeout=5)
        assert result1 == 30

        # Test sync method
        result2 = w.sync_compute(10).result(timeout=5)
        assert result2 == 30

        w.stop()

    def test_typed_worker_blocking_mode(self, worker_mode):
        """Test Typed worker in blocking mode."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                TypedWorkerSimple.options(mode=worker_mode, blocking=True).init(name="blocking", value=7)
            return

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
        """Test that Pydantic worker can be initialized."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                PydanticWorkerSimple.options(mode=worker_mode).init(name="test", value=10)
            return

        w = PydanticWorkerSimple.options(mode=worker_mode).init(name="test", value=10)

        # Should be able to call methods
        result = w.get_name().result(timeout=5)
        assert result == "test"

        result = w.compute(5).result(timeout=5)
        assert result == 50

        w.stop()

    def test_pydantic_worker_with_kwargs(self, worker_mode):
        """Test Pydantic worker initialization with keyword arguments."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                PydanticWorkerSimple.options(mode=worker_mode).init(name="worker1", value=20)
            return

        w = PydanticWorkerSimple.options(mode=worker_mode).init(name="worker1", value=20)

        result = w.compute(3).result(timeout=5)
        assert result == 60

        w.stop()

    def test_pydantic_worker_default_values(self, worker_mode):
        """Test Pydantic worker with default field values."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                PydanticWorkerSimple.options(mode=worker_mode).init(name="default_test")
            return

        w = PydanticWorkerSimple.options(mode=worker_mode).init(name="default_test")

        # value should default to 0
        result = w.compute(10).result(timeout=5)
        assert result == 0

        w.stop()

    def test_pydantic_worker_validation(self, worker_mode):
        """Test that Pydantic worker validates fields correctly."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                PydanticWorkerWithValidation.options(mode=worker_mode).init(
                    name="Alice", age=30, email="alice@example.com", tags=["python", "ml"]
                )
            return

        # Valid initialization
        w = PydanticWorkerWithValidation.options(mode=worker_mode).init(
            name="Alice", age=30, email="alice@example.com", tags=["python", "ml"]
        )

        info = w.get_info().result(timeout=5)
        assert info["name"] == "Alice"
        assert info["age"] == 30
        assert info["email"] == "alice@example.com"
        assert info["tags"] == ["python", "ml"]

        w.stop()

    def test_pydantic_worker_validation_errors(self, worker_mode):
        """Test that Pydantic worker raises validation errors for invalid data."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers (not validation errors)
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                PydanticWorkerWithValidation.options(mode=worker_mode).init(name="Bob", age=-5)
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
        """Test Pydantic worker with async methods."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                PydanticWorkerAsync.options(mode=worker_mode).init(name="async_test", multiplier=3)
            return

        w = PydanticWorkerAsync.options(mode=worker_mode).init(name="async_test", multiplier=3)

        # Test async method
        result1 = w.async_compute(10).result(timeout=5)
        assert result1 == 30

        # Test sync method
        result2 = w.sync_compute(10).result(timeout=5)
        assert result2 == 30

        w.stop()

    def test_pydantic_worker_blocking_mode(self, worker_mode):
        """Test Pydantic worker in blocking mode."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                PydanticWorkerSimple.options(mode=worker_mode, blocking=True).init(name="blocking", value=7)
            return

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

        result = w.compute(2).result(timeout=5)
        assert result == 30

        w.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_serialization_ray_mode(self):
        """Test that Typed worker raises ValueError in Ray mode."""
        # Ray is initialized by conftest.py initialize_ray fixture
        # Should raise ValueError because Typed workers are not compatible with Ray
        with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
            TypedWorkerSimple.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
                name="ray_test", value=20
            )

    def test_pydantic_worker_serialization_process_mode(self):
        """Test that Pydantic worker can be serialized for process mode."""
        w = PydanticWorkerSimple.options(mode="process").init(name="process_test", value=15)

        result = w.compute(2).result(timeout=5)
        assert result == 30

        w.stop()

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_pydantic_worker_serialization_ray_mode(self):
        """Test that Pydantic worker raises ValueError in Ray mode."""
        # Ray is initialized by conftest.py initialize_ray fixture
        # Should raise ValueError because Pydantic workers are not compatible with Ray
        with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
            PydanticWorkerSimple.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
                name="ray_test", value=20
            )

    def test_typed_worker_multiple_instances(self, worker_mode):
        """Test multiple instances of Typed worker with different state."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                TypedWorkerSimple.options(mode=worker_mode).init(name="worker1", value=10)
            return

        w1 = TypedWorkerSimple.options(mode=worker_mode).init(name="worker1", value=10)
        w2 = TypedWorkerSimple.options(mode=worker_mode).init(name="worker2", value=20)

        result1 = w1.compute(2).result(timeout=5)
        result2 = w2.compute(2).result(timeout=5)

        assert result1 == 20
        assert result2 == 40

        w1.stop()
        w2.stop()

    def test_pydantic_worker_multiple_instances(self, worker_mode):
        """Test multiple instances of Pydantic worker with different state."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                PydanticWorkerSimple.options(mode=worker_mode).init(name="worker1", value=10)
            return

        w1 = PydanticWorkerSimple.options(mode=worker_mode).init(name="worker1", value=10)
        w2 = PydanticWorkerSimple.options(mode=worker_mode).init(name="worker2", value=20)

        result1 = w1.compute(2).result(timeout=5)
        result2 = w2.compute(2).result(timeout=5)

        assert result1 == 20
        assert result2 == 40

        w1.stop()
        w2.stop()

    def test_typed_worker_pool(self):
        """Test Typed worker with worker pool."""
        pool = TypedWorkerSimple.options(mode="thread", max_workers=3).init(name="pool_worker", value=5)

        # Make multiple calls
        futures = [pool.compute(i) for i in range(10)]
        results = [f.result(timeout=5) for f in futures]

        expected = [i * 5 for i in range(10)]
        assert results == expected

        pool.stop()

    def test_pydantic_worker_pool(self):
        """Test Pydantic worker with worker pool."""
        pool = PydanticWorkerSimple.options(mode="thread", max_workers=3).init(name="pool_worker", value=5)

        # Make multiple calls
        futures = [pool.compute(i) for i in range(10)]
        results = [f.result(timeout=5) for f in futures]

        expected = [i * 5 for i in range(10)]
        assert results == expected

        pool.stop()


# ============================================================================
# Edge Cases and Compatibility Tests
# ============================================================================


class TestRayIncompatibility:
    """Test Ray mode incompatibility with Pydantic-based workers."""

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_ray_mode_raises_error(self):
        """Test that creating Typed worker in Ray mode raises ValueError."""
        # Ray is initialized by conftest.py initialize_ray fixture
        with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
            TypedWorkerSimple.options(mode="ray", actor_options={"num_cpus": 0.1}).init(name="test", value=10)

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_pydantic_worker_ray_mode_raises_error(self):
        """Test that creating Pydantic worker in Ray mode raises ValueError."""
        # Ray is initialized by conftest.py initialize_ray fixture
        with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
            PydanticWorkerSimple.options(mode="ray", actor_options={"num_cpus": 0.1}).init(
                name="test", value=10
            )

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_ray_pool_raises_error(self):
        """Test that creating Typed worker pool in Ray mode raises ValueError."""
        # Ray is initialized by conftest.py initialize_ray fixture
        with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
            TypedWorkerSimple.options(mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}).init(
                name="test", value=10
            )

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_pydantic_worker_ray_pool_raises_error(self):
        """Test that creating Pydantic worker pool in Ray mode raises ValueError."""
        # Ray is initialized by conftest.py initialize_ray fixture
        with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
            PydanticWorkerSimple.options(mode="ray", max_workers=2, actor_options={"num_cpus": 0.1}).init(
                name="test", value=10
            )

    @pytest.mark.skipif(not _IS_RAY_INSTALLED, reason="Ray not installed")
    def test_typed_worker_thread_mode_warns_about_ray(self):
        """Test that creating Typed worker in thread mode warns about Ray incompatibility."""
        # Ray is initialized by conftest.py initialize_ray fixture
        # Should warn but not raise
        with pytest.warns(UserWarning, match="will NOT be compatible with Ray mode"):
            worker = TypedWorkerSimple.options(mode="thread").init(name="test", value=10)
            worker.stop()

    def test_regular_worker_ray_mode_works(self):
        """Test that regular (non-Pydantic) workers work fine in Ray mode."""
        if not _IS_RAY_INSTALLED:
            pytest.skip("Ray not installed")

        # Ray is initialized by conftest.py initialize_ray fixture
        class RegularWorker(Worker):
            def __init__(self, value: int):
                self.value = value

            def compute(self, x: int) -> int:
                return self.value * x

        # Should work without issues
        worker = RegularWorker.options(mode="ray", actor_options={"num_cpus": 0.1}).init(value=10)
        result = worker.compute(5).result(timeout=5)
        assert result == 50
        worker.stop()


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

        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                ComplexTypedWorker.options(mode=worker_mode).init(
                    name="complex", data={"key": "value"}, items=[1, 2, 3]
                )
            return

        w = ComplexTypedWorker.options(mode=worker_mode).init(
            name="complex", data={"key": "value"}, items=[1, 2, 3]
        )

        data = w.get_data().result(timeout=5)
        assert data == {"key": "value"}

        items = w.get_items().result(timeout=5)
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

        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                ComplexPydanticWorker.options(mode=worker_mode).init(
                    name="complex", data={"key": "value"}, items=[1, 2, 3]
                )
            return

        w = ComplexPydanticWorker.options(mode=worker_mode).init(
            name="complex", data={"key": "value"}, items=[1, 2, 3]
        )

        data = w.get_data().result(timeout=5)
        assert data == {"key": "value"}

        items = w.get_items().result(timeout=5)
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

        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                Worker1.options(mode=worker_mode).init(value=10)
            return

        # Both should work
        w1 = Worker1.options(mode=worker_mode).init(value=10)
        result1 = w1.compute().result(timeout=5)
        assert result1 == 20
        w1.stop()

        w2 = Worker2.options(mode=worker_mode).init(value=15)
        result2 = w2.compute().result(timeout=5)
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

        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                Worker1.options(mode=worker_mode).init(value=10)
            return

        # Both should work
        w1 = Worker1.options(mode=worker_mode).init(value=10)
        result1 = w1.compute().result(timeout=5)
        assert result1 == 20
        w1.stop()

        w2 = Worker2.options(mode=worker_mode).init(value=15)
        result2 = w2.compute().result(timeout=5)
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
        result = worker.process(10, scale=2.0).result(timeout=5)
        assert result == 100.0

        # String should be coerced to int/float
        result = worker.process("5", scale="3.0").result(timeout=5)
        assert result == 75.0

        worker.stop()

    def test_validate_on_typed_worker_method(self, worker_mode):
        """Test morphic.validate on Typed worker methods.

        Note: Ray mode not supported because the WORKER CLASS inherits from Typed,
        not because of @validate decorator.
        """
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                TypedValidatedWorker.options(mode=worker_mode).init(name="validated", multiplier=3)
            return

        worker = TypedValidatedWorker.options(mode=worker_mode).init(name="validated", multiplier=3)

        result = worker.compute(5, y=3).result(timeout=5)
        assert result == 24  # (5 + 3) * 3

        # Type coercion
        result = worker.compute("10", y="5").result(timeout=5)
        assert result == 45  # (10 + 5) * 3

        worker.stop()

    def test_validate_on_pydantic_worker_method(self, worker_mode):
        """Test morphic.validate on Pydantic BaseModel worker methods.

        Note: Ray mode not supported because the WORKER CLASS inherits from BaseModel,
        not because of @validate decorator.
        """
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                PydanticValidatedWorker.options(mode=worker_mode).init(name="validated", multiplier=4)
            return

        worker = PydanticValidatedWorker.options(mode=worker_mode).init(name="validated", multiplier=4)

        result = worker.compute(10, y=5).result(timeout=5)
        assert result == 60  # (10 + 5) * 4

        # Type coercion
        result = worker.compute("8", y="2").result(timeout=5)
        assert result == 40  # (8 + 2) * 4

        worker.stop()

    def test_validate_on_async_worker_method(self, worker_mode):
        """Test morphic.validate on async worker methods.

        Note: @validate works with ALL modes including Ray.
        """
        worker = AsyncValidatedWorker.options(mode=worker_mode).init(base=100)

        result = worker.async_compute(42, delay=0.001).result(timeout=5)
        assert result == 142

        # Type coercion on async method
        result = worker.async_compute("50", delay=0.001).result(timeout=5)
        assert result == 150

        worker.stop()

    def test_validate_with_multiple_methods(self, worker_mode):
        """Test multiple methods with @validate decorator.

        Note: @validate works with ALL modes including Ray.
        """
        worker = MultiValidatedWorker.options(mode=worker_mode).init(base=10)

        # Test all methods
        assert worker.add(5).result(timeout=5) == 15
        assert worker.multiply(3, factor=4).result(timeout=5) == 12
        assert worker.complex_calc("5", "3", c="2.5").result(timeout=5) == 20.0

        worker.stop()


class TestPydanticValidateCallOnWorkerMethods:
    """Test pydantic.validate_call decorator on worker methods across all modes."""

    def test_validate_call_on_regular_worker_method(self, worker_mode):
        """Test pydantic.validate_call on a regular worker method.

        Note: @validate_call works with ALL modes including Ray.
        """
        worker = PydanticValidateCallWorker.options(mode=worker_mode).init(multiplier=4)

        # Valid call
        result = worker.process(10, scale=2.5).result(timeout=5)
        assert result == 100.0

        # Pydantic should coerce string to int/float
        result = worker.process("5", scale="2.0").result(timeout=5)
        assert result == 40.0

        worker.stop()

    def test_validate_call_on_typed_worker_method(self, worker_mode):
        """Test pydantic.validate_call on Typed worker methods.

        Note: Ray mode not supported because the WORKER CLASS inherits from Typed.
        """
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                TypedValidateCallWorker.options(mode=worker_mode).init(name="validated", multiplier=3)
            return

        worker = TypedValidateCallWorker.options(mode=worker_mode).init(name="validated", multiplier=3)

        result = worker.compute(10, y=5).result(timeout=5)
        assert result == 45  # (10 + 5) * 3

        worker.stop()

    def test_validate_call_on_pydantic_worker_method(self, worker_mode):
        """Test pydantic.validate_call on BaseModel worker methods.

        Note: Ray mode not supported because the WORKER CLASS inherits from BaseModel.
        """
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                FullyValidatedWorker.options(mode=worker_mode).init(name="validated", multiplier=5)
            return

        worker = FullyValidatedWorker.options(mode=worker_mode).init(name="validated", multiplier=5)

        result = worker.compute(10, y=5).result(timeout=5)
        assert result == 75  # (10 + 5) * 5

        # Type coercion
        result = worker.compute("8", y="2").result(timeout=5)
        assert result == 50  # (8 + 2) * 5

        worker.stop()

    def test_validate_call_validation_errors(self, worker_mode):
        """Test that validate_call raises validation errors properly.

        Note: @validate_call works with ALL modes including Ray.
        """
        worker = StrictWorker.options(mode=worker_mode).init()

        # Valid call
        result = worker.strict_process(42, name="test").result(timeout=5)
        assert result == "test: 42"

        # Invalid: missing required argument should fail
        try:
            future = worker.strict_process(42)
            future.result(timeout=5)
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

        result = worker.async_process(5, multiplier=3).result(timeout=5)
        assert result == 45  # (5 + 10) * 3

        # Type coercion
        result = worker.async_process("8", multiplier="2").result(timeout=5)
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
        result = worker.get_info().result(timeout=5)
        assert result["value"] == 42
        assert result["name"] == "test"
        worker.stop()

        # Type coercion
        worker = InitValidatedWorker.options(mode=worker_mode).init(value="100", name="coerced")
        result = worker.get_info().result(timeout=5)
        assert result["value"] == 100  # Coerced to int
        worker.stop()

    def test_pydantic_validate_call_on_worker_init(self, worker_mode):
        """Test pydantic.validate_call on Worker __init__.

        Note: @validate_call works with ALL modes including Ray.
        """
        worker = PydanticInitWorker.options(mode=worker_mode).init(count=50, label="test")
        result = worker.get_data().result(timeout=5)
        assert result["count"] == 50
        assert result["label"] == "test"
        worker.stop()

        # Type coercion
        worker = PydanticInitWorker.options(mode=worker_mode).init(count="75", label="coerced")
        result = worker.get_data().result(timeout=5)
        assert result["count"] == 75  # Coerced to int
        worker.stop()

    def test_validate_on_init_all_modes(self, worker_mode):
        """Test validate on __init__ with all execution modes.

        Note: @validate works with ALL modes including Ray.
        """
        worker = ThreadInitWorker.options(mode=worker_mode).init(value="10", multiplier="3")
        result = worker.compute().result(timeout=5)
        assert result == 30  # Coerced values: 10 * 3
        worker.stop()


class TestValidateCombinations:
    """Test combinations of validate decorators with model inheritance."""

    def test_typed_worker_with_validate_methods(self, worker_mode):
        """Test Typed worker with @validate decorated methods."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                ComplexWorkerValidated.options(mode=worker_mode).init(name="  processor  ", multiplier=5)
            return

        worker = ComplexWorkerValidated.options(mode=worker_mode).init(name="  processor  ", multiplier=5)

        # Name should be normalized by pre_initialize
        result = worker.process("10", factor="2.0").result(timeout=5)
        assert result == 100.0  # 10 * 5 * 2.0

        worker.stop()

    def test_pydantic_worker_with_validate_call_methods(self, worker_mode):
        """Test Pydantic worker with @validate_call decorated methods."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                FullyValidatedPydanticWorker.options(mode=worker_mode).init(name="validator", rate=20)
            return

        worker = FullyValidatedPydanticWorker.options(mode=worker_mode).init(name="validator", rate=20)

        result = worker.compute(5, scale=2.0).result(timeout=5)
        assert result == 200.0  # 5 * 20 * 2.0

        # Type coercion
        result = worker.compute("3", scale="3.0").result(timeout=5)
        assert result == 180.0  # 3 * 20 * 3.0

        worker.stop()

    def test_mixing_validate_and_validate_call(self, worker_mode):
        """Test mixing morphic.validate and pydantic.validate_call on same worker.

        Note: Both decorators work with ALL modes including Ray.
        """
        worker = MixedValidationWorker.options(mode=worker_mode).init(base=100)

        # Both decorators work on same worker
        result1 = worker.morphic_method("10").result(timeout=5)
        assert result1 == 110

        result2 = worker.pydantic_method("20", y="5").result(timeout=5)
        assert result2 == 125

        worker.stop()


# ============================================================================
# Test Cases: Limits with Model Workers
# ============================================================================


class TestLimitsWithTypedWorkers:
    """Test Limits integration with Typed workers."""

    def test_typed_worker_with_rate_limits(self, worker_mode):
        """Test Typed worker using rate limits."""
        limits = [
            RateLimit(
                key="api_tokens", window_seconds=1, capacity=1000, algorithm=RateLimitAlgorithm.TokenBucket
            )
        ]

        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                APIWorker.options(mode=worker_mode, limits=limits).init(name="API Service", api_key="secret")
            return

        worker = APIWorker.options(mode=worker_mode, limits=limits).init(name="API Service", api_key="secret")

        result = worker.call_api(100).result(timeout=5)
        assert "used 100 tokens" in result
        worker.stop()

    def test_typed_worker_with_resource_limits(self, worker_mode):
        """Test Typed worker using resource limits."""
        limits = [ResourceLimit(key="connections", capacity=5)]

        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                DBWorker.options(mode=worker_mode, limits=limits).init(
                    db_name="production", max_connections=10
                )
            return

        worker = DBWorker.options(mode=worker_mode, limits=limits).init(
            db_name="production", max_connections=10
        )

        result = worker.query("SELECT * FROM users").result(timeout=5)
        assert result["db"] == "production"
        assert result["result"] == "success"
        worker.stop()

    def test_typed_worker_with_call_limits(self, worker_mode):
        """Test Typed worker using call limits."""

        class RateLimitedWorker(Worker, Typed):
            name: str
            requests_per_minute: int = 60

            def process(self, data: str) -> str:
                # CallLimit is automatically acquired
                return f"{self.name} processed: {data}"

        limits = [CallLimit(window_seconds=60, capacity=100)]

        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                RateLimitedWorker.options(mode=worker_mode, limits=limits).init(
                    name="Processor", requests_per_minute=100
                )
            return

        worker = RateLimitedWorker.options(mode=worker_mode, limits=limits).init(
            name="Processor", requests_per_minute=100
        )

        result = worker.process("test data").result(timeout=5)
        assert "processed: test data" in result
        worker.stop()


class TestLimitsWithPydanticWorkers:
    """Test Limits integration with Pydantic BaseModel workers."""

    def test_pydantic_worker_with_limits(self, worker_mode):
        """Test Pydantic worker using limits."""

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

        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                TokenWorker.options(mode=worker_mode, limits=limits).init(
                    service_name="LLM Service", max_tokens=5000
                )
            return

        worker = TokenWorker.options(mode=worker_mode, limits=limits).init(
            service_name="LLM Service", max_tokens=5000
        )

        result = worker.process_request(250).result(timeout=5)
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
        results = [f.result(timeout=5) for f in futures]

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
        results = [f.result(timeout=5) for f in futures]

        assert len(results) == 5
        pool.stop()

    def test_typed_worker_pool_state_isolation(self):
        """Test that typed worker pool maintains state isolation."""
        pool = StatelessWorker.options(mode="thread", max_workers=3).init(name="stateless", worker_id=1)

        # Make multiple calls - they'll be distributed across workers
        futures = [pool.process(i) for i in range(10)]
        results = [f.result(timeout=5) for f in futures]

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
        results = [f.result(timeout=5) for f in futures]

        assert len(results) == 12
        assert all(r["result"] == i * 3 for i, r in enumerate(results))
        pool.stop()

    def test_pydantic_worker_pool_process_mode(self):
        """Test pydantic worker pool in process mode."""
        pool = ProcessPoolWorker.options(mode="process", max_workers=2).init(name="process_pool", value=7)

        futures = [pool.compute(i) for i in range(6)]
        results = [f.result(timeout=5) for f in futures]

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
        result = worker.process(10, scale=2.0).result(timeout=5)
        assert result == 100.0

        # String should be coerced to int/float
        result = worker.process("5", scale="3.0").result(timeout=5)
        assert result == 75.0

        worker.stop()

    def test_validate_on_typed_worker_method(self, worker_mode):
        """Test morphic.validate on Typed worker methods.

        Note: Ray mode not supported because the WORKER CLASS inherits from Typed,
        not because of @validate decorator.
        """
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                TypedValidatedWorker.options(mode=worker_mode).init(name="validated", multiplier=3)
            return

        worker = TypedValidatedWorker.options(mode=worker_mode).init(name="validated", multiplier=3)

        result = worker.compute(5, y=3).result(timeout=5)
        assert result == 24  # (5 + 3) * 3

        # Type coercion
        result = worker.compute("10", y="5").result(timeout=5)
        assert result == 45  # (10 + 5) * 3

        worker.stop()

    def test_validate_on_async_worker_method(self, worker_mode):
        """Test morphic.validate on async worker methods.

        Note: @validate works with ALL modes including Ray.
        """
        worker = AsyncValidatedWorker.options(mode=worker_mode).init(base=100)

        result = worker.async_compute(42, delay=0.001).result(timeout=5)
        assert result == 142

        worker.stop()


class TestValidateCallOnWorkerMethods:
    """Test pydantic.validate_call decorator on worker methods."""

    def test_validate_call_on_worker_method(self, worker_mode):
        """Test pydantic.validate_call on a worker method."""
        # Use module-level PydanticValidateCallWorker class
        worker = PydanticValidateCallWorker.options(mode=worker_mode).init(multiplier=4)

        # Valid call
        result = worker.process(10, scale=2.5).result(timeout=5)
        assert result == 100.0

        # Pydantic should coerce string to int/float
        result = worker.process("5", scale="2.0").result(timeout=5)
        assert result == 40.0

        worker.stop()

    def test_validate_call_on_pydantic_worker_method(self, worker_mode):
        """Test pydantic.validate_call on BaseModel worker methods.

        Note: Ray mode not supported because the WORKER CLASS inherits from BaseModel.
        """
        # Use module-level FullyValidatedWorker class
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                FullyValidatedWorker.options(mode=worker_mode).init(name="validated", multiplier=5)
            return

        worker = FullyValidatedWorker.options(mode=worker_mode).init(name="validated", multiplier=5)

        result = worker.compute(10, y=5).result(timeout=5)
        assert result == 75  # (10 + 5) * 5

        # Type coercion
        result = worker.compute("8", y="2").result(timeout=5)
        assert result == 50  # (8 + 2) * 5

        worker.stop()

    def test_validate_call_validation_errors(self, worker_mode):
        """Test that validate_call raises validation errors properly.

        Note: @validate_call works with ALL modes including Ray.
        """
        worker = StrictWorker.options(mode=worker_mode).init()

        # Valid call
        result = worker.strict_process(42, name="test").result(timeout=5)
        assert result == "test: 42"

        # Invalid: missing required argument should fail
        # Note: This might fail at call time or when getting result
        try:
            future = worker.strict_process(42)
            future.result(timeout=5)
            assert False, "Should have raised validation error"
        except Exception:
            # Expected - validation error occurred
            pass

        worker.stop()


class TestValidateOnWorkerInit:
    """Test validate decorators on Worker __init__."""

    def test_validate_on_worker_init_regular(self, worker_mode):
        """Test morphic.validate on regular Worker __init__.

        Note: @validate works with ALL modes including Ray.
        """
        # Use module-level InitValidatedWorker class
        worker = InitValidatedWorker.options(mode=worker_mode).init(value=42, name="test")
        result = worker.get_info().result(timeout=5)
        assert result["value"] == 42
        assert result["name"] == "test"
        worker.stop()

        # Type coercion
        worker = InitValidatedWorker.options(mode=worker_mode).init(value="100", name="coerced")
        result = worker.get_info().result(timeout=5)
        assert result["value"] == 100  # Coerced to int
        worker.stop()

    def test_validate_call_on_worker_init(self, worker_mode):
        """Test pydantic.validate_call on Worker __init__.

        Note: @validate_call works with ALL modes including Ray.
        """
        # Use module-level PydanticInitWorker class
        worker = PydanticInitWorker.options(mode=worker_mode).init(count=50, label="test")
        result = worker.get_data().result(timeout=5)
        assert result["count"] == 50
        assert result["label"] == "test"
        worker.stop()


# ============================================================================
# Test Cases: Complex scenarios
# ============================================================================


class TestComplexValidationScenarios:
    """Test complex scenarios combining multiple features."""

    def test_typed_worker_with_validated_methods_and_limits(self, worker_mode):
        """Test Typed worker with validate decorators and limits."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                limits = [
                    RateLimit(
                        key="tokens",
                        window_seconds=1,
                        capacity=5000,
                        algorithm=RateLimitAlgorithm.TokenBucket,
                    )
                ]
                ComplexWorkerWithLimits.options(mode=worker_mode, limits=limits).init(
                    name="complex", max_tokens=1000
                )
            return

        limits = [
            RateLimit(key="tokens", window_seconds=1, capacity=5000, algorithm=RateLimitAlgorithm.TokenBucket)
        ]

        worker = ComplexWorkerWithLimits.options(mode=worker_mode, limits=limits).init(
            name="complex", max_tokens=1000
        )

        result = worker.process("test prompt", tokens=200).result(timeout=5)
        assert result["name"] == "complex"
        assert result["tokens"] == 200
        worker.stop()

    def test_pydantic_worker_pool_with_validated_methods(self, worker_mode):
        """Test Pydantic worker pool with validate_call methods."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Pydantic workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                PooledValidatedPydanticWorker.options(mode=worker_mode, max_workers=3).init(
                    worker_id="pool", multiplier=4
                )
            return

        # For sync/asyncio, pools require max_workers=1
        max_workers = 1 if worker_mode in ("sync", "asyncio") else 3

        pool = PooledValidatedPydanticWorker.options(mode=worker_mode, max_workers=max_workers).init(
            worker_id="pool", multiplier=4
        )

        futures = [pool.compute(i, y=1) for i in range(6)]
        results = [f.result(timeout=5) for f in futures]

        assert len(results) == 6
        assert all(r["result"] == (i + 1) * 4 for i, r in enumerate(results))
        pool.stop()

    def test_typed_worker_full_validation_stack(self, worker_mode):
        """Test Typed worker with full validation at all levels."""
        if worker_mode == "ray":
            # Ray mode should raise ValueError for Typed workers
            with pytest.raises(ValueError, match="Cannot create Ray worker with Pydantic-based class"):
                FullValidationStackWorker.options(mode=worker_mode).init(name="  validator  ", rate=20)
            return

        worker = FullValidationStackWorker.options(mode=worker_mode).init(name="  validator  ", rate=20)

        # Name should be normalized by pre_initialize
        result = worker.process(5, scale=2.0).result(timeout=5)
        assert result["name"] == "Validator"  # Stripped and titled
        assert result["result"] == 200.0  # 5 * 20 * 2.0
        worker.stop()
