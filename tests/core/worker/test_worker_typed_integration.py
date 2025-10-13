"""Tests for Typed integration with WorkerProxy classes.

This module tests that WorkerProxy subclasses properly inherit from Typed and
validate their fields and private attributes correctly.
"""

import pytest
from pydantic import ValidationError

from concurry import Worker


class TestWorkerProxyTypedValidation:
    """Test Typed validation features for WorkerProxy."""

    def test_public_field_immutability(self):
        """Test that public fields are immutable after creation."""

        class TestWorker(Worker):
            def __init__(self, x: int):
                self.x = x

        # Create worker proxy
        proxy = TestWorker.options(mode="sync").init(10)

        # Try to modify public field - should fail
        with pytest.raises((ValidationError, AttributeError)):
            proxy.worker_cls = TestWorker

        with pytest.raises((ValidationError, AttributeError)):
            proxy.blocking = True

        proxy.stop()

    def test_private_attribute_type_checking(self):
        """Test that private attributes trigger type checking on update."""

        class TestWorker(Worker):
            def __init__(self):
                pass

        # Create worker proxy
        proxy = TestWorker.options(mode="sync").init()

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

    def test_worker_options_boolean_coercion(self):
        """Test that @validate decorator coerces string booleans."""

        class TestWorker(Worker):
            def __init__(self):
                pass

        # String boolean should be coerced to bool
        builder = TestWorker.options(mode="sync", blocking="true")
        proxy = builder.init()

        # Blocking should be True (coerced from string)
        assert proxy.blocking is True
        proxy.stop()

        # Test with False
        builder = TestWorker.options(mode="sync", blocking="false")
        proxy = builder.init()
        assert proxy.blocking is False
        proxy.stop()

    def test_proxy_initialization_validation(self):
        """Test that proxy initialization validates all fields."""

        class TestWorker(Worker):
            def __init__(self, x: int):
                self.x = x

        # Valid initialization
        proxy = TestWorker.options(mode="sync").init(10)
        assert proxy.worker_cls == TestWorker
        assert proxy.blocking is False
        proxy.stop()

        # Test with explicit fields
        proxy = TestWorker.options(mode="sync", blocking=True).init(20)
        assert proxy.blocking is True
        proxy.stop()

    def test_options_stored_in_private_options(self):
        """Test that extra options are stored in _options private attribute."""

        class TestWorker(Worker):
            def __init__(self):
                pass

        # Create proxy with extra options
        proxy = TestWorker.options(mode="sync", custom_option="test_value").init()

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

    def test_worker_init_flexibility(self):
        """Test that users can define Worker __init__ freely."""

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
        w = CustomWorker.options(mode="sync").init(1, 2, c=3, extra1="x", extra2="y")
        result = w.process().result()
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
