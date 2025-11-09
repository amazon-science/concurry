"""Tests for @worker decorator warning logic.

This module tests that the UserWarning about mixing decorator and inheritance
parameters is only shown when BOTH approaches are used with actual configuration.

The warning should NOT be shown when:
- Using ONLY @worker decorator (with or without params)
- Using ONLY Worker inheritance (with or without params)
- Using @worker decorator on a class that inherits from Worker without params

The warning SHOULD be shown when:
- Using @worker decorator with params on a class that inherits from Worker with params
- Using @worker decorator with params on a derived class whose parent has inheritance params
"""

import warnings

import pytest

from concurry import Worker, worker
from concurry.core.worker.base_worker import WorkerProxy
from concurry.core.worker.worker_pool import WorkerProxyPool


class TestDecoratorOnlyNoWarning:
    """Test that @worker decorator alone does NOT trigger warning."""

    def test_decorator_with_params_no_inheritance(self):
        """@worker(mode='thread') on plain class should NOT warn."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @worker(mode="thread", max_workers=10)
            class WaitAndRun:
                def __init__(self, val):
                    self.val = val

                def get_val(self, wait=1.0):
                    import time

                    time.sleep(wait)
                    return self.val

            # Should NOT have raised UserWarning
            anti_pattern_warnings = [
                warning for warning in w if "anti-pattern" in str(warning.message).lower()
            ]
            assert len(anti_pattern_warnings) == 0, (
                f"Expected no anti-pattern warning, but got {len(anti_pattern_warnings)}: "
                f"{[str(w.message) for w in anti_pattern_warnings]}"
            )

            # Verify it works correctly
            instance = WaitAndRun(val=42)
            assert isinstance(instance, (WorkerProxy, WorkerProxyPool))
            instance.stop()

    def test_decorator_without_params_no_inheritance(self):
        """@worker on plain class should NOT warn."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @worker
            class WaitAndRun:
                def __init__(self, val):
                    self.val = val

            # Should NOT have raised UserWarning
            anti_pattern_warnings = [
                warning for warning in w if "anti-pattern" in str(warning.message).lower()
            ]
            assert len(anti_pattern_warnings) == 0

            # Verify it does NOT auto-init (no params provided)
            instance = WaitAndRun(val=42)
            assert not isinstance(instance, WorkerProxy)

    def test_decorator_with_params_on_worker_subclass_no_params(self):
        """@worker(mode='thread') on Worker subclass (no params) should NOT warn."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @worker(mode="thread", max_workers=5)
            class WaitAndRun(Worker):
                def __init__(self, val):
                    self.val = val

            # Should NOT have raised UserWarning
            anti_pattern_warnings = [
                warning for warning in w if "anti-pattern" in str(warning.message).lower()
            ]
            assert len(anti_pattern_warnings) == 0

            # Verify it works correctly
            instance = WaitAndRun(val=42)
            assert isinstance(instance, (WorkerProxy, WorkerProxyPool))
            instance.stop()


class TestInheritanceOnlyNoWarning:
    """Test that Worker inheritance alone does NOT trigger warning."""

    def test_inheritance_with_params(self):
        """Worker subclass with params should NOT warn."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            class WaitAndRun(Worker, mode="thread", max_workers=10):
                def __init__(self, val):
                    self.val = val

            # Should NOT have raised UserWarning
            anti_pattern_warnings = [
                warning for warning in w if "anti-pattern" in str(warning.message).lower()
            ]
            assert len(anti_pattern_warnings) == 0

            # Verify it works correctly
            instance = WaitAndRun(val=42)
            assert isinstance(instance, (WorkerProxy, WorkerProxyPool))
            instance.stop()

    def test_inheritance_without_params(self):
        """Worker subclass without params should NOT warn."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            class WaitAndRun(Worker):
                def __init__(self, val):
                    self.val = val

            # Should NOT have raised UserWarning
            anti_pattern_warnings = [
                warning for warning in w if "anti-pattern" in str(warning.message).lower()
            ]
            assert len(anti_pattern_warnings) == 0

            # Verify it does NOT auto-init (no params)
            instance = WaitAndRun(val=42)
            assert not isinstance(instance, WorkerProxy)


class TestMixedDecoratorInheritanceWarning:
    """Test that mixing decorator and inheritance DOES trigger warning."""

    def test_both_decorator_and_inheritance_with_params(self):
        """@worker(params) on Worker subclass with params SHOULD warn."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @worker(mode="process", max_workers=5)
            class WaitAndRun(Worker, mode="thread", max_workers=10):
                def __init__(self, val):
                    self.val = val

            # SHOULD have raised UserWarning about anti-pattern
            anti_pattern_warnings = [
                warning for warning in w if "anti-pattern" in str(warning.message).lower()
            ]
            assert len(anti_pattern_warnings) == 1, (
                f"Expected 1 anti-pattern warning, got {len(anti_pattern_warnings)}"
            )
            assert "decorator" in str(anti_pattern_warnings[0].message).lower()
            assert "inheritance" in str(anti_pattern_warnings[0].message).lower()

            # Verify decorator takes precedence (process mode, not thread)
            instance = WaitAndRun(val=42)
            assert isinstance(instance, (WorkerProxy, WorkerProxyPool))
            # Check that it's process mode (not thread)
            from concurry.core.constants import ExecutionMode

            assert instance.mode == ExecutionMode.Processes
            instance.stop()

    def test_decorator_without_params_on_inheritance_with_params_no_warning(self):
        """@worker (no params) on Worker subclass with params should NOT warn."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @worker
            class WaitAndRun(Worker, mode="thread", max_workers=10):
                def __init__(self, val):
                    self.val = val

            # Should NOT warn (decorator has no config, just ensures Worker inheritance)
            anti_pattern_warnings = [
                warning for warning in w if "anti-pattern" in str(warning.message).lower()
            ]
            assert len(anti_pattern_warnings) == 0

            # Verify inheritance config is used
            instance = WaitAndRun(val=42)
            assert isinstance(instance, (WorkerProxy, WorkerProxyPool))
            instance.stop()


class TestDerivedClassWarning:
    """Test warning behavior with derived classes."""

    def test_derived_class_decorator_with_parent_inheritance_params(self):
        """Decorator on derived class should warn if parent has inheritance params."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            class BaseWorker(Worker, mode="thread", max_workers=5):
                pass

            @worker(mode="process", max_workers=10)
            class DerivedWorker(BaseWorker):
                def __init__(self, val):
                    self.val = val

            # SHOULD warn (derived decorator conflicts with parent inheritance)
            anti_pattern_warnings = [
                warning for warning in w if "anti-pattern" in str(warning.message).lower()
            ]
            assert len(anti_pattern_warnings) == 1

            # Verify decorator takes precedence (process mode, not thread)
            instance = DerivedWorker(val=42)
            assert isinstance(instance, (WorkerProxy, WorkerProxyPool))
            from concurry.core.constants import ExecutionMode

            assert instance.mode == ExecutionMode.Processes
            instance.stop()

    def test_derived_class_decorator_with_parent_no_params(self):
        """Decorator on derived class should NOT warn if parent has no params."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            class BaseWorker(Worker):
                pass

            @worker(mode="process", max_workers=10)
            class DerivedWorker(BaseWorker):
                def __init__(self, val):
                    self.val = val

            # Should NOT warn (parent has no config)
            anti_pattern_warnings = [
                warning for warning in w if "anti-pattern" in str(warning.message).lower()
            ]
            assert len(anti_pattern_warnings) == 0

            # Verify it works
            instance = DerivedWorker(val=42)
            assert isinstance(instance, (WorkerProxy, WorkerProxyPool))
            instance.stop()

    def test_derived_class_inheritance_with_parent_inheritance(self):
        """Derived class with inheritance params + parent with inheritance params."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            class BaseWorker(Worker, mode="thread", max_workers=5):
                pass

            # Derived class also has inheritance params
            class DerivedWorker(BaseWorker, mode="process", max_workers=10):
                def __init__(self, val):
                    self.val = val

            # Should NOT warn (both use inheritance, no decorator involved)
            anti_pattern_warnings = [
                warning for warning in w if "anti-pattern" in str(warning.message).lower()
            ]
            assert len(anti_pattern_warnings) == 0

            # Verify derived config is used (process mode, not thread)
            instance = DerivedWorker(val=42)
            assert isinstance(instance, (WorkerProxy, WorkerProxyPool))
            from concurry.core.constants import ExecutionMode

            assert instance.mode == ExecutionMode.Processes
            instance.stop()


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_decorator_with_only_limits_param(self):
        """@worker with only limits param should NOT warn."""
        from concurry import CallLimit

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @worker(limits=[CallLimit(window_seconds=1.0, capacity=10)])
            class WaitAndRun:
                def __init__(self, val):
                    self.val = val

            # Should NOT warn (decorator has config, no inheritance conflict)
            anti_pattern_warnings = [
                warning for warning in w if "anti-pattern" in str(warning.message).lower()
            ]
            assert len(anti_pattern_warnings) == 0

    def test_decorator_with_mode_options(self):
        """@worker with mode-specific options should NOT warn."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @worker(mode="thread", max_workers=5)
            class WaitAndRun:
                def __init__(self, val):
                    self.val = val

            # Should NOT warn
            anti_pattern_warnings = [
                warning for warning in w if "anti-pattern" in str(warning.message).lower()
            ]
            assert len(anti_pattern_warnings) == 0

            # Verify it works
            instance = WaitAndRun(val=42)
            assert isinstance(instance, (WorkerProxy, WorkerProxyPool))
            instance.stop()

    def test_multiple_decorators_stacked(self):
        """Multiple @worker decorators should use the outermost."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @worker(mode="thread", max_workers=5)
            @worker(mode="process", max_workers=10)
            class WaitAndRun:
                def __init__(self, val):
                    self.val = val

            # May or may not warn depending on implementation
            # Just verify it doesn't crash
            instance = WaitAndRun(val=42)
            assert isinstance(instance, (WorkerProxy, WorkerProxyPool))
            instance.stop()
