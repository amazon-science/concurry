"""Comprehensive tests for enhanced @worker decorator with auto_init support.

This test file covers:
1. Basic functionality - decorator, inheritance, and mixed approaches
2. Configuration priority and merging
3. All execution modes
4. Model inheritance compatibility (Typed, BaseModel)
5. Edge cases and error handling
6. Recursion prevention
7. Context manager support
"""

import warnings
from typing import Optional

import pytest
from morphic import Typed
from pydantic import BaseModel, Field

from concurry import Worker, worker, BaseFuture, wait, gather
from concurry.config import temp_config
from concurry.core.constants import ExecutionMode, LoadBalancingAlgorithm
from concurry.core.worker.worker_pool import WorkerProxyPool
from concurry.core.worker.base_worker import WorkerProxy


class TestBasicFunctionality:
    """Test basic decorator, inheritance, and mixed approaches."""

    def test_decorator_only_auto_init_true(self):
        """Test decorator with auto_init=True creates worker automatically."""

        @worker(mode="sync", auto_init=True)
        class LLM:
            def __init__(self, model: str):
                self.model = model

            def call_llm(self, prompt: str) -> str:
                return f"{self.model}: {prompt}"

        # Direct instantiation creates worker
        llm = LLM(model="gpt-4")
        assert isinstance(llm, WorkerProxy)
        future = llm.call_llm("test")
        assert isinstance(future, BaseFuture)
        result = future.result()
        assert "gpt-4: test" in result
        llm.stop()

    def test_decorator_only_auto_init_false(self):
        """Test decorator with auto_init=False creates plain instance."""

        @worker(mode="thread", max_workers=4, auto_init=False)
        class LLM:
            def __init__(self, model: str):
                self.model = model

            def call_llm(self, prompt: str) -> str:
                return f"{self.model}: {prompt}"

        # Direct instantiation creates plain instance
        llm = LLM(model="gpt-4")
        assert not isinstance(llm, WorkerProxy)
        result = llm.call_llm("test")
        assert not isinstance(result, BaseFuture)
        assert result == "gpt-4: test"

    def test_decorator_only_no_auto_init_specified(self):
        """Test decorator without auto_init defaults to True when other params provided."""

        @worker(mode="sync", max_workers=1)  # auto_init not specified
        class LLM:
            def __init__(self, model: str):
                self.model = model

            def call_llm(self, prompt: str) -> str:
                return f"{self.model}: {prompt}"

        # Since other params provided, auto_init defaults to True
        llm = LLM(model="gpt-4")
        assert isinstance(llm, WorkerProxy)
        llm.stop()

    def test_decorator_parameterless_backward_compat(self):
        """Test parameterless @worker decorator maintains backward compatibility."""

        @worker
        class LLM(Worker):
            def __init__(self, model: str):
                self.model = model

            def call_llm(self, prompt: str) -> str:
                return f"{self.model}: {prompt}"

        # No config, so auto_init=False (backward compat)
        llm = LLM(model="gpt-4")
        assert not isinstance(llm, WorkerProxy)

        # .options().init() still works
        llm2 = LLM.options(mode="sync").init(model="gpt-4")
        assert isinstance(llm2, WorkerProxy)
        llm2.stop()

    def test_inheritance_only_auto_init_true(self):
        """Test inheritance with auto_init=True creates worker automatically."""

        class LLM(Worker, mode="sync", auto_init=True):
            def __init__(self, model: str):
                self.model = model

            def call_llm(self, prompt: str) -> str:
                return f"{self.model}: {prompt}"

        # Direct instantiation creates worker
        llm = LLM(model="gpt-4")
        assert isinstance(llm, WorkerProxy)
        future = llm.call_llm("test")
        assert isinstance(future, BaseFuture)
        llm.stop()

    def test_inheritance_only_auto_init_false(self):
        """Test inheritance with auto_init=False creates plain instance."""

        class LLM(Worker, mode="thread", max_workers=4, auto_init=False):
            def __init__(self, model: str):
                self.model = model

            def call_llm(self, prompt: str) -> str:
                return f"{self.model}: {prompt}"

        # Direct instantiation creates plain instance
        llm = LLM(model="gpt-4")
        assert not isinstance(llm, WorkerProxy)

    def test_inheritance_only_no_auto_init_specified(self):
        """Test inheritance without auto_init defaults to True when other params provided."""

        class LLM(Worker, mode="sync", max_workers=1):  # auto_init not specified
            def __init__(self, model: str):
                self.model = model

        # Since other params provided, auto_init defaults to True
        llm = LLM(model="gpt-4")
        assert isinstance(llm, WorkerProxy)
        llm.stop()

    def test_inheritance_no_params(self):
        """Test inheritance without parameters doesn't auto-init."""

        class LLM(Worker):  # No parameters
            def __init__(self, model: str):
                self.model = model

        # No config, so auto_init=False
        llm = LLM(model="gpt-4")
        assert not isinstance(llm, WorkerProxy)


class TestMixedDecoratorInheritance:
    """Test mixed decorator + inheritance approaches."""

    def test_mixed_decorator_overrides_inheritance(self):
        """Test decorator config overrides inheritance config."""

        @worker(mode="sync", max_workers=1)
        class LLM(Worker, mode="thread", max_workers=4):
            def __init__(self, model: str):
                self.model = model

            def call_llm(self, prompt: str) -> str:
                return f"{self.model}: {prompt}"

        # Should use decorator config (sync mode)
        llm = LLM(model="gpt-4")
        assert isinstance(llm, WorkerProxy)
        # Sync mode creates SyncWorkerProxy
        from concurry.core.worker.sync_worker import SyncWorkerProxy

        assert isinstance(llm, SyncWorkerProxy)
        llm.stop()

    def test_mixed_raises_warning(self):
        """Test mixed decorator + inheritance raises UserWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @worker(mode="sync")
            class LLM(Worker, mode="thread"):
                pass

            # Should have raised UserWarning about anti-pattern
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "anti-pattern" in str(w[0].message).lower()

    def test_mixed_auto_init_priority(self):
        """Test decorator auto_init overrides inheritance auto_init."""

        @worker(mode="sync", auto_init=False)
        class LLM(Worker, auto_init=True):
            def __init__(self, model: str):
                self.model = model

        # Decorator wins: auto_init=False
        llm = LLM(model="gpt-4")
        assert not isinstance(llm, WorkerProxy)


class TestOptionsOverride:
    """Test .options() override behavior."""

    def test_options_override_decorator_mode(self):
        """Test .options() can override decorator mode."""

        @worker(mode="sync", max_workers=1)
        class LLM:
            def __init__(self, model: str):
                self.model = model

            def call_llm(self, prompt: str) -> str:
                return f"{self.model}: {prompt}"

        # Override mode, keep max_workers
        llm = LLM.options(mode="thread").init(model="gpt-4")
        assert isinstance(llm, WorkerProxy)
        from concurry.core.worker.thread_worker import ThreadWorkerProxy

        assert isinstance(llm, ThreadWorkerProxy)
        llm.stop()

    def test_options_override_inheritance_all_params(self):
        """Test .options() can override all inheritance params."""

        class LLM(Worker, mode="sync", blocking=False):
            def __init__(self, model: str):
                self.model = model

        # Override everything (thread mode creates pool by default)
        llm = LLM.options(mode="thread", max_workers=1, blocking=True).init(model="gpt-4")
        assert isinstance(llm, WorkerProxy)
        assert llm.blocking is True
        llm.stop()


class TestAllExecutionModes:
    """Test decorator and inheritance across all execution modes."""

    @pytest.mark.parametrize("worker_mode", ["sync", "thread"])
    def test_decorator_all_modes(self, worker_mode):
        """Test decorator with all execution modes."""
        from concurry import worker as worker_decorator

        @worker_decorator(mode=worker_mode, max_workers=1, auto_init=True)
        class TestWorker:
            def task(self, x: int) -> int:
                return x * 2

        w = TestWorker()
        assert isinstance(w, WorkerProxy)

        future = w.task(5)
        result = future.result()
        assert result == 10
        w.stop()

    @pytest.mark.parametrize("worker_mode", ["sync", "thread"])
    def test_inheritance_all_modes(self, worker_mode):
        """Test inheritance with all execution modes."""

        class TestWorker(Worker, mode=worker_mode, max_workers=1, auto_init=True):
            def task(self, x: int) -> int:
                return x * 2

        w = TestWorker()
        assert isinstance(w, WorkerProxy)

        future = w.task(5)
        result = future.result()
        assert result == 10
        w.stop()


class TestModelInheritance:
    """Test compatibility with Typed and BaseModel."""

    def test_decorator_with_typed_worker(self):
        """Test decorator works with Typed workers."""
        from concurry import worker as worker_decorator

        @worker_decorator(mode="sync", max_workers=1, auto_init=True)
        class TypedWorker(Worker, Typed):
            name: str = Field(..., min_length=1)
            value: int = Field(default=10, ge=0)

            def process(self, x: int) -> int:
                return x * self.value

        # Auto-init with validation
        w = TypedWorker(name="test", value=20)
        assert isinstance(w, WorkerProxy)
        result = w.process(5).result()
        assert result == 100
        w.stop()

    def test_inheritance_with_typed_worker(self):
        """Test inheritance works with Typed workers."""

        class TypedWorker(Worker, Typed, mode="sync", max_workers=1, auto_init=True):
            name: str
            value: int = 10

            def process(self, x: int) -> int:
                return x * self.value

        w = TypedWorker(name="test", value=5)
        assert isinstance(w, WorkerProxy)
        result = w.process(10).result()
        assert result == 50
        w.stop()

    def test_decorator_with_basemodel_worker(self):
        """Test decorator works with BaseModel workers."""
        from concurry import worker as worker_decorator

        @worker_decorator(mode="sync", max_workers=1, auto_init=True)
        class PydanticWorker(Worker, BaseModel):
            name: str = Field(..., min_length=1)
            value: int = Field(default=10)

            def process(self, x: int) -> int:
                return x * self.value

        w = PydanticWorker(name="test", value=3)
        assert isinstance(w, WorkerProxy)
        result = w.process(7).result()
        assert result == 21
        w.stop()


class TestRecursionPrevention:
    """Test that _from_proxy flag prevents infinite recursion."""

    def test_no_infinite_recursion_auto_init(self):
        """Test auto_init doesn't cause infinite recursion."""

        @worker(mode="sync", auto_init=True)
        class LLM:
            def __init__(self, model: str):
                self.model = model

        # Should not cause infinite recursion
        llm = LLM(model="gpt-4")
        assert isinstance(llm, WorkerProxy)
        llm.stop()


class TestContextManager:
    """Test context manager support."""

    def test_decorator_with_context_manager(self):
        """Test decorator works with context manager."""

        @worker(mode="sync", auto_init=True)
        class LLM:
            def __init__(self, model: str):
                self.model = model

            def task(self, x: int) -> int:
                return x * 2

        with LLM(model="gpt-4") as llm:
            result = llm.task(5).result()
            assert result == 10
        # llm.stop() called automatically

    def test_inheritance_with_context_manager(self):
        """Test inheritance works with context manager."""

        class LLM(Worker, mode="sync", auto_init=True):
            def __init__(self, model: str):
                self.model = model

            def task(self, x: int) -> int:
                return x * 2

        with LLM(model="gpt-4") as llm:
            result = llm.task(5).result()
            assert result == 10
        # Automatic cleanup


class TestErrorCases:
    """Test error handling."""

    def test_no_mode_raises_clear_error(self):
        """Test missing mode raises clear error."""

        @worker(max_workers=4)  # No mode
        class LLM:
            def __init__(self, model: str):
                self.model = model

        with pytest.raises(ValueError, match="mode parameter is required"):
            llm = LLM(model="gpt-4")

    def test_inheritance_no_mode_raises_error(self):
        """Test inheritance without mode raises error when instantiating."""

        class LLM(Worker, max_workers=4):  # No mode
            def __init__(self, model: str):
                self.model = model

        with pytest.raises(ValueError, match="mode parameter is required"):
            llm = LLM(model="gpt-4")


class TestConfigurationPriority:
    """Test configuration priority chain."""

    def test_priority_options_beats_decorator(self):
        """Test .options() overrides decorator."""

        @worker(mode="sync", max_workers=1)
        class LLM:
            def __init__(self, model: str):
                self.model = model

        llm = LLM.options(mode="thread").init(model="gpt-4")
        # .options() should win
        from concurry.core.worker.thread_worker import ThreadWorkerProxy

        assert isinstance(llm, ThreadWorkerProxy)
        llm.stop()

    def test_priority_decorator_beats_inheritance(self):
        """Test decorator overrides inheritance."""

        @worker(mode="sync")
        class LLM(Worker, mode="thread"):
            def __init__(self, model: str):
                self.model = model

        llm = LLM(model="gpt-4")
        # Decorator should win (sync mode)
        from concurry.core.worker.sync_worker import SyncWorkerProxy

        assert isinstance(llm, SyncWorkerProxy)
        llm.stop()


class TestSpecialCases:
    """Test special cases and edge conditions."""

    def test_auto_init_with_blocking_mode(self):
        """Test auto_init works with blocking mode."""

        @worker(mode="sync", blocking=True, auto_init=True)
        class BlockingWorker:
            def task(self, x: int) -> int:
                return x * 2

        w = BlockingWorker()
        result = w.task(5)  # Should return result directly
        assert not isinstance(result, BaseFuture)
        assert result == 10
        w.stop()

    def test_sync_mode_with_auto_init(self):
        """Test sync mode with auto_init."""

        @worker(mode="sync", auto_init=True)
        class SyncWorker:
            def task(self, x: int) -> int:
                return x * 2

        w = SyncWorker()
        from concurry.core.worker.sync_worker import SyncWorkerProxy

        assert isinstance(w, SyncWorkerProxy)
        future = w.task(5)
        # Sync mode completes immediately
        assert future.done()
        assert future.result() == 10
        w.stop()
