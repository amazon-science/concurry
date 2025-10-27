"""Unit tests for per-method retry configuration helper functions."""

import pytest
from concurry import Worker
from concurry.core.worker.base_worker import (
    _get_user_defined_methods,
    _normalize_retry_param,
)


# ===== Test Workers for _get_user_defined_methods =====


class PlainWorker(Worker):
    """Plain worker with no BaseModel/Typed inheritance."""

    def method1(self):
        pass

    def method2(self):
        pass

    def _private_method(self):
        pass


class TestGetUserDefinedMethods:
    """Test _get_user_defined_methods helper function."""

    def test_plain_worker(self):
        """Test extracting methods from plain worker."""
        methods = _get_user_defined_methods(PlainWorker)

        # Should include user-defined methods
        assert "method1" in methods
        assert "method2" in methods

        # Should NOT include private methods
        assert "_private_method" not in methods
        assert "__init__" not in methods

        # Should NOT include Worker infrastructure methods
        assert "stop" not in methods  # Inherited from Worker

    def test_excludes_private_methods(self):
        """Test that private and dunder methods are excluded."""
        methods = _get_user_defined_methods(PlainWorker)

        # No method should start with "_"
        for method in methods:
            assert not method.startswith("_"), f"Method {method} should not be included"

    def test_excludes_inherited_methods(self):
        """Test that inherited methods from Worker are excluded."""
        methods = _get_user_defined_methods(PlainWorker)

        # These are Worker methods, should not be included
        worker_methods = ["stop"]
        for method in worker_methods:
            assert method not in methods, f"Inherited method {method} should not be included"


class TestNormalizeRetryParam:
    """Test _normalize_retry_param helper function."""

    def test_single_value_to_dict(self):
        """Test normalizing single value to dict."""
        method_names = ["method1", "method2", "method3"]

        result = _normalize_retry_param(5, "num_retries", method_names)

        # Should have "*" and all method names
        assert "*" in result
        assert result["*"] == 5
        assert result["method1"] == 5
        assert result["method2"] == 5
        assert result["method3"] == 5
        assert len(result) == 4  # "*" + 3 methods

    def test_dict_with_star_expands_to_all_methods(self):
        """Test dict with "*" expands to all methods."""
        method_names = ["method1", "method2", "method3"]

        result = _normalize_retry_param({"*": 3, "method2": 10}, "num_retries", method_names)

        # Should have "*" and all method names
        assert "*" in result
        assert result["*"] == 3
        assert result["method1"] == 3  # Default
        assert result["method2"] == 10  # Override
        assert result["method3"] == 3  # Default
        assert len(result) == 4  # "*" + 3 methods

    def test_dict_without_star_raises_error(self):
        """Test that dict without "*" key raises error."""
        method_names = ["method1", "method2"]

        with pytest.raises(ValueError, match="must include '\\*' key"):
            _normalize_retry_param({"method1": 5}, "num_retries", method_names)

    def test_dict_with_unknown_method_raises_error(self):
        """Test that dict with unknown method name raises error."""
        method_names = ["method1", "method2"]

        with pytest.raises(ValueError, match="unknown method names"):
            _normalize_retry_param({"*": 3, "unknown_method": 5}, "num_retries", method_names)

    def test_dict_with_only_star(self):
        """Test dict with only "*" key expands to all methods."""
        method_names = ["method1", "method2"]

        result = _normalize_retry_param({"*": 7}, "num_retries", method_names)

        assert result["*"] == 7
        assert result["method1"] == 7
        assert result["method2"] == 7
        assert len(result) == 3

    def test_empty_method_names(self):
        """Test with empty method names list."""
        method_names = []

        # Single value
        result = _normalize_retry_param(5, "num_retries", method_names)
        assert result == {"*": 5}

        # Dict with "*"
        result = _normalize_retry_param({"*": 3}, "num_retries", method_names)
        assert result == {"*": 3}

    def test_different_param_types(self):
        """Test normalization with different parameter types."""
        method_names = ["method1"]

        # Integer
        result = _normalize_retry_param(5, "num_retries", method_names)
        assert result["method1"] == 5

        # Float
        result = _normalize_retry_param(1.5, "retry_wait", method_names)
        assert result["method1"] == 1.5

        # String (RetryAlgorithm)
        result = _normalize_retry_param("exponential", "retry_algorithm", method_names)
        assert result["method1"] == "exponential"

        # List (retry_on)
        retry_on_list = [ValueError, TypeError]
        result = _normalize_retry_param(retry_on_list, "retry_on", method_names)
        assert result["method1"] == retry_on_list

    def test_preserves_dict_values(self):
        """Test that dict values are preserved correctly."""
        method_names = ["m1", "m2", "m3"]

        input_dict = {
            "*": 0,
            "m1": 3,
            "m2": 0,
            "m3": 10,
        }

        result = _normalize_retry_param(input_dict, "num_retries", method_names)

        assert result["*"] == 0
        assert result["m1"] == 3
        assert result["m2"] == 0
        assert result["m3"] == 10

    def test_parameter_name_in_error_message(self):
        """Test that parameter name appears in error messages."""
        method_names = ["method1"]

        # Missing "*" key
        with pytest.raises(ValueError, match="num_retries"):
            _normalize_retry_param({"method1": 5}, "num_retries", method_names)

        # Unknown method
        with pytest.raises(ValueError, match="retry_wait"):
            _normalize_retry_param({"*": 1.0, "unknown": 2.0}, "retry_wait", method_names)
