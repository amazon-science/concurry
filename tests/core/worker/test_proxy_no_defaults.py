"""Test that WorkerProxy and WorkerProxyPool have no default values on public attributes.

This is a critical requirement to ensure all defaults are centralized in global_config
and can be overridden globally via temp_config().
"""

import pytest
from pydantic_core import PydanticUndefined

from concurry.core.worker.base_worker import WorkerProxy
from concurry.core.worker.worker_pool import WorkerProxyPool


class TestProxyNoDefaults:
    """Test that proxy classes have no default values on public attributes."""

    def test_worker_proxy_no_public_defaults(self):
        """Verify WorkerProxy has NO default values on public attributes.

        This ensures all defaults come from WorkerBuilder via global_config,
        maintaining centralized configuration management.

        Private attributes (prefixed with _) are allowed to have defaults via PrivateAttr().
        """
        # Get all fields from WorkerProxy
        fields = WorkerProxy.model_fields

        # Check each field for defaults
        fields_with_defaults = []
        for field_name, field_info in fields.items():
            # Skip private attributes (they're defined via PrivateAttr and not in model_fields)
            if field_name.startswith("_"):
                continue

            # Skip ClassVar fields (like 'mode')
            if field_info.annotation and "ClassVar" in str(field_info.annotation):
                continue

            # Check if field has a default value
            # PydanticUndefined means "no default" - this is what we want!
            if field_info.default is not PydanticUndefined or field_info.default_factory is not None:
                fields_with_defaults.append((field_name, field_info.default, field_info.default_factory))

        # Assert no fields have defaults
        if len(fields_with_defaults) > 0:
            field_details = "\n".join(
                [
                    f"  - {name}: default={default}, factory={factory}"
                    for name, default, factory in fields_with_defaults
                ]
            )
            pytest.fail(
                f"WorkerProxy has {len(fields_with_defaults)} public attribute(s) with default values:\n{field_details}\n\n"
                "CRITICAL: Public attributes MUST NOT have default values.\n"
                "All defaults must come from WorkerBuilder via global_config.\n"
                "This ensures defaults are centralized and can be overridden globally.\n\n"
                "To fix: Remove the default value and ensure WorkerBuilder passes the value explicitly."
            )

    def test_worker_proxy_pool_no_public_defaults(self):
        """Verify WorkerProxyPool has NO default values on public attributes.

        This ensures all defaults come from WorkerBuilder via global_config,
        maintaining centralized configuration management.

        Private attributes (prefixed with _) are allowed to have defaults via PrivateAttr().
        """
        # Get all fields from WorkerProxyPool
        fields = WorkerProxyPool.model_fields

        # Check each field for defaults
        fields_with_defaults = []
        for field_name, field_info in fields.items():
            # Skip private attributes (they're defined via PrivateAttr and not in model_fields)
            if field_name.startswith("_"):
                continue

            # Skip ClassVar fields
            if field_info.annotation and "ClassVar" in str(field_info.annotation):
                continue

            # Check if field has a default value
            # PydanticUndefined means "no default" - this is what we want!
            if field_info.default is not PydanticUndefined or field_info.default_factory is not None:
                fields_with_defaults.append((field_name, field_info.default, field_info.default_factory))

        # Assert no fields have defaults
        if len(fields_with_defaults) > 0:
            field_details = "\n".join(
                [
                    f"  - {name}: default={default}, factory={factory}"
                    for name, default, factory in fields_with_defaults
                ]
            )
            pytest.fail(
                f"WorkerProxyPool has {len(fields_with_defaults)} public attribute(s) with default values:\n{field_details}\n\n"
                "CRITICAL: Public attributes MUST NOT have default values.\n"
                "All defaults must come from WorkerBuilder via global_config.\n"
                "This ensures defaults are centralized and can be overridden globally.\n\n"
                "To fix: Remove the default value and ensure WorkerBuilder passes the value explicitly."
            )

    def test_private_attributes_can_have_defaults(self):
        """Verify that private attributes (with PrivateAttr) can have defaults.

        This is the correct pattern - private attributes can and should have defaults,
        but public attributes must not.

        This test simply verifies that the concept is understood by checking
        that WorkerProxy's source code includes PrivateAttr with defaults.
        """
        # Read the WorkerProxy source to verify it uses PrivateAttr with defaults
        import inspect

        source = inspect.getsource(WorkerProxy)

        # Check that PrivateAttr is used with defaults
        assert "PrivateAttr(default=" in source or "PrivateAttr(default_factory=" in source, (
            "Expected WorkerProxy to use PrivateAttr with defaults for private attributes. "
            "Private attributes CAN and SHOULD have defaults via PrivateAttr()."
        )
