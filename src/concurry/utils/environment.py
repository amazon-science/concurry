import os
from typing import Optional, Tuple


class EnvConfig:
    """Environment-based configuration with type conversion."""

    def __new__(cls, *args, **kwargs):
        raise TypeError(f"{cls.__name__} may not be instantiated")

    @staticmethod
    def get_int(key: str, default: Optional[int] = None) -> Optional[int]:
        """Get integer from environment."""
        value: Optional[str] = os.environ.get(key)
        if value is None:
            return default

        try:
            return int(value)
        except ValueError as e:
            raise ValueError(f"Invalid value for {key}: {e}")

    @staticmethod
    def get_float(key: str, default: Optional[float] = None) -> Optional[float]:
        """Get float from environment."""
        value: Optional[str] = os.environ.get(key)
        if value is None:
            return default

        try:
            return float(value)
        except ValueError as e:
            raise ValueError(f"Invalid value for {key}: {e}")

    @staticmethod
    def get_bool(
        key: str,
        default: Optional[bool] = None,
        *,
        true_values: Tuple[str, ...] = ("true", "1", "yes", "on"),
        false_values: Tuple[str, ...] = ("false", "0", "no", "off"),
    ) -> Optional[bool]:
        """Get boolean from environment."""
        value: Optional[str] = os.environ.get(key)
        if value is None:
            return default
        if str(value).lower() in true_values:
            return True
        elif str(value).lower() in false_values:
            return False
        raise ValueError(f"Invalid value for {key}: {value}")
