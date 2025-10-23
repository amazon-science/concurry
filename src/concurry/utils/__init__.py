"""Utility modules for concurry."""

from typing import Literal

from .frameworks import _IS_IPYWIDGETS_INSTALLED, _IS_RAY_INSTALLED, RayContext
from .progress import ProgressBar
from .timer import Timer, TimerError

# Sentinel value to distinguish "no argument provided" from "None provided"
# This is useful when a function parameter can legitimately be None, but you need
# to know if the user explicitly passed None vs. didn't pass anything at all.
#
# Example usage:
#   def foo(x: Union[str, _NO_ARG_TYPE] = _NO_ARG):
#       if x is _NO_ARG:
#           # User didn't provide x
#       elif x is None:
#           # User explicitly passed None
#       else:
#           # User passed some value
_NO_ARG = object()
# Use Any for the type since object() instances can't be typed more precisely
_NO_ARG_TYPE = Literal[_NO_ARG]

__all__ = [
    "ProgressBar",
    "Timer",
    "TimerError",
    "_IS_IPYWIDGETS_INSTALLED",
    "_IS_RAY_INSTALLED",
    "RayContext",
    "_NO_ARG",
    "_NO_ARG_TYPE",
]
