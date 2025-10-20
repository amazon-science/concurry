"""Utility modules for concurry."""

from .frameworks import _IS_IPYWIDGETS_INSTALLED, _IS_RAY_INSTALLED, RayContext
from .progress import ProgressBar

# Sentinel value to distinguish "no argument provided" from "None provided"
# This is useful when a function parameter can legitimately be None, but you need
# to know if the user explicitly passed None vs. didn't pass anything at all.
#
# Example usage:
#   def foo(x=_NO_ARG):
#       if x is _NO_ARG:
#           # User didn't provide x
#       elif x is None:
#           # User explicitly passed None
#       else:
#           # User passed some value
_NO_ARG = object()

__all__ = [
    "ProgressBar",
    "_IS_IPYWIDGETS_INSTALLED",
    "_IS_RAY_INSTALLED",
    "RayContext",
    "_NO_ARG",
]
