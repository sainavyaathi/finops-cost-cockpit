"""Shared app plumbing. Features depend on this package; this package never imports a feature."""

from .context import AppContext
from .feature import Feature

__all__ = ["AppContext", "Feature"]
