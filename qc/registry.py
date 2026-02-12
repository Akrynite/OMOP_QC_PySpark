"""Global check registry with decorator-based registration."""

from typing import List, Type

_REGISTRY: List[Type] = []


def register_check(cls):
    """Class decorator that adds a check class to the global registry."""
    if cls not in _REGISTRY:
        _REGISTRY.append(cls)
    return cls


def get_all_checks():
    """Return fresh instances of every registered check class."""
    return [cls() for cls in _REGISTRY]


def clear_registry():
    """Remove all registered checks (useful for testing)."""
    _REGISTRY.clear()
