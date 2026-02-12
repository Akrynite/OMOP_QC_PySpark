"""Configuration loading and merging utilities."""

import copy
from typing import Optional


def merge_configs(base: dict, overrides: dict) -> dict:
    """Deep-merge *overrides* into a copy of *base* and return the result.

    Dict values are merged recursively; all other types are replaced.
    """
    result = copy.deepcopy(base)
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = merge_configs(result[k], v)
        else:
            result[k] = copy.deepcopy(v)
    return result


def load_config(
    overrides: Optional[dict] = None,
    yaml_path: Optional[str] = None,
) -> dict:
    """Return a fully-resolved configuration dict.

    Resolution order (later wins):
      1. ``DEFAULT_CONFIG`` (always loaded)
      2. YAML file at *yaml_path* (requires ``pyyaml``)
      3. Python *overrides* dict
    """
    from config.defaults import DEFAULT_CONFIG

    config = copy.deepcopy(DEFAULT_CONFIG)

    if yaml_path is not None:
        try:
            import yaml  # type: ignore[import-untyped]

            with open(yaml_path, "r") as fh:
                yaml_overrides = yaml.safe_load(fh) or {}
            config = merge_configs(config, yaml_overrides)
        except ImportError:
            raise ImportError(
                "PyYAML is required to load YAML config files. "
                "Install with: pip install pyyaml"
            )

    if overrides:
        config = merge_configs(config, overrides)

    return config
