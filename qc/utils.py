"""Shared utility helpers for QC checks."""

import json
from typing import Dict, List, Optional

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def has_column(df: DataFrame, col_name: str) -> bool:
    """Return True if *df* contains column *col_name*."""
    return col_name in df.columns


def get_example_values(
    df: DataFrame, column: str, limit: int = 5
) -> Optional[str]:
    """Collect up to *limit* distinct sample values as a JSON list string.

    Returns None when the column is absent or collection fails.
    """
    if not has_column(df, column):
        return None
    try:
        rows = df.select(column).distinct().limit(limit).collect()
        vals = [str(r[0]) for r in rows]
        return json.dumps(vals)
    except Exception:
        return None


def safe_count(df: DataFrame) -> int:
    """Return the row count of *df*, returning 0 on analysis exceptions."""
    try:
        return df.count()
    except Exception:
        return 0


def get_table_config(config: dict, table_name: str) -> dict:
    """Fetch per-table configuration, defaulting to an empty dict."""
    return config.get("tables", {}).get(table_name, {})


def get_clinical_tables(config: dict) -> List[str]:
    """Return the list of clinical-event table names from config."""
    return config.get("clinical_tables", [])


def classify_level(
    value: float, warn_threshold: float, error_threshold: float
) -> str:
    """Return 'ERROR', 'WARN', or 'INFO' based on thresholds.

    Assumes higher *value* is worse.
    """
    if value >= error_threshold:
        return "ERROR"
    if value >= warn_threshold:
        return "WARN"
    return "INFO"


def broadcast_if_small(
    df: DataFrame,
    threshold: int = 5_000_000,
    precomputed_count: Optional[int] = None,
) -> DataFrame:
    """Wrap *df* with a broadcast hint when it is small enough."""
    cnt = precomputed_count if precomputed_count is not None else df.count()
    if cnt <= threshold:
        return F.broadcast(df)
    return df
