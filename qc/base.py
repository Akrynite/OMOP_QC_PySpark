"""Base classes and result schema for OMOP CDM QC checks."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    LongType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Standard result schema -- every check emits rows conforming to this schema.
# ---------------------------------------------------------------------------
RESULT_SCHEMA = StructType(
    [
        StructField("check_id", StringType(), nullable=False),
        StructField("check_name", StringType(), nullable=False),
        StructField("table_name", StringType(), nullable=False),
        StructField("column_name", StringType(), nullable=True),
        StructField("level", StringType(), nullable=False),
        StructField("metric_name", StringType(), nullable=False),
        StructField("metric_value", DoubleType(), nullable=True),
        StructField("threshold", DoubleType(), nullable=True),
        StructField("passed", BooleanType(), nullable=False),
        StructField("n_affected", LongType(), nullable=True),
        StructField("example_values", StringType(), nullable=True),
        StructField("query_or_logic", StringType(), nullable=True),
        StructField("run_ts", TimestampType(), nullable=False),
    ]
)


class BaseCheck(ABC):
    """Abstract base class for all QC checks.

    Subclasses must set class-level attributes ``check_id``, ``check_name``,
    and ``description`` and implement the ``run`` method.
    """

    check_id: str = ""
    check_name: str = ""
    description: str = ""
    severity: str = "WARN"

    def is_applicable(
        self, tables: Dict[str, DataFrame], config: dict
    ) -> bool:
        """Return True when this check can execute given *tables* / *config*.

        Override in subclasses to skip checks when prerequisite tables or
        columns are absent.  The default implementation always returns True.
        """
        return True

    @abstractmethod
    def run(
        self,
        tables: Dict[str, DataFrame],
        config: dict,
        spark: SparkSession,
    ) -> DataFrame:
        """Execute the check and return a DataFrame conforming to RESULT_SCHEMA."""
        ...


# ---------------------------------------------------------------------------
# Helper functions for building result rows
# ---------------------------------------------------------------------------


def make_result_row(
    check_id: str,
    check_name: str,
    table_name: str,
    level: str,
    metric_name: str,
    passed: bool,
    column_name: Optional[str] = None,
    metric_value: Optional[float] = None,
    threshold: Optional[float] = None,
    n_affected: Optional[int] = None,
    example_values: Optional[str] = None,
    query_or_logic: Optional[str] = None,
) -> dict:
    """Return a dict representing one result row."""
    return {
        "check_id": check_id,
        "check_name": check_name,
        "table_name": table_name,
        "column_name": column_name,
        "level": level,
        "metric_name": metric_name,
        "metric_value": float(metric_value) if metric_value is not None else None,
        "threshold": float(threshold) if threshold is not None else None,
        "passed": bool(passed),
        "n_affected": int(n_affected) if n_affected is not None else None,
        "example_values": example_values,
        "query_or_logic": query_or_logic,
        "run_ts": datetime.utcnow(),
    }


def rows_to_df(spark: SparkSession, rows: List[dict]) -> DataFrame:
    """Convert a list of result-row dicts into a DataFrame with RESULT_SCHEMA."""
    if not rows:
        return spark.createDataFrame([], schema=RESULT_SCHEMA)
    return spark.createDataFrame(rows, schema=RESULT_SCHEMA)
