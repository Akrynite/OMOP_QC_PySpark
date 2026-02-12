"""Result summarization and formatting utilities."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
import pyspark.sql.functions as F

_SUMMARY_SCHEMA = StructType(
    [
        StructField("table_name", StringType()),
        StructField("total_checks", LongType()),
        StructField("passed", LongType()),
        StructField("failed", LongType()),
        StructField("error_count", LongType()),
        StructField("warn_count", LongType()),
        StructField("info_count", LongType()),
    ]
)


def generate_summary(results_df: DataFrame, spark: SparkSession) -> DataFrame:
    """Produce a per-table summary DataFrame from raw QC results.

    Returns columns: table_name, total_checks, passed, failed,
    error_count, warn_count, info_count.  Includes a final ``__OVERALL__`` row.
    """
    if len(results_df.head(1)) == 0:
        return spark.createDataFrame([], schema=_SUMMARY_SCHEMA)

    per_table = results_df.groupBy("table_name").agg(
        F.count("*").alias("total_checks"),
        F.sum(F.when(F.col("passed"), 1).otherwise(0)).alias("passed"),
        F.sum(F.when(~F.col("passed"), 1).otherwise(0)).alias("failed"),
        F.sum(F.when(F.col("level") == "ERROR", 1).otherwise(0)).alias(
            "error_count"
        ),
        F.sum(F.when(F.col("level") == "WARN", 1).otherwise(0)).alias(
            "warn_count"
        ),
        F.sum(F.when(F.col("level") == "INFO", 1).otherwise(0)).alias(
            "info_count"
        ),
    )

    overall = results_df.agg(
        F.lit("__OVERALL__").alias("table_name"),
        F.count("*").alias("total_checks"),
        F.sum(F.when(F.col("passed"), 1).otherwise(0)).alias("passed"),
        F.sum(F.when(~F.col("passed"), 1).otherwise(0)).alias("failed"),
        F.sum(F.when(F.col("level") == "ERROR", 1).otherwise(0)).alias(
            "error_count"
        ),
        F.sum(F.when(F.col("level") == "WARN", 1).otherwise(0)).alias(
            "warn_count"
        ),
        F.sum(F.when(F.col("level") == "INFO", 1).otherwise(0)).alias(
            "info_count"
        ),
    )

    return per_table.unionByName(overall).orderBy("table_name")


def format_summary_text(summary_df: DataFrame) -> str:
    """Return a formatted text table from *summary_df*."""
    rows = summary_df.orderBy("table_name").collect()
    if not rows:
        return "No QC results."

    header = (
        f"{'Table':<30} {'Total':>6} {'Pass':>6} {'Fail':>6} "
        f"{'ERROR':>6} {'WARN':>6} {'INFO':>6}"
    )
    sep = "-" * len(header)
    lines = [sep, header, sep]

    for r in rows:
        tname = r["table_name"]
        if tname == "__OVERALL__":
            lines.append(sep)
            tname = "OVERALL"
        lines.append(
            f"{tname:<30} {r['total_checks']:>6} {r['passed']:>6} "
            f"{r['failed']:>6} {r['error_count']:>6} {r['warn_count']:>6} "
            f"{r['info_count']:>6}"
        )

    lines.append(sep)
    return "\n".join(lines)
