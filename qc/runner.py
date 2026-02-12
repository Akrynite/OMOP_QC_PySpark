"""QC execution engine: discovers checks, runs them, produces results."""

import time
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from qc.base import BaseCheck, RESULT_SCHEMA, make_result_row, rows_to_df
from qc.registry import get_all_checks
from config.loader import load_config


def discover_checks() -> List[BaseCheck]:
    """Import all check modules (triggering registration) and return instances.

    Call this once before ``run_qc`` if you are not supplying your own
    *check_instances* list.
    """
    import checks.table_checks  # noqa: F401
    import checks.cross_table_checks  # noqa: F401
    import checks.vocab_checks  # noqa: F401
    return get_all_checks()


def run_qc(
    tables: Dict[str, DataFrame],
    config: Optional[dict] = None,
    spark: Optional[SparkSession] = None,
    check_instances: Optional[List[BaseCheck]] = None,
    verbose: bool = True,
) -> Tuple[DataFrame, DataFrame]:
    """Run all applicable QC checks and return ``(results_df, summary_df)``.

    Parameters
    ----------
    tables : dict
        Mapping of OMOP table name (lower-case) -> PySpark DataFrame.
    config : dict, optional
        Configuration dict.  Defaults to ``DEFAULT_CONFIG``.
    spark : SparkSession, optional
        Active Spark session.  Auto-detected when *None*.
    check_instances : list[BaseCheck], optional
        Explicit list of check objects.  When *None* the global registry
        (populated via ``discover_checks()``) is used.
    verbose : bool
        Print progress information to stdout.

    Returns
    -------
    results_df : DataFrame
        All individual check results with ``RESULT_SCHEMA``.
    summary_df : DataFrame
        Per-table aggregated summary.
    """
    # -- resolve Spark session ------------------------------------------------
    if spark is None:
        spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    # -- resolve config -------------------------------------------------------
    if config is None:
        config = load_config()

    # -- normalise table names to lower-case ----------------------------------
    tables = {k.lower(): v for k, v in tables.items()}

    # -- resolve checks -------------------------------------------------------
    if check_instances is None:
        check_instances = discover_checks()

    if verbose:
        print(
            f"[OMOP-QC] {len(check_instances)} checks registered, "
            f"{len(tables)} tables available"
        )

    # -- execute each applicable check ----------------------------------------
    result_dfs: List[DataFrame] = []
    t0 = time.time()

    for chk in check_instances:
        if not chk.is_applicable(tables, config):
            if verbose:
                print(f"  SKIP  {chk.check_id} {chk.check_name}")
            continue

        if verbose:
            print(
                f"  RUN   {chk.check_id} {chk.check_name} ...",
                end="",
                flush=True,
            )
        try:
            ts = time.time()
            df = chk.run(tables, config, spark)
            elapsed = time.time() - ts
            if df is not None:
                result_dfs.append(df)
            if verbose:
                n = df.count() if df is not None else 0
                print(f" {n} results ({elapsed:.1f}s)")
        except Exception as exc:
            if verbose:
                print(f" ERROR: {exc}")
            err_row = make_result_row(
                check_id=chk.check_id,
                check_name=chk.check_name,
                table_name="N/A",
                level="ERROR",
                metric_name="check_execution_error",
                passed=False,
                query_or_logic=str(exc)[:500],
            )
            result_dfs.append(rows_to_df(spark, [err_row]))

    # -- union all results ----------------------------------------------------
    if not result_dfs:
        results_df = spark.createDataFrame([], schema=RESULT_SCHEMA)
    else:
        results_df = result_dfs[0]
        for df in result_dfs[1:]:
            results_df = results_df.unionByName(df)

    total_elapsed = time.time() - t0

    # -- optionally persist to Delta ------------------------------------------
    delta_path = config.get("delta_output_path")
    if delta_path:
        results_df.write.format("delta").mode("append").save(delta_path)
        if verbose:
            print(f"[OMOP-QC] Results written to {delta_path}")

    # -- build summary --------------------------------------------------------
    from reporting.summary import generate_summary

    summary_df = generate_summary(results_df, spark)

    if verbose:
        _print_summary(results_df, total_elapsed)

    return results_df, summary_df


def _print_summary(results_df: DataFrame, elapsed: float) -> None:
    """Print a human-friendly summary to stdout."""
    total = results_df.count()
    if total == 0:
        print("[OMOP-QC] No results produced.")
        return

    counts = (
        results_df.groupBy("level", "passed")
        .count()
        .orderBy("level")
        .collect()
    )

    passed = sum(r["count"] for r in counts if r["passed"])
    failed = sum(r["count"] for r in counts if not r["passed"])

    print(f"\n{'=' * 60}")
    print(f"  OMOP QC Summary  |  {total} checks  |  {elapsed:.1f}s")
    print(f"{'=' * 60}")
    print(f"  PASSED: {passed}   FAILED: {failed}")
    for r in counts:
        status = "PASS" if r["passed"] else "FAIL"
        print(f"    {r['level']:>5}  {status}: {r['count']}")
    print(f"{'=' * 60}\n")
