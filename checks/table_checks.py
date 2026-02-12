"""Table-level QC checks for individual OMOP CDM tables (TBL-001 … TBL-016).

Each check is a self-contained class registered via ``@register_check``.
Checks iterate over all configured tables and skip absent ones automatically.
"""

from typing import Dict

from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F

from qc.base import BaseCheck, make_result_row, rows_to_df
from qc.registry import register_check
from qc.utils import (
    has_column,
    get_example_values,
    get_table_config,
    classify_level,
)


# ═══════════════════════════════════════════════════════════════════════════
# TBL-001  Row Count
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class RowCountCheck(BaseCheck):
    check_id = "TBL-001"
    check_name = "Row Count"
    description = "Reports the row count of each available table."
    severity = "INFO"

    def run(self, tables, config, spark):
        rows = []
        for tname, df in sorted(tables.items()):
            cnt = df.count()
            rows.append(
                make_result_row(
                    check_id=self.check_id,
                    check_name=self.check_name,
                    table_name=tname,
                    level="INFO" if cnt > 0 else "WARN",
                    metric_name="row_count",
                    metric_value=cnt,
                    passed=cnt > 0,
                    n_affected=cnt,
                    query_or_logic=f"SELECT COUNT(*) FROM {tname}",
                )
            )
        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# TBL-002  Required Column Null Rate
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class NullRateCheck(BaseCheck):
    check_id = "TBL-002"
    check_name = "Required Column Null Rate"
    description = "Checks null rate of required columns per table."
    severity = "WARN"

    def run(self, tables, config, spark):
        rows = []
        warn_thr = config.get("null_rate_warn_threshold", 0.05)
        err_thr = config.get("null_rate_error_threshold", 0.50)
        table_cfgs = config.get("tables", {})

        for tname, tcfg in table_cfgs.items():
            if tname not in tables:
                continue
            df = tables[tname]
            req_cols = [c for c in tcfg.get("required_columns", []) if has_column(df, c)]
            if not req_cols:
                continue

            total = df.count()
            if total == 0:
                continue

            # Compute all null counts in a single pass
            null_exprs = [
                F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"_null_{c}")
                for c in req_cols
            ]
            agg_row = df.select(null_exprs).collect()[0]

            for col_name in req_cols:
                null_cnt = agg_row[f"_null_{col_name}"]
                rate = null_cnt / total
                level = classify_level(rate, warn_thr, err_thr)
                passed = level == "INFO"

                rows.append(
                    make_result_row(
                        check_id=self.check_id,
                        check_name=self.check_name,
                        table_name=tname,
                        column_name=col_name,
                        level=level,
                        metric_name="null_rate",
                        metric_value=rate,
                        threshold=warn_thr,
                        passed=passed,
                        n_affected=null_cnt,
                        query_or_logic=f"Null rate of {col_name} in {tname}",
                    )
                )

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# TBL-003  Duplicate Primary Keys
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class DuplicatePrimaryKeyCheck(BaseCheck):
    check_id = "TBL-003"
    check_name = "Duplicate Primary Key"
    description = "Checks for duplicate values in the declared primary key column."
    severity = "ERROR"

    def run(self, tables, config, spark):
        rows = []
        table_cfgs = config.get("tables", {})

        for tname, tcfg in table_cfgs.items():
            if tname not in tables:
                continue
            pk = tcfg.get("primary_key")
            if not pk:
                continue
            df = tables[tname]
            if not has_column(df, pk):
                continue

            total = df.count()
            if total == 0:
                continue
            distinct = df.select(pk).distinct().count()
            n_dupes = total - distinct
            passed = n_dupes == 0

            ex = None
            if not passed:
                dupe_df = df.groupBy(pk).count().filter("count > 1").select(pk)
                ex = get_example_values(dupe_df, pk)

            rows.append(
                make_result_row(
                    check_id=self.check_id,
                    check_name=self.check_name,
                    table_name=tname,
                    column_name=pk,
                    level="ERROR" if not passed else "INFO",
                    metric_name="duplicate_pk_count",
                    metric_value=n_dupes,
                    threshold=0.0,
                    passed=passed,
                    n_affected=n_dupes,
                    example_values=ex,
                    query_or_logic=f"COUNT(*) - COUNT(DISTINCT {pk}) in {tname}",
                )
            )

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# TBL-004  Start / End Date Logic
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class StartEndDateLogicCheck(BaseCheck):
    check_id = "TBL-004"
    check_name = "Start/End Date Logic"
    description = "Validates that end_date >= start_date for configured date pairs."
    severity = "ERROR"

    def run(self, tables, config, spark):
        rows = []
        table_cfgs = config.get("tables", {})

        for tname, tcfg in table_cfgs.items():
            if tname not in tables:
                continue
            df = tables[tname]
            pairs = tcfg.get("start_end_date_pairs", [])

            for start_col, end_col in pairs:
                if not (has_column(df, start_col) and has_column(df, end_col)):
                    continue
                # Only check rows where both dates are non-null
                both = df.filter(
                    F.col(start_col).isNotNull() & F.col(end_col).isNotNull()
                )
                total = both.count()
                if total == 0:
                    continue

                bad = both.filter(F.col(end_col) < F.col(start_col))
                n_bad = bad.count()
                rate = n_bad / total
                passed = n_bad == 0

                pk = tcfg.get("primary_key")
                ex = get_example_values(bad, pk) if (not passed and pk and has_column(df, pk)) else None

                rows.append(
                    make_result_row(
                        check_id=self.check_id,
                        check_name=self.check_name,
                        table_name=tname,
                        column_name=f"{start_col},{end_col}",
                        level="ERROR" if not passed else "INFO",
                        metric_name="end_before_start_rate",
                        metric_value=rate,
                        threshold=0.0,
                        passed=passed,
                        n_affected=n_bad,
                        example_values=ex,
                        query_or_logic=f"{end_col} < {start_col} in {tname}",
                    )
                )

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# TBL-005  Future Dates
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class FutureDateCheck(BaseCheck):
    check_id = "TBL-005"
    check_name = "Future Date"
    description = "Flags date values beyond a configurable maximum date."
    severity = "WARN"

    def run(self, tables, config, spark):
        rows = []
        max_date = config.get("max_future_date", "2026-12-31")
        table_cfgs = config.get("tables", {})

        for tname, tcfg in table_cfgs.items():
            if tname not in tables:
                continue
            df = tables[tname]

            for date_col in tcfg.get("date_columns", []):
                if not has_column(df, date_col):
                    continue

                non_null = df.filter(F.col(date_col).isNotNull())
                total = non_null.count()
                if total == 0:
                    continue

                future = non_null.filter(F.col(date_col) > F.lit(max_date))
                n_future = future.count()
                rate = n_future / total
                passed = n_future == 0

                rows.append(
                    make_result_row(
                        check_id=self.check_id,
                        check_name=self.check_name,
                        table_name=tname,
                        column_name=date_col,
                        level="WARN" if not passed else "INFO",
                        metric_name="future_date_rate",
                        metric_value=rate,
                        threshold=0.0,
                        passed=passed,
                        n_affected=n_future,
                        example_values=get_example_values(future, date_col) if not passed else None,
                        query_or_logic=f"{date_col} > '{max_date}' in {tname}",
                    )
                )

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# TBL-006  Birth Year Plausibility
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class BirthYearPlausibilityCheck(BaseCheck):
    check_id = "TBL-006"
    check_name = "Birth Year Plausibility"
    description = "Checks that year_of_birth falls within a plausible range."
    severity = "ERROR"

    def is_applicable(self, tables, config):
        return "person" in tables

    def run(self, tables, config, spark):
        df = tables["person"]
        if not has_column(df, "year_of_birth"):
            return rows_to_df(spark, [])

        min_yr = config.get("min_birth_year", 1900)
        max_yr = config.get("max_birth_year", 2026)
        total = df.filter(F.col("year_of_birth").isNotNull()).count()
        if total == 0:
            return rows_to_df(spark, [])

        oob = df.filter(
            (F.col("year_of_birth") < min_yr)
            | (F.col("year_of_birth") > max_yr)
        )
        n_oob = oob.count()
        rate = n_oob / total
        passed = n_oob == 0

        return rows_to_df(
            spark,
            [
                make_result_row(
                    check_id=self.check_id,
                    check_name=self.check_name,
                    table_name="person",
                    column_name="year_of_birth",
                    level="ERROR" if not passed else "INFO",
                    metric_name="implausible_birth_year_rate",
                    metric_value=rate,
                    threshold=0.0,
                    passed=passed,
                    n_affected=n_oob,
                    example_values=get_example_values(oob, "year_of_birth") if not passed else None,
                    query_or_logic=f"year_of_birth NOT BETWEEN {min_yr} AND {max_yr}",
                )
            ],
        )


# ═══════════════════════════════════════════════════════════════════════════
# TBL-007  Gender Concept Presence
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class GenderConceptPresenceCheck(BaseCheck):
    check_id = "TBL-007"
    check_name = "Gender Concept Presence"
    description = "Checks that gender_concept_id is populated and non-zero in PERSON."
    severity = "WARN"

    def is_applicable(self, tables, config):
        return "person" in tables

    def run(self, tables, config, spark):
        df = tables["person"]
        if not has_column(df, "gender_concept_id"):
            return rows_to_df(spark, [])

        total = df.count()
        if total == 0:
            return rows_to_df(spark, [])

        missing = df.filter(
            F.col("gender_concept_id").isNull() | (F.col("gender_concept_id") == 0)
        )
        n_miss = missing.count()
        rate = n_miss / total
        passed = n_miss == 0

        return rows_to_df(
            spark,
            [
                make_result_row(
                    check_id=self.check_id,
                    check_name=self.check_name,
                    table_name="person",
                    column_name="gender_concept_id",
                    level="WARN" if not passed else "INFO",
                    metric_name="missing_gender_rate",
                    metric_value=rate,
                    threshold=0.0,
                    passed=passed,
                    n_affected=n_miss,
                    query_or_logic="gender_concept_id IS NULL OR = 0",
                )
            ],
        )


# ═══════════════════════════════════════════════════════════════════════════
# TBL-008  Missing Visit End Date
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class MissingVisitEndDateCheck(BaseCheck):
    check_id = "TBL-008"
    check_name = "Missing Visit End Date"
    description = "Reports the rate of NULL visit_end_date in visit_occurrence."
    severity = "WARN"

    def is_applicable(self, tables, config):
        return "visit_occurrence" in tables

    def run(self, tables, config, spark):
        df = tables["visit_occurrence"]
        if not has_column(df, "visit_end_date"):
            return rows_to_df(spark, [])

        total = df.count()
        if total == 0:
            return rows_to_df(spark, [])

        n_null = df.filter(F.col("visit_end_date").isNull()).count()
        rate = n_null / total
        warn_thr = config.get("null_rate_warn_threshold", 0.05)
        level = classify_level(rate, warn_thr, 0.50)
        passed = level == "INFO"

        return rows_to_df(
            spark,
            [
                make_result_row(
                    check_id=self.check_id,
                    check_name=self.check_name,
                    table_name="visit_occurrence",
                    column_name="visit_end_date",
                    level=level,
                    metric_name="null_visit_end_date_rate",
                    metric_value=rate,
                    threshold=warn_thr,
                    passed=passed,
                    n_affected=n_null,
                    query_or_logic="visit_end_date IS NULL",
                )
            ],
        )


# ═══════════════════════════════════════════════════════════════════════════
# TBL-009  Zero / Missing Concept ID (Unmapped Records)
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class ZeroConceptIdCheck(BaseCheck):
    check_id = "TBL-009"
    check_name = "Zero/Missing Concept ID"
    description = (
        "Reports the rate of concept_id = 0 (unmapped) for primary concept "
        "columns in clinical tables."
    )
    severity = "WARN"

    # Only check primary domain-defining concept columns
    _TARGET_COLUMNS = [
        ("condition_occurrence", "condition_concept_id"),
        ("drug_exposure", "drug_concept_id"),
        ("procedure_occurrence", "procedure_concept_id"),
        ("measurement", "measurement_concept_id"),
        ("observation", "observation_concept_id"),
        ("visit_occurrence", "visit_concept_id"),
        ("device_exposure", "device_concept_id"),
        ("specimen", "specimen_concept_id"),
    ]

    def run(self, tables, config, spark):
        rows = []
        warn_thr = config.get("zero_concept_id_warn_threshold", 0.10)
        err_thr = config.get("zero_concept_id_error_threshold", 0.50)

        for tname, col_name in self._TARGET_COLUMNS:
            if tname not in tables:
                continue
            df = tables[tname]
            if not has_column(df, col_name):
                continue

            total = df.count()
            if total == 0:
                continue

            n_zero = df.filter(
                F.col(col_name).isNull() | (F.col(col_name) == 0)
            ).count()
            rate = n_zero / total
            level = classify_level(rate, warn_thr, err_thr)
            passed = level == "INFO"

            rows.append(
                make_result_row(
                    check_id=self.check_id,
                    check_name=self.check_name,
                    table_name=tname,
                    column_name=col_name,
                    level=level,
                    metric_name="zero_concept_id_rate",
                    metric_value=rate,
                    threshold=warn_thr,
                    passed=passed,
                    n_affected=n_zero,
                    query_or_logic=f"{col_name} IS NULL OR = 0 in {tname}",
                )
            )

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# TBL-010  Missing Source Value
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class MissingSourceValueCheck(BaseCheck):
    check_id = "TBL-010"
    check_name = "Missing Source Value"
    description = "Reports null/empty source_value columns in clinical tables."
    severity = "WARN"

    def run(self, tables, config, spark):
        rows = []
        warn_thr = config.get("missing_source_value_warn_threshold", 0.20)
        err_thr = config.get("missing_source_value_error_threshold", 0.80)
        table_cfgs = config.get("tables", {})

        for tname, tcfg in table_cfgs.items():
            if tname not in tables:
                continue
            df = tables[tname]
            sv_cols = [
                c for c in tcfg.get("source_value_columns", []) if has_column(df, c)
            ]
            if not sv_cols:
                continue

            total = df.count()
            if total == 0:
                continue

            # Single-pass aggregation
            exprs = [
                F.sum(
                    F.when(
                        F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1
                    ).otherwise(0)
                ).alias(f"_miss_{c}")
                for c in sv_cols
            ]
            agg_row = df.select(exprs).collect()[0]

            for c in sv_cols:
                n_miss = agg_row[f"_miss_{c}"]
                rate = n_miss / total
                level = classify_level(rate, warn_thr, err_thr)
                passed = level == "INFO"

                rows.append(
                    make_result_row(
                        check_id=self.check_id,
                        check_name=self.check_name,
                        table_name=tname,
                        column_name=c,
                        level=level,
                        metric_name="missing_source_value_rate",
                        metric_value=rate,
                        threshold=warn_thr,
                        passed=passed,
                        n_affected=n_miss,
                        query_or_logic=f"{c} IS NULL OR TRIM({c})='' in {tname}",
                    )
                )

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# TBL-011  Measurement Value Plausibility
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class MeasurementValuePlausibilityCheck(BaseCheck):
    check_id = "TBL-011"
    check_name = "Measurement Value Plausibility"
    description = (
        "Checks value_as_number against configured plausible ranges per "
        "measurement_concept_id."
    )
    severity = "WARN"

    def is_applicable(self, tables, config):
        return "measurement" in tables

    def run(self, tables, config, spark):
        df = tables["measurement"]
        if not (has_column(df, "value_as_number") and has_column(df, "measurement_concept_id")):
            return rows_to_df(spark, [])

        ranges = config.get("measurement_ranges", {})
        if not ranges:
            return rows_to_df(spark, [])

        rows = []
        # Build a lookup DataFrame from the ranges config to avoid per-concept queries
        range_rows = [
            (int(cid), float(r["min"]), float(r["max"]))
            for cid, r in ranges.items()
        ]
        range_df = spark.createDataFrame(
            range_rows, ["_cid", "_min", "_max"]
        )

        # Join measurement with ranges on concept_id
        meas = df.filter(
            F.col("value_as_number").isNotNull()
            & F.col("measurement_concept_id").isNotNull()
        ).select("measurement_concept_id", "value_as_number")

        joined = meas.join(
            F.broadcast(range_df),
            meas["measurement_concept_id"] == range_df["_cid"],
            how="inner",
        )
        total_checked = joined.count()
        if total_checked == 0:
            return rows_to_df(spark, [])

        oob = joined.filter(
            (F.col("value_as_number") < F.col("_min"))
            | (F.col("value_as_number") > F.col("_max"))
        )
        n_oob = oob.count()
        rate = n_oob / total_checked
        passed = n_oob == 0

        # Per-concept breakdown for examples
        if not passed:
            concept_counts = (
                oob.groupBy("measurement_concept_id")
                .count()
                .orderBy(F.desc("count"))
                .limit(5)
                .collect()
            )
            ex = str(
                [
                    {"concept_id": r["measurement_concept_id"], "n": r["count"]}
                    for r in concept_counts
                ]
            )
        else:
            ex = None

        rows.append(
            make_result_row(
                check_id=self.check_id,
                check_name=self.check_name,
                table_name="measurement",
                column_name="value_as_number",
                level="WARN" if not passed else "INFO",
                metric_name="value_out_of_range_rate",
                metric_value=rate,
                threshold=0.0,
                passed=passed,
                n_affected=n_oob,
                example_values=ex,
                query_or_logic="value_as_number outside configured min/max per concept",
            )
        )

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# TBL-012  Observation Period Overlap
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class ObservationPeriodOverlapCheck(BaseCheck):
    check_id = "TBL-012"
    check_name = "Observation Period Overlap"
    description = "Detects overlapping observation periods for the same person."
    severity = "ERROR"

    def is_applicable(self, tables, config):
        return "observation_period" in tables

    def run(self, tables, config, spark):
        df = tables["observation_period"]
        needed = ["person_id", "observation_period_start_date", "observation_period_end_date"]
        if not all(has_column(df, c) for c in needed):
            return rows_to_df(spark, [])

        total_persons = df.select("person_id").distinct().count()
        if total_persons == 0:
            return rows_to_df(spark, [])

        w = Window.partitionBy("person_id").orderBy("observation_period_start_date")
        with_lag = df.withColumn(
            "_prev_end", F.lag("observation_period_end_date").over(w)
        )
        overlaps = with_lag.filter(
            F.col("observation_period_start_date") <= F.col("_prev_end")
        )
        persons_with_overlap = overlaps.select("person_id").distinct().count()
        rate = persons_with_overlap / total_persons
        passed = persons_with_overlap == 0

        return rows_to_df(
            spark,
            [
                make_result_row(
                    check_id=self.check_id,
                    check_name=self.check_name,
                    table_name="observation_period",
                    column_name="person_id",
                    level="ERROR" if not passed else "INFO",
                    metric_name="persons_with_overlapping_obs_period_rate",
                    metric_value=rate,
                    threshold=0.0,
                    passed=passed,
                    n_affected=persons_with_overlap,
                    example_values=get_example_values(overlaps, "person_id") if not passed else None,
                    query_or_logic="obs_period_start_date <= LAG(obs_period_end_date)",
                )
            ],
        )


# ═══════════════════════════════════════════════════════════════════════════
# TBL-013  Death Date After Birth
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class DeathDateAfterBirthCheck(BaseCheck):
    check_id = "TBL-013"
    check_name = "Death Date After Birth"
    description = "Ensures death_date is on or after the person's birth date."
    severity = "ERROR"

    def is_applicable(self, tables, config):
        return "death" in tables and "person" in tables

    def run(self, tables, config, spark):
        death = tables["death"]
        person = tables["person"]
        if not has_column(death, "death_date"):
            return rows_to_df(spark, [])

        # Construct birth_date from year (+ optional month/day)
        if has_column(person, "birth_datetime") and person.filter(F.col("birth_datetime").isNotNull()).limit(1).count() > 0:
            person_dates = person.select("person_id", F.col("birth_datetime").cast("date").alias("_birth_date"))
        elif has_column(person, "year_of_birth"):
            person_dates = person.select(
                "person_id",
                F.to_date(
                    F.concat_ws(
                        "-",
                        F.col("year_of_birth").cast("string"),
                        F.coalesce(F.col("month_of_birth").cast("string"), F.lit("1")),
                        F.coalesce(F.col("day_of_birth").cast("string"), F.lit("1")),
                    )
                ).alias("_birth_date"),
            )
        else:
            return rows_to_df(spark, [])

        from qc.utils import broadcast_if_small

        person_dates = broadcast_if_small(
            person_dates,
            config.get("broadcast_threshold_rows", 5_000_000),
        )
        joined = death.join(person_dates, on="person_id", how="inner")
        total = joined.count()
        if total == 0:
            return rows_to_df(spark, [])

        bad = joined.filter(F.col("death_date") < F.col("_birth_date"))
        n_bad = bad.count()
        passed = n_bad == 0

        return rows_to_df(
            spark,
            [
                make_result_row(
                    check_id=self.check_id,
                    check_name=self.check_name,
                    table_name="death",
                    column_name="death_date",
                    level="ERROR" if not passed else "INFO",
                    metric_name="death_before_birth_count",
                    metric_value=n_bad,
                    threshold=0.0,
                    passed=passed,
                    n_affected=n_bad,
                    example_values=get_example_values(bad, "person_id") if not passed else None,
                    query_or_logic="death_date < birth_date (derived from person)",
                )
            ],
        )


# ═══════════════════════════════════════════════════════════════════════════
# TBL-014  Value-as-Number Outliers (IQR-based)
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class ValueAsNumberOutlierCheck(BaseCheck):
    check_id = "TBL-014"
    check_name = "Measurement Value Outliers"
    description = (
        "Flags extreme outliers in measurement.value_as_number using the "
        "IQR method via approxQuantile (no driver collection of raw data)."
    )
    severity = "WARN"

    def is_applicable(self, tables, config):
        return "measurement" in tables

    def run(self, tables, config, spark):
        df = tables["measurement"]
        if not has_column(df, "value_as_number"):
            return rows_to_df(spark, [])

        vals = df.filter(F.col("value_as_number").isNotNull()).select("value_as_number")
        total = vals.count()
        if total == 0:
            return rows_to_df(spark, [])

        q1, q3 = vals.approxQuantile("value_as_number", [0.25, 0.75], 0.01)
        iqr = q3 - q1
        lower = q1 - 3.0 * iqr
        upper = q3 + 3.0 * iqr

        n_outlier = vals.filter(
            (F.col("value_as_number") < lower) | (F.col("value_as_number") > upper)
        ).count()
        rate = n_outlier / total
        passed = rate < 0.01  # <1 % extreme outliers

        return rows_to_df(
            spark,
            [
                make_result_row(
                    check_id=self.check_id,
                    check_name=self.check_name,
                    table_name="measurement",
                    column_name="value_as_number",
                    level="WARN" if not passed else "INFO",
                    metric_name="extreme_outlier_rate_iqr3",
                    metric_value=rate,
                    threshold=0.01,
                    passed=passed,
                    n_affected=n_outlier,
                    query_or_logic=f"value_as_number outside [{lower:.2f}, {upper:.2f}] (IQR*3)",
                )
            ],
        )


# ═══════════════════════════════════════════════════════════════════════════
# TBL-015  Death Table Person Uniqueness
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class DeathPersonUniquenessCheck(BaseCheck):
    check_id = "TBL-015"
    check_name = "Death Person Uniqueness"
    description = "Checks that each person_id appears at most once in the death table."
    severity = "ERROR"

    def is_applicable(self, tables, config):
        return "death" in tables

    def run(self, tables, config, spark):
        df = tables["death"]
        if not has_column(df, "person_id"):
            return rows_to_df(spark, [])

        total = df.count()
        distinct = df.select("person_id").distinct().count()
        n_dupes = total - distinct
        passed = n_dupes == 0

        ex = None
        if not passed:
            dupe_df = df.groupBy("person_id").count().filter("count > 1")
            ex = get_example_values(dupe_df, "person_id")

        return rows_to_df(
            spark,
            [
                make_result_row(
                    check_id=self.check_id,
                    check_name=self.check_name,
                    table_name="death",
                    column_name="person_id",
                    level="ERROR" if not passed else "INFO",
                    metric_name="duplicate_death_person_count",
                    metric_value=n_dupes,
                    threshold=0.0,
                    passed=passed,
                    n_affected=n_dupes,
                    example_values=ex,
                    query_or_logic="person_id duplicates in death",
                )
            ],
        )


# ═══════════════════════════════════════════════════════════════════════════
# TBL-016  Historical Date Range
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class DateRangeCheck(BaseCheck):
    check_id = "TBL-016"
    check_name = "Historical Date Range"
    description = "Flags date values before the minimum plausible date (default 1900-01-01)."
    severity = "WARN"

    def run(self, tables, config, spark):
        rows = []
        min_date = config.get("min_plausible_date", "1900-01-01")
        table_cfgs = config.get("tables", {})

        for tname, tcfg in table_cfgs.items():
            if tname not in tables:
                continue
            df = tables[tname]

            for date_col in tcfg.get("date_columns", []):
                if not has_column(df, date_col):
                    continue

                non_null = df.filter(F.col(date_col).isNotNull())
                total = non_null.count()
                if total == 0:
                    continue

                ancient = non_null.filter(F.col(date_col) < F.lit(min_date))
                n_ancient = ancient.count()
                rate = n_ancient / total
                passed = n_ancient == 0

                rows.append(
                    make_result_row(
                        check_id=self.check_id,
                        check_name=self.check_name,
                        table_name=tname,
                        column_name=date_col,
                        level="WARN" if not passed else "INFO",
                        metric_name="pre_min_date_rate",
                        metric_value=rate,
                        threshold=0.0,
                        passed=passed,
                        n_affected=n_ancient,
                        example_values=get_example_values(ancient, date_col) if not passed else None,
                        query_or_logic=f"{date_col} < '{min_date}' in {tname}",
                    )
                )

        return rows_to_df(spark, rows)
