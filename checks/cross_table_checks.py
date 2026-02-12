"""Cross-table QC checks for OMOP CDM (XTB-001 … XTB-009).

These checks validate referential integrity, temporal consistency, and
distributional sanity across multiple OMOP tables.
"""

from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from qc.base import BaseCheck, make_result_row, rows_to_df
from qc.registry import register_check
from qc.utils import (
    has_column,
    get_example_values,
    get_table_config,
    classify_level,
    broadcast_if_small,
)


# ═══════════════════════════════════════════════════════════════════════════
# Helper: generic referential integrity check
# ═══════════════════════════════════════════════════════════════════════════

def _ref_integrity(
    child_df: DataFrame,
    child_table: str,
    fk_col: str,
    parent_df: DataFrame,
    parent_pk: str,
    check_id: str,
    check_name: str,
    config: dict,
    spark: SparkSession,
) -> dict:
    """Return a single result-row dict for an FK -> PK integrity test."""
    if not (has_column(child_df, fk_col) and has_column(parent_df, parent_pk)):
        return None  # type: ignore[return-value]

    child_fk = child_df.select(fk_col).filter(F.col(fk_col).isNotNull())
    total = child_fk.count()
    if total == 0:
        return make_result_row(
            check_id=check_id,
            check_name=check_name,
            table_name=child_table,
            column_name=fk_col,
            level="INFO",
            metric_name="orphan_fk_rate",
            metric_value=0.0,
            threshold=0.0,
            passed=True,
            n_affected=0,
            query_or_logic=f"{child_table}.{fk_col} vs parent PK (0 rows)",
        )

    parent_keys = broadcast_if_small(
        parent_df.select(F.col(parent_pk).alias(fk_col)).distinct(),
        config.get("broadcast_threshold_rows", 5_000_000),
    )

    orphans = child_fk.join(parent_keys, on=fk_col, how="left_anti")
    n_orphans = orphans.count()
    rate = n_orphans / total

    warn_thr = config.get("orphan_rate_warn_threshold", 0.001)
    err_thr = config.get("orphan_rate_error_threshold", 0.05)
    level = classify_level(rate, warn_thr, err_thr)
    passed = level == "INFO"

    return make_result_row(
        check_id=check_id,
        check_name=check_name,
        table_name=child_table,
        column_name=fk_col,
        level=level,
        metric_name="orphan_fk_rate",
        metric_value=rate,
        threshold=warn_thr,
        passed=passed,
        n_affected=n_orphans,
        example_values=get_example_values(orphans.distinct(), fk_col) if n_orphans > 0 else None,
        query_or_logic=f"{child_table}.{fk_col} NOT IN parent.{parent_pk}",
    )


# ═══════════════════════════════════════════════════════════════════════════
# XTB-001  Person Referential Integrity
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class PersonReferentialIntegrityCheck(BaseCheck):
    check_id = "XTB-001"
    check_name = "Person Referential Integrity"
    description = "Verifies that person_id in every clinical table exists in PERSON."
    severity = "ERROR"

    def is_applicable(self, tables, config):
        return "person" in tables

    def run(self, tables, config, spark):
        person_df = tables["person"]
        clinical = config.get("clinical_tables", [])
        rows = []

        for tname in clinical:
            if tname not in tables or tname == "person":
                continue
            r = _ref_integrity(
                tables[tname], tname, "person_id",
                person_df, "person_id",
                self.check_id, self.check_name, config, spark,
            )
            if r is not None:
                rows.append(r)

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# XTB-002  Visit Occurrence Referential Integrity
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class VisitReferentialIntegrityCheck(BaseCheck):
    check_id = "XTB-002"
    check_name = "Visit Referential Integrity"
    description = (
        "Verifies that visit_occurrence_id in clinical tables exists in "
        "VISIT_OCCURRENCE."
    )
    severity = "ERROR"

    def is_applicable(self, tables, config):
        return "visit_occurrence" in tables

    def run(self, tables, config, spark):
        visit_df = tables["visit_occurrence"]
        target_tables = config.get("tables_with_visit_id", [])
        rows = []

        for tname in target_tables:
            if tname not in tables:
                continue
            r = _ref_integrity(
                tables[tname], tname, "visit_occurrence_id",
                visit_df, "visit_occurrence_id",
                self.check_id, self.check_name, config, spark,
            )
            if r is not None:
                rows.append(r)

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# XTB-003  Provider Referential Integrity
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class ProviderReferentialIntegrityCheck(BaseCheck):
    check_id = "XTB-003"
    check_name = "Provider Referential Integrity"
    description = "Verifies that provider_id in clinical tables exists in PROVIDER."
    severity = "WARN"

    def is_applicable(self, tables, config):
        return "provider" in tables

    def run(self, tables, config, spark):
        provider_df = tables["provider"]
        # Tables that commonly carry provider_id
        candidates = [
            "visit_occurrence", "condition_occurrence", "drug_exposure",
            "procedure_occurrence", "measurement", "observation",
            "note", "device_exposure",
        ]
        rows = []
        for tname in candidates:
            if tname not in tables:
                continue
            if not has_column(tables[tname], "provider_id"):
                continue
            r = _ref_integrity(
                tables[tname], tname, "provider_id",
                provider_df, "provider_id",
                self.check_id, self.check_name, config, spark,
            )
            if r is not None:
                rows.append(r)

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# XTB-004  Care Site Referential Integrity
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class CareSiteReferentialIntegrityCheck(BaseCheck):
    check_id = "XTB-004"
    check_name = "Care Site Referential Integrity"
    description = "Verifies that care_site_id in PERSON/VISIT exists in CARE_SITE."
    severity = "WARN"

    def is_applicable(self, tables, config):
        return "care_site" in tables

    def run(self, tables, config, spark):
        cs_df = tables["care_site"]
        candidates = ["person", "visit_occurrence", "provider"]
        rows = []
        for tname in candidates:
            if tname not in tables:
                continue
            if not has_column(tables[tname], "care_site_id"):
                continue
            r = _ref_integrity(
                tables[tname], tname, "care_site_id",
                cs_df, "care_site_id",
                self.check_id, self.check_name, config, spark,
            )
            if r is not None:
                rows.append(r)

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# XTB-005  Location Referential Integrity
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class LocationReferentialIntegrityCheck(BaseCheck):
    check_id = "XTB-005"
    check_name = "Location Referential Integrity"
    description = "Verifies that location_id in PERSON/CARE_SITE exists in LOCATION."
    severity = "WARN"

    def is_applicable(self, tables, config):
        return "location" in tables

    def run(self, tables, config, spark):
        loc_df = tables["location"]
        candidates = ["person", "care_site"]
        rows = []
        for tname in candidates:
            if tname not in tables:
                continue
            if not has_column(tables[tname], "location_id"):
                continue
            r = _ref_integrity(
                tables[tname], tname, "location_id",
                loc_df, "location_id",
                self.check_id, self.check_name, config, spark,
            )
            if r is not None:
                rows.append(r)

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# XTB-006  Event Within Observation Period
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class EventWithinObservationPeriodCheck(BaseCheck):
    check_id = "XTB-006"
    check_name = "Event Within Observation Period"
    description = (
        "Checks that each clinical event date falls within at least one "
        "observation period for that person."
    )
    severity = "WARN"

    def is_applicable(self, tables, config):
        return "observation_period" in tables

    def run(self, tables, config, spark):
        op = tables["observation_period"]
        needed_op = ["person_id", "observation_period_start_date", "observation_period_end_date"]
        if not all(has_column(op, c) for c in needed_op):
            return rows_to_df(spark, [])

        op_bc = broadcast_if_small(
            op.select(*needed_op),
            config.get("broadcast_threshold_rows", 5_000_000),
        )

        clinical = config.get("clinical_tables", [])
        table_cfgs = config.get("tables", {})
        rows = []

        for tname in clinical:
            if tname not in tables or tname == "observation_period":
                continue
            tcfg = table_cfgs.get(tname, {})
            date_col = tcfg.get("event_date_column")
            if not date_col:
                continue
            df = tables[tname]
            if not (has_column(df, "person_id") and has_column(df, date_col)):
                continue

            events = df.select(
                "person_id", F.col(date_col).alias("_evt_date")
            ).filter(F.col("_evt_date").isNotNull())

            total = events.count()
            if total == 0:
                continue

            # Left-anti join: events NOT covered by ANY observation period
            outside = events.alias("e").join(
                op_bc.alias("op"),
                (F.col("e.person_id") == F.col("op.person_id"))
                & (F.col("e._evt_date") >= F.col("op.observation_period_start_date"))
                & (F.col("e._evt_date") <= F.col("op.observation_period_end_date")),
                how="left_anti",
            )
            n_outside = outside.count()
            rate = n_outside / total

            warn_thr = 0.05
            level = classify_level(rate, warn_thr, 0.25)
            passed = level == "INFO"

            rows.append(
                make_result_row(
                    check_id=self.check_id,
                    check_name=self.check_name,
                    table_name=tname,
                    column_name=date_col,
                    level=level,
                    metric_name="event_outside_obs_period_rate",
                    metric_value=rate,
                    threshold=warn_thr,
                    passed=passed,
                    n_affected=n_outside,
                    query_or_logic=f"{tname}.{date_col} not within any observation_period",
                )
            )

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# XTB-007  Event After Birth Date
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class EventAfterBirthCheck(BaseCheck):
    check_id = "XTB-007"
    check_name = "Event After Birth Date"
    description = "Ensures clinical event dates are on or after the person's birth date."
    severity = "ERROR"

    def is_applicable(self, tables, config):
        return "person" in tables

    def run(self, tables, config, spark):
        person = tables["person"]

        # Derive birth_date
        if has_column(person, "birth_datetime"):
            person_dates = person.select(
                "person_id",
                F.col("birth_datetime").cast("date").alias("_birth_date"),
            )
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

        person_dates = person_dates.filter(F.col("_birth_date").isNotNull())
        person_bc = broadcast_if_small(
            person_dates,
            config.get("broadcast_threshold_rows", 5_000_000),
        )

        clinical = config.get("clinical_tables", [])
        table_cfgs = config.get("tables", {})
        rows = []

        for tname in clinical:
            if tname not in tables or tname in ("person", "death"):
                continue
            tcfg = table_cfgs.get(tname, {})
            date_col = tcfg.get("event_date_column")
            if not date_col:
                continue
            df = tables[tname]
            if not (has_column(df, "person_id") and has_column(df, date_col)):
                continue

            joined = df.select("person_id", date_col).filter(
                F.col(date_col).isNotNull()
            ).join(person_bc, on="person_id", how="inner")

            total = joined.count()
            if total == 0:
                continue

            before_birth = joined.filter(F.col(date_col) < F.col("_birth_date"))
            n_bad = before_birth.count()
            rate = n_bad / total
            passed = n_bad == 0

            rows.append(
                make_result_row(
                    check_id=self.check_id,
                    check_name=self.check_name,
                    table_name=tname,
                    column_name=date_col,
                    level="ERROR" if not passed else "INFO",
                    metric_name="event_before_birth_rate",
                    metric_value=rate,
                    threshold=0.0,
                    passed=passed,
                    n_affected=n_bad,
                    query_or_logic=f"{tname}.{date_col} < person.birth_date",
                )
            )

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# XTB-008  Event Before Death Date
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class EventBeforeDeathCheck(BaseCheck):
    check_id = "XTB-008"
    check_name = "Event Before Death Date"
    description = "Ensures clinical event dates are on or before the person's death date."
    severity = "WARN"

    def is_applicable(self, tables, config):
        return "death" in tables

    def run(self, tables, config, spark):
        death = tables["death"]
        if not has_column(death, "death_date"):
            return rows_to_df(spark, [])

        death_dates = broadcast_if_small(
            death.select("person_id", "death_date").filter(
                F.col("death_date").isNotNull()
            ),
            config.get("broadcast_threshold_rows", 5_000_000),
        )

        clinical = config.get("clinical_tables", [])
        table_cfgs = config.get("tables", {})
        rows = []

        for tname in clinical:
            if tname not in tables or tname == "death":
                continue
            tcfg = table_cfgs.get(tname, {})
            date_col = tcfg.get("event_date_column")
            if not date_col:
                continue
            df = tables[tname]
            if not (has_column(df, "person_id") and has_column(df, date_col)):
                continue

            joined = df.select("person_id", date_col).filter(
                F.col(date_col).isNotNull()
            ).join(death_dates, on="person_id", how="inner")

            total = joined.count()
            if total == 0:
                continue

            after_death = joined.filter(F.col(date_col) > F.col("death_date"))
            n_bad = after_death.count()
            rate = n_bad / total
            passed = n_bad == 0

            rows.append(
                make_result_row(
                    check_id=self.check_id,
                    check_name=self.check_name,
                    table_name=tname,
                    column_name=date_col,
                    level="WARN" if not passed else "INFO",
                    metric_name="event_after_death_rate",
                    metric_value=rate,
                    threshold=0.0,
                    passed=passed,
                    n_affected=n_bad,
                    query_or_logic=f"{tname}.{date_col} > death.death_date",
                )
            )

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# XTB-009  Events-Per-Person Distribution
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class EventsPerPersonDistributionCheck(BaseCheck):
    check_id = "XTB-009"
    check_name = "Events Per Person Distribution"
    description = (
        "Reports distribution statistics (min, max, median, p95, p99) of "
        "events per person for major clinical tables.  Uses approxQuantile."
    )
    severity = "INFO"

    _TARGET_TABLES = [
        "visit_occurrence",
        "condition_occurrence",
        "drug_exposure",
        "procedure_occurrence",
        "measurement",
        "observation",
    ]

    def run(self, tables, config, spark):
        rows = []

        for tname in self._TARGET_TABLES:
            if tname not in tables:
                continue
            df = tables[tname]
            if not has_column(df, "person_id"):
                continue

            epp = df.groupBy("person_id").count()
            total_persons = epp.count()
            if total_persons == 0:
                continue

            stats = epp.agg(
                F.min("count").alias("min_epp"),
                F.max("count").alias("max_epp"),
                F.mean("count").alias("mean_epp"),
            ).collect()[0]

            quantiles = epp.approxQuantile("count", [0.5, 0.95, 0.99], 0.01)
            median_epp = quantiles[0] if len(quantiles) > 0 else None
            p95_epp = quantiles[1] if len(quantiles) > 1 else None
            p99_epp = quantiles[2] if len(quantiles) > 2 else None

            # Flag if max is extremely high relative to p99 (possible data issue)
            max_val = stats["max_epp"]
            passed = True
            level = "INFO"
            if p99_epp and max_val > p99_epp * 10 and max_val > 1000:
                level = "WARN"
                passed = False

            import json
            dist_info = json.dumps({
                "min": stats["min_epp"],
                "max": max_val,
                "mean": round(stats["mean_epp"], 2),
                "median": median_epp,
                "p95": p95_epp,
                "p99": p99_epp,
                "n_persons": total_persons,
            })

            rows.append(
                make_result_row(
                    check_id=self.check_id,
                    check_name=self.check_name,
                    table_name=tname,
                    column_name="person_id",
                    level=level,
                    metric_name="events_per_person_median",
                    metric_value=median_epp,
                    passed=passed,
                    n_affected=total_persons,
                    example_values=dist_info,
                    query_or_logic=f"GROUP BY person_id, COUNT(*) distribution in {tname}",
                )
            )

        return rows_to_df(spark, rows)
