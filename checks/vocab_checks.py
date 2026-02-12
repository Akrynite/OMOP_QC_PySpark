"""Vocabulary-based QC checks (VOC-001 … VOC-002).

These checks validate concept usage against the OMOP vocabulary tables.
They gracefully skip when the ``concept`` table is not available.
"""

import json
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from qc.base import BaseCheck, make_result_row, rows_to_df
from qc.registry import register_check
from qc.utils import has_column, broadcast_if_small, classify_level


# ═══════════════════════════════════════════════════════════════════════════
# VOC-001  Concept Domain Validation
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class ConceptDomainValidationCheck(BaseCheck):
    check_id = "VOC-001"
    check_name = "Concept Domain Validation"
    description = (
        "Validates that the primary concept_id column in each clinical table "
        "maps to the expected OMOP domain (e.g. condition_concept_id -> "
        "'Condition').  Requires the 'concept' vocabulary table."
    )
    severity = "WARN"

    def is_applicable(self, tables, config):
        return "concept" in tables

    def run(self, tables, config, spark):
        concept = tables["concept"]
        if not (has_column(concept, "concept_id") and has_column(concept, "domain_id")):
            return rows_to_df(spark, [])

        concept_domain = broadcast_if_small(
            concept.select("concept_id", "domain_id").distinct(),
            config.get("broadcast_threshold_rows", 5_000_000),
        )

        expected_domains = config.get("expected_domains", {})
        rows = []

        for concept_col, expected_domain in expected_domains.items():
            # Find which table(s) have this concept column
            for tname, df in tables.items():
                if tname in config.get("vocab_tables", []):
                    continue
                if not has_column(df, concept_col):
                    continue

                # Get distinct non-zero concept_ids from the clinical table
                cids = (
                    df.select(concept_col)
                    .filter(
                        F.col(concept_col).isNotNull()
                        & (F.col(concept_col) != 0)
                    )
                    .distinct()
                )
                total = cids.count()
                if total == 0:
                    continue

                # Join with concept to get domain_id
                joined = cids.join(
                    concept_domain,
                    cids[concept_col] == concept_domain["concept_id"],
                    how="left",
                )

                # Concepts with wrong domain
                wrong = joined.filter(
                    F.col("domain_id").isNotNull()
                    & (F.col("domain_id") != expected_domain)
                )
                n_wrong = wrong.count()

                # Concepts not found in vocabulary at all
                unmapped = joined.filter(F.col("domain_id").isNull())
                n_unmapped = unmapped.count()

                rate = n_wrong / total
                passed = n_wrong == 0

                ex = None
                if not passed:
                    sample = (
                        wrong.groupBy("domain_id")
                        .count()
                        .orderBy(F.desc("count"))
                        .limit(5)
                        .collect()
                    )
                    ex = json.dumps(
                        [{"domain": r["domain_id"], "n": r["count"]} for r in sample]
                    )

                rows.append(
                    make_result_row(
                        check_id=self.check_id,
                        check_name=self.check_name,
                        table_name=tname,
                        column_name=concept_col,
                        level="WARN" if not passed else "INFO",
                        metric_name="wrong_domain_concept_rate",
                        metric_value=rate,
                        threshold=0.0,
                        passed=passed,
                        n_affected=n_wrong,
                        example_values=ex,
                        query_or_logic=(
                            f"{tname}.{concept_col} -> concept.domain_id "
                            f"!= '{expected_domain}'"
                        ),
                    )
                )

                # Also report unmapped rate as a separate INFO row
                if n_unmapped > 0:
                    rows.append(
                        make_result_row(
                            check_id=self.check_id,
                            check_name=self.check_name,
                            table_name=tname,
                            column_name=concept_col,
                            level="INFO",
                            metric_name="concept_not_in_vocabulary_count",
                            metric_value=n_unmapped,
                            passed=True,
                            n_affected=n_unmapped,
                            query_or_logic=(
                                f"{tname}.{concept_col} not found in concept table"
                            ),
                        )
                    )

        return rows_to_df(spark, rows)


# ═══════════════════════════════════════════════════════════════════════════
# VOC-002  Standard Concept Usage
# ═══════════════════════════════════════════════════════════════════════════
@register_check
class StandardConceptUsageCheck(BaseCheck):
    check_id = "VOC-002"
    check_name = "Standard Concept Usage"
    description = (
        "Reports the proportion of concept_ids that are Standard Concepts "
        "(standard_concept = 'S') vs non-standard or source concepts.  "
        "Requires the 'concept' vocabulary table."
    )
    severity = "WARN"

    def is_applicable(self, tables, config):
        return "concept" in tables

    def run(self, tables, config, spark):
        concept = tables["concept"]
        needed = ["concept_id", "standard_concept"]
        if not all(has_column(concept, c) for c in needed):
            return rows_to_df(spark, [])

        concept_std = broadcast_if_small(
            concept.select(
                "concept_id",
                F.when(F.col("standard_concept") == "S", True)
                .otherwise(False)
                .alias("_is_standard"),
            ).distinct(),
            config.get("broadcast_threshold_rows", 5_000_000),
        )

        expected_domains = config.get("expected_domains", {})
        rows = []

        for concept_col, _domain in expected_domains.items():
            for tname, df in tables.items():
                if tname in config.get("vocab_tables", []):
                    continue
                if not has_column(df, concept_col):
                    continue

                cids = (
                    df.select(concept_col)
                    .filter(
                        F.col(concept_col).isNotNull()
                        & (F.col(concept_col) != 0)
                    )
                    .distinct()
                )
                total = cids.count()
                if total == 0:
                    continue

                joined = cids.join(
                    concept_std,
                    cids[concept_col] == concept_std["concept_id"],
                    how="left",
                )

                n_standard = joined.filter(F.col("_is_standard") == True).count()  # noqa: E712
                n_nonstandard = total - n_standard
                standard_rate = n_standard / total
                nonstandard_rate = n_nonstandard / total

                # A high non-standard rate is suspicious
                warn_thr = 0.10
                err_thr = 0.50
                level = classify_level(nonstandard_rate, warn_thr, err_thr)
                passed = level == "INFO"

                rows.append(
                    make_result_row(
                        check_id=self.check_id,
                        check_name=self.check_name,
                        table_name=tname,
                        column_name=concept_col,
                        level=level,
                        metric_name="non_standard_concept_rate",
                        metric_value=nonstandard_rate,
                        threshold=warn_thr,
                        passed=passed,
                        n_affected=n_nonstandard,
                        example_values=json.dumps({
                            "standard": n_standard,
                            "non_standard": n_nonstandard,
                            "standard_rate": round(standard_rate, 4),
                        }),
                        query_or_logic=(
                            f"Distinct {concept_col} in {tname} where "
                            f"concept.standard_concept != 'S'"
                        ),
                    )
                )

        return rows_to_df(spark, rows)
