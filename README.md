# OMOP QC PySpark

A PySpark-based data quality checking framework for [OMOP CDM v5.4](https://ohdsi.github.io/CommonDataModel/) datasets. Runs 27 configurable checks across table-level, cross-table, and vocabulary categories, producing structured results and summary reports.

Designed to work in **Databricks** (Spark Connect compatible) and **local PySpark** environments.

## Checks

### Table-Level (TBL-001 – TBL-016)

| ID | Name | Severity | Description |
|----|------|----------|-------------|
| TBL-001 | Row Count | INFO | Row count of each available table |
| TBL-002 | Required Column Null Rate | WARN | Null rate of required columns per table |
| TBL-003 | Duplicate Primary Key | ERROR | Duplicate values in primary key columns |
| TBL-004 | Start/End Date Logic | ERROR | end_date >= start_date for configured date pairs |
| TBL-005 | Future Date | WARN | Date values beyond configurable max date |
| TBL-006 | Birth Year Plausibility | ERROR | year_of_birth within plausible range (default 1900–2026) |
| TBL-007 | Gender Concept Presence | WARN | gender_concept_id populated and non-zero |
| TBL-008 | Missing Visit End Date | WARN | NULL visit_end_date rate |
| TBL-009 | Zero/Missing Concept ID | WARN | concept_id = 0 (unmapped) rate for clinical tables |
| TBL-010 | Missing Source Value | WARN | Null/empty source_value columns |
| TBL-011 | Measurement Value Plausibility | WARN | value_as_number against configured ranges per concept |
| TBL-012 | Observation Period Overlap | ERROR | Overlapping observation periods per person |
| TBL-013 | Death Date After Birth | ERROR | death_date on or after birth date |
| TBL-014 | Measurement Value Outliers | WARN | Extreme outliers via IQR method (3x IQR) |
| TBL-015 | Death Person Uniqueness | ERROR | Each person_id appears at most once in death |
| TBL-016 | Historical Date Range | WARN | Date values before minimum plausible date |

### Cross-Table (XTB-001 – XTB-009)

| ID | Name | Severity | Description |
|----|------|----------|-------------|
| XTB-001 | Person Referential Integrity | ERROR | person_id in clinical tables exists in person |
| XTB-002 | Visit Referential Integrity | ERROR | visit_occurrence_id exists in visit_occurrence |
| XTB-003 | Provider Referential Integrity | WARN | provider_id exists in provider |
| XTB-004 | Care Site Referential Integrity | WARN | care_site_id exists in care_site |
| XTB-005 | Location Referential Integrity | WARN | location_id exists in location |
| XTB-006 | Event Within Observation Period | WARN | Clinical events fall within an observation period |
| XTB-007 | Event After Birth Date | ERROR | Clinical events on or after birth date |
| XTB-008 | Event Before Death Date | WARN | Clinical events on or before death date |
| XTB-009 | Events Per Person Distribution | INFO | Distribution stats (median, p95, p99) per table |

### Vocabulary (VOC-001 – VOC-002)

| ID | Name | Severity | Description |
|----|------|----------|-------------|
| VOC-001 | Concept Domain Validation | WARN | concept_id maps to expected OMOP domain |
| VOC-002 | Standard Concept Usage | WARN | Proportion of standard vs non-standard concepts |

Vocabulary checks require a `concept` table to be present.

## Quick Start

### Databricks

```python
import sys
sys.path.insert(0, "/Workspace/Repos/<your-path>/OMOP_QC_PySpark")

from config.loader import load_config
from qc.runner import run_qc
from reporting.summary import format_summary_text

# Build tables dict from your catalog
tables = {
    "person": spark.table("your_catalog.omop.person"),
    "visit_occurrence": spark.table("your_catalog.omop.visit_occurrence"),
    "condition_occurrence": spark.table("your_catalog.omop.condition_occurrence"),
    # ... add all available OMOP tables
}

config = load_config()
results_df, summary_df = run_qc(tables, config=config, spark=spark)

print(format_summary_text(summary_df))

# Inspect failures
results_df.filter(~results_df.passed).orderBy("level", "table_name").display()
```

### Local PySpark

```bash
python examples/synthetic_demo.py
```

Runs the full suite against synthetic data with seeded quality issues.

## Configuration

Default config covers OMOP CDM v5.4 metadata. Override via Python dict or YAML:

```python
config = load_config(overrides={
    "max_future_date": "2025-12-31",
    "null_rate_warn_threshold": 0.10,
})
```

Key settings:

| Setting | Default | Description |
|---------|---------|-------------|
| `max_future_date` | 2026-12-31 | Max allowed date before flagging |
| `min_birth_year` / `max_birth_year` | 1900 / 2026 | Plausible birth year range |
| `null_rate_warn_threshold` | 0.05 | Null rate to trigger WARN |
| `null_rate_error_threshold` | 0.50 | Null rate to trigger ERROR |
| `zero_concept_id_warn_threshold` | 0.10 | Unmapped concept rate for WARN |
| `orphan_rate_error_threshold` | 0.05 | FK orphan rate for ERROR |
| `measurement_ranges` | 10 common labs | Per-concept min/max ranges |

## Result Schema

Every check emits rows with:

| Field | Type | Description |
|-------|------|-------------|
| `check_id` | String | e.g., "TBL-001" |
| `check_name` | String | e.g., "Row Count" |
| `table_name` | String | Target table |
| `column_name` | String | Target column (nullable) |
| `level` | String | INFO, WARN, or ERROR |
| `metric_name` | String | e.g., "null_rate" |
| `metric_value` | Double | Computed metric |
| `threshold` | Double | Threshold that determined level |
| `passed` | Boolean | Whether the check passed |
| `n_affected` | Long | Count of affected rows |
| `run_ts` | Timestamp | UTC execution timestamp |

## Project Structure

```
OMOP_QC_PySpark/
  checks/
    table_checks.py        # TBL-001 through TBL-016
    cross_table_checks.py  # XTB-001 through XTB-009
    vocab_checks.py        # VOC-001, VOC-002
  config/
    defaults.py            # Default OMOP CDM v5.4 config
    loader.py              # Config loading and merging
  qc/
    base.py                # BaseCheck class and result schema
    registry.py            # Check registration decorator
    runner.py              # Main run_qc() entry point
    utils.py               # Helpers (broadcast joins, column checks)
  reporting/
    summary.py             # Summary aggregation and text formatting
  examples/
    run_qc.py              # Databricks notebook template
    synthetic_demo.py      # Standalone demo with synthetic data
```
