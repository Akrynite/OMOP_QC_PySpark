"""Synthetic OMOP data for end-to-end QC framework demonstration.

Creates small DataFrames with deliberate quality issues so that the QC
checks produce a mix of PASS / WARN / ERROR results.
"""

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
    DateType,
    DoubleType,
)


def build_synthetic_tables(spark: SparkSession) -> dict:
    """Return a ``tables`` dict of small synthetic OMOP DataFrames.

    Deliberate issues seeded for testing:
    - Person 999: implausible birth year (1850), missing gender
    - Visit 3: end_date < start_date
    - Condition 4: concept_id = 0 (unmapped), orphan person_id 888
    - Drug 3: future start date (2099)
    - Measurement 5: extreme value_as_number (99999)
    - Observation period: person 2 has overlapping periods
    - Death: person 5 death_date before birth; person 3 duplicate death
    """
    tables = {}

    # ── PERSON ──────────────────────────────────────────────────────────
    person_schema = StructType(
        [
            StructField("person_id", LongType()),
            StructField("gender_concept_id", LongType()),
            StructField("year_of_birth", IntegerType()),
            StructField("month_of_birth", IntegerType()),
            StructField("day_of_birth", IntegerType()),
            StructField("birth_datetime", DateType()),
            StructField("race_concept_id", LongType()),
            StructField("ethnicity_concept_id", LongType()),
            StructField("location_id", LongType()),
            StructField("provider_id", LongType()),
            StructField("care_site_id", LongType()),
            StructField("gender_source_value", StringType()),
            StructField("race_source_value", StringType()),
            StructField("ethnicity_source_value", StringType()),
        ]
    )
    tables["person"] = spark.createDataFrame(
        [
            (1, 8507, 1980, 5, 15, date(1980, 5, 15), 8527, 38003563, 1, 1, 1, "M", "White", "Not Hispanic"),
            (2, 8532, 1990, 3, 20, date(1990, 3, 20), 8527, 38003563, 1, 1, 1, "F", "White", "Not Hispanic"),
            (3, 8507, 1975, 8, 10, date(1975, 8, 10), 8516, 38003564, 1, 2, 1, "M", "Black", "Not Hispanic"),
            (4, 8532, 2000, 1, 1, date(2000, 1, 1), 8527, 38003563, 2, 1, 2, "F", "White", "Hispanic"),
            (5, 8507, 1960, 12, 25, date(1960, 12, 25), 8527, 38003563, 1, 1, 1, "M", "White", "Not Hispanic"),
            # Implausible birth year + missing gender
            (999, 0, 1850, None, None, None, 0, 0, None, None, None, None, None, None),
        ],
        schema=person_schema,
    )

    # ── OBSERVATION_PERIOD ──────────────────────────────────────────────
    op_schema = StructType(
        [
            StructField("observation_period_id", LongType()),
            StructField("person_id", LongType()),
            StructField("observation_period_start_date", DateType()),
            StructField("observation_period_end_date", DateType()),
            StructField("period_type_concept_id", LongType()),
        ]
    )
    tables["observation_period"] = spark.createDataFrame(
        [
            (1, 1, date(2010, 1, 1), date(2023, 12, 31), 44814724),
            (2, 2, date(2015, 1, 1), date(2022, 6, 30), 44814724),
            # Overlapping period for person 2
            (3, 2, date(2020, 1, 1), date(2024, 12, 31), 44814724),
            (4, 3, date(2005, 1, 1), date(2023, 12, 31), 44814724),
            (5, 4, date(2018, 1, 1), date(2024, 12, 31), 44814724),
            (6, 5, date(2000, 1, 1), date(2023, 12, 31), 44814724),
        ],
        schema=op_schema,
    )

    # ── VISIT_OCCURRENCE ────────────────────────────────────────────────
    visit_schema = StructType(
        [
            StructField("visit_occurrence_id", LongType()),
            StructField("person_id", LongType()),
            StructField("visit_concept_id", LongType()),
            StructField("visit_start_date", DateType()),
            StructField("visit_end_date", DateType()),
            StructField("visit_type_concept_id", LongType()),
            StructField("provider_id", LongType()),
            StructField("care_site_id", LongType()),
            StructField("visit_source_value", StringType()),
        ]
    )
    tables["visit_occurrence"] = spark.createDataFrame(
        [
            (1, 1, 9201, date(2020, 3, 1), date(2020, 3, 5), 44818517, 1, 1, "IP"),
            (2, 2, 9202, date(2021, 6, 15), date(2021, 6, 15), 44818517, 1, 1, "OP"),
            # end_date < start_date
            (3, 3, 9201, date(2022, 1, 10), date(2021, 12, 20), 44818517, 2, 1, "IP"),
            (4, 4, 9202, date(2023, 4, 1), None, 44818517, 1, 2, "OP"),  # missing end_date
            (5, 5, 9202, date(2022, 8, 20), date(2022, 8, 20), 44818517, 1, 1, "OP"),
        ],
        schema=visit_schema,
    )

    # ── CONDITION_OCCURRENCE ────────────────────────────────────────────
    cond_schema = StructType(
        [
            StructField("condition_occurrence_id", LongType()),
            StructField("person_id", LongType()),
            StructField("condition_concept_id", LongType()),
            StructField("condition_start_date", DateType()),
            StructField("condition_end_date", DateType()),
            StructField("condition_type_concept_id", LongType()),
            StructField("visit_occurrence_id", LongType()),
            StructField("condition_source_value", StringType()),
        ]
    )
    tables["condition_occurrence"] = spark.createDataFrame(
        [
            (1, 1, 201826, date(2020, 3, 2), date(2020, 3, 5), 32817, 1, "E11.9"),
            (2, 2, 4329847, date(2021, 6, 15), date(2021, 7, 15), 32817, 2, "J06.9"),
            (3, 3, 313217, date(2022, 1, 10), None, 32817, 3, "I10"),
            # Zero concept_id + orphan person_id (888 not in person)
            (4, 888, 0, date(2022, 5, 1), None, 32817, None, ""),
            (5, 4, 4185711, date(2023, 4, 1), date(2023, 4, 10), 32817, 4, "M54.5"),
        ],
        schema=cond_schema,
    )

    # ── DRUG_EXPOSURE ───────────────────────────────────────────────────
    drug_schema = StructType(
        [
            StructField("drug_exposure_id", LongType()),
            StructField("person_id", LongType()),
            StructField("drug_concept_id", LongType()),
            StructField("drug_exposure_start_date", DateType()),
            StructField("drug_exposure_end_date", DateType()),
            StructField("drug_type_concept_id", LongType()),
            StructField("visit_occurrence_id", LongType()),
            StructField("drug_source_value", StringType()),
        ]
    )
    tables["drug_exposure"] = spark.createDataFrame(
        [
            (1, 1, 1127078, date(2020, 3, 2), date(2020, 3, 10), 32817, 1, "metformin"),
            (2, 2, 1154343, date(2021, 6, 15), date(2021, 6, 22), 32817, 2, "amoxicillin"),
            # Future start date
            (3, 3, 1118084, date(2099, 1, 1), date(2099, 1, 14), 32817, 3, "atorvastatin"),
            (4, 4, 1125315, date(2023, 4, 1), date(2023, 4, 7), 32817, 4, "ibuprofen"),
        ],
        schema=drug_schema,
    )

    # ── PROCEDURE_OCCURRENCE ────────────────────────────────────────────
    proc_schema = StructType(
        [
            StructField("procedure_occurrence_id", LongType()),
            StructField("person_id", LongType()),
            StructField("procedure_concept_id", LongType()),
            StructField("procedure_date", DateType()),
            StructField("procedure_end_date", DateType()),
            StructField("procedure_type_concept_id", LongType()),
            StructField("visit_occurrence_id", LongType()),
            StructField("procedure_source_value", StringType()),
        ]
    )
    tables["procedure_occurrence"] = spark.createDataFrame(
        [
            (1, 1, 2213572, date(2020, 3, 3), date(2020, 3, 3), 32817, 1, "99213"),
            (2, 3, 2213473, date(2022, 1, 11), None, 32817, 3, "99232"),
        ],
        schema=proc_schema,
    )

    # ── MEASUREMENT ─────────────────────────────────────────────────────
    meas_schema = StructType(
        [
            StructField("measurement_id", LongType()),
            StructField("person_id", LongType()),
            StructField("measurement_concept_id", LongType()),
            StructField("measurement_date", DateType()),
            StructField("measurement_type_concept_id", LongType()),
            StructField("value_as_number", DoubleType()),
            StructField("unit_concept_id", LongType()),
            StructField("visit_occurrence_id", LongType()),
            StructField("measurement_source_value", StringType()),
            StructField("unit_source_value", StringType()),
            StructField("value_source_value", StringType()),
        ]
    )
    tables["measurement"] = spark.createDataFrame(
        [
            # Normal systolic BP
            (1, 1, 3004249, date(2020, 3, 2), 32817, 120.0, 8876, 1, "SBP", "mmHg", "120"),
            # Normal diastolic BP
            (2, 1, 3012888, date(2020, 3, 2), 32817, 80.0, 8876, 1, "DBP", "mmHg", "80"),
            # Normal body temp
            (3, 2, 3020891, date(2021, 6, 15), 32817, 37.1, 586323, 2, "Temp", "Cel", "37.1"),
            # Normal heart rate
            (4, 3, 3027018, date(2022, 1, 10), 32817, 72.0, 8541, 3, "HR", "bpm", "72"),
            # Extreme outlier value (implausible)
            (5, 4, 3004249, date(2023, 4, 1), 32817, 99999.0, 8876, 4, "SBP", "mmHg", "99999"),
            # Normal BMI
            (6, 5, 3038553, date(2022, 8, 20), 32817, 24.5, 9531, 5, "BMI", "kg/m2", "24.5"),
        ],
        schema=meas_schema,
    )

    # ── OBSERVATION ─────────────────────────────────────────────────────
    obs_schema = StructType(
        [
            StructField("observation_id", LongType()),
            StructField("person_id", LongType()),
            StructField("observation_concept_id", LongType()),
            StructField("observation_date", DateType()),
            StructField("observation_type_concept_id", LongType()),
            StructField("visit_occurrence_id", LongType()),
            StructField("observation_source_value", StringType()),
        ]
    )
    tables["observation"] = spark.createDataFrame(
        [
            (1, 1, 4058131, date(2020, 3, 2), 32817, 1, "Tobacco use"),
            (2, 2, 4058131, date(2021, 6, 15), 32817, 2, "Tobacco use"),
        ],
        schema=obs_schema,
    )

    # ── DEATH ───────────────────────────────────────────────────────────
    death_schema = StructType(
        [
            StructField("person_id", LongType()),
            StructField("death_date", DateType()),
            StructField("death_type_concept_id", LongType()),
            StructField("cause_source_value", StringType()),
        ]
    )
    tables["death"] = spark.createDataFrame(
        [
            # Death before birth (person 5 born 1960, death 1950)
            (5, date(1950, 6, 1), 32817, "cardiac arrest"),
            # Duplicate death record for person 3
            (3, date(2024, 1, 15), 32817, "respiratory failure"),
            (3, date(2024, 2, 20), 32817, "respiratory failure"),
        ],
        schema=death_schema,
    )

    # ── CARE_SITE ───────────────────────────────────────────────────────
    cs_schema = StructType(
        [
            StructField("care_site_id", LongType()),
            StructField("care_site_name", StringType()),
            StructField("place_of_service_concept_id", LongType()),
            StructField("location_id", LongType()),
            StructField("care_site_source_value", StringType()),
        ]
    )
    tables["care_site"] = spark.createDataFrame(
        [
            (1, "General Hospital", 8717, 1, "GH-001"),
            (2, "City Clinic", 8756, 2, "CC-001"),
        ],
        schema=cs_schema,
    )

    # ── PROVIDER ────────────────────────────────────────────────────────
    prov_schema = StructType(
        [
            StructField("provider_id", LongType()),
            StructField("provider_name", StringType()),
            StructField("specialty_concept_id", LongType()),
            StructField("care_site_id", LongType()),
            StructField("provider_source_value", StringType()),
            StructField("specialty_source_value", StringType()),
        ]
    )
    tables["provider"] = spark.createDataFrame(
        [
            (1, "Dr. Smith", 38004446, 1, "NPI-001", "Internal Medicine"),
            (2, "Dr. Jones", 38004459, 1, "NPI-002", "Cardiology"),
        ],
        schema=prov_schema,
    )

    # ── LOCATION ────────────────────────────────────────────────────────
    loc_schema = StructType(
        [
            StructField("location_id", LongType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("zip", StringType()),
            StructField("location_source_value", StringType()),
        ]
    )
    tables["location"] = spark.createDataFrame(
        [
            (1, "Boston", "MA", "02115", "LOC-001"),
            (2, "Cambridge", "MA", "02139", "LOC-002"),
        ],
        schema=loc_schema,
    )

    return tables


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys, os

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Ensure PySpark workers use the same Python interpreter (avoids
    # Windows Store alias / wrong-python issues).
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("omop-qc-demo")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    tables = build_synthetic_tables(spark)

    from config.loader import load_config
    from qc.runner import run_qc
    from reporting.summary import format_summary_text

    config = load_config()
    results_df, summary_df = run_qc(tables, config=config, spark=spark)

    print("\n\n" + format_summary_text(summary_df))
    print("\n--- Failed checks ---")
    results_df.filter(~results_df.passed).select(
        "check_id", "check_name", "table_name", "column_name",
        "level", "metric_name", "metric_value", "n_affected",
    ).show(50, truncate=False)

    spark.stop()
