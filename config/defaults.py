"""Default configuration for OMOP CDM v5.4 QC framework.

This module encodes table metadata (primary keys, required columns, date
columns, concept-ID columns, expected domains) along with global thresholds
and measurement plausibility ranges.  Override any value at runtime by
passing an *overrides* dict to ``load_config()``.
"""

DEFAULT_CONFIG = {
    # ── Global settings ────────────────────────────────────────────────
    "cdm_version": "5.4",
    "max_future_date": "2026-12-31",
    "min_birth_year": 1900,
    "max_birth_year": 2026,
    "min_plausible_date": "1900-01-01",
    "example_value_limit": 5,
    # Null-rate thresholds (fraction of rows)
    "null_rate_warn_threshold": 0.05,
    "null_rate_error_threshold": 0.50,
    # Duplicate PK threshold (absolute count)
    "duplicate_pk_threshold": 0,
    # Concept-ID = 0 (unmapped) rate thresholds
    "zero_concept_id_warn_threshold": 0.10,
    "zero_concept_id_error_threshold": 0.50,
    # Missing source_value thresholds
    "missing_source_value_warn_threshold": 0.20,
    "missing_source_value_error_threshold": 0.80,
    # Referential-integrity orphan-rate thresholds
    "orphan_rate_warn_threshold": 0.001,
    "orphan_rate_error_threshold": 0.05,
    # Broadcast join row-count ceiling
    "broadcast_threshold_rows": 5_000_000,
    # Delta output path (None = do not persist)
    "delta_output_path": None,
    # ── Per-table metadata ─────────────────────────────────────────────
    # Keys: primary_key, required_columns, date_columns,
    #        start_end_date_pairs, concept_id_columns (col -> expected domain),
    #        source_value_columns, event_date_column
    "tables": {
        "person": {
            "primary_key": "person_id",
            "required_columns": [
                "person_id",
                "gender_concept_id",
                "year_of_birth",
                "race_concept_id",
                "ethnicity_concept_id",
            ],
            "date_columns": ["birth_datetime"],
            "start_end_date_pairs": [],
            "concept_id_columns": {
                "gender_concept_id": "Gender",
                "race_concept_id": "Race",
                "ethnicity_concept_id": "Ethnicity",
            },
            "source_value_columns": [
                "gender_source_value",
                "race_source_value",
                "ethnicity_source_value",
            ],
            "event_date_column": None,
        },
        "observation_period": {
            "primary_key": "observation_period_id",
            "required_columns": [
                "observation_period_id",
                "person_id",
                "observation_period_start_date",
                "observation_period_end_date",
                "period_type_concept_id",
            ],
            "date_columns": [
                "observation_period_start_date",
                "observation_period_end_date",
            ],
            "start_end_date_pairs": [
                ("observation_period_start_date", "observation_period_end_date")
            ],
            "concept_id_columns": {"period_type_concept_id": "Type Concept"},
            "source_value_columns": [],
            "event_date_column": "observation_period_start_date",
        },
        "visit_occurrence": {
            "primary_key": "visit_occurrence_id",
            "required_columns": [
                "visit_occurrence_id",
                "person_id",
                "visit_concept_id",
                "visit_start_date",
                "visit_type_concept_id",
            ],
            "date_columns": ["visit_start_date", "visit_end_date"],
            "start_end_date_pairs": [("visit_start_date", "visit_end_date")],
            "concept_id_columns": {
                "visit_concept_id": "Visit",
                "visit_type_concept_id": "Type Concept",
            },
            "source_value_columns": ["visit_source_value"],
            "event_date_column": "visit_start_date",
        },
        "condition_occurrence": {
            "primary_key": "condition_occurrence_id",
            "required_columns": [
                "condition_occurrence_id",
                "person_id",
                "condition_concept_id",
                "condition_start_date",
                "condition_type_concept_id",
            ],
            "date_columns": ["condition_start_date", "condition_end_date"],
            "start_end_date_pairs": [
                ("condition_start_date", "condition_end_date")
            ],
            "concept_id_columns": {
                "condition_concept_id": "Condition",
                "condition_type_concept_id": "Type Concept",
                "condition_status_concept_id": "Condition Status",
            },
            "source_value_columns": ["condition_source_value"],
            "event_date_column": "condition_start_date",
        },
        "drug_exposure": {
            "primary_key": "drug_exposure_id",
            "required_columns": [
                "drug_exposure_id",
                "person_id",
                "drug_concept_id",
                "drug_exposure_start_date",
                "drug_type_concept_id",
            ],
            "date_columns": [
                "drug_exposure_start_date",
                "drug_exposure_end_date",
            ],
            "start_end_date_pairs": [
                ("drug_exposure_start_date", "drug_exposure_end_date")
            ],
            "concept_id_columns": {
                "drug_concept_id": "Drug",
                "drug_type_concept_id": "Type Concept",
                "route_concept_id": "Route",
            },
            "source_value_columns": ["drug_source_value"],
            "event_date_column": "drug_exposure_start_date",
        },
        "procedure_occurrence": {
            "primary_key": "procedure_occurrence_id",
            "required_columns": [
                "procedure_occurrence_id",
                "person_id",
                "procedure_concept_id",
                "procedure_date",
                "procedure_type_concept_id",
            ],
            "date_columns": ["procedure_date", "procedure_end_date"],
            "start_end_date_pairs": [("procedure_date", "procedure_end_date")],
            "concept_id_columns": {
                "procedure_concept_id": "Procedure",
                "procedure_type_concept_id": "Type Concept",
                "modifier_concept_id": "Modifier",
            },
            "source_value_columns": ["procedure_source_value"],
            "event_date_column": "procedure_date",
        },
        "measurement": {
            "primary_key": "measurement_id",
            "required_columns": [
                "measurement_id",
                "person_id",
                "measurement_concept_id",
                "measurement_date",
                "measurement_type_concept_id",
            ],
            "date_columns": ["measurement_date"],
            "start_end_date_pairs": [],
            "concept_id_columns": {
                "measurement_concept_id": "Measurement",
                "measurement_type_concept_id": "Type Concept",
                "unit_concept_id": "Unit",
                "operator_concept_id": "Meas Operator",
                "value_as_concept_id": "Meas Value",
            },
            "source_value_columns": [
                "measurement_source_value",
                "unit_source_value",
                "value_source_value",
            ],
            "event_date_column": "measurement_date",
        },
        "observation": {
            "primary_key": "observation_id",
            "required_columns": [
                "observation_id",
                "person_id",
                "observation_concept_id",
                "observation_date",
                "observation_type_concept_id",
            ],
            "date_columns": ["observation_date"],
            "start_end_date_pairs": [],
            "concept_id_columns": {
                "observation_concept_id": "Observation",
                "observation_type_concept_id": "Type Concept",
                "unit_concept_id": "Unit",
                "value_as_concept_id": "Obs Value",
                "qualifier_concept_id": "Qualifier",
            },
            "source_value_columns": [
                "observation_source_value",
                "unit_source_value",
                "value_source_value",
            ],
            "event_date_column": "observation_date",
        },
        "death": {
            "primary_key": None,
            "required_columns": [
                "person_id",
                "death_date",
                "death_type_concept_id",
            ],
            "date_columns": ["death_date"],
            "start_end_date_pairs": [],
            "concept_id_columns": {
                "death_type_concept_id": "Type Concept",
                "cause_concept_id": "Condition",
            },
            "source_value_columns": ["cause_source_value"],
            "event_date_column": "death_date",
        },
        "device_exposure": {
            "primary_key": "device_exposure_id",
            "required_columns": [
                "device_exposure_id",
                "person_id",
                "device_concept_id",
                "device_exposure_start_date",
                "device_type_concept_id",
            ],
            "date_columns": [
                "device_exposure_start_date",
                "device_exposure_end_date",
            ],
            "start_end_date_pairs": [
                ("device_exposure_start_date", "device_exposure_end_date")
            ],
            "concept_id_columns": {
                "device_concept_id": "Device",
                "device_type_concept_id": "Type Concept",
            },
            "source_value_columns": ["device_source_value"],
            "event_date_column": "device_exposure_start_date",
        },
        "specimen": {
            "primary_key": "specimen_id",
            "required_columns": [
                "specimen_id",
                "person_id",
                "specimen_concept_id",
                "specimen_type_concept_id",
                "specimen_date",
            ],
            "date_columns": ["specimen_date"],
            "start_end_date_pairs": [],
            "concept_id_columns": {
                "specimen_concept_id": "Specimen",
                "specimen_type_concept_id": "Type Concept",
                "unit_concept_id": "Unit",
                "anatomic_site_concept_id": "Spec Anatomic Site",
                "disease_status_concept_id": "Disease Status",
            },
            "source_value_columns": ["specimen_source_value"],
            "event_date_column": "specimen_date",
        },
        "note": {
            "primary_key": "note_id",
            "required_columns": [
                "note_id",
                "person_id",
                "note_date",
                "note_type_concept_id",
                "note_class_concept_id",
                "note_text",
                "encoding_concept_id",
                "language_concept_id",
            ],
            "date_columns": ["note_date"],
            "start_end_date_pairs": [],
            "concept_id_columns": {
                "note_type_concept_id": "Type Concept",
                "note_class_concept_id": "Note Class",
                "encoding_concept_id": "Note Encoding",
                "language_concept_id": "Language",
            },
            "source_value_columns": ["note_source_value"],
            "event_date_column": "note_date",
        },
        "payer_plan_period": {
            "primary_key": "payer_plan_period_id",
            "required_columns": [
                "payer_plan_period_id",
                "person_id",
                "payer_plan_period_start_date",
                "payer_plan_period_end_date",
            ],
            "date_columns": [
                "payer_plan_period_start_date",
                "payer_plan_period_end_date",
            ],
            "start_end_date_pairs": [
                ("payer_plan_period_start_date", "payer_plan_period_end_date")
            ],
            "concept_id_columns": {
                "payer_concept_id": "Payer",
                "plan_concept_id": "Plan",
            },
            "source_value_columns": ["payer_source_value", "plan_source_value"],
            "event_date_column": "payer_plan_period_start_date",
        },
        "care_site": {
            "primary_key": "care_site_id",
            "required_columns": ["care_site_id"],
            "date_columns": [],
            "start_end_date_pairs": [],
            "concept_id_columns": {
                "place_of_service_concept_id": "Place of Service"
            },
            "source_value_columns": ["care_site_source_value"],
            "event_date_column": None,
        },
        "provider": {
            "primary_key": "provider_id",
            "required_columns": ["provider_id"],
            "date_columns": [],
            "start_end_date_pairs": [],
            "concept_id_columns": {
                "specialty_concept_id": "Specialty",
                "gender_concept_id": "Gender",
            },
            "source_value_columns": [
                "provider_source_value",
                "specialty_source_value",
            ],
            "event_date_column": None,
        },
        "location": {
            "primary_key": "location_id",
            "required_columns": ["location_id"],
            "date_columns": [],
            "start_end_date_pairs": [],
            "concept_id_columns": {},
            "source_value_columns": ["location_source_value"],
            "event_date_column": None,
        },
        "cost": {
            "primary_key": "cost_id",
            "required_columns": [
                "cost_id",
                "cost_event_id",
                "cost_domain_id",
                "cost_type_concept_id",
            ],
            "date_columns": [],
            "start_end_date_pairs": [],
            "concept_id_columns": {
                "cost_type_concept_id": "Type Concept",
                "currency_concept_id": "Currency",
            },
            "source_value_columns": [],
            "event_date_column": None,
        },
    },
    # ── Clinical event tables (have person_id, not dimension tables) ───
    "clinical_tables": [
        "visit_occurrence",
        "condition_occurrence",
        "drug_exposure",
        "procedure_occurrence",
        "measurement",
        "observation",
        "death",
        "device_exposure",
        "specimen",
        "note",
        "payer_plan_period",
        "observation_period",
    ],
    # ── Tables carrying a visit_occurrence_id FK ───────────────────────
    "tables_with_visit_id": [
        "condition_occurrence",
        "drug_exposure",
        "procedure_occurrence",
        "measurement",
        "observation",
        "device_exposure",
        "note",
    ],
    # ── Measurement plausibility ranges ────────────────────────────────
    # concept_id -> {min, max, unit_concept_id (optional)}
    "measurement_ranges": {
        3020891: {"min": 25.0, "max": 45.0, "unit_concept_id": 586323},   # Body temp (C)
        3004249: {"min": 40.0, "max": 300.0, "unit_concept_id": 8876},    # Systolic BP
        3012888: {"min": 20.0, "max": 200.0, "unit_concept_id": 8876},    # Diastolic BP
        3027018: {"min": 10.0, "max": 300.0, "unit_concept_id": 8541},    # Heart rate
        3038553: {"min": 5.0, "max": 100.0, "unit_concept_id": 9531},     # BMI
        3025315: {"min": 0.5, "max": 300.0, "unit_concept_id": 9529},     # Weight (kg)
        3036277: {"min": 20.0, "max": 280.0, "unit_concept_id": 8582},    # Height (cm)
        3004501: {"min": 0.0, "max": 1500.0, "unit_concept_id": 8840},    # Glucose
        3000963: {"min": 0.0, "max": 25.0, "unit_concept_id": 8713},      # Hemoglobin
        3016723: {"min": 0.0, "max": 30.0, "unit_concept_id": 8840},      # Creatinine
    },
    # ── Expected concept domains by concept_id column ──────────────────
    "expected_domains": {
        "condition_concept_id": "Condition",
        "drug_concept_id": "Drug",
        "procedure_concept_id": "Procedure",
        "measurement_concept_id": "Measurement",
        "observation_concept_id": "Observation",
        "device_concept_id": "Device",
        "specimen_concept_id": "Specimen",
        "visit_concept_id": "Visit",
    },
    # ── Vocabulary table names ─────────────────────────────────────────
    "vocab_tables": [
        "concept",
        "concept_relationship",
        "concept_ancestor",
        "vocabulary",
        "domain",
        "concept_class",
        "relationship",
        "drug_strength",
    ],
}
