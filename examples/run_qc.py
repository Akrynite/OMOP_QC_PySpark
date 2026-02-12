# Databricks notebook source
# MAGIC %md
# MAGIC # OMOP CDM Quality Control — Entrypoint Notebook
# MAGIC
# MAGIC This notebook runs the full QC suite against OMOP CDM tables loaded as
# MAGIC PySpark DataFrames.  Adjust the `tables` dict and optional config
# MAGIC overrides below to match your environment.

# COMMAND ----------

import sys, os

# Add the repo root to the Python path so imports resolve.
# In Databricks Repos this is automatic; adjust if running locally.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

from pyspark.sql import SparkSession
from config.loader import load_config
from qc.runner import run_qc

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ── 1. Load your OMOP tables ────────────────────────────────────────────
# Replace these with your actual table references.  Examples for Unity
# Catalog / Hive metastore:
#
#   tables = {
#       "person":                spark.table("omop.person"),
#       "visit_occurrence":      spark.table("omop.visit_occurrence"),
#       "condition_occurrence":  spark.table("omop.condition_occurrence"),
#       "drug_exposure":         spark.table("omop.drug_exposure"),
#       "procedure_occurrence":  spark.table("omop.procedure_occurrence"),
#       "measurement":           spark.table("omop.measurement"),
#       "observation":           spark.table("omop.observation"),
#       "observation_period":    spark.table("omop.observation_period"),
#       "death":                 spark.table("omop.death"),
#       "device_exposure":       spark.table("omop.device_exposure"),
#       "specimen":              spark.table("omop.specimen"),
#       "note":                  spark.table("omop.note"),
#       "care_site":             spark.table("omop.care_site"),
#       "provider":              spark.table("omop.provider"),
#       "location":              spark.table("omop.location"),
#       "payer_plan_period":     spark.table("omop.payer_plan_period"),
#       "cost":                  spark.table("omop.cost"),
#       # Vocabulary (optional — enables VOC checks)
#       "concept":               spark.table("omop_vocab.concept"),
#   }

# For demo purposes, run the synthetic demo instead:
from examples.synthetic_demo import build_synthetic_tables

tables = build_synthetic_tables(spark)

# COMMAND ----------

# ── 2. (Optional) Override default configuration ────────────────────────
config = load_config(
    overrides={
        "max_future_date": "2026-12-31",
        "min_birth_year": 1900,
        # Persist results to Delta (uncomment to enable):
        # "delta_output_path": "/mnt/qc_results/omop_qc",
    }
)

# COMMAND ----------

# ── 3. Run QC ───────────────────────────────────────────────────────────
results_df, summary_df = run_qc(tables, config=config, spark=spark, verbose=True)

# COMMAND ----------

# ── 4. Inspect results ──────────────────────────────────────────────────
# All failures
display(results_df.filter(~results_df.passed).orderBy("level", "table_name"))

# COMMAND ----------

# Per-table summary
from reporting.summary import format_summary_text

print(format_summary_text(summary_df))

# COMMAND ----------

# Top failures by affected row count
display(
    results_df.filter(~results_df.passed)
    .orderBy(results_df.n_affected.desc())
    .limit(20)
)
