# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 07: Workflow Productionization (Retries, Repair, Deployment as Code)
# MAGIC **Exam Coverage**: Section 4 (Production Pipelines) + modern workflow practices
# MAGIC **Duration**: 45–60 minutes
# MAGIC ---
# MAGIC ## Why this notebook exists
# MAGIC The core lab shows “Jobs” basics, but newer study material expands into:
# MAGIC - Retry semantics and idempotency
# MAGIC - Repairing/rehydrating runs
# MAGIC - Dependency graphs at scale
# MAGIC - Serverless execution (where supported)
# MAGIC - **Databricks Asset Bundles (DAB)** for “deploy workflows as code”
# MAGIC 
# MAGIC **Safety promise**: This notebook doesn’t create/modify UC tables. It uses temporary views and prints examples.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG_NAME}")
print("✅ Using catalog:", spark.catalog.currentCatalog())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Production workflow mental model
# MAGIC A robust workflow combines:
# MAGIC - **Orchestration**: DAG of tasks, dependencies, scheduling
# MAGIC - **Compute selection**: classic job cluster vs serverless (when available)
# MAGIC - **Resilience**: retries for transient failures, timeouts, alerts
# MAGIC - **Idempotency**: safe re-runs without duplicating or corrupting outputs
# MAGIC - **Observability**: structured logs + metrics
# MAGIC 
# MAGIC Exam-style takeaway: you’re often tested on *which problems retries solve* and *what they do not solve* (e.g., deterministic data bugs).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: A simple retry wrapper (local demonstration)
# MAGIC In real Workflows, retries are usually configured per task.
# MAGIC Here we build the core idea in Python: retry transient errors.

# COMMAND ----------

import time
import random

def run_with_retry(fn, *, max_attempts: int = 3, base_sleep_seconds: float = 1.0):
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn(attempt)
        except Exception as exc:
            last_exc = exc
            is_last = attempt == max_attempts
            print(f"Attempt {attempt}/{max_attempts} failed: {exc}")
            if is_last:
                raise
            sleep_for = base_sleep_seconds * (2 ** (attempt - 1))
            print(f"Sleeping {sleep_for:.1f}s then retrying...")
            time.sleep(sleep_for)
    raise last_exc

# Simulate a flaky dependency (e.g., intermittent network)

def flaky_task(attempt: int):
    if random.random() < 0.6:
        raise RuntimeError("Transient error: simulated network timeout")
    return f"SUCCESS on attempt {attempt}"

print(run_with_retry(flaky_task, max_attempts=4))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Idempotency pattern (read-only demo using temp views)
# MAGIC If you re-run a task, the result should be consistent.
# MAGIC 
# MAGIC **Common exam pattern**: “write-once” outputs should be produced using deterministic keys and merge/upsert logic.
# MAGIC In this lab, we avoid writing tables and instead demonstrate the *keyed* thinking.

# COMMAND ----------

from pyspark.sql import functions as F

# Use whichever table exists depending on how far you've progressed in the lab
source_table = None
for candidate in [SALES_SILVER_TABLE, SALES_BRONZE_TABLE]:
    try:
        spark.table(candidate)
        source_table = candidate
        break
    except Exception:
        pass

if not source_table:
    print("⚠️ Sales tables not found yet. Run Notebook 02/03 first, then re-run this section.")
else:
    print("Using source table:", source_table)
    df = spark.table(source_table)

    # Create a deterministic aggregation keyed by date
    agg = (
        df.withColumn("sale_date", F.to_date("order_timestamp"))
          .groupBy("sale_date")
          .agg(
              F.countDistinct("order_id").alias("orders"),
              F.sum("total_amount").alias("revenue")
          )
          .orderBy("sale_date")
    )

    agg.createOrReplaceTempView("tmp_daily_sales_metrics")
    display(spark.sql("SELECT * FROM tmp_daily_sales_metrics LIMIT 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Repair runs (concept)
# MAGIC “Repair” means re-running only the failed tasks (or a subset), without re-running successful upstream tasks.
# MAGIC 
# MAGIC Exam-style considerations:
# MAGIC - If tasks are **not idempotent**, repair can create duplicated side effects.
# MAGIC - If downstream depends on upstream output that changed, you may need a full backfill.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Serverless task execution (concept)
# MAGIC Many workspaces increasingly default to serverless for supported workloads (especially SQL).
# MAGIC 
# MAGIC Key exam angle: choose serverless when it meets requirements; choose classic compute when you need custom libs/configs/networking.
# MAGIC 
# MAGIC This notebook can’t switch your compute; it only prints runtime tags when available.

# COMMAND ----------

keys_to_try = [
    "spark.databricks.clusterUsageTags.clusterName",
    "spark.databricks.clusterUsageTags.clusterId",
    "spark.databricks.clusterUsageTags.clusterNodeType",
    "spark.databricks.clusterUsageTags.sparkVersion",
]

for k in keys_to_try:
    try:
        print(f"{k}: {spark.conf.get(k)}")
    except Exception:
        print(f"{k}: <not available>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Databricks Asset Bundles (DAB) — deploy workflows as code
# MAGIC DAB is the modern recommended pattern to package and deploy:
# MAGIC - Workflows/Jobs
# MAGIC - Pipelines
# MAGIC - Notebooks / Python code
# MAGIC - Environments + variables
# MAGIC 
# MAGIC **This notebook does not run DAB commands**. It provides a minimal example you can adapt.

# COMMAND ----------

bundle_yml_example = """
# Example (minimal) databricks.yml snippet for a bundle
bundle:
  name: cert-prep-workflows

workspace:
  host: https://<your-workspace-host>

targets:
  dev:
    default: true

resources:
  jobs:
    cert_prep_job:
      name: cert-prep-job
      tasks:
        - task_key: validate
          notebook_task:
            notebook_path: ./notebooks/05_Production_Workflows_Jobs
""".strip()

print(bundle_yml_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CLI commands you’d run locally (example)
# MAGIC ```bash
# MAGIC databricks bundle init
# MAGIC databricks bundle validate -t dev
# MAGIC databricks bundle deploy -t dev
# MAGIC databricks bundle run cert_prep_job -t dev
# MAGIC ```
# MAGIC 
# MAGIC Exam takeaway: know what DAB is *for* (repeatable deployments) even if you don’t memorize CLI flags.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC - Retries are for **transient failures**; idempotency is what makes retries/repair safe.
# MAGIC - “Repair” workflows only works reliably with idempotent tasks.
# MAGIC - Modern Databricks emphasizes **Workflows/Lakeflow Jobs** and **Asset Bundles** for production deployments.
