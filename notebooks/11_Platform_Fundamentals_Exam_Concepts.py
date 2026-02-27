# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 11: Databricks Platform Fundamentals (Exam Concepts)
# MAGIC **Exam Coverage**: Section 1 (Platform concepts) + conceptual questions across sections
# MAGIC **Duration**: 30–45 minutes
# MAGIC ---
# MAGIC ## Why this notebook exists
# MAGIC Some exam questions are conceptual and not fully covered by hands-on notebooks:
# MAGIC - How clusters run Spark (driver/executors)
# MAGIC - How jobs/workflows execute tasks
# MAGIC - Workspace vs account concepts (metastore, UC)
# MAGIC - Networking/workspace relationships (high level)
# MAGIC - Platform value proposition / lakehouse concepts
# MAGIC 
# MAGIC **Safety promise**: read-only + small Spark actions only (no table modifications).

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Spark execution model (driver + executors)
# MAGIC - **Driver**: coordinates the job, builds the plan, schedules tasks
# MAGIC - **Executors**: run tasks on partitions
# MAGIC 
# MAGIC Exam-style takeaway: transformations are lazy; actions trigger execution.

# COMMAND ----------

# Minimal demo: lazy vs action
rdd = spark.sparkContext.parallelize(range(1, 21), 4)

mapped = rdd.map(lambda x: x * 2)  # lazy

# Action triggers computation
print("Sum:", mapped.sum())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Jobs/Workflows execution (high level)
# MAGIC In Workflows:
# MAGIC - A **job run** executes a set of **tasks**
# MAGIC - Tasks can be notebooks, Spark jobs, SQL tasks, etc.
# MAGIC - Each task has configuration: compute, parameters, retries, timeout
# MAGIC 
# MAGIC Exam focus: know where parameters come from (widgets/task parameters) and what retries/repair mean.

# COMMAND ----------

# Best-effort runtime tags (availability varies)
keys_to_try = [
    "spark.databricks.clusterUsageTags.clusterName",
    "spark.databricks.clusterUsageTags.clusterId",
    "spark.databricks.clusterUsageTags.sparkVersion",
]

for k in keys_to_try:
    try:
        print(f"{k}: {spark.conf.get(k)}")
    except Exception:
        print(f"{k}: <not available>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Workspace + Unity Catalog mental model
# MAGIC - **Workspace**: UI + compute + notebooks + jobs + users/groups (workspace-level)
# MAGIC - **Unity Catalog metastore**: governance layer for catalogs/schemas/tables/volumes (account-level in many setups)
# MAGIC - **Catalog**: top-level container in UC (often environment or domain)
# MAGIC - **Schema**: grouping within a catalog
# MAGIC 
# MAGIC Exam focus: separate *where something lives* (workspace vs metastore) from *who can do what* (privileges/ownership).

# COMMAND ----------

spark.sql("SHOW CATALOGS").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: “Under the hood” concept checks
# MAGIC - Why do small files hurt performance?
# MAGIC - What is the difference between batch and streaming semantics?
# MAGIC - Why does checkpointing matter?
# MAGIC - Why does partitioning help only when you filter on the partition column?
# MAGIC 
# MAGIC Use your lab tables to practice reading metadata.

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG_NAME}")

candidate = None
for t in [SALES_SILVER_TABLE, SALES_BRONZE_TABLE]:
    try:
        spark.sql(f"DESCRIBE DETAIL {t}")
        candidate = t
        break
    except Exception:
        pass

if not candidate:
    print("⚠️ Sales tables not found yet. Run Notebook 02/03 first.")
else:
    print("Inspecting:", candidate)
    display(spark.sql(f"DESCRIBE DETAIL {candidate}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC - Expect a mix of hands-on and conceptual questions.
# MAGIC - Translate legacy terms when needed (Jobs → Workflows/Lakeflow Jobs; DLT → Lakeflow Declarative Pipelines).
# MAGIC - Use metadata (`DESCRIBE DETAIL`, grants, and system tables when available) to reason about performance and governance.
