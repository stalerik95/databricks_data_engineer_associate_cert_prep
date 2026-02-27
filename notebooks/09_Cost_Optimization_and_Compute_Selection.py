# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 09: Cost Optimization & Compute Selection (Concepts + Safe Signals)
# MAGIC **Exam Coverage**: Platform concepts + production best practices
# MAGIC **Duration**: 30–45 minutes
# MAGIC ---
# MAGIC ## Why this notebook exists
# MAGIC Newer exam material expects you to reason about:
# MAGIC - Serverless vs classic compute selection
# MAGIC - Cost drivers (uptime vs per-use)
# MAGIC - Table/file optimization signals (small files, partitioning)
# MAGIC - Platform-level optimizations (Photon, caching) at a conceptual level
# MAGIC 
# MAGIC **Safety promise**: This notebook is read-only (no OPTIMIZE/VACUUM writes). It only inspects metadata and prints guidance.

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG_NAME}")
print("✅ Using catalog:", spark.catalog.currentCatalog())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Compute selection quick framework
# MAGIC Choose **serverless** when:
# MAGIC - Workload is supported by serverless in your workspace
# MAGIC - You want fast start, simplified operations, pay-per-use
# MAGIC 
# MAGIC Choose **classic clusters** when:
# MAGIC - You need custom libraries, specific instance types, Spark configs, networking controls
# MAGIC - You need predictable long-running compute characteristics
# MAGIC 
# MAGIC Exam takeaway: the “right” answer is often “it depends” on requirements and workspace features.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Inspect runtime tags (best-effort)
# MAGIC These tags help you recognize what environment you’re in. Availability varies.

# COMMAND ----------

keys_to_try = [
    "spark.databricks.clusterUsageTags.clusterName",
    "spark.databricks.clusterUsageTags.clusterId",
    "spark.databricks.clusterUsageTags.clusterNodeType",
    "spark.databricks.clusterUsageTags.clusterWorkers",
    "spark.databricks.clusterUsageTags.sparkVersion",
]

for k in keys_to_try:
    try:
        print(f"{k}: {spark.conf.get(k)}")
    except Exception:
        print(f"{k}: <not available>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Cost signals from your tables (read-only)
# MAGIC A common cost/performance anti-pattern is **too many small files**.
# MAGIC We’ll inspect `DESCRIBE DETAIL` for one of your tables.

# COMMAND ----------

candidate_tables = [SALES_SILVER_TABLE, SALES_BRONZE_TABLE, DAILY_SALES_SUMMARY_TABLE]
selected = None
for t in candidate_tables:
    try:
        spark.sql(f"DESCRIBE DETAIL {t}")
        selected = t
        break
    except Exception:
        pass

if not selected:
    print("⚠️ No candidate tables found yet. Run earlier notebooks first, then re-run this section.")
else:
    print("Inspecting:", selected)
    detail = spark.sql(f"DESCRIBE DETAIL {selected}")
    display(detail)

    # Pull a few common signals when present
    cols = set(detail.columns)
    select_cols = [c for c in ["format", "numFiles", "sizeInBytes", "partitionColumns", "clusteringColumns"] if c in cols]
    if select_cols:
        display(detail.select(*select_cols))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Explain plans (read-only)
# MAGIC You don’t need to memorize Spark plan formats, but you should understand:
# MAGIC - Filters/predicates help file skipping
# MAGIC - Joins can be broadcast vs shuffle
# MAGIC - Partition pruning matters when partition columns are used

# COMMAND ----------

from pyspark.sql import functions as F

source_table = None
for t in [SALES_SILVER_TABLE, SALES_BRONZE_TABLE]:
    try:
        spark.table(t)
        source_table = t
        break
    except Exception:
        pass

if not source_table:
    print("⚠️ Sales table not found yet. Run Notebook 02/03 first.")
else:
    df = spark.table(source_table)
    # A small query with a filter that should be easy to reason about
    q = df.filter(F.col("total_amount") > 500).groupBy("category").agg(F.count("order_id").alias("orders"))
    print("Logical/physical plan:")
    q.explain(True)
    display(q.orderBy(F.desc("orders")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Concept checklist
# MAGIC - What drives cost in serverless vs classic compute?
# MAGIC - How do partitions and clustering affect performance?
# MAGIC - Why are small files expensive?
# MAGIC - What are the tradeoffs of “optimize everything” vs targeted optimization?
# MAGIC 
# MAGIC This notebook intentionally avoided running `OPTIMIZE`/`VACUUM` to keep your lab state unchanged.
