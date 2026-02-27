# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 08: Lakehouse Federation (Concepts + Safe Exploration)
# MAGIC **Exam Coverage**: Modern platform capabilities (conceptual)
# MAGIC **Duration**: 30–45 minutes
# MAGIC ---
# MAGIC ## Why this notebook exists
# MAGIC Lakehouse Federation allows you to query data that lives outside Databricks (external databases) through governed connections.
# MAGIC 
# MAGIC Typical exam angles:
# MAGIC - What federation is (query external sources without copying all data)
# MAGIC - What objects are involved (**connections**, **foreign catalogs**, permissions)
# MAGIC - When to federate vs ingest (latency, cost, governance)
# MAGIC 
# MAGIC **Safety promise**: This notebook is **read-only**. It won’t create connections or catalogs.

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
# MAGIC ## Section 1: Federation vs ingestion (decision frame)
# MAGIC **Federation** is often a good fit when:
# MAGIC - You need *fresh* data from an external system
# MAGIC - You can tolerate external source latency/limits
# MAGIC - Copying data is expensive or prohibited
# MAGIC 
# MAGIC **Ingestion** (Auto Loader/COPY INTO/ETL) is often better when:
# MAGIC - You need high-performance analytics on large volumes
# MAGIC - You need full Delta features, time travel, optimizations
# MAGIC - You want to decouple from external system availability

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: What objects exist in Unity Catalog
# MAGIC Depending on your workspace features/permissions, you may be able to list connections.
# MAGIC 
# MAGIC We attempt common discovery commands. If they fail, that’s expected in many environments.

# COMMAND ----------

discovery_statements = [
    "SHOW CONNECTIONS",
    "SHOW FOREIGN CATALOGS",
]

for stmt in discovery_statements:
    print("=" * 80)
    print("Attempting:", stmt)
    try:
        display(spark.sql(stmt))
    except Exception as exc:
        print("ℹ️ Not available / insufficient privileges:", str(exc))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Typical federation flow (non-executing example)
# MAGIC The exact SQL depends on the external system (Snowflake, Postgres, etc.) and your workspace.
# MAGIC 
# MAGIC Below is **example SQL**. It is intentionally **commented out** to avoid modifying your environment.
# MAGIC 
# MAGIC ```sql
# MAGIC -- 1) Create a connection (admin/governance-managed)
# MAGIC -- CREATE CONNECTION my_pg_conn
# MAGIC -- TYPE postgresql
# MAGIC -- OPTIONS (
# MAGIC --   host '...',
# MAGIC --   port '5432',
# MAGIC --   user '...',
# MAGIC --   password '...'
# MAGIC -- );
# MAGIC 
# MAGIC -- 2) Create a foreign catalog backed by that connection
# MAGIC -- CREATE FOREIGN CATALOG my_pg_catalog
# MAGIC -- USING CONNECTION my_pg_conn
# MAGIC -- OPTIONS (database '...');
# MAGIC 
# MAGIC -- 3) Grant access
# MAGIC -- GRANT USE CATALOG ON CATALOG my_pg_catalog TO `data_analysts`;
# MAGIC 
# MAGIC -- 4) Query federated tables
# MAGIC -- SELECT * FROM my_pg_catalog.public.some_table LIMIT 10;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: “Federation mindset” practice using your lab data
# MAGIC This mini-exercise uses the lab dataset to practice a common exam question:
# MAGIC *“Should we federate or ingest this dataset?”*
# MAGIC 
# MAGIC We’ll compute a small, reusable metric from your lab tables and discuss the tradeoffs.

# COMMAND ----------

from pyspark.sql import functions as F

source_table = None
for candidate in [CUSTOMERS_SILVER_TABLE, CUSTOMERS_BRONZE_TABLE]:
    try:
        spark.table(candidate)
        source_table = candidate
        break
    except Exception:
        pass

if not source_table:
    print("⚠️ Customer tables not found yet. Run Notebook 02/03 first, then re-run this section.")
else:
    df = spark.table(source_table)
    metrics = (
        df.select(
            F.count("customer_id").alias("rows"),
            F.countDistinct("customer_id").alias("distinct_customers"),
            F.sum(F.when(F.col("email").isNull(), 1).otherwise(0)).alias("null_emails"),
        )
    )
    display(metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC - Federation is about **governed query access** to external systems without fully ingesting.
# MAGIC - Ingestion is about **performance, Delta capabilities, and decoupling**.
# MAGIC - On the exam, focus on the *tradeoff reasoning* and the UC objects involved (connections/foreign catalogs).
