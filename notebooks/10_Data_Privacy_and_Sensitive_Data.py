# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 10: Data Privacy & Sensitive Data Handling (PII patterns)
# MAGIC **Exam Coverage**: Data governance + practical privacy patterns
# MAGIC **Duration**: 30–45 minutes
# MAGIC ---
# MAGIC ## Why this notebook exists
# MAGIC Newer exam material and real-world practice include:
# MAGIC - Identifying PII and sensitive fields
# MAGIC - Masking/obfuscation patterns
# MAGIC - Row-level filtering and least privilege
# MAGIC - Governance features that may require admin/steward privileges
# MAGIC 
# MAGIC **Safety promise**: This notebook does not modify existing UC tables.
# MAGIC It demonstrates privacy patterns using **temporary views only**.

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG_NAME}")
print("✅ Using catalog:", spark.catalog.currentCatalog())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Identify PII candidates in the lab dataset
# MAGIC We’ll search for common PII-like column names.

# COMMAND ----------

from pyspark.sql import functions as F

candidate_tables = [
    CUSTOMERS_SILVER_TABLE,
    CUSTOMERS_BRONZE_TABLE,
    SALES_SILVER_TABLE,
    SALES_BRONZE_TABLE,
]

pii_keywords = ["email", "phone", "name", "address", "ip", "device", "user"]

hits = []
for t in candidate_tables:
    try:
        df = spark.table(t)
        for c in df.columns:
            if any(k in c.lower() for k in pii_keywords):
                hits.append((t, c))
    except Exception:
        pass

if hits:
    display(spark.createDataFrame(hits, ["table", "candidate_sensitive_column"]))
else:
    print("⚠️ No tables found yet (run earlier notebooks) or no matching columns.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Masking patterns without changing tables (TEMP VIEW)
# MAGIC In production with Unity Catalog you might use **dynamic views** or **column masks**.
# MAGIC Those typically require privileges and they *change* metadata.
# MAGIC 
# MAGIC Here we create a **temporary view** that masks sensitive columns using expressions.

# COMMAND ----------

source_table = None
for t in [CUSTOMERS_SILVER_TABLE, CUSTOMERS_BRONZE_TABLE]:
    try:
        spark.table(t)
        source_table = t
        break
    except Exception:
        pass

if not source_table:
    print("⚠️ Customer table not found yet. Run Notebook 02/03 first.")
else:
    df = spark.table(source_table)

    # Be defensive: not all columns exist in every version
    cols = set(df.columns)

    masked = df
    if "email" in cols:
        masked = masked.withColumn(
            "email_masked",
            F.when(F.col("email").isNull(), F.lit(None))
             .otherwise(F.concat(F.lit("***@"), F.element_at(F.split(F.col("email"), "@"), -1)))
        )

    if "phone" in cols:
        digits = F.regexp_replace(F.col("phone"), "[^0-9]", "")
        masked = masked.withColumn(
            "phone_masked",
            F.when(F.col("phone").isNull(), F.lit(None))
             .otherwise(F.concat(F.lit("***-***-"), F.substring(digits, -4, 4)))
        )

    # Hash a stable identifier for privacy-preserving joins (example)
    if "customer_id" in cols:
        masked = masked.withColumn("customer_id_hash", F.sha2(F.col("customer_id").cast("string"), 256))

    masked.createOrReplaceTempView("tmp_customers_privacy")

    display(spark.sql("SELECT * FROM tmp_customers_privacy LIMIT 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Row-level filtering (concept)
# MAGIC A classic pattern is to restrict rows by region/tenant/business unit.
# MAGIC 
# MAGIC This notebook doesn’t enforce row filters (that would require UC policies / dynamic views).
# MAGIC Instead, we show the *idea* using a temp view filter.

# COMMAND ----------

try:
    df = spark.table(source_table)
    if "location" in df.columns:
        df.filter(F.col("location").isNotNull()).createOrReplaceTempView("tmp_customers_row_filtered")
        display(spark.sql("SELECT location, COUNT(*) AS n FROM tmp_customers_row_filtered GROUP BY location ORDER BY n DESC"))
    else:
        print("No 'location' column found to demo row filtering.")
except Exception as exc:
    print("Skipping row-level demo:", str(exc))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: What UC-native controls might look like (commented examples)
# MAGIC These examples are **not executed** to keep your lab unchanged.
# MAGIC 
# MAGIC ```sql
# MAGIC -- Example: dynamic view (row filtering)
# MAGIC -- CREATE OR REPLACE VIEW catalog.schema.v_customers AS
# MAGIC -- SELECT * FROM catalog.schema.customers
# MAGIC -- WHERE is_member('eu_analysts') AND region = 'EU';
# MAGIC 
# MAGIC -- Example: column masking (requires UC features + privileges)
# MAGIC -- ALTER TABLE catalog.schema.customers
# MAGIC -- ALTER COLUMN email
# MAGIC -- SET MASK <masking_function>;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC - You can practice privacy patterns safely using **temporary views**.
# MAGIC - In production, prefer UC-native enforcement (dynamic views, column masks, least privilege), managed by the right roles.
# MAGIC - Exam focus: know *what the controls do* and the high-level design choices.
