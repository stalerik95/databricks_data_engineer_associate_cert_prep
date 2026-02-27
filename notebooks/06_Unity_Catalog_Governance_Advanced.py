# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 06: Unity Catalog Governance (Beyond Basic GRANT/REVOKE)
# MAGIC **Exam Coverage**: Section 5 (Data Governance) + modern governance concepts
# MAGIC **Duration**: 45–60 minutes
# MAGIC ---
# MAGIC ## Why this notebook exists
# MAGIC The core lab covers Unity Catalog basics, but newer Databricks materials frequently test additional governance concepts:
# MAGIC - Lineage (what reads/writes what)
# MAGIC - Auditability (who did what)
# MAGIC - Separation of duties (roles & responsibilities)
# MAGIC - External locations & storage credentials (how UC governs cloud storage)
# MAGIC - Delta Sharing (secure data sharing)
# MAGIC 
# MAGIC **Safety promise**: This notebook is **read-only**. It does **not** modify your existing tables, schemas, catalogs, shares, or permissions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Import shared variables and point Spark at the lab catalog.

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG_NAME}")
print("✅ Using catalog:", spark.catalog.currentCatalog())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Roles & responsibilities (conceptual)
# MAGIC Unity Catalog governance is typically split across responsibilities:
# MAGIC - **Account admin / Metastore admin**: creates metastores, assigns workspaces, sets up storage credentials/external locations
# MAGIC - **Workspace admin**: identity, group entitlements, workspace-level policies
# MAGIC - **Data steward / catalog owner**: defines schemas, ownership, data quality expectations
# MAGIC - **Data engineer**: builds pipelines, creates/maintains tables and permissions
# MAGIC - **Analyst / scientist**: consumes governed data
# MAGIC 
# MAGIC Exam-style takeaway: expect questions like *"Who can create external locations?"* or *"What does ownership imply?"*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Inspect governance surface area (what UC can govern)
# MAGIC This section is safe to run and helps you connect the concepts to real objects.

# COMMAND ----------

print("Current user:")
spark.sql("SELECT current_user() AS user").show(truncate=False)

print("\nCatalogs:")
spark.sql("SHOW CATALOGS").show(truncate=False)

print(f"\nSchemas in {CATALOG_NAME}:")
spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Effective permissions (GRANTS)
# MAGIC You already practiced `GRANT`/`REVOKE` in Notebook 01.
# MAGIC Here we show **how to inspect** permissions across objects.

# COMMAND ----------

schemas_to_check = [SYSTEM_SCHEMA, LANDING_SCHEMA, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA]

for schema in schemas_to_check:
    full_name = f"{CATALOG_NAME}.{schema}"
    print("=" * 80)
    print(f"Grants on schema: {full_name}")
    try:
        display(spark.sql(f"SHOW GRANTS ON SCHEMA {full_name}"))
    except Exception as exc:
        print("⚠️ Could not show grants (permissions/UI edition may differ):", str(exc))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Lineage (read-only)
# MAGIC Lineage answers questions like:
# MAGIC - *Which upstream tables feed this table?*
# MAGIC - *If a table changes, what downstream assets are impacted?*
# MAGIC 
# MAGIC In many workspaces, lineage is available through the UI and system tables.
# MAGIC We’ll **attempt** to query common lineage system tables, but availability varies by workspace/SKU.

# COMMAND ----------

lineage_queries = [
    # Common information_schema-style lineage tables (availability varies)
    "SELECT * FROM system.information_schema.table_lineage LIMIT 10",
    "SELECT * FROM system.information_schema.column_lineage LIMIT 10",
]

for query in lineage_queries:
    print("=" * 80)
    print("Attempting lineage query:")
    print(query)
    try:
        display(spark.sql(query))
    except Exception as exc:
        print("ℹ️ Lineage system table not available in this workspace or you lack privileges.")
        print("   Error:", str(exc))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Audit logs (read-only)
# MAGIC Audit logs answer questions like:
# MAGIC - *Who granted a privilege?*
# MAGIC - *Who accessed a table?*
# MAGIC - *Who created/dropped an object?*
# MAGIC 
# MAGIC Many workspaces expose audit data via system tables. We’ll attempt a few common entry points.

# COMMAND ----------

audit_queries = [
    "SELECT * FROM system.access.audit LIMIT 10",
    "SELECT * FROM system.access.table_access LIMIT 10",
]

for query in audit_queries:
    print("=" * 80)
    print("Attempting audit query:")
    print(query)
    try:
        display(spark.sql(query))
    except Exception as exc:
        print("ℹ️ Audit system table not available in this workspace or you lack privileges.")
        print("   Error:", str(exc))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: External locations & storage credentials (read-only)
# MAGIC In Unity Catalog, *external locations* and *storage credentials* govern access to cloud object storage.
# MAGIC 
# MAGIC These are often **metastore-level** objects, so you might not have permission to list them.

# COMMAND ----------

show_statements = [
    "SHOW STORAGE CREDENTIALS",
    "SHOW EXTERNAL LOCATIONS",
]

for stmt in show_statements:
    print("=" * 80)
    print("Attempting:", stmt)
    try:
        display(spark.sql(stmt))
    except Exception as exc:
        print("ℹ️ Not available / insufficient privileges:", str(exc))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Delta Sharing (read-only)
# MAGIC Delta Sharing enables secure sharing of live datasets.
# MAGIC 
# MAGIC This notebook **does not create** shares/recipients.
# MAGIC We only attempt to list if your workspace allows it.

# COMMAND ----------

sharing_statements = [
    "SHOW SHARES",
    "SHOW RECIPIENTS",
]

for stmt in sharing_statements:
    print("=" * 80)
    print("Attempting:", stmt)
    try:
        display(spark.sql(stmt))
    except Exception as exc:
        print("ℹ️ Delta Sharing objects not available / insufficient privileges:", str(exc))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Hands-on mini exercise (read-only): find sensitive columns
# MAGIC Without changing any metadata, practice identifying likely PII fields.
# MAGIC 
# MAGIC **Goal**: Inspect schemas of your lab tables and list columns that *might* be sensitive.

# COMMAND ----------

from pyspark.sql import functions as F

candidate_tables = [
    CUSTOMERS_BRONZE_TABLE,
    PRODUCTS_BRONZE_TABLE,
    SALES_BRONZE_TABLE,
    EVENTS_BRONZE_TABLE,
]

pii_keywords = ["email", "phone", "address", "name", "ssn", "dob"]

rows = []
for table in candidate_tables:
    try:
        df = spark.table(table)
        cols = df.columns
        for col in cols:
            lowered = col.lower()
            if any(k in lowered for k in pii_keywords):
                rows.append((table, col))
    except Exception:
        # Table may not exist yet if prior notebooks weren't run
        pass

if rows:
    display(spark.createDataFrame(rows, ["table", "candidate_sensitive_column"]))
else:
    print("No candidate columns found (or tables not created yet).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary (exam alignment)
# MAGIC - Governance is broader than `GRANT`/`REVOKE`: include **lineage**, **audit**, **external locations**, and **sharing**.
# MAGIC - Availability is **workspace/SKU/permissions dependent**; the exam often tests the *concepts* and who is responsible for what.
# MAGIC - If your workspace didn’t expose system tables here, use the **Unity Catalog UI** to explore lineage and permissions.
