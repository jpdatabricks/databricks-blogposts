# Databricks notebook source
# MAGIC %md
# MAGIC # Demo Setup
# MAGIC
# MAGIC This notebook initializes the catalog, schemas, and configuration for the
# MAGIC Multi-Country Retirement Advisory demo.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Creates Unity Catalog and schemas if they don't exist
# MAGIC - Sets up configuration variables
# MAGIC - Prepares the environment for data loading

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Configuration is loaded from config.yaml

# COMMAND ----------

# Import configuration from config.yaml
import sys
import os
repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from src.config import UNITY_CATALOG, UNITY_SCHEMA, FUNCTIONS_SCHEMA

catalog = UNITY_CATALOG
schema = UNITY_SCHEMA
functions_schema = FUNCTIONS_SCHEMA

# Reset data widget
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset All Data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Functions Schema: {functions_schema}")
print(f"Reset data: {reset_all_data}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema

# COMMAND ----------

# Create catalog if it doesn't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
print(f"✓ Catalog '{catalog}' ready")

# Use the catalog
spark.sql(f"USE CATALOG {catalog}")

# Create schemas if they don't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
print(f"✓ Schema '{schema}' ready")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {functions_schema}")
print(f"✓ Schema '{functions_schema}' ready (for UC functions)")

# Use the schema
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Configuration Variables
# MAGIC
# MAGIC These variables will be used by other notebooks

# COMMAND ----------

# Configuration variables are now loaded directly from src.config in each notebook
# Cloud storage path for streaming checkpoints
cloud_storage_path = f"/tmp/{catalog}/{schema}"

print(f"✓ Configuration (loaded from src.config):")
print(f"  - Catalog: {catalog}")
print(f"  - Schema: {schema}")
print(f"  - Storage path: {cloud_storage_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset Data (Optional)
# MAGIC
# MAGIC If reset_all_data is true, drop all tables

# COMMAND ----------

if reset_all_data:
    print("⚠️  Resetting all data...")

    # Get list of tables
    tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()

    for table_row in tables:
        table_name = table_row.tableName
        full_table_name = f"{catalog}.{schema}.{table_name}"
        print(f"  Dropping {full_table_name}...")
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

    print(f"✓ All tables dropped in {catalog}.{schema}")
else:
    print("ℹ️  Keeping existing data (reset_all_data=false)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

# List schemas in catalog
print("Schemas in catalog:")
display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))

# List tables in schema
print(f"\nTables in {catalog}.{schema}:")
display(spark.sql(f"SHOW TABLES IN {catalog}.{schema}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete
# MAGIC
# MAGIC The environment is ready for data loading. Proceed to the next notebook:
# MAGIC - **00-load-data**: Generate and load synthetic member data

# COMMAND ----------

print("✅ Setup complete!")
print(f"   Catalog: {catalog}")
print(f"   Schema: {schema}")
print(f"   Ready for data loading")
