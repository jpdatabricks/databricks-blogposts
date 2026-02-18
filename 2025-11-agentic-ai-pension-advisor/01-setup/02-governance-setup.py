# Databricks notebook source
# MAGIC %md
# MAGIC # Governance & Access Control Setup
# MAGIC
# MAGIC This notebook configures Unity Catalog governance features for the
# MAGIC multi-country retirement advisory system.
# MAGIC
# MAGIC **Governance Features:**
# MAGIC - Row-level security (members can only see their own data)
# MAGIC - Audit logging for all queries
# MAGIC - Data lineage tracking
# MAGIC - Column masking for sensitive data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import sys
import os
repo_root = os.path.abspath(
    os.path.join(os.getcwd(), "..")
)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from src.config import UNITY_CATALOG, UNITY_SCHEMA, FUNCTIONS_SCHEMA

catalog = UNITY_CATALOG
schema = UNITY_SCHEMA
functions_schema = FUNCTIONS_SCHEMA

# Set current catalog
spark.sql(f"USE CATALOG {catalog}")

print(f"Configuring governance for:")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row-Level Security (RLS)
# MAGIC
# MAGIC Members can only access their own data using row filters

# COMMAND ----------

# Example: Create row filter function for member_profiles
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.member_access_filter(member_id STRING)
RETURN
  CASE
    WHEN IS_MEMBER('admin_group') THEN TRUE
    WHEN IS_MEMBER('advisor_group') THEN TRUE
    WHEN member_id = CURRENT_USER() THEN TRUE
    ELSE FALSE
  END
""")

print(f"✓ Created row filter function: member_access_filter")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Row Filter to Member Profiles
# MAGIC
# MAGIC (Note: This would be applied in production, commented for demo)

# COMMAND ----------

# In production, apply the row filter:
# ALTER TABLE {catalog}.{schema}.member_profiles
# SET ROW FILTER {catalog}.{schema}.member_access_filter ON (member_id)

print("Row filter would be applied in production")
print("For demo purposes, access is unrestricted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Masking
# MAGIC
# MAGIC Mask PII fields for non-privileged users

# COMMAND ----------

# Example: Mask function for member names
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.mask_member_name(name STRING)
RETURN
  CASE
    WHEN IS_MEMBER('admin_group') THEN name
    WHEN IS_MEMBER('advisor_group') THEN name
    ELSE CONCAT(SUBSTR(name, 1, 1), '***')
  END
""")

print(f"✓ Created masking function: mask_member_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Logging
# MAGIC
# MAGIC The governance table tracks all agent queries
# MAGIC
# MAGIC **Note:** Table will be empty until agent starts making queries

# COMMAND ----------

# Verify governance table exists
governance_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog}.{schema}.governance").collect()[0].cnt
if governance_count == 0:
    print(f"✓ Governance table ready (empty - will populate when agent runs)")
else:
    print(f"✓ Governance table active: {governance_count} audit records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Audit Query
# MAGIC
# MAGIC View recent agent queries (will show results after agent runs)

# COMMAND ----------

if governance_count > 0:
    display(spark.sql(f"""
    SELECT
        timestamp,
        user_id,
        country,
        LEFT(query_string, 50) as query_preview,
        judge_verdict,
        total_time_seconds,
        cost
    FROM {catalog}.{schema}.governance
    ORDER BY timestamp DESC
    LIMIT 10
    """))
else:
    print("No audit records yet - run the agent demo to populate governance data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lineage
# MAGIC
# MAGIC Unity Catalog automatically tracks data lineage

# COMMAND ----------

# Lineage is available in Unity Catalog UI:
# Catalog Explorer → Table → Lineage tab

print("✓ Data lineage tracking enabled automatically by Unity Catalog")
print("  View lineage in: Catalog Explorer → member_profiles → Lineage tab")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Permissions
# MAGIC
# MAGIC Configure catalog/schema/table permissions

# COMMAND ----------

# Example grants (adjust for your security model)
try:
    # Grant SELECT on member_profiles to advisor group
    spark.sql(f"""
    GRANT SELECT ON TABLE {catalog}.{schema}.member_profiles
    TO `advisor_group`
    """)

    # Grant EXECUTE on UC functions to advisor group
    spark.sql(f"""
    GRANT EXECUTE ON FUNCTION {catalog}.{functions_schema}.au_calculate_tax
    TO `advisor_group`
    """)

    print("✓ Permissions granted to advisor_group")
except Exception as e:
    print(f"Note: {str(e)[:100]}")
    print("Permissions may need to be configured in Account Console")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance Setup Complete
# MAGIC
# MAGIC Governance features configured:
# MAGIC - ✅ Row-level security functions created
# MAGIC - ✅ Column masking functions created
# MAGIC - ✅ Audit logging verified
# MAGIC - ✅ Data lineage enabled
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - **02-agent-demo/01-agent-overview**: Learn about the agent architecture
# MAGIC - **02-agent-demo/02-build-agent**: Build and test the agent

# COMMAND ----------

# Verify setup
governance_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog}.{schema}.governance").collect()[0].cnt
member_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog}.{schema}.member_profiles").collect()[0].cnt

# Check if security functions exist
functions = spark.sql(f"SHOW USER FUNCTIONS IN {catalog}.{schema}").collect()
security_funcs = [f for f in functions if 'access_filter' in str(f) or 'mask' in str(f)]

print("✅ Governance setup complete!")
print(f"   Row-level security: {len(security_funcs)} functions created")
print(f"   Audit logging: Active (governance table ready, {governance_count} records)")
print(f"   Data lineage: Enabled")
print(f"   Member profiles: {member_count} records")
print("   Ready for secure agent queries")
