# Databricks notebook source
# MAGIC %md  
# MAGIC # Agent Observability
# MAGIC
# MAGIC Monitor agent behavior, latency, and tool usage patterns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import logging
# Suppress Streamlit warnings when running in notebook context
logging.getLogger("streamlit").setLevel(logging.ERROR)
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)
logging.getLogger("streamlit.runtime.state.session_state_proxy").setLevel(logging.ERROR)

import sys
import os
repo_root = os.path.abspath(
    os.path.join(os.getcwd(), "..")
)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from src.config import UNITY_CATALOG, UNITY_SCHEMA

print("✓ Observability modules imported")
print(f"  Governance table: {UNITY_CATALOG}.{UNITY_SCHEMA}.governance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Latency Analysis

# COMMAND ----------

latency_stats = spark.sql(f"""
SELECT
    country,
    COUNT(*) as query_count,
    AVG(total_time_seconds) as avg_latency,
    MIN(total_time_seconds) as min_latency,
    MAX(total_time_seconds) as max_latency,
    PERCENTILE(total_time_seconds, 0.95) as p95_latency
FROM {UNITY_CATALOG}.{UNITY_SCHEMA}.governance
GROUP BY country
ORDER BY country
""")

display(latency_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool Usage Patterns

# COMMAND ----------

tool_usage = spark.sql(f"""
SELECT
    tool_used,
    country,
    COUNT(*) as usage_count,
    AVG(total_time_seconds) as avg_time
FROM {UNITY_CATALOG}.{UNITY_SCHEMA}.governance
WHERE tool_used IS NOT NULL
GROUP BY tool_used, country
ORDER BY usage_count DESC
""")

display(tool_usage)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Performance

# COMMAND ----------

validation_stats = spark.sql(f"""
SELECT
    country,
    judge_verdict,
    COUNT(*) as query_count,
    AVG(validation_attempts) as avg_attempts,
    AVG(total_time_seconds) as avg_latency
FROM {UNITY_CATALOG}.{UNITY_SCHEMA}.governance
WHERE judge_verdict IS NOT NULL
GROUP BY country, judge_verdict
ORDER BY country, judge_verdict
""")

display(validation_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Volume by Country

# COMMAND ----------

query_volume = spark.sql(f"""
SELECT
    country,
    COUNT(*) as query_count,
    AVG(total_time_seconds) as avg_time,
    AVG(cost) as avg_cost,
    MIN(timestamp) as first_query,
    MAX(timestamp) as last_query
FROM {UNITY_CATALOG}.{UNITY_SCHEMA}.governance
GROUP BY country
ORDER BY query_count DESC
""")

display(query_volume)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recent Activity

# COMMAND ----------

recent_queries = spark.sql(f"""
SELECT
    timestamp,
    country,
    user_id,
    tool_used,
    judge_verdict,
    total_time_seconds,
    cost
FROM {UNITY_CATALOG}.{UNITY_SCHEMA}.governance
ORDER BY timestamp DESC
LIMIT 20
""")

display(recent_queries)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Trends

# COMMAND ----------

cost_trends = spark.sql(f"""
SELECT
    DATE(timestamp) as date,
    country,
    COUNT(*) as query_count,
    SUM(cost) as total_cost,
    AVG(cost) as avg_cost
FROM {UNITY_CATALOG}.{UNITY_SCHEMA}.governance
WHERE cost IS NOT NULL
GROUP BY DATE(timestamp), country
ORDER BY date DESC, country
LIMIT 50
""")

display(cost_trends)

# COMMAND ----------

print("✅ Observability metrics available")
print("   Query governance table for detailed agent analytics")
