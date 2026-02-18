# Databricks notebook source
# MAGIC %md
# MAGIC # 08: Automated Quality Scoring Job
# MAGIC
# MAGIC **📚 Navigation:** [← Previous: 07-Production Monitoring]($./07-production-monitoring)
# MAGIC
# MAGIC This notebook runs automated quality scorers on production queries to monitor quality trends over time.
# MAGIC
# MAGIC **Purpose:**
# MAGIC - Asynchronous background monitoring (doesn't block user responses)
# MAGIC - Sampled evaluation (10% by default to control costs)
# MAGIC - Detects quality drift and trends over time
# MAGIC - Complements real-time LLM-as-a-Judge validation
# MAGIC
# MAGIC **Schedule:** Runs every 6 hours via Databricks job (configurable)
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC Production Queries (MLflow traces)
# MAGIC     ↓
# MAGIC Sample 10% of queries
# MAGIC     ↓
# MAGIC Run Automated Scorers
# MAGIC     ↓
# MAGIC Store Results → scoring_results table
# MAGIC     ↓
# MAGIC Display in Observability Dashboard
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json

# Add project root to path
repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from src.scorers import get_all_scorers, score_query
from src.config import MLFLOW_PROD_EXPERIMENT_PATH, UNITY_CATALOG, UNITY_SCHEMA
from src.shared.logging_config import get_logger
import mlflow
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, expr, date_trunc
from databricks.sdk import WorkspaceClient

logger = get_logger(__name__)

# Get Spark session
spark = SparkSession.builder.getOrCreate()

# Get workspace client
w = WorkspaceClient()

print("✅ Setup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Job parameters (can be overridden via Databricks job parameters)
SAMPLING_RATE = float(dbutils.widgets.get("sampling_rate")) if "sampling_rate" in [w.name for w in dbutils.widgets.getAll()] else 0.1
LOOKBACK_HOURS = int(dbutils.widgets.get("lookback_hours")) if "lookback_hours" in [w.name for w in dbutils.widgets.getAll()] else 6
SCORING_TABLE = f"{UNITY_CATALOG}.{UNITY_SCHEMA}.scoring_results"

print(f"Configuration:")
print(f"  Sampling Rate: {SAMPLING_RATE * 100}%")
print(f"  Lookback Hours: {LOOKBACK_HOURS}")
print(f"  Scoring Table: {SCORING_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Scoring Results Table

# COMMAND ----------

# Create scoring_results table if it doesn't exist
# This ensures proper Delta Lake configuration and table properties
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCORING_TABLE} (
  run_id STRING NOT NULL COMMENT 'MLflow run ID for the query',
  user_id STRING COMMENT 'User ID who made the query',
  country STRING COMMENT 'Country code (AU, US, UK, IN)',
  query_timestamp TIMESTAMP COMMENT 'When the query was made',
  scoring_timestamp TIMESTAMP NOT NULL COMMENT 'When the scoring was performed',
  overall_score DOUBLE COMMENT 'Average score across all scorers (0-1)',
  pass_rate DOUBLE COMMENT 'Percentage of scorers that passed',
  passed_count INT COMMENT 'Number of scorers that passed',
  total_count INT COMMENT 'Total number of scorers run',
  verdict STRING COMMENT 'Overall verdict (PASS, FAIL, ERROR)',
  individual_scores STRING COMMENT 'JSON string with individual scorer results'
)
USING DELTA
COMMENT 'Automated quality scoring results for production queries'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")

# Grant SELECT permission to account users for observability dashboards
spark.sql(f"GRANT SELECT ON TABLE {SCORING_TABLE} TO `account users`")

# Grant MODIFY permission for INSERT operations
spark.sql(f"GRANT MODIFY ON TABLE {SCORING_TABLE} TO `account users`")

print(f"✅ Table ready: {SCORING_TABLE}")
print(f"✅ Permissions granted: SELECT and MODIFY to account users")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Query Recent Production Traces

# COMMAND ----------

# Get traces from last N hours
client = mlflow.MlflowClient()

# Calculate timestamp filter (last N hours)
lookback_timestamp = int((datetime.now() - timedelta(hours=LOOKBACK_HOURS)).timestamp() * 1000)

# Get experiment
experiment = mlflow.get_experiment_by_name(MLFLOW_PROD_EXPERIMENT_PATH)
if not experiment:
    print(f"⚠️ Experiment not found: {MLFLOW_PROD_EXPERIMENT_PATH}")
    dbutils.notebook.exit("No experiment found")

# Search for recent runs
recent_runs = client.search_runs(
    experiment_ids=[experiment.experiment_id],
    filter_string=f"attributes.start_time >= {lookback_timestamp}",
    max_results=1000,
    order_by=["start_time DESC"]
)

print(f"📊 Found {len(recent_runs)} runs in last {LOOKBACK_HOURS} hours")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Sample Queries for Scoring

# COMMAND ----------

import random

# Extract query data from runs
queries_to_score = []
for run in recent_runs:
    try:
        # Get run parameters
        user_id = run.data.params.get('user_id', 'unknown')
        country = run.data.params.get('country', 'AU')

        # Get query and response from artifacts (they are logged as text files)
        # Download artifacts to read the content
        artifact_uri = run.info.artifact_uri

        # Read query and response from artifacts
        query_text = ''
        response_text = ''
        try:
            query_path = client.download_artifacts(run.info.run_id, "query.txt")
            with open(query_path, 'r') as f:
                query_text = f.read()
        except Exception as e:
            print(f"  ⚠️ Could not read query.txt for run {run.info.run_id[:8]}: {e}")

        try:
            response_path = client.download_artifacts(run.info.run_id, "response.txt")
            with open(response_path, 'r') as f:
                response_text = f.read()
        except Exception as e:
            print(f"  ⚠️ Could not read response.txt for run {run.info.run_id[:8]}: {e}")

        # Only score if we have both query and response
        if query_text and response_text:
            queries_to_score.append({
                'run_id': run.info.run_id,
                'user_id': user_id,
                'country': country,
                'query': query_text,
                'response': response_text,
                'timestamp': datetime.fromtimestamp(run.info.start_time / 1000)
            })
            print(f"  ✅ Run {run.info.run_id[:8]} - has both artifacts")
    except Exception as e:
        logger.error(f"Error processing run {run.info.run_id}: {e}")
        continue

print(f"📋 Found {len(queries_to_score)} queries with complete data")

# Check if we have queries to score
if len(queries_to_score) == 0:
    print(f"⚠️ No queries found with complete data in last {LOOKBACK_HOURS} hours")
    print("💡 Tip: Make sure production queries are being logged to MLflow with query.txt and response.txt artifacts")
    dbutils.notebook.exit("No queries to score")

# Sample queries based on sampling rate
sample_size = max(1, int(len(queries_to_score) * SAMPLING_RATE))  # At least 1 query
sampled_queries = random.sample(queries_to_score, min(sample_size, len(queries_to_score)))

print(f"🎯 Sampled {len(sampled_queries)} queries ({SAMPLING_RATE * 100}% of total)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Run Automated Scorers

# COMMAND ----------

# Get all available scorers
scorers = get_all_scorers()
print(f"📊 Running {len(scorers)} scorers:")
for scorer in scorers:
    print(f"  - {scorer.name}")

# Run scorers on sampled queries
scoring_results = []

for idx, query_data in enumerate(sampled_queries):
    print(f"\nScoring query {idx + 1}/{len(sampled_queries)} (run_id: {query_data['run_id']})")

    try:
        # Run all scorers on this query
        result = score_query(
            query=query_data['query'],
            response=query_data['response'],
            country=query_data['country'],
            context=None,  # Context not available from traces
            tool_output=None,  # Tool output not available from traces
            scorers=None  # Use all scorers
        )

        # Store result
        scoring_results.append({
            'run_id': query_data['run_id'],
            'user_id': query_data['user_id'],
            'country': query_data['country'],
            'query_timestamp': query_data['timestamp'],
            'scoring_timestamp': datetime.now(),
            'overall_score': result['overall_score'],
            'pass_rate': result['pass_rate'],
            'passed_count': result['passed_count'],
            'total_count': result['total_count'],
            'verdict': result['verdict'],
            'individual_scores': json.dumps(result['individual_scores'])
        })

        print(f"  Overall Score: {result['overall_score']:.2f} ({result['verdict']})")

    except Exception as e:
        logger.error(f"Error scoring query {query_data['run_id']}: {e}")
        scoring_results.append({
            'run_id': query_data['run_id'],
            'user_id': query_data['user_id'],
            'country': query_data['country'],
            'query_timestamp': query_data['timestamp'],
            'scoring_timestamp': datetime.now(),
            'overall_score': 0.0,
            'pass_rate': 0.0,
            'passed_count': 0,
            'total_count': 0,
            'verdict': 'ERROR',
            'individual_scores': json.dumps({'error': str(e)})
        })

print(f"\n✅ Scoring complete: {len(scoring_results)} results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Store Results in Delta Table

# COMMAND ----------

# Create DataFrame from results
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("query_timestamp", TimestampType(), True),
    StructField("scoring_timestamp", TimestampType(), False),
    StructField("overall_score", DoubleType(), True),
    StructField("pass_rate", DoubleType(), True),
    StructField("passed_count", IntegerType(), True),
    StructField("total_count", IntegerType(), True),
    StructField("verdict", StringType(), True),
    StructField("individual_scores", StringType(), True)
])

results_df = spark.createDataFrame(scoring_results, schema)

# Write to Delta table (append mode)
results_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(SCORING_TABLE)

print(f"✅ Stored {results_df.count()} scoring results in {SCORING_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Summary Statistics

# COMMAND ----------

# Query the table for recent stats
scoring_df = spark.table(SCORING_TABLE)

# Overall statistics
total_scored = scoring_df.count()
recent_scored = scoring_df.filter(col("scoring_timestamp") >= expr(f"current_timestamp() - INTERVAL '{LOOKBACK_HOURS}' HOUR")).count()

print(f"\n📊 Scoring Statistics:")
print(f"  Total Queries Scored (all time): {total_scored}")
print(f"  Queries Scored (last {LOOKBACK_HOURS}h): {recent_scored}")

# Average scores by country
country_stats = scoring_df \
    .groupBy("country") \
    .agg(
        {"overall_score": "avg", "pass_rate": "avg"}
    ) \
    .orderBy(col("country"))

print(f"\n📈 Average Scores by Country:")
country_stats.show()

# Verdict distribution
verdict_stats = scoring_df \
    .groupBy("verdict") \
    .count() \
    .orderBy(col("count").desc())

print(f"\n🎯 Verdict Distribution:")
verdict_stats.show()

# Recent trend (last 7 days)
trend_df = scoring_df \
    .filter(col("scoring_timestamp") >= expr("current_timestamp() - INTERVAL '7' DAY")) \
    .withColumn("day", date_trunc("day", col("scoring_timestamp"))) \
    .groupBy("day") \
    .agg(
        {"overall_score": "avg", "pass_rate": "avg"}
    ) \
    .orderBy(col("day"))

print(f"\n📉 Quality Trend (last 7 days):")
trend_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Alerting (Optional)

# COMMAND ----------

# Check for quality degradation
recent_avg_score = results_df.agg({"overall_score": "avg"}).collect()[0][0]

# Define threshold
QUALITY_THRESHOLD = 0.6

if recent_avg_score < QUALITY_THRESHOLD:
    print(f"⚠️ ALERT: Quality score below threshold!")
    print(f"  Current Score: {recent_avg_score:.2f}")
    print(f"  Threshold: {QUALITY_THRESHOLD}")

    # In production, send alert via email/Slack/PagerDuty
    # Example:
    # dbutils.jobs.taskValues.set(key="alert_triggered", value=True)
    # dbutils.jobs.taskValues.set(key="alert_message", value=f"Quality score {recent_avg_score:.2f} below threshold {QUALITY_THRESHOLD}")
else:
    print(f"✅ Quality score OK: {recent_avg_score:.2f} (threshold: {QUALITY_THRESHOLD})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job Completion Summary
# MAGIC
# MAGIC This automated scoring job:
# MAGIC 1. ✅ Queried production traces from MLflow
# MAGIC 2. ✅ Sampled queries based on configured rate
# MAGIC 3. ✅ Ran 5 automated scorers on each query
# MAGIC 4. ✅ Stored results in Delta table
# MAGIC 5. ✅ Generated summary statistics
# MAGIC 6. ✅ Checked for quality alerts
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - **View results** in Streamlit app Observability tab
# MAGIC - **Set up Databricks job** to run this notebook on schedule (every 6 hours recommended)
# MAGIC - **Configure alerting** based on quality score thresholds
# MAGIC - **Monitor trends** over time to detect quality degradation
# MAGIC
# MAGIC **Scheduling Instructions:**
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service.jobs import *
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC job = w.jobs.create(
# MAGIC     name="pension-advisor-quality-scoring",
# MAGIC     tasks=[
# MAGIC         Task(
# MAGIC             task_key="automated_scoring",
# MAGIC             notebook_task=NotebookTask(
# MAGIC                 notebook_path="/path/to/09-automated-scoring-job",
# MAGIC                 base_parameters={
# MAGIC                     "sampling_rate": "0.1",
# MAGIC                     "lookback_hours": "6"
# MAGIC                 }
# MAGIC             ),
# MAGIC             new_cluster=ClusterSpec(
# MAGIC                 spark_version="15.3.x-scala2.12",
# MAGIC                 node_type_id="i3.xlarge",
# MAGIC                 num_workers=2
# MAGIC             )
# MAGIC         )
# MAGIC     ],
# MAGIC     schedule=CronSchedule(
# MAGIC         quartz_cron_expression="0 0 */6 * * ?",  # Every 6 hours
# MAGIC         timezone_id="Australia/Sydney"
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC print(f"✅ Job created: {job.job_id}")
# MAGIC ```
