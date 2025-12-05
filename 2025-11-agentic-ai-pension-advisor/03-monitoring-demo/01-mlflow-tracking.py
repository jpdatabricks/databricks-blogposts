# Databricks notebook source
# MAGIC %md
# MAGIC # MLflow Experiment Tracking
# MAGIC
# MAGIC Track agent performance, costs, and quality metrics using MLflow.

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

from src.config import MLFLOW_PROD_EXPERIMENT_PATH
import mlflow
import pandas as pd

print(f"✓ MLflow experiment: {MLFLOW_PROD_EXPERIMENT_PATH}")
print(f"✓ Tracking URI: databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Experiment Runs

# COMMAND ----------

# Set experiment
mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(MLFLOW_PROD_EXPERIMENT_PATH)

print(f"Experiment path: {MLFLOW_PROD_EXPERIMENT_PATH}")

# Get recent runs
try:
    runs = mlflow.search_runs(
        experiment_names=[MLFLOW_PROD_EXPERIMENT_PATH],
        max_results=10,
        order_by=["start_time DESC"]
    )

    if len(runs) > 0:
        print(f"\nFound {len(runs)} recent runs")

        # Select relevant columns
        display_cols = []
        for col in ['run_id', 'start_time', 'status', 'metrics.total.cost_usd', 'metrics.total.duration_sec', 'params.country', 'params.user_id']:
            if col in runs.columns:
                display_cols.append(col)

        display(runs[display_cols] if display_cols else runs.head())
    else:
        print("\nNo runs found yet. Run the agent demo notebooks to generate tracking data.")
except Exception as e:
    print(f"\nExperiment not found or no runs yet: {e}")
    print("Run the 02-agent-demo notebooks to generate MLflow data.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tracked Metrics

# COMMAND ----------

metrics = {
    "Performance": ["query_latency", "tool_execution_time", "total_time"],
    "Cost": ["total_cost", "llm_cost", "token_count"],
    "Quality": ["validation_pass_rate", "confidence_score", "tool_success_rate"]
}

for category, metric_list in metrics.items():
    print(f"\n{category}:")
    for metric in metric_list:
        print(f"  ✓ {metric}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Analysis

# COMMAND ----------

# Analyze costs across runs
try:
    runs = mlflow.search_runs(
        experiment_names=[MLFLOW_PROD_EXPERIMENT_PATH],
        max_results=50
    )

    if len(runs) > 0 and 'metrics.total.cost_usd' in runs.columns:
        total_cost = runs['metrics.total.cost_usd'].sum()
        avg_cost = runs['metrics.total.cost_usd'].mean()
        max_cost = runs['metrics.total.cost_usd'].max()

        print(f"Cost Analysis (last {len(runs)} runs):")
        print(f"  Total: ${total_cost:.4f}")
        print(f"  Average per query: ${avg_cost:.4f}")
        print(f"  Max: ${max_cost:.4f}")

        # Cost breakdown by component
        if 'metrics.synthesis.cost_usd' in runs.columns:
            synthesis_cost = runs['metrics.synthesis.cost_usd'].sum()
            validation_cost = runs['metrics.validation.cost_usd'].sum()
            classification_cost = runs['metrics.classification.cost_usd'].sum()

            print(f"\nCost Breakdown:")
            print(f"  Synthesis (Opus 4): ${synthesis_cost:.4f} ({synthesis_cost/total_cost*100:.1f}%)")
            print(f"  Validation (Sonnet 4): ${validation_cost:.4f} ({validation_cost/total_cost*100:.1f}%)")
            print(f"  Classification: ${classification_cost:.4f} ({classification_cost/total_cost*100:.1f}%)")

        # Cost by country
        if 'params.country' in runs.columns:
            country_costs = runs.groupby('params.country')['metrics.total.cost_usd'].agg(['sum', 'mean', 'count'])
            print(f"\nCost by Country:")
            display(country_costs)
    else:
        print("No cost data available yet. Expected metric: metrics.total.cost_usd")
except Exception as e:
    print(f"Cost analysis not available: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Analysis

# COMMAND ----------

# Analyze latency across runs
try:
    runs = mlflow.search_runs(
        experiment_names=[MLFLOW_PROD_EXPERIMENT_PATH],
        max_results=50
    )

    if len(runs) > 0 and 'metrics.total.duration_sec' in runs.columns:
        avg_time = runs['metrics.total.duration_sec'].mean()
        p95_time = runs['metrics.total.duration_sec'].quantile(0.95)
        p99_time = runs['metrics.total.duration_sec'].quantile(0.99)

        print(f"Performance Analysis (last {len(runs)} runs):")
        print(f"  Average latency: {avg_time:.2f}s")
        print(f"  P95 latency: {p95_time:.2f}s")
        print(f"  P99 latency: {p99_time:.2f}s")

        # Performance breakdown by phase
        if 'metrics.synthesis.duration_sec' in runs.columns:
            synthesis_time = runs['metrics.synthesis.duration_sec'].mean()
            validation_time = runs['metrics.validation.duration_sec'].mean()

            print(f"\nAverage Time by Phase:")
            print(f"  Synthesis: {synthesis_time:.2f}s")
            print(f"  Validation: {validation_time:.2f}s")

        # Performance by country
        if 'params.country' in runs.columns:
            country_perf = runs.groupby('params.country')['metrics.total.duration_sec'].agg(['mean', 'min', 'max', 'count'])
            print(f"\nPerformance by Country:")
            display(country_perf)
    else:
        print("No performance data available yet. Expected metric: metrics.total.duration_sec")
except Exception as e:
    print(f"Performance analysis not available: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## AgentMonitor Class
# MAGIC
# MAGIC The agent automatically tracks metrics using the AgentMonitor class

# COMMAND ----------

from src.monitoring import AgentMonitor

print("AgentMonitor tracks:")
print("  ✓ Query performance (latency, tokens)")
print("  ✓ Cost per query (LLM + tool calls)")
print("  ✓ Validation metrics (pass rate, confidence)")
print("  ✓ Tool usage (success rate, execution time)")
print("  ✓ Business metrics (queries by country)")
print("\nAll metrics automatically logged to MLflow during agent execution")

# COMMAND ----------

print("✅ MLflow tracking active")
print(f"   Experiment: {MLFLOW_PROD_EXPERIMENT_PATH}")
print("   View runs in Databricks MLflow UI")
