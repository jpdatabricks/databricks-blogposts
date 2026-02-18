# Databricks notebook source
# MAGIC %md
# MAGIC # 07: Production Monitoring with MLflow Tracing & Automated Scorers
# MAGIC
# MAGIC **📚 Navigation:** [← Previous: 06-AI Guardrails]($./06-ai-guardrails) | [Next: 08-Automated Scoring →]($./08-automated-scoring-job)
# MAGIC
# MAGIC This notebook demonstrates **Production Monitoring** capabilities:
# MAGIC
# MAGIC **1. MLflow Tracing (Already Enabled)**
# MAGIC - Automatic tracing of agent execution via `@mlflow.trace` decorator
# MAGIC - Captures all LLM calls, tool executions, and validation steps
# MAGIC - Visual trace viewer in MLflow UI
# MAGIC - No additional setup required
# MAGIC
# MAGIC **2. Automated Quality Scorers (Optional)**
# MAGIC - Asynchronous background monitoring of production quality
# MAGIC - Built-in scorers: relevance, faithfulness, toxicity
# MAGIC - Custom scorers for country-specific validation
# MAGIC - Sampled evaluation (10%) to control costs
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC User Query
# MAGIC     ↓
# MAGIC @mlflow.trace → Automatic tracing (real-time)
# MAGIC     ↓
# MAGIC Agent Processing
# MAGIC     ↓
# MAGIC LLM-as-a-Judge → Real-time validation + retry
# MAGIC     ↓
# MAGIC Response to User
# MAGIC     ↓
# MAGIC Automated Scorers → Background monitoring (async, sampled)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: View MLflow Traces

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Query with Tracing
# MAGIC
# MAGIC Let's run a query and see the trace captured automatically.

# COMMAND ----------

import logging
# Suppress Streamlit warnings when running in notebook context
logging.getLogger("streamlit").setLevel(logging.ERROR)
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)
logging.getLogger("streamlit.runtime.state.session_state_proxy").setLevel(logging.ERROR)

import sys
import os

# Add project root to path
repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from src.agent_processor import agent_query
from src.config import MLFLOW_PROD_EXPERIMENT_PATH
import mlflow

# Set experiment for tracing
mlflow.set_experiment(MLFLOW_PROD_EXPERIMENT_PATH)

print("✅ Tracing enabled via @mlflow.trace decorator")
print(f"   Experiment: {MLFLOW_PROD_EXPERIMENT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Test Query

# COMMAND ----------

import uuid
from src.utils.lakehouse import execute_sql_query
from src.config import get_member_profiles_table_path

# Get real member ID
def get_test_member_id(country='AU'):
    """Get first real member ID for testing"""
    try:
        table_path = get_member_profiles_table_path()
        query = f"SELECT member_id FROM {table_path} WHERE country = '{country}' LIMIT 1"
        df = execute_sql_query(query)
        if not df.empty:
            return df['member_id'].iloc[0]
    except:
        pass
    return f"{country}001"

test_member_id = get_test_member_id('AU')

# Run a test query - tracing happens automatically!
result = agent_query(
    user_id=test_member_id,
    session_id=str(uuid.uuid4())[:8],
    country='AU',
    query_string='What is my preservation age?',
    validation_mode='llm_judge',
    enable_observability=True
)

print("\n✅ Query completed!")
print(f"   Member ID: {test_member_id}")
print(f"   Answer: {result.get('answer', 'N/A')[:100]}...")
print(f"   Cost: ${result.get('cost', 0.0):.6f}")
if 'error' in result:
    print(f"   Error: {result['error']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Trace in MLflow
# MAGIC
# MAGIC The trace is automatically captured! To view it:
# MAGIC
# MAGIC 1. Go to MLflow Experiments (sidebar link in app)
# MAGIC 2. Find the latest run
# MAGIC 3. Click on the "Traces" tab
# MAGIC 4. See the full execution graph:
# MAGIC    - Agent query span
# MAGIC    - LLM calls (synthesis, validation)
# MAGIC    - Tool executions
# MAGIC    - Timing for each step
# MAGIC
# MAGIC **What the trace shows:**
# MAGIC - 🕐 Total execution time
# MAGIC - 🔄 Each phase (classify, reason, act, validate)
# MAGIC - 🤖 LLM calls with inputs/outputs
# MAGIC - 🛠️ Tool executions
# MAGIC - ✅ Validation steps

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Automated Quality Scorers (Optional)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What Are Automated Scorers?
# MAGIC
# MAGIC Automated scorers run **asynchronously in the background** to evaluate production quality:
# MAGIC
# MAGIC **vs. Our LLM-as-a-Judge:**
# MAGIC - **LLM-as-a-Judge**: Real-time quality gate (blocks bad responses)
# MAGIC - **Automated Scorers**: Background monitoring (detects trends, drift)
# MAGIC
# MAGIC **Both are needed!** They complement each other.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Requirements
# MAGIC
# MAGIC Automated scorers require:
# MAGIC 1. **MLflow Experiments** with traces ✅ (already enabled)
# MAGIC 2. **Inference table** or logged data
# MAGIC 3. **Databricks job** to run scorers on schedule
# MAGIC
# MAGIC **Note:** This is **optional** and only needed if you want background quality monitoring.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Custom Country Compliance Scorer

# COMMAND ----------

from typing import Dict, Any
import mlflow

@mlflow.trace(name="country_compliance_scorer", span_type="EVALUATOR")
def country_compliance_scorer(
    query: str,
    response: str,
    country: str,
    **kwargs
) -> Dict[str, Any]:
    """
    Custom scorer that validates country-specific compliance.

    Checks:
    - AU: Mentions preservation age (55-60)
    - US: Mentions age 59.5
    - UK: Mentions State Pension age
    - IN: Mentions EPF age 58

    Returns:
        dict with score (0-1), pass/fail, and reasoning
    """
    compliance_rules = {
        'AU': ['preservation age', '60', '55'],
        'US': ['59.5', '401(k)', 'IRA'],
        'UK': ['State Pension', '66', '67'],
        'IN': ['EPF', '58', 'Aadhaar']
    }

    keywords = compliance_rules.get(country, [])
    response_lower = response.lower()

    matches = sum(1 for keyword in keywords if keyword.lower() in response_lower)
    score = min(1.0, matches / len(keywords)) if keywords else 1.0

    passed = score >= 0.3  # At least 30% of keywords present

    return {
        'score': score,
        'passed': passed,
        'confidence': score,
        'reasoning': f"Found {matches}/{len(keywords)} country-specific keywords for {country}",
        'verdict': 'PASS' if passed else 'FAIL'
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Custom Scorer

# COMMAND ----------

# Test the custom scorer
test_response = "Your preservation age is 60. As someone born after 1964, you can access your super at age 60 upon retirement."

scorer_result = country_compliance_scorer(
    query="What is my preservation age?",
    response=test_response,
    country="AU"
)

print("Custom Scorer Result:")
print(f"  Score: {scorer_result['score']:.2f}")
print(f"  Passed: {scorer_result['passed']}")
print(f"  Reasoning: {scorer_result['reasoning']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Automated Scoring Job (Optional)
# MAGIC
# MAGIC To run scorers automatically on schedule:
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service.jobs import *
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC # Create job that runs scorers every 6 hours
# MAGIC job = w.jobs.create(
# MAGIC     name="pension-advisor-quality-scoring",
# MAGIC     tasks=[
# MAGIC         Task(
# MAGIC             task_key="score_production_queries",
# MAGIC             notebook_task=NotebookTask(
# MAGIC                 notebook_path="/path/to/scoring-notebook",
# MAGIC                 base_parameters={"sampling_rate": "0.1"}
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
# MAGIC         timezone_id="UTC"
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC print(f"✅ Scoring job created: {job.job_id}")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Monitoring Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Trace Metrics from MLflow

# COMMAND ----------

import mlflow
from datetime import datetime, timedelta

# Get traces from last 24 hours
client = mlflow.MlflowClient()

# Get recent runs with traces
recent_runs = client.search_runs(
    experiment_ids=[mlflow.get_experiment_by_name(MLFLOW_PROD_EXPERIMENT_PATH).experiment_id],
    filter_string=f"attributes.start_time >= {int((datetime.now() - timedelta(days=1)).timestamp() * 1000)}",
    max_results=100,
    order_by=["start_time DESC"]
)

print(f"📊 Found {len(recent_runs)} runs in last 24 hours")

# Analyze trace metrics
if recent_runs:
    # Use correct metric names from observability module (with dots, not underscores)
    total_cost = sum(run.data.metrics.get('total.cost_usd', 0) for run in recent_runs)
    avg_latency = sum(run.data.metrics.get('total.duration_sec', 0) for run in recent_runs) / len(recent_runs)
    avg_confidence = sum(run.data.metrics.get('validation.confidence', 0) for run in recent_runs) / len(recent_runs)

    print(f"\n📈 Metrics Summary:")
    print(f"  Total Cost: ${total_cost:.4f}")
    print(f"  Avg Latency: {avg_latency:.2f}s")
    print(f"  Avg Validation Confidence: {avg_confidence:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Production Monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ### What We Covered
# MAGIC
# MAGIC **Part 1: MLflow Tracing** ✅
# MAGIC - Enabled via `@mlflow.trace` decorator
# MAGIC - Automatic capture of all agent execution
# MAGIC - Visual trace viewer in MLflow UI
# MAGIC - No additional code changes needed
# MAGIC
# MAGIC **Part 2: Automated Scorers** 📊
# MAGIC - Custom country compliance scorer example
# MAGIC - Optional background quality monitoring
# MAGIC - Runs asynchronously, doesn't block responses
# MAGIC - 10% sampling to control costs
# MAGIC
# MAGIC **Part 3: Monitoring Dashboard** 📈
# MAGIC - Query traces from MLflow
# MAGIC - Analyze cost, latency, confidence trends
# MAGIC - Detect quality drift over time
# MAGIC
# MAGIC ### Comparison: Our Approach vs. Databricks Native
# MAGIC
# MAGIC | Capability | Status | Implementation |
# MAGIC |------------|--------|----------------|
# MAGIC | **Real-time quality gate** | ✅ | Our LLM-as-a-Judge |
# MAGIC | **Execution tracing** | ✅ | @mlflow.trace decorator |
# MAGIC | **Background monitoring** | ⚠️ Optional | Automated scorers |
# MAGIC | **Trend analysis** | ✅ | MLflow metrics + traces |
# MAGIC | **Custom scorers** | ✅ | country_compliance_scorer |
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 1. **Run this notebook** in Databricks to see tracing in action
# MAGIC 2. **View traces** in MLflow UI for detailed execution analysis
# MAGIC 3. **[08-automated-scoring-job]($./08-automated-scoring-job)**: Set up automated quality scoring
# MAGIC 4. **Monitor trends** in MLflow experiments dashboard
# MAGIC
# MAGIC **For most use cases, MLflow Tracing + LLM-as-a-Judge real-time validation is sufficient!**
