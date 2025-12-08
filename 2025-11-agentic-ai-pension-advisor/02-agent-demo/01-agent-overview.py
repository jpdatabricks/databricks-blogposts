# Databricks notebook source
# MAGIC %md
# MAGIC # 01: Multi-Country Agent Architecture Overview
# MAGIC
# MAGIC **📚 Navigation:** [Next: 02-Build Agent →]($./02-build-agent)
# MAGIC
# MAGIC This notebook provides an overview of the retirement advisory agent architecture
# MAGIC and how it handles multi-country queries with LLM orchestration.
# MAGIC
# MAGIC **Agent Architecture:**
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    User Query                               │
# MAGIC └────────────────────┬────────────────────────────────────────┘
# MAGIC                      │
# MAGIC                      ▼
# MAGIC         ┌────────────────────────┐
# MAGIC         │   Query Classifier     │ ← LLM (GPT-oss-120B)
# MAGIC         │  - Determines intent   │
# MAGIC         │  - Identifies tools    │
# MAGIC         └────────┬───────────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC         ┌────────────────────────┐
# MAGIC         │   Agent Orchestrator   │ ← LLM (Claude Opus)
# MAGIC         │  - Manages flow        │
# MAGIC         │  - Calls tools         │
# MAGIC         └────────┬───────────────┘
# MAGIC                  │
# MAGIC     ┌────────────┼────────────┐
# MAGIC     ▼            ▼            ▼
# MAGIC ┌────────┐  ┌────────┐  ┌────────┐
# MAGIC │UC Tool │  │UC Tool │  │UC Tool │
# MAGIC │ Tax    │  │Balance │  │Project │
# MAGIC └────┬───┘  └────┬───┘  └────┬───┘
# MAGIC      └───────────┼───────────┘
# MAGIC                  ▼
# MAGIC         ┌────────────────────────┐
# MAGIC         │  Response Builder      │
# MAGIC         │  - Formats response    │
# MAGIC         │  - Adds citations      │
# MAGIC         └────────┬───────────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC         ┌────────────────────────┐
# MAGIC         │   LLM-as-a-Judge       │ ← LLM (Claude Sonnet)
# MAGIC         │  - Validates response  │
# MAGIC         │  - Quality check       │
# MAGIC         └────────┬───────────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC         ┌────────────────────────┐
# MAGIC         │  Validated Response    │
# MAGIC         └────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Components

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys
import os
repo_root = os.path.abspath(
    os.path.join(os.getcwd(), "..")
)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Show the agent modules
from src.agents import orchestrator, context_formatter, response_builder

print("Agent Framework Modules:")
print(f"  ✓ Orchestrator: {orchestrator.__file__}")
print(f"  ✓ Context Formatter: {context_formatter.__file__}")
print(f"  ✓ Response Builder: {response_builder.__file__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multi-Country Support

# COMMAND ----------

from src.country_config import COUNTRY_CONFIGS

import pandas as pd

# Show country configurations
configs = []
for code, config in COUNTRY_CONFIGS.items():
    configs.append({
        'Code': code,
        'Country': config.name,
        'Currency': f"{config.currency_symbol} {config.currency}",
        'Account Type': config.retirement_account_term,
        'Balance Term': config.balance_term
    })

config_df = pd.DataFrame(configs)
display(config_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LLM Configuration
# MAGIC
# MAGIC The agent uses 3 different LLMs for different purposes

# COMMAND ----------

from src.config import MAIN_LLM_ENDPOINT, JUDGE_LLM_ENDPOINT, CLASSIFIER_LLM_ENDPOINT

llm_config = pd.DataFrame([
    {
        'Purpose': 'Main Agent',
        'Model': MAIN_LLM_ENDPOINT,
        'Use Case': 'Response synthesis, reasoning, tool orchestration',
        'Temperature': 0.2
    },
    {
        'Purpose': 'Validation',
        'Model': JUDGE_LLM_ENDPOINT,
        'Use Case': 'Response quality validation (LLM-as-a-Judge)',
        'Temperature': 0.1
    },
    {
        'Purpose': 'Classification',
        'Model': CLASSIFIER_LLM_ENDPOINT,
        'Use Case': 'Query classification (Stage 3 fallback)',
        'Temperature': 0.0
    }
])

display(llm_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool Integration
# MAGIC
# MAGIC The agent can call Unity Catalog functions as tools

# COMMAND ----------

from src.tools import AVAILABLE_TOOLS

# Show available tools
tools_data = []
for tool_key, tool_config in AVAILABLE_TOOLS.items():
    tools_data.append({
        'Country': tool_config.get('country', ''),
        'Tool Type': tool_config.get('tool_id', ''),
        'Description': tool_config.get('description', ''),
        'UC Function': tool_config.get('function', ''),
        'Authority': tool_config.get('authority', '')[:30] + '...' if len(tool_config.get('authority', '')) > 30 else tool_config.get('authority', '')
    })

tools_df = pd.DataFrame(tools_data)
display(tools_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Flow Example

# COMMAND ----------

# Example query flow
query = "How much tax will I pay if I withdraw $50,000 from my super?"
country = "AU"
member_id = "AU001"

print(f"Query: {query}")
print(f"Country: {country}")
print(f"Member: {member_id}\n")

print("Flow:")
print("1. Classifier identifies this as a 'tax_questions' query")
print("2. Orchestrator determines tool needed: calculate_tax")
print("3. Tool executor calls: au_calculate_tax(member_id, 50000, age)")
print("4. Response builder formats result with citations")
print("5. Validator checks response quality")
print("6. User receives validated, country-specific response")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing Pipeline

# COMMAND ----------

from src.agents.orchestrator import AgentOrchestrator

# Show orchestrator flow
orchestrator = AgentOrchestrator()

print("Orchestrator Pipeline:")
print("  1. build_context() - Prepare query context")
print("  2. classify_query() - Determine query type")
print("  3. execute_tools() - Run required calculations")
print("  4. build_response() - Synthesize response")
print("  5. validate_response() - Quality check")
print("  6. log_to_governance() - Audit log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring & Observability

# COMMAND ----------

# Get actual tracked metrics from governance table
from src.config import UNITY_CATALOG, UNITY_SCHEMA

# Set catalog context
spark.sql(f"USE CATALOG {UNITY_CATALOG}")

# Check if we have any governance data
governance_count = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {UNITY_CATALOG}.{UNITY_SCHEMA}.governance
""").collect()[0].cnt

if governance_count > 0:
    # Get actual metrics
    metrics_df = spark.sql(f"""
    SELECT
        COUNT(*) as total_queries,
        AVG(total_time_seconds) as avg_latency,
        SUM(cost) as total_cost,
        AVG(cost) as avg_cost_per_query,
        COUNT(CASE WHEN judge_verdict = 'Pass' THEN 1 END) * 100.0 / COUNT(*) as validation_pass_rate,
        COUNT(CASE WHEN tool_used IS NOT NULL AND tool_used != '' THEN 1 END) as queries_with_tools
    FROM {UNITY_CATALOG}.{UNITY_SCHEMA}.governance
    """).collect()[0]

    print("Tracked Metrics (from actual agent runs):")
    print(f"  ✓ Total queries: {metrics_df['total_queries']}")
    print(f"  ✓ Avg query latency: {metrics_df['avg_latency']:.2f}s")
    print(f"  ✓ Avg cost per query: ${metrics_df['avg_cost_per_query']:.4f}")
    print(f"  ✓ Total cost: ${metrics_df['total_cost']:.4f}")
    print(f"  ✓ Validation pass rate: {metrics_df['validation_pass_rate']:.1f}%")
    print(f"  ✓ Queries using tools: {metrics_df['queries_with_tools']}")
else:
    print("Tracked Metrics (will be populated after agent runs):")
    print("  • Query latency (total_time_seconds)")
    print("  • Tool execution (tool_used)")
    print("  • Cost per query (cost)")
    print("  • Validation pass rate (judge_verdict)")
    print("  • User activity (user_id, country)")
    print("\nRun the agent demo to see actual metrics!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Overview Complete
# MAGIC
# MAGIC You've learned about:
# MAGIC - Agent architecture and flow
# MAGIC - Multi-country support
# MAGIC - LLM configuration
# MAGIC - Tool integration
# MAGIC - Processing pipeline
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - **[02-build-agent]($./02-build-agent)**: See the agent in action
# MAGIC - **[03-tool-integration]($./03-tool-integration)**: Deep dive into tool calling
# MAGIC - **[04-validation]($./04-validation)**: Learn about response validation

# COMMAND ----------

print("✅ Agent architecture overview complete!")
