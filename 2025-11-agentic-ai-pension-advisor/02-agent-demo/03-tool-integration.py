# Databricks notebook source
# MAGIC %md
# MAGIC # 03: Tool Integration Deep Dive
# MAGIC
# MAGIC **📚 Navigation:** [← Previous: 02-Build Agent]($./02-build-agent) | [Next: 04-Validation →]($./04-validation)
# MAGIC
# MAGIC This notebook explores how the agent integrates with Unity Catalog functions
# MAGIC as tools for retirement calculations.
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC - Tool configuration
# MAGIC - Tool executor implementation
# MAGIC - UC function calling
# MAGIC - Error handling
# MAGIC - Result formatting

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
from pathlib import Path

# Determine repo root - works in both local and Databricks environments
current_dir = Path(os.getcwd())
# Try to find repo root by looking for 'src' directory
repo_root = current_dir.parent if (current_dir.parent / 'src').exists() else current_dir

print(f"📂 Setup:")
print(f"  Current directory: {current_dir}")
print(f"  Repo root: {repo_root}")
print(f"  Config path: {repo_root / 'src' / 'config' / 'config.yaml'}")
print(f"  Config exists: {(repo_root / 'src' / 'config' / 'config.yaml').exists()}")

if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.tools import AVAILABLE_TOOLS, call_individual_tool
from src.config import SQL_WAREHOUSE_ID
import pandas as pd

print("\n✓ Tool modules imported")
print(f"  Warehouse ID: {SQL_WAREHOUSE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Available Tools

# COMMAND ----------

# Display all available tools
tools_data = []
for tool_name, tool_config in AVAILABLE_TOOLS.items():
    tools_data.append({
        'Tool': tool_name,
        'Country': tool_config.get('country', ''),
        'UC Function': tool_config.get('function', ''),
        'Description': tool_config.get('description', '')[:80] + '...'
    })

tools_df = pd.DataFrame(tools_data)
display(tools_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool Configuration
# MAGIC
# MAGIC Tools are defined in `tools/tool_config.yaml`

# COMMAND ----------

import yaml

with open('../src/tools/tool_config.yaml', 'r') as f:
    tool_config = yaml.safe_load(f)

print("Tool Configuration Structure:")
print(f"  Countries: {list(tool_config.get('countries', {}).keys())}")

# Count total tools across all countries
total_tools = sum(len(country_data.get('tools', {})) for country_data in tool_config.get('countries', {}).values())
print(f"  Total tools: {total_tools}")

# Show one example from AU
au_tools = tool_config['countries']['AU']['tools']
example_tool_id = list(au_tools.keys())[0]
example_tool = au_tools[example_tool_id]

print(f"\nExample Tool: {example_tool_id}")
print(f"  Name: {example_tool['name']}")
print(f"  UC Function: {example_tool['uc_function']}")
print(f"  Authority: {example_tool['authority']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute a Tool
# MAGIC
# MAGIC Call a UC function directly using call_individual_tool

# COMMAND ----------

# Example: Calculate tax for Australian member
result = call_individual_tool(
    tool_id="tax",
    member_id="AU004",
    withdrawal_amount=50000,
    country="AU",
    warehouse_id=SQL_WAREHOUSE_ID
)

print("Tool Execution Result:")
if "error" in result:
    print(f"  Error: {result['error']}")
else:
    print(f"  Tool: {result.get('tool_name', 'N/A')}")
    print(f"  Duration: {result.get('duration', 0):.3f}s")
    print(f"  Authority: {result.get('authority', 'N/A')}")
    print(f"\nCalculation Result:")
    print(f"  {result.get('calculation', 'N/A')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multi-Country Tool Execution

# COMMAND ----------

# Test tools across all countries
test_cases = [
    {
        "tool_id": "tax",
        "country": "AU",
        "member_id": "AU004",
        "withdrawal_amount": 50000,
        "description": "AU Tax Calculation"
    },
    {
        "tool_id": "tax",
        "country": "US",
        "member_id": "US001",
        "withdrawal_amount": 30000,
        "description": "US Tax Calculation"
    },
    {
        "tool_id": "tax",
        "country": "UK",
        "member_id": "UK001",
        "withdrawal_amount": 40000,
        "description": "UK Tax Calculation"
    },
    {
        "tool_id": "tax",
        "country": "IN",
        "member_id": "IN001",
        "withdrawal_amount": 500000,
        "description": "IN Tax Calculation"
    }
]

# Execute all test cases
results = []
for test in test_cases:
    result = call_individual_tool(
        tool_id=test['tool_id'],
        member_id=test['member_id'],
        withdrawal_amount=test['withdrawal_amount'],
        country=test['country'],
        warehouse_id=SQL_WAREHOUSE_ID
    )

    results.append({
        'Test': test['description'],
        'Tool': result.get('tool_name', 'N/A'),
        'Success': 'error' not in result,
        'Duration': f"{result.get('duration', 0):.3f}s",
        'Result Preview': str(result.get('calculation', result.get('error', '')))[:50] + '...'
    })

results_df = pd.DataFrame(results)
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Handling

# COMMAND ----------

# Test error handling with invalid parameters
error_result = call_individual_tool(
    tool_id="tax",
    member_id="INVALID_ID",  # Invalid member
    withdrawal_amount=50000,
    country="AU",
    warehouse_id=SQL_WAREHOUSE_ID
)

print("Error Handling:")
print(f"  Success: {'error' not in error_result}")
if "error" in error_result:
    print(f"  Error: {error_result.get('error', 'Unknown error')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool Result Format
# MAGIC
# MAGIC Tool results are structured dictionaries with calculation results and metadata

# COMMAND ----------

# Example tool result structure
example_tool_result = {
    "tool_name": "ATO Tax Calculator",
    "tool_id": "tax",
    "uc_function": "au_calculate_tax",
    "authority": "Australian Taxation Office",
    "calculation": "Based on your withdrawal...",
    "citations": [{"code": "AU-TAX-001", "title": "Tax on Super Withdrawals"}],
    "duration": 0.342
}

print("Tool Result Structure:")
for key, value in example_tool_result.items():
    print(f"  {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool Calling Flow

# COMMAND ----------

# Demonstrate the full tool calling flow
def demonstrate_tool_flow(query, country, member_id):
    """Show the complete tool calling flow"""

    print(f"Query: {query}")
    print(f"Country: {country}, Member: {member_id}\n")

    # 1. Classifier determines tools needed
    print("1. Classifier: Identifies required tools")
    required_tools = ["projection", "tax"]
    print(f"   Tools needed: {required_tools}\n")

    # 2. Execute tools
    print("2. Tool Executor: Calls UC functions")
    results = []
    for tool in required_tools:
        result = call_individual_tool(
            tool_id=tool,
            member_id=member_id,
            withdrawal_amount=50000 if tool == "tax" else None,
            country=country,
            warehouse_id=SQL_WAREHOUSE_ID
        )
        results.append(result)
        if "error" not in result:
            print(f"   ✓ {tool}: {result.get('duration', 0):.3f}s")
        else:
            print(f"   ✗ {tool}: {result.get('error', 'Failed')}")

    print("\n3. Response Builder: Formats results")
    print("   Creates natural language response with tool data\n")

    return results

# Run demonstration
demo_results = demonstrate_tool_flow(
    "How much tax will I pay on a $50,000 withdrawal?",
    "AU",
    "AU004"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool Performance Metrics

# COMMAND ----------

# Analyze tool performance
import time

# Benchmark tool execution
benchmark_results = []

for i in range(5):
    result = call_individual_tool(
        tool_id="projection",
        member_id="AU001",
        withdrawal_amount=None,
        country="AU",
        warehouse_id=SQL_WAREHOUSE_ID
    )

    if "duration" in result:
        benchmark_results.append(result['duration'])

if benchmark_results:
    avg_duration = sum(benchmark_results) / len(benchmark_results)
    min_duration = min(benchmark_results)
    max_duration = max(benchmark_results)

    print(f"Tool Performance (projection, n=5):")
    print(f"  Average: {avg_duration:.3f}s")
    print(f"  Min: {min_duration:.3f}s")
    print(f"  Max: {max_duration:.3f}s")
else:
    print("Benchmark failed - no results collected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool Integration Complete
# MAGIC
# MAGIC You've learned about:
# MAGIC - Tool configuration and executor
# MAGIC - UC function integration
# MAGIC - Multi-country tool execution
# MAGIC - Error handling
# MAGIC - Result formatting
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - **[04-validation]($./04-validation)**: LLM-as-a-Judge validation
# MAGIC - **[05-mlflow-deployment]($./05-mlflow-deployment)**: Package agent as MLflow model
# MAGIC - **[06-ai-guardrails]($./06-ai-guardrails)**: Add AI safety guardrails

# COMMAND ----------

print("✅ Tool integration complete!")
print("   Tools tested across 4 countries")
print("   Error handling verified")
print("   Ready for agent orchestration")
