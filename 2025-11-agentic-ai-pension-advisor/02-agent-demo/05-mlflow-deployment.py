# Databricks notebook source
# MAGIC %md
# MAGIC # 05: MLflow Model Deployment - Pension Advisor Agent
# MAGIC
# MAGIC **📚 Navigation:** [← Previous: 04-Validation]($./04-validation) | [Next: 06-AI Guardrails →]($./06-ai-guardrails)
# MAGIC
# MAGIC This notebook demonstrates MLflow Model Packaging
# MAGIC
# MAGIC **What This Achieves:**
# MAGIC - Package agent as MLflow PyFunc model
# MAGIC - Register to Unity Catalog for versioning
# MAGIC - Enable batch inference at scale
# MAGIC - Support model aliases (@champion, @challenger)
# MAGIC - Production-ready batch processing 
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC agent_processor.py → MLflow PyFunc Wrapper → Unity Catalog Model Registry
# MAGIC                                            ↓
# MAGIC                                      @champion alias
# MAGIC                                            ↓
# MAGIC                                  Batch Inference / Serving
# MAGIC ```
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Local Model Testing
# MAGIC 2. Model Registration to Unity Catalog
# MAGIC 3. Alias Management (@champion, @challenger)
# MAGIC 4. Batch Inference Testing
# MAGIC 5. Performance Analysis

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
import uuid
import pandas as pd
import mlflow

# Add project root to path
repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from src.mlflow_model import (
    PensionAdvisorModel,
    log_model_to_mlflow,
    set_model_alias,
    load_model_from_registry,
    run_batch_inference
)
from src.config import UNITY_CATALOG, UNITY_SCHEMA

print("✓ MLflow model modules imported")
print(f"  Unity Catalog: {UNITY_CATALOG}")
print(f"  Schema: {UNITY_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Local Model Instance
# MAGIC
# MAGIC Test the model locally before registering to Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Real Member IDs from Database
# MAGIC
# MAGIC Fetch actual member IDs instead of using hardcoded test IDs.

# COMMAND ----------

from src.utils.lakehouse import execute_sql_query, get_member_by_id
from src.config import get_member_profiles_table_path

# Fetch sample member IDs by country
def get_sample_members():
    """Get real member IDs from database"""
    table_path = get_member_profiles_table_path()

    query = f"""
    SELECT country, member_id, name
    FROM {table_path}
    GROUP BY country, member_id, name
    ORDER BY country, member_id
    """

    df = execute_sql_query(query)

    # Get first member from each country for testing
    members = {}
    for country in ['AU', 'US', 'UK', 'IN']:
        country_members = df[df['country'] == country]['member_id'].tolist()
        if country_members:
            members[country] = country_members[:3]  # Get first 3 members per country

    return members

# Get real member IDs
SAMPLE_MEMBERS = get_sample_members()

print("✅ Sample member IDs loaded from database:")
for country, member_list in SAMPLE_MEMBERS.items():
    print(f"  {country}: {', '.join(member_list[:3])}")

# COMMAND ----------

# Create model instance
model = PensionAdvisorModel()

# Load dependencies
model.load_context(None)

print("✅ Model instance created and context loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Single Prediction
# MAGIC
# MAGIC Test with a single query.

# COMMAND ----------

# Create test input using real member ID
test_member_id = SAMPLE_MEMBERS['AU'][0] if 'AU' in SAMPLE_MEMBERS and SAMPLE_MEMBERS['AU'] else 'AU001'

test_input = pd.DataFrame([{
    'user_id': test_member_id,
    'session_id': str(uuid.uuid4())[:8],
    'country': 'AU',
    'query': 'What is my preservation age?',
    'validation_mode': 'llm_judge',
    'enable_observability': False
}])

print("Test Input:")
print(test_input)

# Run prediction
predictions = model.predict(None, test_input)

print("\nPredictions:")
print(predictions[['user_id', 'query', 'answer', 'cost', 'latency_ms', 'blocked']])

if predictions['blocked'].values[0]:
    print(f"\n🚫 Query blocked: {predictions['violations'].values[0]}")
else:
    print(f"\n✅ Query processed successfully")
    print(f"   Answer: {predictions['answer'].values[0][:100]}...")
    print(f"   Cost: ${predictions['cost'].values[0]:.6f}")
    print(f"   Latency: {predictions['latency_ms'].values[0]:.0f}ms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Batch Prediction
# MAGIC
# MAGIC Test with multiple queries including normal and blocked scenarios.

# COMMAND ----------

# Create batch test input using real member IDs
test_batch = pd.DataFrame([
    {
        'user_id': SAMPLE_MEMBERS['AU'][0] if 'AU' in SAMPLE_MEMBERS and len(SAMPLE_MEMBERS['AU']) > 0 else 'AU001',
        'session_id': str(uuid.uuid4())[:8],
        'country': 'AU',
        'query': 'What is my preservation age?',
        'validation_mode': 'llm_judge',
        'enable_observability': False
    },
    {
        'user_id': SAMPLE_MEMBERS['AU'][1] if 'AU' in SAMPLE_MEMBERS and len(SAMPLE_MEMBERS['AU']) > 1 else 'AU002',
        'session_id': str(uuid.uuid4())[:8],
        'country': 'AU',
        'query': 'Can I access my super before retirement?',
        'validation_mode': 'llm_judge',
        'enable_observability': False
    },
    {
        'user_id': SAMPLE_MEMBERS['AU'][2] if 'AU' in SAMPLE_MEMBERS and len(SAMPLE_MEMBERS['AU']) > 2 else 'AU003',
        'session_id': str(uuid.uuid4())[:8],
        'country': 'AU',
        'query': 'My SSN is 123-45-6789, what is my balance?',  # Should be blocked
        'validation_mode': 'llm_judge',
        'enable_observability': False
    },
    {
        'user_id': SAMPLE_MEMBERS['US'][0] if 'US' in SAMPLE_MEMBERS and len(SAMPLE_MEMBERS['US']) > 0 else 'US001',
        'session_id': str(uuid.uuid4())[:8],
        'country': 'US',
        'query': 'What is the early withdrawal penalty?',
        'validation_mode': 'llm_judge',
        'enable_observability': False
    },
])

print(f"Processing batch of {len(test_batch)} queries...")

# Run batch prediction
batch_predictions = model.predict(None, test_batch)

print("\nBatch Predictions Summary:")
print(batch_predictions[['user_id', 'query', 'blocked', 'cost', 'latency_ms']])

# Summary statistics
print(f"\n📊 Batch Statistics:")
print(f"  Total queries: {len(batch_predictions)}")
print(f"  Blocked: {batch_predictions['blocked'].sum()}")
print(f"  Successful: {(~batch_predictions['blocked']).sum()}")
print(f"  Total cost: ${batch_predictions['cost'].sum():.6f}")
print(f"  Avg latency: {batch_predictions['latency_ms'].mean():.0f}ms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Register Model to Unity Catalog
# MAGIC
# MAGIC Package and register the model to Unity Catalog for versioning and deployment.

# COMMAND ----------

# Define model details
MODEL_NAME = "pension_advisor"
FULL_MODEL_NAME = f"{UNITY_CATALOG}.{UNITY_SCHEMA}.{MODEL_NAME}"

print(f"Registering model: {FULL_MODEL_NAME}")
print(f"This will:")
print(f"  1. Package the agent as MLflow PyFunc model")
print(f"  2. Infer input/output schema")
print(f"  3. Register to Unity Catalog")
print(f"  4. Create version 1")

# Create example input for signature
example_input = pd.DataFrame([{
    'user_id': 'AU001',
    'session_id': 'example-session',
    'country': 'AU',
    'query': 'What is my preservation age?',
    'validation_mode': 'llm_judge',
    'enable_observability': False
}])

# Log and register model
try:
    model_uri = log_model_to_mlflow(
        model_name=MODEL_NAME,
        catalog=UNITY_CATALOG,
        schema=UNITY_SCHEMA,
        description="Multi-country pension advisor with AI guardrails and LLM-as-a-Judge validation",
        example_input=example_input
    )
    print(f"\n✅ Model registered successfully!")
    print(f"   Model URI: {model_uri}")
except Exception as e:
    print(f"\n⚠️  Model registration: {str(e)}")
    print(f"   This is expected if model already exists")
    model_uri = f"models:/{FULL_MODEL_NAME}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Set Model Alias (@champion)
# MAGIC
# MAGIC Set the @champion alias to the latest version for production use.

# COMMAND ----------

try:
    # Set champion alias to latest version
    set_model_alias(
        model_name=MODEL_NAME,
        catalog=UNITY_CATALOG,
        schema=UNITY_SCHEMA,
        alias="champion",
        version=None  # Uses latest
    )
    print("✅ @champion alias set successfully")
except Exception as e:
    print(f"⚠️  Setting alias: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: Load Model from Unity Catalog
# MAGIC
# MAGIC Load the model using the @champion alias.

# COMMAND ----------

try:
    # Load model from registry
    loaded_model = load_model_from_registry(
        model_name=MODEL_NAME,
        catalog=UNITY_CATALOG,
        schema=UNITY_SCHEMA,
        alias="champion"
    )

    print("✅ Model loaded from Unity Catalog")

    # Test loaded model using real member ID
    test_member = SAMPLE_MEMBERS['AU'][0] if 'AU' in SAMPLE_MEMBERS and SAMPLE_MEMBERS['AU'] else 'AU001'
    test_query = pd.DataFrame([{
        'user_id': test_member,
        'session_id': str(uuid.uuid4())[:8],
        'country': 'AU',
        'query': 'When can I access my super without penalties?',
        'validation_mode': 'llm_judge',
        'enable_observability': False
    }])

    result = loaded_model.predict(test_query)

    print("\nTest Prediction from Registered Model:")
    print(f"  Query: {test_query['query'].values[0]}")
    print(f"  Answer: {result['answer'].values[0][:100]}...")
    print(f"  Cost: ${result['cost'].values[0]:.6f}")
    print(f"  Blocked: {result['blocked'].values[0]}")

except Exception as e:
    print(f"❌ Loading model failed: {str(e)}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7: Batch Inference from Registry
# MAGIC
# MAGIC Test batch inference using the model from Unity Catalog.

# COMMAND ----------

# Create larger test batch using real member IDs
batch_queries = []
test_queries = [
    'What is my preservation age?',
    'Can I access my super early?',
    'What are the tax implications?',
    'How do I check my balance?',
]

# Use real members from database
for country in ['AU', 'US', 'UK', 'IN']:
    if country in SAMPLE_MEMBERS and SAMPLE_MEMBERS[country]:
        for i, member_id in enumerate(SAMPLE_MEMBERS[country][:2]):  # Use first 2 members per country
            batch_queries.append({
                'user_id': member_id,
                'session_id': str(uuid.uuid4())[:8],
                'country': country,
                'query': test_queries[i % len(test_queries)],
                'validation_mode': 'llm_judge',
                'enable_observability': False
            })

batch_df = pd.DataFrame(batch_queries)

print(f"Running batch inference on {len(batch_df)} queries...")

try:
    # Run batch inference using registry model
    batch_results = run_batch_inference(
        input_df=batch_df,
        model_name=MODEL_NAME,
        catalog=UNITY_CATALOG,
        schema=UNITY_SCHEMA,
        alias="champion"
    )

    print("\n✅ Batch inference complete!")
    print(f"\nResults by Country:")
    print(batch_results.groupby(batch_df['country']).agg({
        'cost': 'sum',
        'latency_ms': 'mean',
        'blocked': 'sum'
    }).round(4))

    print(f"\n📊 Overall Statistics:")
    print(f"  Total queries: {len(batch_results)}")
    print(f"  Total cost: ${batch_results['cost'].sum():.6f}")
    print(f"  Avg cost per query: ${batch_results['cost'].mean():.6f}")
    print(f"  Avg latency: {batch_results['latency_ms'].mean():.0f}ms")
    print(f"  Blocked queries: {batch_results['blocked'].sum()}")

except Exception as e:
    print(f"❌ Batch inference failed: {str(e)}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 8: Model Versioning Demo
# MAGIC
# MAGIC Demonstrate model versioning with @champion and @challenger aliases.

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()

try:
    # Get all versions of the model
    versions = client.search_model_versions(f"name='{FULL_MODEL_NAME}'")

    print(f"Model Versions for {FULL_MODEL_NAME}:")
    print("=" * 70)

    for v in sorted(versions, key=lambda x: int(x.version), reverse=True):
        print(f"\nVersion {v.version}:")
        print(f"  Status: {v.status}")
        print(f"  Stage: {v.current_stage if hasattr(v, 'current_stage') else 'N/A'}")

        # Get aliases for this version
        model_info = client.get_model_version(FULL_MODEL_NAME, v.version)
        if hasattr(model_info, 'aliases') and model_info.aliases:
            print(f"  Aliases: {', '.join(model_info.aliases)}")

        print(f"  Created: {v.creation_timestamp}")

    print("\n✅ Model versioning information retrieved")

except Exception as e:
    print(f"⚠️  Could not retrieve versions: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 9: Performance Comparison
# MAGIC
# MAGIC Compare direct agent_query() vs MLflow model performance.

# COMMAND ----------

import time
from src.agent_processor import agent_query

# Test query using real member ID
test_user = SAMPLE_MEMBERS['AU'][0] if 'AU' in SAMPLE_MEMBERS and SAMPLE_MEMBERS['AU'] else 'AU001'
test_session = str(uuid.uuid4())[:8]
test_country = 'AU'
test_query = 'What is my preservation age?'

# Method 1: Direct agent_query()
print("Method 1: Direct agent_query()")
start = time.time()
direct_result = agent_query(
    user_id=test_user,
    session_id=test_session,
    country=test_country,
    query_string=test_query,
    validation_mode='llm_judge',
    enable_observability=False
)
direct_time = (time.time() - start) * 1000

print(f"  Latency: {direct_time:.0f}ms")
print(f"  Cost: ${direct_result['cost']:.6f}")

# Method 2: MLflow model
print("\nMethod 2: MLflow Model (local)")
model_local = PensionAdvisorModel()
model_local.load_context(None)

df_input = pd.DataFrame([{
    'user_id': test_user,
    'session_id': test_session,
    'country': test_country,
    'query': test_query,
    'validation_mode': 'llm_judge',
    'enable_observability': False
}])

start = time.time()
model_result = model_local.predict(None, df_input)
model_time = (time.time() - start) * 1000

print(f"  Latency: {model_time:.0f}ms")
print(f"  Cost: ${model_result['cost'].values[0]:.6f}")

# Comparison
print(f"\nComparison:")
print(f"  MLflow overhead: {model_time - direct_time:.0f}ms ({((model_time/direct_time - 1) * 100):.1f}%)")
print(f"  Both methods use same agent_query() internally")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: MLflow Deployment Complete ✅
# MAGIC
# MAGIC **What We Accomplished:**
# MAGIC - ✅ Created MLflow PyFunc model wrapper
# MAGIC - ✅ Tested local inference (single & batch)
# MAGIC - ✅ Registered model to Unity Catalog
# MAGIC - ✅ Set @champion alias for production
# MAGIC - ✅ Loaded and tested from registry
# MAGIC - ✅ Ran batch inference at scale
# MAGIC - ✅ Verified model versioning
# MAGIC - ✅ Measured performance overhead
# MAGIC
# MAGIC **Model Details:**
# MAGIC - **Name:** `{FULL_MODEL_NAME}`
# MAGIC - **Type:** mlflow.pyfunc.PythonModel
# MAGIC - **Alias:** @champion (production)
# MAGIC - **Features:** AI Guardrails, LLM-as-a-Judge validation, multi-country support
# MAGIC
# MAGIC **Benefits:**
# MAGIC 1. **Versioning:** Track model changes over time
# MAGIC 2. **Reproducibility:** Exact same model behavior in dev/prod
# MAGIC 3. **Batch Inference:** Process 1000s of queries efficiently
# MAGIC 4. **Alias Management:** Easy promotion from @challenger → @champion
# MAGIC 5. **Production Ready:** Batch processing at scale with UC governance
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - **[06-ai-guardrails]($./06-ai-guardrails)**: Add safety guardrails
# MAGIC - **[07-production-monitoring]($./07-production-monitoring)**: Monitor in production
# MAGIC - **[08-automated-scoring-job]($./08-automated-scoring-job)**: Quality scoring pipeline
# MAGIC
# MAGIC **Key Modules:**
# MAGIC - MLflow Model: `src/mlflow_model.py`
# MAGIC - Agent Processor: `src/agent_processor.py`
# MAGIC - Configuration: `src/config/config.yaml`

# COMMAND ----------

print("✅ MLflow Model Packaging complete!")
print(f"   Model: {FULL_MODEL_NAME}@champion")
print(f"   Ready for production batch inference")
