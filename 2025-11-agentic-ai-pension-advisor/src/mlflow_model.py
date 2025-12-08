"""
MLflow model wrapper for pension advisor agent.

This module packages the agent as mlflow.pyfunc.PythonModel for:
- Versioning and reproducibility via Unity Catalog
- Batch inference capabilities
- Model alias management (@champion, @challenger)
- Serving endpoint deployment (Phase 3)

Usage:
    # Local development
    model = PensionAdvisorModel()
    predictions = model.predict(None, input_df)

    # Production (from Unity Catalog)
    model = mlflow.pyfunc.load_model("models:/pension_advisor@champion")
    predictions = model.predict(input_df)

Input Schema:
    - user_id: string
    - session_id: string
    - country: string (AU, US, UK, IN)
    - query: string
    - validation_mode: string (optional, default: "llm_judge")
    - enable_observability: boolean (optional, default: False)

Output Schema:
    - user_id: string
    - session_id: string
    - query: string
    - answer: string
    - evidence: array<struct>
    - cost: double
    - latency_ms: double
    - blocked: boolean (AI guardrails)
    - violations: array<string> (AI guardrails)
    - error: string (if any)
"""

import mlflow
import pandas as pd
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class PensionAdvisorModel(mlflow.pyfunc.PythonModel):
    """
    MLflow PyFunc model wrapper for pension advisor agent.

    Wraps the agent_processor.agent_query() function for MLflow deployment.
    """

    def load_context(self, context):
        """
        Load agent dependencies and configuration.

        This is called once when the model is loaded, either:
        - When loading from Unity Catalog
        - When deploying to a serving endpoint
        - During batch inference initialization

        Args:
            context: MLflow PythonModelContext with artifacts and configuration
        """
        import sys
        import os

        # Add src directory to path if not already there
        src_dir = os.path.join(os.path.dirname(__file__), '..')
        if src_dir not in sys.path:
            sys.path.insert(0, src_dir)

        # Import agent processor
        try:
            from src.agent_processor import agent_query
            from src.config import (
                AI_GUARDRAILS_ENABLED,
                validate_configuration
            )

            self.agent_query = agent_query
            self.guardrails_enabled = AI_GUARDRAILS_ENABLED

            # Validate configuration
            config_valid = validate_configuration()
            if not config_valid:
                logger.warning("Configuration has issues - check config.yaml")

            logger.info("✅ Pension Advisor Model loaded successfully")
            logger.info(f"   AI Guardrails: {self.guardrails_enabled}")

        except Exception as e:
            logger.error(f"❌ Failed to load model dependencies: {str(e)}")
            raise

    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        """
        Process a batch of queries through the pension advisor agent.

        Args:
            context: MLflow PythonModelContext (not used, kept for compatibility)
            model_input: DataFrame with columns:
                - user_id: string
                - session_id: string
                - country: string (AU, US, UK, IN)
                - query: string
                - validation_mode: string (optional, default "llm_judge")
                - enable_observability: boolean (optional, default False)

        Returns:
            DataFrame with columns:
                - user_id: string
                - session_id: string
                - query: string
                - answer: string
                - evidence: list of dicts
                - cost: float
                - latency_ms: float
                - blocked: boolean
                - violations: list of strings
                - error: string (if any)
        """
        results = []

        for idx, row in model_input.iterrows():
            try:
                # Extract inputs with defaults
                user_id = row['user_id']
                session_id = row['session_id']
                country = row['country']
                query = row['query']
                validation_mode = row.get('validation_mode', 'llm_judge')
                enable_observability = row.get('enable_observability', False)

                # Process query through agent
                result = self.agent_query(
                    user_id=user_id,
                    session_id=session_id,
                    country=country,
                    query_string=query,
                    validation_mode=validation_mode,
                    enable_observability=enable_observability
                )

                # Format result
                formatted_result = {
                    'user_id': user_id,
                    'session_id': session_id,
                    'query': query,
                    'answer': result.get('answer', None),
                    'evidence': result.get('evidence', []),
                    'cost': result.get('cost', 0.0),
                    'latency_ms': result.get('latency_ms', 0.0),
                    'blocked': result.get('blocked', False),
                    'violations': result.get('violations', []),
                    'error': result.get('error', None)
                }

                results.append(formatted_result)

            except Exception as e:
                logger.error(f"Error processing row {idx}: {str(e)}")
                # Add error result
                results.append({
                    'user_id': row.get('user_id', 'unknown'),
                    'session_id': row.get('session_id', 'unknown'),
                    'query': row.get('query', ''),
                    'answer': None,
                    'evidence': [],
                    'cost': 0.0,
                    'latency_ms': 0.0,
                    'blocked': False,
                    'violations': [],
                    'error': str(e)
                })

        return pd.DataFrame(results)


def log_model_to_mlflow(
    model_name: str,
    catalog: str,
    schema: str,
    description: str = "Pension advisor agentic AI with guardrails",
    example_input: Optional[pd.DataFrame] = None
) -> str:
    """
    Log the Pension Advisor model to MLflow and register to Unity Catalog.

    Args:
        model_name: Name for the model (e.g., "pension_advisor")
        catalog: Unity Catalog name (e.g., "pension_blog")
        schema: Schema name (e.g., "pension_advisory")
        description: Model description
        example_input: Example input DataFrame for signature inference

    Returns:
        Model URI (e.g., "models:/pension_blog.pension_advisory.pension_advisor@champion")
    """
    import mlflow
    from mlflow.models import infer_signature

    # Create example input if not provided
    if example_input is None:
        example_input = pd.DataFrame([{
            'user_id': 'AU001',
            'session_id': 'test-session',
            'country': 'AU',
            'query': 'What is my preservation age?',
            'validation_mode': 'llm_judge',
            'enable_observability': False
        }])

    # Create model instance
    model = PensionAdvisorModel()

    # Infer signature (input/output schema)
    logger.info("Inferring model signature...")
    try:
        # Load context for signature inference
        model.load_context(None)
        signature = infer_signature(
            example_input,
            model.predict(None, example_input)
        )
    except Exception as e:
        logger.warning(f"Could not infer signature: {e}")
        signature = None

    # Define conda environment with dependencies
    conda_env = {
        'channels': ['conda-forge'],
        'dependencies': [
            'python=3.11',
            'pip',
            {
                'pip': [
                    'mlflow',
                    'pandas',
                    'numpy',
                    'pyyaml',
                    'databricks-sql-connector',
                    'databricks-sdk',
                ]
            }
        ],
        'name': 'pension_advisor_env'
    }

    # Log model to MLflow
    model_uri = f"{catalog}.{schema}.{model_name}"

    with mlflow.start_run(run_name=f"register_{model_name}") as run:
        mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=model,
            registered_model_name=model_uri,
            signature=signature,
            conda_env=conda_env,
            input_example=example_input,
            metadata={
                "description": description,
                "guardrails_enabled": True,
                "validation_mode": "llm_judge",
            }
        )

        logger.info(f"✅ Model logged successfully")
        logger.info(f"   Run ID: {run.info.run_id}")
        logger.info(f"   Model: {model_uri}")

    return f"models:/{model_uri}"


def set_model_alias(
    model_name: str,
    catalog: str,
    schema: str,
    alias: str,
    version: Optional[int] = None
) -> None:
    """
    Set an alias for a model version.

    Args:
        model_name: Model name (e.g., "pension_advisor")
        catalog: Unity Catalog name
        schema: Schema name
        alias: Alias to set (e.g., "champion", "challenger")
        version: Model version number (defaults to latest)
    """
    import mlflow
    from mlflow import MlflowClient

    client = MlflowClient()
    model_uri = f"{catalog}.{schema}.{model_name}"

    # Get latest version if not specified
    if version is None:
        versions = client.search_model_versions(f"name='{model_uri}'")
        if not versions:
            raise ValueError(f"No versions found for model {model_uri}")
        version = max(int(v.version) for v in versions)

    # Set alias
    client.set_registered_model_alias(model_uri, alias, version)
    logger.info(f"✅ Set alias '{alias}' to version {version} of {model_uri}")


def load_model_from_registry(
    model_name: str,
    catalog: str,
    schema: str,
    alias: str = "champion"
) -> Any:
    """
    Load a model from Unity Catalog by alias.

    Args:
        model_name: Model name (e.g., "pension_advisor")
        catalog: Unity Catalog name
        schema: Schema name
        alias: Model alias (e.g., "champion", "challenger")

    Returns:
        Loaded MLflow model
    """
    import mlflow

    model_uri = f"models:/{catalog}.{schema}.{model_name}@{alias}"
    logger.info(f"Loading model: {model_uri}")

    model = mlflow.pyfunc.load_model(model_uri)
    logger.info(f"✅ Model loaded successfully")

    return model


# Convenience functions for batch inference

def run_batch_inference(
    input_df: pd.DataFrame,
    model_name: str = "pension_advisor",
    catalog: str = None,  # Will use UNITY_CATALOG from config
    schema: str = None,   # Will use UNITY_SCHEMA from config
    alias: str = "champion"
) -> pd.DataFrame:
    """
    Run batch inference using a model from Unity Catalog.

    Args:
        input_df: DataFrame with user_id, session_id, country, query columns
        model_name: Model name in Unity Catalog
        catalog: Unity Catalog name (defaults to config if None)
        schema: Schema name (defaults to config if None)
        alias: Model alias to use (champion, challenger, etc.)

    Returns:
        DataFrame with predictions
    """
    # Use config defaults if not provided
    if catalog is None:
        from src.config import UNITY_CATALOG
        catalog = UNITY_CATALOG
    if schema is None:
        from src.config import UNITY_SCHEMA
        schema = UNITY_SCHEMA

    # Load model from registry
    model = load_model_from_registry(model_name, catalog, schema, alias)

    # Run inference
    logger.info(f"Processing {len(input_df)} queries...")
    predictions = model.predict(input_df)
    logger.info(f"✅ Processed {len(predictions)} queries")

    return predictions


def run_batch_inference_from_table(
    input_table: str,
    output_table: str,
    model_name: str = "pension_advisor",
    catalog: str = None,  # Will use UNITY_CATALOG from config
    schema: str = None,   # Will use UNITY_SCHEMA from config
    alias: str = "champion"
) -> None:
    """
    Run batch inference from a Delta table and save to output table.

    Args:
        input_table: Fully qualified input table name (e.g., "catalog.schema.queries")
        output_table: Fully qualified output table name (e.g., "catalog.schema.predictions")
        model_name: Model name in Unity Catalog
        catalog: Unity Catalog name
        schema: Schema name
        alias: Model alias to use
    """
    from pyspark.sql import SparkSession

    # Use config defaults if not provided
    if catalog is None:
        from src.config import UNITY_CATALOG
        catalog = UNITY_CATALOG
    if schema is None:
        from src.config import UNITY_SCHEMA
        schema = UNITY_SCHEMA

    spark = SparkSession.builder.getOrCreate()

    # Read input table
    logger.info(f"Reading from {input_table}...")
    input_df = spark.table(input_table).toPandas()

    # Run inference
    predictions_df = run_batch_inference(input_df, model_name, catalog, schema, alias)

    # Convert back to Spark DataFrame and save
    logger.info(f"Writing to {output_table}...")
    predictions_spark = spark.createDataFrame(predictions_df)
    predictions_spark.write.format("delta").mode("overwrite").saveAsTable(output_table)

    logger.info(f"✅ Batch inference complete")
    logger.info(f"   Input: {len(input_df)} rows from {input_table}")
    logger.info(f"   Output: {len(predictions_df)} rows to {output_table}")
