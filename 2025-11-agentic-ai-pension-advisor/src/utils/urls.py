"""
URL helper utilities for Databricks native UI links.

Provides functions to generate links to Databricks workspaces, MLflow, Unity Catalog, etc.
"""

import os
from typing import Optional


def get_workspace_url() -> Optional[str]:
    """
    Get the Databricks workspace URL from environment or WorkspaceClient.

    Returns:
        Workspace URL (e.g., "https://adb-1234567890123456.7.azuredatabricks.net")
        or None if not available
    """
    # Try multiple environment variables first
    workspace_url = (
        os.environ.get("DATABRICKS_HOST") or
        os.environ.get("DATABRICKS_URL") or
        os.environ.get("DB_WORKSPACE_URL")
    )

    if workspace_url:
        # Remove trailing slash
        return workspace_url.rstrip("/")

    # If no env vars, try WorkspaceClient (will use .databrickscfg)
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        if hasattr(w.config, 'host') and w.config.host:
            return w.config.host.rstrip("/")
    except Exception:
        pass

    return None


def get_mlflow_experiment_url(experiment_path: str) -> Optional[str]:
    """
    Get URL to MLflow experiment in Databricks UI.

    Args:
        experiment_path: Experiment path (e.g., "/Users/user@example.com/my-experiment")

    Returns:
        Full URL to experiment or None if workspace URL not available
    """
    workspace_url = get_workspace_url()
    if not workspace_url:
        return None

    # URL encode the experiment path
    from urllib.parse import quote
    encoded_path = quote(experiment_path, safe="")

    return f"{workspace_url}/ml/experiments?searchFilter=name%3D%22{encoded_path}%22"


def get_mlflow_run_url(run_id: str) -> Optional[str]:
    """
    Get URL to specific MLflow run in Databricks UI.

    Args:
        run_id: MLflow run ID

    Returns:
        Full URL to run or None if workspace URL not available
    """
    workspace_url = get_workspace_url()
    if not workspace_url:
        return None

    return f"{workspace_url}/ml/experiments/{run_id}"


def get_unity_catalog_url(catalog: Optional[str] = None, schema: Optional[str] = None) -> Optional[str]:
    """
    Get URL to Unity Catalog browser in Databricks UI.

    Args:
        catalog: Optional catalog name to navigate to
        schema: Optional schema name (requires catalog)

    Returns:
        Full URL to Unity Catalog or None if workspace URL not available
    """
    workspace_url = get_workspace_url()
    if not workspace_url:
        return None

    base_url = f"{workspace_url}/explore/data"

    if catalog:
        if schema:
            return f"{base_url}/{catalog}/{schema}"
        else:
            return f"{base_url}/{catalog}"

    return base_url


def get_model_registry_url(model_name: Optional[str] = None) -> Optional[str]:
    """
    Get URL to Unity Catalog model registry.

    Args:
        model_name: Optional full model name (e.g., "catalog.schema.model_name")

    Returns:
        Full URL to model registry or None if workspace URL not available
    """
    workspace_url = get_workspace_url()
    if not workspace_url:
        return None

    if model_name:
        # Split model name into parts
        parts = model_name.split(".")
        if len(parts) == 3:
            catalog, schema, model = parts
            return f"{workspace_url}/explore/data/{catalog}/{schema}/model/{model}"

    return f"{workspace_url}/explore/data/models"


def get_billing_console_url() -> Optional[str]:
    """
    Get URL to Databricks billing console.

    Returns:
        Full URL to billing console or None if workspace URL not available
    """
    workspace_url = get_workspace_url()
    if not workspace_url:
        return None

    return f"{workspace_url}/settings/account/usage"


def get_serving_endpoint_url(endpoint_name: str) -> Optional[str]:
    """
    Get URL to serving endpoint in Databricks UI.

    Args:
        endpoint_name: Name of the serving endpoint

    Returns:
        Full URL to endpoint or None if workspace URL not available
    """
    workspace_url = get_workspace_url()
    if not workspace_url:
        return None

    return f"{workspace_url}/ml/endpoints/{endpoint_name}"


def get_inference_table_url(catalog: str, schema: str, table_name: str) -> Optional[str]:
    """
    Get URL to inference table in Unity Catalog.

    Args:
        catalog: Catalog name
        schema: Schema name
        table_name: Table name

    Returns:
        Full URL to table or None if workspace URL not available
    """
    workspace_url = get_workspace_url()
    if not workspace_url:
        return None

    return f"{workspace_url}/explore/data/{catalog}/{schema}/{table_name}"


def format_external_link(label: str, url: Optional[str], icon: str = "🔗") -> str:
    """
    Format an external link for Streamlit that opens in new tab.

    Args:
        label: Link text
        url: URL (if None, returns plain text)
        icon: Optional emoji icon

    Returns:
        HTML link with target="_blank" or plain text
    """
    if url:
        # Use HTML link with target="_blank" to open in new tab
        # This works correctly in Databricks Apps
        return f'{icon} <a href="{url}" target="_blank" rel="noopener noreferrer">{label}</a>'
    else:
        return f"{icon} {label} (not available)"
