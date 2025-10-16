import os
import pickle
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineStateInfo

import logging
logger = logging.getLogger(__name__)


@dataclass
class WorkspacePipeline:
    workspace_id: str
    pipeline_id: str
    snapshot_timestamp: datetime
    pipeline_definition: PipelineStateInfo
    pipeline_name: str


def get_workspace_pipelines(workspace_client: WorkspaceClient) -> List[WorkspacePipeline]:
    """
    Takes a snapshot of all pipelines in a workspace and returns their definitions.

    This function retrieves all pipelines in a specified workspace and captures their current state.

    Args:
        workspace_client (WorkspaceClient): The Databricks workspace client instance 

    Returns:
        List[WorkspacePipeline]: A list of WorkspacePipeline objects containing:
            - workspace_id (str): ID of the workspace
            - pipeline_id (str): ID of the pipeline 
            - snapshot_timestamp (datetime): UTC timestamp when snapshot was taken
            - pipeline_definition (PipelineStateInfo): Pipeline definition object

    Example:
        >>> workspace_client = WorkspaceClient()
        >>> pipelines = get_workspace_pipelines(workspace_client)
    """
    # Get list of pipeline IDs
    pipeline_list = [p.pipeline_id for p in workspace_client.pipelines.list_pipelines()]

    # Create WorkspacePipeline objects for each pipeline
    pipeline_configs = []
    for pipeline_id in pipeline_list:
        logger.info(f"Fetching pipeline {pipeline_id} configuration")
        config = workspace_client.pipelines.get(pipeline_id=pipeline_id)
        pipeline = WorkspacePipeline(
            workspace_id=workspace_client.get_workspace_id(),
            pipeline_id=pipeline_id,
            pipeline_name=config.name,
            snapshot_timestamp=datetime.now(timezone.utc),
            pipeline_definition=config
        )
        pipeline_configs.append(pipeline)

    return pipeline_configs


# Save the workspace_pipelines object to a file
def save_pipelines_to_file(pipelines: List[WorkspacePipeline], filename=None):
    """
    Save pipeline data to a local file using pickle.

    Args:
        pipelines: The pipelines data to save
        filename: Optional filename, defaults to timestamped file

    Returns:
        str: Path to the saved file
    """
    if filename is None:
        filename = f"workspace_{pipelines[0].workspace_id}_{pipelines[0].snapshot_timestamp.strftime('%Y%m%d%H%M%S')}.pkl"

    file_path = os.path.join(os.getcwd(), "pipelines_backup", filename)

    with open(file_path, 'wb') as f:
        pickle.dump(pipelines, f)

    logger.info(f"Saved pipelines data to {file_path}")
    return file_path


def load_pipelines_from_file(filename):
    """
    Load pipeline data from a pickle file.

    Args:
        filename: Path to the pickle file

    Returns:
        The loaded pipelines data
    """
    with open(filename, 'rb') as f:
        pipelines = pickle.load(f)

    logger.info(f"Loaded pipelines data from {filename}")
    return pipelines


def convert_pipelines_to_serverless(
    workspace_client: WorkspaceClient,
    pipelines: List[WorkspacePipeline],
    budget_policies: Dict[str, Optional[str]],
    dry_run: bool = False
) -> Dict[str, List]:
    """
    Convert pipelines to use serverless compute.

    Args:
        workspace_client: Databricks workspace client
        pipelines: List of WorkspacePipeline objects
        budget_policies: Dict mapping pipeline_id to budget_policy_id
        dry_run: If True, preview changes without executing them

    Returns:
        dict: Summary of conversion operation
    """
    logger.info(f"Starting serverless conversion for {len(pipelines)} pipelines")
    results = {
        "successful": [],
        "failed": []
    }

    for pipeline in pipelines:
        p = pipeline.pipeline_definition
        pipeline_id = pipeline.pipeline_id
        pipeline_name = pipeline.pipeline_name

        logger.info(f"Converting pipeline: {pipeline_name} ({pipeline_id})")

        try:
            # Explicit attributes to set to convert to serverless
            update_params = {
                'pipeline_id': pipeline_id,
                'budget_policy_id': budget_policies.get(pipeline_id, None),
                'clusters': None,
                'photon': True,
                'serverless': True
            }

            # Add other parameters from the original pipeline spec
            for k in p.spec.as_dict().keys():
                if k in update_params.keys():
                    continue
                update_params[k] = getattr(p.spec, k, None)


            if dry_run:
                logger.info(f"DRY RUN: Would update pipeline {pipeline_name} ({pipeline_id}) to serverless")
                results["successful"].append(pipeline_id)
            else:
                # Update the pipeline with unpacked parameters. 
                # Currently there is no PATCH API available, so you need to provide the entire configuration.
                workspace_client.pipelines.update(**update_params)
                logger.info(f"Successfully updated pipeline {pipeline_name} ({pipeline_id}) to serverless")
                results["successful"].append(pipeline_id)

        except Exception as e:
            logger.error(f"Failed to update pipeline {pipeline_name} ({pipeline_id}): {str(e)}")
            results["failed"].append({
                'pipeline_id': pipeline_id,
                'exception': str(e)
            })

    # Generate summary
    logger.info(f"Conversion completed:")
    logger.info(f"  - Successfully converted: {len(results['successful'])}")
    logger.info(f"  - Failed to convert: {len(results['failed'])}")

    return results


def rollback_pipelines(
    workspace_client: WorkspaceClient,
    backed_up_pipelines: List[WorkspacePipeline],
    dry_run: bool = False
):
    """
    Rollback pipelines to their previous state from a backup file.

    Args:
        workspace_client: Databricks workspace client
        backed_up_pipelines: List of WorkspacePipeline objects from backup
        dry_run: If True, preview changes without executing them

    Returns:
        dict: Summary of the rollback operation
    """
    logger.info(f"Starting pipeline rollback for {len(backed_up_pipelines)} pipelines")

    # Get current pipelines for reference
    current_pipelines = {p.pipeline_id: p for p in get_workspace_pipelines(workspace_client)}
    logger.info(f"Found {len(current_pipelines)} pipelines currently in workspace")

    # Track rollback results
    results = {
        "successful": [],
        "failed": [],
        "skipped": []
    }

    # Process each pipeline in the backup
    for pipeline in backed_up_pipelines:
        pipeline_id = pipeline.pipeline_id
        original_config = pipeline.pipeline_definition
        pipeline_name = pipeline.pipeline_name

        if pipeline_id not in current_pipelines:
            logger.warning(f"Pipeline {pipeline_name} ({pipeline_id}) no longer exists in workspace - cannot rollback")
            results["skipped"].append({
                "pipeline_id": pipeline_id,
                "reason": "Pipeline no longer exists in workspace"
            })
            continue

        logger.info(f"Rolling back pipeline: {pipeline_name} ({pipeline_id})")

        try:

            spec = original_config.spec
            update_params = {'pipeline_id': pipeline_id}

            # Add other parameters from the original pipeline spec
            for k in spec.as_dict().keys():
                update_params[k] = getattr(spec, k, None)

            if dry_run:
                logger.info(f"DRY RUN: Would update pipeline {pipeline_name} ({pipeline_id}) with original configuration")
                results["successful"].append(pipeline_id)
            else:
                # Execute the update
                # Currently there is no PATCH API available, so you need to provide the entire configuration.
                workspace_client.pipelines.update(**update_params)
                
                logger.info(f"Successfully rolled back pipeline {pipeline_name} ({pipeline_id})")
                results["successful"].append(pipeline_id)

        except Exception as e:
            logger.error(f"Failed to rollback pipeline {pipeline_name} ({pipeline_id}): {str(e)}")
            results["failed"].append({
                "pipeline_id": pipeline_id,
                "exception": str(e)
            })

    # Generate summary
    logger.info(f"Rollback completed:")
    logger.info(f"  - Successfully rolled back: {len(results['successful'])}")
    logger.info(f"  - Failed to roll back: {len(results['failed'])}")
    logger.info(f"  - Skipped: {len(results['skipped'])}")

    return results