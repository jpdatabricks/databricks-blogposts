import re 
from typing import List

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors.platform import AlreadyExists
from databricks.sdk.service.billing import BudgetPolicy
from databricks.sdk.service.compute import CustomPolicyTag
from databricks.sdk.service.iam import GrantRule

from serverless_converter.utils.pipelines import WorkspacePipeline

import logging
logger = logging.getLogger(__name__)


def get_budget_policy_permissions(account_client: AccountClient, budget_policy_id: str):
    """
    Retrieves the current rule set for a given budget policy.

    Args:
        databricks_account_id (str): The Databricks account ID.
        budget_policy_id (str): The budget policy ID.

    Returns:
        RuleSetResponse: The current rule set for the budget policy.
    """
    # Construct the rule set URL
    account_id = account_client.config.account_id
    rule_set_url = f"accounts/{account_id}/budgetPolicies/{budget_policy_id}/ruleSets/default"

    # Get the current rule set
    return account_client.access_control.get_rule_set(name=rule_set_url, etag="")


def update_budget_policy_permissions(account_client: AccountClient, budget_policy_id:str, extra_grant_rules=[]):
  """
  Updates the permissions for a given budget policy by adding extra grant rules.

  Args:
    databricks_account_id (str): The Databricks account ID.
    budget_policy_id (str): The budget policy ID.
    extra_grant_rules (list, optional): Additional grant rules to be added. Defaults to [].

  Returns:
    RuleSetResponse: The updated rule set for the budget policy.
  """
  # Construct the rule set URL
  account_id = account_client.config.account_id
  rule_set_url = f"accounts/{account_id}/budgetPolicies/{budget_policy_id}/ruleSets/default"

  # Get the current rule set
  current_ruleset = account_client.access_control.get_rule_set(name=rule_set_url, etag="")

  # Update the grant rules with extra grant rules
  current_ruleset.grant_rules = [*current_ruleset.grant_rules, *extra_grant_rules]

  # Update the rule set
  return account_client.access_control.update_rule_set(name=rule_set_url, rule_set=current_ruleset)


def generate_rule_set_grants(workspace_client:WorkspaceClient, pipelines:List[WorkspacePipeline]):
    rule_set_grants = {}

    for p in pipelines:
        p_def = p.pipeline_definition
        run_as_user_name =  p_def.run_as_user_name

        if next(workspace_client.users.list(filter=f"displayName eq '{run_as_user_name}'"), None):
            grant_rule = GrantRule(role='roles/budgetPolicy.user', principals=[f"servicePrincipals/{run_as_user_name}"])
        else:
            grant_rule = GrantRule(role='roles/budgetPolicy.user', principals=[f"users/{run_as_user_name}"])

        rule_set_grants[p_def.pipeline_id] = grant_rule

    return rule_set_grants

def check_budget_policy_exists(account_client: AccountClient, budget_policy_id: str) -> bool:
    try:
        account_client.budget_policy.get(budget_policy_id)
        return True
    except Exception as e:
        logger.error("Error checking budget policy existence: %s", str(e))
        return False

def is_user(user_list, userName):
    return any([user.userName == userName for user in user_list])


def is_principal(principal_list, displayName):
    return any([principal.displayName == displayName for principal in principal_list])


def generate_policy_name(input_str: str) -> str:
  return re.sub(r'[^0-9a-zA-Z\-=\.:/@_+\s]+', '', input_str)


def generate_budget_policies_from_workspace_pipelines(account_client: AccountClient, workspace_client: WorkspaceClient, target_pipelines: List[WorkspacePipeline]):
    """
    Generates budget policies for each pipeline in a workspace based on pipeline tags.

    This function iterates through all pipelines in a workspace, extracts their cluster tags,
    and creates corresponding budget policies with permissions for the pipeline service principals.

    Args:
        account_client (AccountClient): Client for Databricks account-level operations
        workspace_client (WorkspaceClient): Client for workspace-level operations

    Returns:
        dict: Mapping of generated policy names to their IDs where:
            - key (str): Budget policy name derived from pipeline tags
            - value (str): Generated budget policy ID

    Example:
        >>> account_client = AccountClient()
        >>> workspace_client = WorkspaceClient() 
        >>> policies = generate_budget_policies_from_workspace_pipelines(account_client, workspace_client)
    """
    # Get list of all pipeline IDs in workspace
    generated_policies = {}
    grant_rules = generate_rule_set_grants(workspace_client, target_pipelines)

    for pipeline in target_pipelines:
        # Get pipeline configuration
        pipeline_def = pipeline.pipeline_definition
        pipeline_id = pipeline.pipeline_id
        pipeline_name = pipeline.pipeline_name

        # Only process pipelines that have cluster configurations
        if pipeline_def.spec.clusters:
            logger.info(f"Generating budget policy {pipeline_name}.")
            # Extract tags from first cluster in pipeline
            current_tags = pipeline_def.spec.clusters[0].custom_tags
            budget_policy_name = generate_policy_name(pipeline_def.spec.name)

            # Convert tags to CustomPolicyTag format
            budget_policy_tags = [CustomPolicyTag(key=k, value=v) for k,v in current_tags.items()] if current_tags else []
            budget_policy = BudgetPolicy(policy_name=budget_policy_name, custom_tags=budget_policy_tags)

            # Create new budget policy and set permissions for the principal to use it
            try:
                new_budget_policy = account_client.budget_policy.create(policy=budget_policy)
                update_budget_policy_permissions(account_client, new_budget_policy.policy_id,
                                            extra_grant_rules=[grant_rules[pipeline_def.pipeline_id]])
                generated_policies[pipeline_id] = new_budget_policy.policy_id
                logger.info(f"Budget policy {pipeline_name} generated.")
            except AlreadyExists:
                logger.info(f"Policy {budget_policy_name} already exists")
                generated_policies[pipeline_id] = None

        else:
            logger.info(f"Skipped {pipeline_name} ({pipeline_id})")

    return generated_policies