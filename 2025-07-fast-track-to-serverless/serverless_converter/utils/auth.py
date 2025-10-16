import logging
import os
import sys
from databricks.sdk import AccountClient, WorkspaceClient
from typing import Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def initialize_clients(args) -> Tuple[Optional[WorkspaceClient], Optional[AccountClient]]:
    """Initialize Databricks clients based on command-line arguments."""
    
    workspace_client = None
    account_client = None
    
    # Initialize workspace client
    if args.workspace_host and args.client_id and args.client_secret:
        workspace_client = WorkspaceClient(
            host=args.workspace_host,
            client_id=args.client_id,
            client_secret=args.client_secret
        )
    elif os.environ.get('DATABRICKS_WORKSPACE_HOST') and os.environ.get('DATABRICKS_CLIENT_ID') and os.environ.get('DATABRICKS_CLIENT_SECRET'):
        workspace_client = WorkspaceClient()
    else:
        logger.error("Missing workspace client credentials. Provide workspace-host, client-id, and client-secret arguments or set environment variables.")
        sys.exit(1)
    
    # Initialize account client if needed
    if args.command == 'convert':
        if args.account_host and args.account_id and args.client_id and args.client_secret:
            account_client = AccountClient(
                host=args.account_host,
                account_id=args.account_id,
                client_id=args.client_id,
                client_secret=args.client_secret
            )
        elif os.environ.get('DATABRICKS_ACCOUNT_HOST') and os.environ.get('DATABRICKS_ACCOUNT_ID'):
            account_client = AccountClient()
        else:
            logger.warning("Missing account client credentials. Provide account-host and account-id arguments or set environment variables to interact with budget policies.")
    
    return workspace_client, account_client