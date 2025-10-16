import logging
import sys
from serverless_converter.commands import command_convert, command_list, command_rollback, parse_arguments
from serverless_converter.utils.auth import initialize_clients

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    """Main entry point for the Lakeflow Declarative Pipelines converter tool."""
    args = parse_arguments()
    
    # Initialize Databricks clients
    workspace_client, account_client = initialize_clients(args)
    
    # Execute the appropriate command
    if args.command == 'convert':
        command_convert(args, workspace_client, account_client)
    elif args.command == 'rollback':
        command_rollback(args, workspace_client, account_client)
    elif args.command == 'list':
        command_list(args, workspace_client, account_client)
        
    return 0
    
if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)