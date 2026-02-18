"""Tools package for superannuation agent."""

import sys
import os
import importlib.util
from pathlib import Path

# Import from tools.tool_executor (within this package)
from src.tools.tool_executor import UnifiedToolExecutor, create_executor

# Initialize AVAILABLE_TOOLS first to ensure it's always defined
AVAILABLE_TOOLS = {}

# Load AVAILABLE_TOOLS from tool_config.yaml
try:
    import yaml

    # Try multiple paths for Databricks compatibility
    _config_path = Path(__file__).parent / 'tool_config.yaml'
    if not _config_path.exists():
        # Try relative to current working directory (for Databricks notebooks)
        _config_path = Path(os.getcwd()) / 'src' / 'tools' / 'tool_config.yaml'

    if _config_path.exists():
        with open(_config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Build AVAILABLE_TOOLS dictionary from config
        # Format: {country-tool_id: {country, description, function, authority}}
        for country, country_config in config.get('countries', {}).items():
            for tool_id, tool_config in country_config.get('tools', {}).items():
                key = f"{country}-{tool_id}"
                AVAILABLE_TOOLS[key] = {
                    'country': country,
                    'tool_id': tool_id,
                    'description': tool_config.get('name', ''),
                    'authority': tool_config.get('authority', ''),
                    'function': tool_config.get('uc_function', '')
                }
    else:
        import warnings
        warnings.warn(f"tool_config.yaml not found at {_config_path}")
except Exception as e:
    import warnings
    warnings.warn(f"Failed to load tool_config.yaml: {e}")

# Also expose SuperAdvisorTools and call_individual_tool from the root-level tools.py file
# This resolves the naming conflict between tools.py file and tools/ package
try:
    _tools_module_path = Path(__file__).parent.parent / 'tools.py'
    if _tools_module_path.exists():
        _spec = importlib.util.spec_from_file_location("_root_tools", _tools_module_path)
        if _spec and _spec.loader:
            _root_tools = importlib.util.module_from_spec(_spec)
            _spec.loader.exec_module(_root_tools)
            SuperAdvisorTools = _root_tools.SuperAdvisorTools
            call_individual_tool = _root_tools.call_individual_tool
            # Alias for notebooks
            execute_tool = call_individual_tool
            __all__ = ['UnifiedToolExecutor', 'create_executor', 'SuperAdvisorTools', 'AVAILABLE_TOOLS', 'call_individual_tool', 'execute_tool']
    else:
        __all__ = ['UnifiedToolExecutor', 'create_executor', 'AVAILABLE_TOOLS']
except Exception as e:
    import warnings
    warnings.warn(f"Failed to load root-level tools.py: {e}")
    __all__ = ['UnifiedToolExecutor', 'create_executor', 'AVAILABLE_TOOLS']
