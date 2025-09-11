import json
import logging
from pathlib import Path
from typing import Dict, Set, Optional

logger = logging.getLogger(__name__)

# Path to the scopes JSON file (relative to module location)
module_dir = Path(__file__).parent.parent  # Go up from mcp_clickhouse to project root
CONFIG_FILE_PATH = module_dir / 'config' / 'scopes.json'

# Global variable to store allowed tables
ALLOWED_TABLES_BY_DB: Optional[Dict[str, Set[str]]] = None

def load_scopes_from_json(file_path: Path = CONFIG_FILE_PATH) -> Optional[Dict[str, any]]:
    """Load scopes configuration from JSON file.
    
    Returns None if file doesn't exist or is malformed.
    """
    if not file_path.exists():
        logger.info(f"Scopes config file not found at {file_path} - filtering disabled")
        return None
    
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in scopes config file {file_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error loading scopes config from {file_path}: {e}")
        return None

def load_and_set_scope(scope_name: Optional[str] = None, scope_file: Optional[Path] = None):
    """Load and set the active scope for filtering.
    
    Args:
        scope_name: Name of scope to load from default scopes.json
        scope_file: Path to a custom scope file
    """
    global ALLOWED_TABLES_BY_DB
    
    # Load scopes data from appropriate source
    if scope_file:
        scopes_data = load_scopes_from_json(scope_file)
    else:
        scopes_data = load_scopes_from_json()
    
    # Validate scopes data
    if not scopes_data or 'scopes' not in scopes_data:
        logger.error("No valid scopes configuration found")
        ALLOWED_TABLES_BY_DB = None
        return
    
    # Select the appropriate scope
    if scope_name:
        if scope_name in scopes_data['scopes']:
            scope = scopes_data['scopes'][scope_name]
            logger.info(f"Loaded scope '{scope_name}' with {len(scope.get('allowed_databases', {}))} databases")
        else:
            logger.error(f"Scope '{scope_name}' not found in scopes configuration")
            ALLOWED_TABLES_BY_DB = None
            return
    else:
        # Use first scope as default
        first_scope_name = next(iter(scopes_data['scopes'].keys()))
        scope = scopes_data['scopes'][first_scope_name]
        logger.info(f"Loaded default scope '{first_scope_name}' with {len(scope.get('allowed_databases', {}))} databases")
    
    # Set the allowed tables from the selected scope
    ALLOWED_TABLES_BY_DB = {
        db: set(tables) for db, tables in scope.get('allowed_databases', {}).items()
    }

# Initialize with default behavior on module import
load_and_set_scope()