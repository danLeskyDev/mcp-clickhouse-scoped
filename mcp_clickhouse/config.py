import json
import logging
from pathlib import Path
from typing import Dict, Set, Optional

logger = logging.getLogger(__name__)

# Hardcoded path to the scopes JSON file
CONFIG_FILE_PATH = Path('scopes/default.json')

# Load scopes from JSON
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

# Try to load scopes, but don't crash if it fails
scopes_data = load_scopes_from_json()

# ALLOWED_TABLES_BY_DB as dict: {db_name: set(allowed_tables)}
ALLOWED_TABLES_BY_DB: Optional[Dict[str, Set[str]]] = None

if scopes_data and 'allowed_databases' in scopes_data:
    ALLOWED_TABLES_BY_DB = {
        db: set(tables) for db, tables in scopes_data.get('allowed_databases', {}).items()
    }
    logger.info(f"Loaded scopes for {len(ALLOWED_TABLES_BY_DB)} databases")