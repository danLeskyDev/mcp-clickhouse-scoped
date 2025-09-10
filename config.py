import json
from pathlib import Path
from typing import Dict, Set, Optional

# Hardcoded path to the scopes JSON file
CONFIG_FILE_PATH = Path('scopes/default.json')

# Load scopes from JSON
def load_scopes_from_json(file_path: Path = CONFIG_FILE_PATH) -> Dict[str, any]:
    with open(file_path, 'r') as f:
        return json.load(f)

scopes_data = load_scopes_from_json()

# ALLOWED_TABLES_BY_DB as dict: {db_name: set(allowed_tables)}
ALLOWED_TABLES_BY_DB: Optional[Dict[str, Set[str]]] = {
    db: set(tables) for db, tables in scopes_data.get('allowed_databases', {}).items()
} if scopes_data else None