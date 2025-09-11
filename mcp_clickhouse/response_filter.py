"""Response filtering for ClickHouse query results.

This module implements fingerprint-based filtering of QueryResult objects
to enforce table/database scoping without SQL parsing.
"""

from typing import Optional
import logging
from . import config

logger = logging.getLogger(__name__)


class ResponseFilter:
    """Filters ClickHouse responses based on allowed tables configuration."""
    
    def __init__(self):
        """Initialize the response filter."""
        pass
    
    def filter_result(self, result) -> list:
        """Filter QueryResult based on response fingerprint.
        
        Args:
            result: clickhouse_connect QueryResult object to filter
            
        Returns:
            Filtered rows list
        """
        if not config.ALLOWED_TABLES_BY_DB:
            return result.result_rows
        
        fingerprint = self._identify_response_type(result)
        
        if fingerprint:
            logger.debug(f"Detected response type: {fingerprint}")
            
            # Log before state
            logger.info(f"BEFORE: {len(result.result_rows)} rows")
            
            filtered_rows = self._apply_filter(result, fingerprint)
            
            # Log after state
            logger.info(f"AFTER: {len(filtered_rows)} rows")
            
            return filtered_rows
        
        return result.result_rows
    
    def _identify_response_type(self, result) -> Optional[str]:
        """Identify the type of response based on column structure.
        
        Returns:
            String identifier of response type, or None if not recognized
        """
        cols = [c.lower() for c in result.column_names]
        
        # Single column results (SHOW TABLES, SHOW DATABASES)
        if len(cols) == 1:
            return 'single_column_listing'
        
        # system.tables or similar (has database and name columns)
        if 'database' in cols and 'name' in cols:
            return 'database_and_name'
        
        # system.columns (has database and table columns)
        if 'database' in cols and 'table' in cols:
            return 'database_and_table'
        
        return None
    
    def _apply_filter(self, result, fingerprint: str) -> list:
        """Apply appropriate filter based on response type.
        
        Returns:
            Filtered list of rows
        """
        cols = result.column_names
        cols_lower = [c.lower() for c in cols]
        rows = result.result_rows
        
        if fingerprint == 'single_column_listing':
            # For single column, we can't reliably distinguish between SHOW TABLES and SHOW DATABASES
            # Solution: Filter conservatively - keep items that are EITHER valid databases OR valid tables
            # This may let some extra items through, but won't incorrectly hide valid data
            
            valid_databases = config.ALLOWED_TABLES_BY_DB.keys()
            valid_tables = set()
            for tables in config.ALLOWED_TABLES_BY_DB.values():
                valid_tables.update(tables)
            
            return [
                r for r in rows 
                if r[0] in valid_databases or r[0] in valid_tables
            ]
        
        elif fingerprint == 'database_and_name':
            # system.tables query (has database and name columns)
            db_idx = cols_lower.index('database')
            name_idx = cols_lower.index('name')
            return [
                r for r in rows
                if r[db_idx] in config.ALLOWED_TABLES_BY_DB
                and r[name_idx] in config.ALLOWED_TABLES_BY_DB.get(r[db_idx], set())
            ]
        
        elif fingerprint == 'database_and_table':
            # system.columns query (has database and table columns)
            db_idx = cols_lower.index('database')
            table_idx = cols_lower.index('table')
            return [
                r for r in rows
                if r[db_idx] in config.ALLOWED_TABLES_BY_DB
                and r[table_idx] in config.ALLOWED_TABLES_BY_DB.get(r[db_idx], set())
            ]
        
        # Unknown fingerprint, return original rows
        return rows