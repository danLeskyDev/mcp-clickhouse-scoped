"""Response filtering for ClickHouse query results.

This module implements fingerprint-based filtering of QueryResult objects
to enforce table/database scoping without SQL parsing.
"""

from typing import Dict, Set, Optional
import logging

logger = logging.getLogger(__name__)


class ResponseFilter:
    """Filters ClickHouse responses based on allowed tables configuration."""
    
    def __init__(self, allowed_tables_by_db: Optional[Dict[str, Set[str]]]):
        self.allowed_tables_by_db = allowed_tables_by_db
    
    def filter_result(self, result) -> None:
        """Filter QueryResult in-place based on response fingerprint.
        
        Args:
            result: clickhouse_connect QueryResult object to filter
        """
        if not self.allowed_tables_by_db:
            return
        
        fingerprint = self._identify_response_type(result)
        
        if fingerprint:
            logger.debug(f"Detected response type: {fingerprint}")
            
            # Log before state
            logger.info(f"BEFORE: {len(result.result_rows)} rows")
            
            filtered_rows = self._apply_filter(result, fingerprint)
            
            # Log after state
            logger.info(f"AFTER: {len(filtered_rows)} rows")
            
            result.result_rows = filtered_rows
    
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
            # SHOW TABLES or SHOW DATABASES - figure out which by checking first value
            if rows and rows[0][0] in self.allowed_tables_by_db:
                # It's databases
                return [r for r in rows if r[0] in self.allowed_tables_by_db]
            else:
                # It's tables - check if table exists in any allowed database
                return [
                    r for r in rows 
                    if any(r[0] in tables for tables in self.allowed_tables_by_db.values())
                ]
        
        elif fingerprint == 'database_and_name':
            # system.tables query (has database and name columns)
            db_idx = cols_lower.index('database')
            name_idx = cols_lower.index('name')
            return [
                r for r in rows
                if r[db_idx] in self.allowed_tables_by_db
                and r[name_idx] in self.allowed_tables_by_db.get(r[db_idx], set())
            ]
        
        elif fingerprint == 'database_and_table':
            # system.columns query (has database and table columns)
            db_idx = cols_lower.index('database')
            table_idx = cols_lower.index('table')
            return [
                r for r in rows
                if r[db_idx] in self.allowed_tables_by_db
                and r[table_idx] in self.allowed_tables_by_db.get(r[db_idx], set())
            ]
        
        # Unknown fingerprint, return original rows
        return rows