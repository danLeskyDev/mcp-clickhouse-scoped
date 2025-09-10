#!/usr/bin/env python
"""Test script to verify filtering is working correctly."""

import json
import sys
sys.path.insert(0, '.')

from mcp_clickhouse.mcp_server import list_databases, list_tables
from mcp_clickhouse.config import ALLOWED_TABLES_BY_DB

print("Current scopes configuration:")
if ALLOWED_TABLES_BY_DB:
    scopes_dict = {db: list(tables) for db, tables in ALLOWED_TABLES_BY_DB.items()}
    print(json.dumps(scopes_dict, indent=2))
else:
    print("No scopes configured")
print()

print("Testing list_databases():")
databases = json.loads(list_databases())
print(f"Returned databases: {databases}")
print()

print("Testing list_tables('stagingcom'):")
tables_stagingcom = list_tables('stagingcom')
print(f"Tables in stagingcom: {len(tables_stagingcom)} tables")
if tables_stagingcom:
    print("Table names:", [t['name'] for t in tables_stagingcom])
print()

print("Testing list_tables('stagingio'):")
tables_stagingio = list_tables('stagingio')
print(f"Tables in stagingio: {len(tables_stagingio)} tables")
if tables_stagingio:
    print("Table names:", [t['name'] for t in tables_stagingio])