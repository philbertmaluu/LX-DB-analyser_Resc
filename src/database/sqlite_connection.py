"""SQLite3 database connection implementation."""

import sqlite3
from typing import Optional
from .base import BaseDatabaseConnection
from config.settings import SQLITE_DATABASE
from src.utils.db_helpers import exec_query, exec_update, safe_disconnect, check_connection


class SQLiteConnection(BaseDatabaseConnection):
    """Handles SQLite3 database connections."""
    
    def __init__(self, connection_name: str = "sqlite_default", database: Optional[str] = None, **kwargs):
        self.database = database or SQLITE_DATABASE
        super().__init__(connection_name, database=self.database, **kwargs)
    
    def connect(self) -> bool:
        try:
            self.connection = sqlite3.connect(self.database, check_same_thread=False)
            self.connection.row_factory = sqlite3.Row
            print(f"✓ Successfully connected to SQLite3 database '{self.connection_name}': {self.database}")
            return True
        except Exception as e:
            print(f"✗ SQLite3 database error ({self.connection_name}): {str(e)}")
            return False
    
    def disconnect(self) -> None:
        safe_disconnect(self.connection, lambda: self.connection.close(), self.connection_name, "SQLite3")
        self.connection = None
    
    def is_connected(self) -> bool:
        return check_connection(self.connection, lambda: self.connection.execute("SELECT 1"))
    
    def get_connection(self) -> Optional[sqlite3.Connection]:
        return self.connection if self.is_connected() else None
    
    def execute_query(self, query: str) -> Optional[list]:
        results = exec_query(self.connection, lambda: self.connection.cursor(), self.is_connected,
                            self.connection_name, "SQLite3", query)
        return [tuple(row) for row in results] if results else None
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> bool:
        return exec_update(self.connection, lambda: self.connection.cursor(), lambda: self.connection.commit(),
                          lambda: self.connection.rollback(), self.is_connected, self.connection_name,
                          "SQLite3", query, params)
