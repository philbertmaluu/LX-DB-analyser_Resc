"""SQLite3 database connection implementation."""

import sqlite3
from typing import Optional, List, Any
from .base import BaseDatabaseConnection
from config.settings import SQLITE_DATABASE


class SQLiteConnection(BaseDatabaseConnection):
    """Handles SQLite3 database connections."""
    
    def __init__(
        self,
        connection_name: str = "sqlite_default",
        database: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize SQLite3 database connection parameters.
        
        Args:
            connection_name: Unique name for this connection
            database: Path to SQLite database file or ":memory:" for in-memory
                     (defaults to SQLITE_DATABASE from .env)
            **kwargs: Additional connection parameters
        """
        # Use environment variable if not provided
        self.database = database or SQLITE_DATABASE
        
        super().__init__(
            connection_name=connection_name,
            database=self.database,
            **kwargs
        )
    
    def connect(self) -> bool:
        """Establish connection to the SQLite3 database."""
        try:
            self.connection = sqlite3.connect(
                self.database,
                check_same_thread=False
            )
            self.connection.row_factory = sqlite3.Row  # Return rows as dictionaries
            
            print(f"✓ Successfully connected to SQLite3 database '{self.connection_name}': {self.database}")
            return True
            
        except sqlite3.Error as e:
            print(f"✗ SQLite3 database error ({self.connection_name}): {str(e)}")
            return False
            
        except Exception as e:
            print(f"✗ Unexpected error connecting to SQLite3 ({self.connection_name}): {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Close the SQLite3 database connection."""
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                print(f"✓ SQLite3 connection '{self.connection_name}' closed")
        except Exception as e:
            print(f"✗ Error closing SQLite3 connection ({self.connection_name}): {str(e)}")
    
    def is_connected(self) -> bool:
        """Check if SQLite3 database connection is active."""
        try:
            if self.connection:
                # Try to execute a simple query to check connection
                self.connection.execute("SELECT 1")
                return True
            return False
        except Exception:
            return False
    
    def get_connection(self) -> Optional[sqlite3.Connection]:
        """Get the current SQLite3 database connection."""
        if self.is_connected():
            return self.connection
        return None
    
    def execute_query(self, query: str) -> Optional[List]:
        """Execute a SELECT query and return results."""
        try:
            if not self.is_connected():
                print(f"✗ Not connected to SQLite3 database '{self.connection_name}'. Please connect first.")
                return None
            
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            # Convert Row objects to tuples
            return [tuple(row) for row in results]
            
        except sqlite3.Error as e:
            print(f"✗ SQLite3 query error ({self.connection_name}): {str(e)}")
            return None
            
        except Exception as e:
            print(f"✗ Unexpected error executing SQLite3 query ({self.connection_name}): {str(e)}")
            return None
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> bool:
        """Execute an INSERT/UPDATE/DELETE query."""
        try:
            if not self.is_connected():
                print(f"✗ Not connected to SQLite3 database '{self.connection_name}'. Please connect first.")
                return False
            
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.connection.commit()
            cursor.close()
            
            return True
            
        except sqlite3.Error as e:
            print(f"✗ SQLite3 update error ({self.connection_name}): {str(e)}")
            if self.connection:
                self.connection.rollback()
            return False
            
        except Exception as e:
            print(f"✗ Unexpected error executing SQLite3 update ({self.connection_name}): {str(e)}")
            if self.connection:
                self.connection.rollback()
            return False

