"""Oracle database connection implementation."""

import oracledb
from typing import Optional, List, Any
from .base import BaseDatabaseConnection
from config.settings import (
    ORACLE_HOST,
    ORACLE_PORT,
    ORACLE_SID,
    ORACLE_USER,
    ORACLE_PASSWORD,
)


class OracleConnection(BaseDatabaseConnection):
    """Handles Oracle database connections."""
    
    def __init__(
        self,
        connection_name: str = "oracle_default",
        host: Optional[str] = None,
        port: Optional[int] = None,
        sid: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize Oracle database connection parameters.
        
        Args:
            connection_name: Unique name for this connection
            host: Database host address (defaults to ORACLE_HOST from .env)
            port: Database port number (defaults to ORACLE_PORT from .env)
            sid: Oracle SID (defaults to ORACLE_SID from .env)
            user: Database username (defaults to ORACLE_USER from .env)
            password: Database password (defaults to ORACLE_PASSWORD from .env)
            **kwargs: Additional connection parameters
        """
        # Use environment variables if not provided
        self.host = host or ORACLE_HOST
        self.port = port or ORACLE_PORT
        self.sid = sid or ORACLE_SID
        self.user = user or ORACLE_USER
        self.password = password or ORACLE_PASSWORD
        
        super().__init__(
            connection_name=connection_name,
            host=self.host,
            port=self.port,
            sid=self.sid,
            user=self.user,
            password=self.password,
            **kwargs
        )
    
    def connect(self) -> bool:
        """Establish connection to the Oracle database."""
        try:
            # Construct DSN (Data Source Name) for Oracle
            dsn = oracledb.makedsn(
                host=self.host,
                port=self.port,
                sid=self.sid
            )
            
            # Create connection
            self.connection = oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=dsn
            )
            
            print(f"✓ Successfully connected to Oracle database '{self.connection_name}': {self.host}:{self.port}/{self.sid}")
            return True
            
        except oracledb.Error as e:
            error, = e.args
            print(f"✗ Oracle database error ({self.connection_name}): {error.message}")
            return False
            
        except Exception as e:
            print(f"✗ Unexpected error connecting to Oracle ({self.connection_name}): {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Close the Oracle database connection."""
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                print(f"✓ Oracle connection '{self.connection_name}' closed")
        except Exception as e:
            print(f"✗ Error closing Oracle connection ({self.connection_name}): {str(e)}")
    
    def is_connected(self) -> bool:
        """Check if Oracle database connection is active."""
        try:
            if self.connection:
                self.connection.ping()
                return True
            return False
        except Exception:
            return False
    
    def get_connection(self) -> Optional[oracledb.Connection]:
        """Get the current Oracle database connection."""
        if self.is_connected():
            return self.connection
        return None
    
    def execute_query(self, query: str) -> Optional[List]:
        """Execute a SELECT query and return results."""
        try:
            if not self.is_connected():
                print(f"✗ Not connected to Oracle database '{self.connection_name}'. Please connect first.")
                return None
            
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            return results
            
        except oracledb.Error as e:
            error, = e.args
            print(f"✗ Oracle query error ({self.connection_name}): {error.message}")
            return None
            
        except Exception as e:
            print(f"✗ Unexpected error executing Oracle query ({self.connection_name}): {str(e)}")
            return None
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> bool:
        """Execute an INSERT/UPDATE/DELETE query."""
        try:
            if not self.is_connected():
                print(f"✗ Not connected to Oracle database '{self.connection_name}'. Please connect first.")
                return False
            
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.connection.commit()
            cursor.close()
            
            return True
            
        except oracledb.Error as e:
            error, = e.args
            print(f"✗ Oracle update error ({self.connection_name}): {error.message}")
            if self.connection:
                self.connection.rollback()
            return False
            
        except Exception as e:
            print(f"✗ Unexpected error executing Oracle update ({self.connection_name}): {str(e)}")
            if self.connection:
                self.connection.rollback()
            return False

