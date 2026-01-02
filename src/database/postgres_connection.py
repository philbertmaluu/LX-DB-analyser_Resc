"""PostgreSQL database connection implementation."""

import psycopg2
from psycopg2 import Error as PostgresError
from typing import Optional, List, Any
from .base import BaseDatabaseConnection
from config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DATABASE,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)


class PostgreSQLConnection(BaseDatabaseConnection):
    """Handles PostgreSQL database connections."""
    
    def __init__(
        self,
        connection_name: str = "postgres_default",
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize PostgreSQL database connection parameters.
        
        Args:
            connection_name: Unique name for this connection
            host: Database host address (defaults to POSTGRES_HOST from .env)
            port: Database port number (defaults to POSTGRES_PORT from .env)
            database: Database name (defaults to POSTGRES_DATABASE from .env)
            user: Database username (defaults to POSTGRES_USER from .env)
            password: Database password (defaults to POSTGRES_PASSWORD from .env)
            **kwargs: Additional connection parameters
        """
        # Use environment variables if not provided
        self.host = host or POSTGRES_HOST
        self.port = port or POSTGRES_PORT
        self.database = database or POSTGRES_DATABASE
        self.user = user or POSTGRES_USER
        self.password = password or POSTGRES_PASSWORD
        
        super().__init__(
            connection_name=connection_name,
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            **kwargs
        )
    
    def connect(self) -> bool:
        """Establish connection to the PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            
            print(f"✓ Successfully connected to PostgreSQL database '{self.connection_name}': {self.host}:{self.port}/{self.database}")
            return True
            
        except PostgresError as e:
            print(f"✗ PostgreSQL database error ({self.connection_name}): {str(e)}")
            return False
            
        except Exception as e:
            print(f"✗ Unexpected error connecting to PostgreSQL ({self.connection_name}): {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Close the PostgreSQL database connection."""
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                print(f"✓ PostgreSQL connection '{self.connection_name}' closed")
        except Exception as e:
            print(f"✗ Error closing PostgreSQL connection ({self.connection_name}): {str(e)}")
    
    def is_connected(self) -> bool:
        """Check if PostgreSQL database connection is active."""
        try:
            if self.connection:
                # Try to execute a simple query to check connection
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                return True
            return False
        except Exception:
            return False
    
    def get_connection(self) -> Optional[psycopg2.extensions.connection]:
        """Get the current PostgreSQL database connection."""
        if self.is_connected():
            return self.connection
        return None
    
    def execute_query(self, query: str) -> Optional[List]:
        """Execute a SELECT query and return results."""
        try:
            if not self.is_connected():
                print(f"✗ Not connected to PostgreSQL database '{self.connection_name}'. Please connect first.")
                return None
            
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            return results
            
        except PostgresError as e:
            print(f"✗ PostgreSQL query error ({self.connection_name}): {str(e)}")
            return None
            
        except Exception as e:
            print(f"✗ Unexpected error executing PostgreSQL query ({self.connection_name}): {str(e)}")
            return None
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> bool:
        """Execute an INSERT/UPDATE/DELETE query."""
        try:
            if not self.is_connected():
                print(f"✗ Not connected to PostgreSQL database '{self.connection_name}'. Please connect first.")
                return False
            
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.connection.commit()
            cursor.close()
            
            return True
            
        except PostgresError as e:
            print(f"✗ PostgreSQL update error ({self.connection_name}): {str(e)}")
            if self.connection:
                self.connection.rollback()
            return False
            
        except Exception as e:
            print(f"✗ Unexpected error executing PostgreSQL update ({self.connection_name}): {str(e)}")
            if self.connection:
                self.connection.rollback()
            return False

