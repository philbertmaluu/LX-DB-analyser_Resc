"""MySQL database connection implementation."""

import mysql.connector
from mysql.connector import Error as MySQLError
from typing import Optional, List, Any
from .base import BaseDatabaseConnection
from config.settings import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_DATABASE,
    MYSQL_USER,
    MYSQL_PASSWORD,
)


class MySQLConnection(BaseDatabaseConnection):
    """Handles MySQL database connections."""
    
    def __init__(
        self,
        connection_name: str = "mysql_default",
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize MySQL database connection parameters.
        
        Args:
            connection_name: Unique name for this connection
            host: Database host address (defaults to MYSQL_HOST from .env)
            port: Database port number (defaults to MYSQL_PORT from .env)
            database: Database name (defaults to MYSQL_DATABASE from .env)
            user: Database username (defaults to MYSQL_USER from .env)
            password: Database password (defaults to MYSQL_PASSWORD from .env)
            **kwargs: Additional connection parameters
        """
        # Use environment variables if not provided
        self.host = host or MYSQL_HOST
        self.port = port or MYSQL_PORT
        self.database = database or MYSQL_DATABASE
        self.user = user or MYSQL_USER
        self.password = password or MYSQL_PASSWORD
        
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
        """Establish connection to the MySQL database."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            
            print(f"✓ Successfully connected to MySQL database '{self.connection_name}': {self.host}:{self.port}/{self.database}")
            return True
            
        except MySQLError as e:
            print(f"✗ MySQL database error ({self.connection_name}): {str(e)}")
            return False
            
        except Exception as e:
            print(f"✗ Unexpected error connecting to MySQL ({self.connection_name}): {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Close the MySQL database connection."""
        try:
            if self.connection and self.connection.is_connected():
                self.connection.close()
                self.connection = None
                print(f"✓ MySQL connection '{self.connection_name}' closed")
        except Exception as e:
            print(f"✗ Error closing MySQL connection ({self.connection_name}): {str(e)}")
    
    def is_connected(self) -> bool:
        """Check if MySQL database connection is active."""
        try:
            if self.connection:
                return self.connection.is_connected()
            return False
        except Exception:
            return False
    
    def get_connection(self) -> Optional[mysql.connector.MySQLConnection]:
        """Get the current MySQL database connection."""
        if self.is_connected():
            return self.connection
        return None
    
    def execute_query(self, query: str) -> Optional[List]:
        """Execute a SELECT query and return results."""
        try:
            if not self.is_connected():
                print(f"✗ Not connected to MySQL database '{self.connection_name}'. Please connect first.")
                return None
            
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            return results
            
        except MySQLError as e:
            print(f"✗ MySQL query error ({self.connection_name}): {str(e)}")
            return None
            
        except Exception as e:
            print(f"✗ Unexpected error executing MySQL query ({self.connection_name}): {str(e)}")
            return None
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> bool:
        """Execute an INSERT/UPDATE/DELETE query."""
        try:
            if not self.is_connected():
                print(f"✗ Not connected to MySQL database '{self.connection_name}'. Please connect first.")
                return False
            
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.connection.commit()
            cursor.close()
            
            return True
            
        except MySQLError as e:
            print(f"✗ MySQL update error ({self.connection_name}): {str(e)}")
            if self.connection:
                self.connection.rollback()
            return False
            
        except Exception as e:
            print(f"✗ Unexpected error executing MySQL update ({self.connection_name}): {str(e)}")
            if self.connection:
                self.connection.rollback()
            return False

