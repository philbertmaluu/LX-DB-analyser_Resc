"""Base database connection interface."""

from abc import ABC, abstractmethod
from typing import Optional, Any, Dict, List


class BaseDatabaseConnection(ABC):
    """Abstract base class for all database connections."""
    
    def __init__(self, connection_name: str = "default", **kwargs):
        """
        Initialize base database connection.
        
        Args:
            connection_name: Unique name for this connection
            **kwargs: Database-specific connection parameters
        """
        self.connection_name = connection_name
        self.connection: Optional[Any] = None
        self.config = kwargs
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the database.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection."""
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if database connection is active.
        
        Returns:
            True if connected, False otherwise
        """
        pass
    
    @abstractmethod
    def get_connection(self) -> Optional[Any]:
        """
        Get the current database connection object.
        
        Returns:
            Connection object or None if not connected
        """
        pass
    
    @abstractmethod
    def execute_query(self, query: str) -> Optional[List]:
        """
        Execute a SELECT query and return results.
        
        Args:
            query: SQL query string
            
        Returns:
            List of tuples containing query results, or None on error
        """
        pass
    
    @abstractmethod
    def execute_update(self, query: str, params: Optional[tuple] = None) -> bool:
        """
        Execute an INSERT/UPDATE/DELETE query.
        
        Args:
            query: SQL query string
            params: Optional parameters for parameterized queries
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    def get_info(self) -> Dict[str, Any]:
        """
        Get connection information.
        
        Returns:
            Dictionary with connection details
        """
        return {
            "name": self.connection_name,
            "type": self.__class__.__name__,
            "connected": self.is_connected(),
            "config": {k: v for k, v in self.config.items() if k != "password"}
        }
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

