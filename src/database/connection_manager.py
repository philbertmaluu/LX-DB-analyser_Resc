"""Manager for handling multiple database connections."""

from typing import Dict, Optional, List
from .base import BaseDatabaseConnection
from .factory import DatabaseFactory


class ConnectionManager:
    """Manages multiple database connections."""
    
    def __init__(self):
        """Initialize the connection manager."""
        self.connections: Dict[str, BaseDatabaseConnection] = {}
    
    def add_connection(
        self,
        name: str,
        db_type: str,
        **kwargs
    ) -> bool:
        """
        Add a new database connection.
        
        Args:
            name: Unique name for this connection
            db_type: Type of database (oracle, mysql, sqlite, postgres)
            **kwargs: Database-specific connection parameters
            
        Returns:
            True if connection created successfully, False otherwise
            
        Example:
            # Uses credentials from .env file
            manager.add_connection(
                "oracle_main",
                "oracle"
            )
            # Or with explicit parameters
            manager.add_connection(
                "oracle_main",
                "oracle",
                host="your_host",
                port=1521,
                sid="your_sid",
                user="your_user",
                password="your_password"
            )
        """
        if name in self.connections:
            print(f"✗ Connection '{name}' already exists. Use remove_connection() first.")
            return False
        
        connection = DatabaseFactory.create(
            db_type=db_type,
            connection_name=name,
            **kwargs
        )
        
        if connection:
            self.connections[name] = connection
            return True
        return False
    
    def get_connection(self, name: str) -> Optional[BaseDatabaseConnection]:
        """
        Get a connection by name.
        
        Args:
            name: Connection name
            
        Returns:
            Connection instance or None if not found
        """
        return self.connections.get(name)
    
    def connect(self, name: str) -> bool:
        """
        Connect to a database by name.
        
        Args:
            name: Connection name
            
        Returns:
            True if connection successful, False otherwise
        """
        connection = self.get_connection(name)
        if not connection:
            print(f"✗ Connection '{name}' not found.")
            return False
        return connection.connect()
    
    def disconnect(self, name: str) -> None:
        """
        Disconnect from a database by name.
        
        Args:
            name: Connection name
        """
        connection = self.get_connection(name)
        if connection:
            connection.disconnect()
    
    def disconnect_all(self) -> None:
        """Disconnect from all databases."""
        for name in list(self.connections.keys()):
            self.disconnect(name)
    
    def remove_connection(self, name: str) -> bool:
        """
        Remove a connection from the manager.
        
        Args:
            name: Connection name
            
        Returns:
            True if removed successfully, False otherwise
        """
        connection = self.get_connection(name)
        if connection:
            connection.disconnect()
            del self.connections[name]
            print(f"✓ Connection '{name}' removed")
            return True
        print(f"✗ Connection '{name}' not found")
        return False
    
    def list_connections(self) -> List[Dict]:
        """
        List all connections and their status.
        
        Returns:
            List of connection information dictionaries
        """
        return [conn.get_info() for conn in self.connections.values()]
    
    def get_connection_info(self, name: str) -> Optional[Dict]:
        """
        Get information about a specific connection.
        
        Args:
            name: Connection name
            
        Returns:
            Connection info dictionary or None if not found
        """
        connection = self.get_connection(name)
        if connection:
            return connection.get_info()
        return None
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - disconnect all connections."""
        self.disconnect_all()

