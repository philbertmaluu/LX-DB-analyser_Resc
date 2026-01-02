"""Factory for creating database connections."""

from typing import Dict, Any, Optional
from .base import BaseDatabaseConnection
from .oracle_connection import OracleConnection
from .mysql_connection import MySQLConnection
from .sqlite_connection import SQLiteConnection
from .postgres_connection import PostgreSQLConnection


class DatabaseFactory:
    """Factory class for creating database connections based on type."""
    
    _connection_classes: Dict[str, type] = {
        "oracle": OracleConnection,
        "mysql": MySQLConnection,
        "sqlite": SQLiteConnection,
        "sqlite3": SQLiteConnection,
        "postgres": PostgreSQLConnection,
        "postgresql": PostgreSQLConnection,
    }
    
    @classmethod
    def create(
        cls,
        db_type: str,
        connection_name: str = "default",
        **kwargs
    ) -> Optional[BaseDatabaseConnection]:
        """
        Create a database connection based on type.
        
        Args:
            db_type: Type of database (oracle, mysql, sqlite, postgres)
            connection_name: Unique name for this connection
            **kwargs: Database-specific connection parameters
            
        Returns:
            Database connection instance or None if type not supported
            
        Example:
            # Oracle (uses .env if parameters not provided)
            conn = DatabaseFactory.create(
                "oracle",
                connection_name="oracle_main"
            )
            # Or with explicit parameters
            conn = DatabaseFactory.create(
                "oracle",
                connection_name="oracle_main",
                host="your_host",
                port=1521,
                sid="your_sid",
                user="your_user",
                password="your_password"
            )
            
            # MySQL (uses .env if parameters not provided)
            conn = DatabaseFactory.create(
                "mysql",
                connection_name="mysql_main"
            )
            
            # SQLite
            conn = DatabaseFactory.create(
                "sqlite",
                connection_name="sqlite_main",
                database="data.db"
            )
            
            # PostgreSQL (uses .env if parameters not provided)
            conn = DatabaseFactory.create(
                "postgres",
                connection_name="postgres_main"
            )
        """
        db_type_lower = db_type.lower()
        
        if db_type_lower not in cls._connection_classes:
            supported = ", ".join(cls._connection_classes.keys())
            print(f"✗ Unsupported database type: {db_type}. Supported types: {supported}")
            return None
        
        connection_class = cls._connection_classes[db_type_lower]
        
        try:
            return connection_class(connection_name=connection_name, **kwargs)
        except Exception as e:
            print(f"✗ Error creating {db_type} connection: {str(e)}")
            return None
    
    @classmethod
    def get_supported_types(cls) -> list:
        """Get list of supported database types."""
        return list(cls._connection_classes.keys())

