"""PostgreSQL database connection implementation."""

import psycopg2
from typing import Optional
from .base import BaseDatabaseConnection
from config.settings import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DATABASE, POSTGRES_USER, POSTGRES_PASSWORD
from src.utils.db_helpers import exec_query, exec_update, safe_disconnect, check_connection


class PostgreSQLConnection(BaseDatabaseConnection):
    """Handles PostgreSQL database connections."""
    
    def __init__(self, connection_name: str = "postgres_default", host: Optional[str] = None,
                 port: Optional[int] = None, database: Optional[str] = None,
                 user: Optional[str] = None, password: Optional[str] = None, **kwargs):
        self.host, self.port = host or POSTGRES_HOST, port or POSTGRES_PORT
        self.database, self.user, self.password = database or POSTGRES_DATABASE, user or POSTGRES_USER, password or POSTGRES_PASSWORD
        super().__init__(connection_name, host=self.host, port=self.port, database=self.database,
                        user=self.user, password=self.password, **kwargs)
    
    def connect(self) -> bool:
        try:
            self.connection = psycopg2.connect(host=self.host, port=self.port, database=self.database,
                                              user=self.user, password=self.password)
            print(f"✓ Successfully connected to PostgreSQL database '{self.connection_name}': {self.host}:{self.port}/{self.database}")
            return True
        except Exception as e:
            print(f"✗ PostgreSQL database error ({self.connection_name}): {str(e)}")
            return False
    
    def disconnect(self) -> None:
        safe_disconnect(self.connection, lambda: self.connection.close(), self.connection_name, "PostgreSQL")
        self.connection = None
    
    def is_connected(self) -> bool:
        return check_connection(self.connection, lambda: (c := self.connection.cursor(), c.execute("SELECT 1"), c.close()) or True)
    
    def get_connection(self) -> Optional[psycopg2.extensions.connection]:
        return self.connection if self.is_connected() else None
    
    def execute_query(self, query: str) -> Optional[list]:
        return exec_query(self.connection, lambda: self.connection.cursor(), self.is_connected,
                         self.connection_name, "PostgreSQL", query)
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> bool:
        return exec_update(self.connection, lambda: self.connection.cursor(), lambda: self.connection.commit(),
                          lambda: self.connection.rollback(), self.is_connected, self.connection_name,
                          "PostgreSQL", query, params)
