"""MySQL database connection implementation."""

import mysql.connector
from typing import Optional
from .base import BaseDatabaseConnection
from config.settings import MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD
from src.utils.db_helpers import exec_query, exec_update, safe_disconnect, check_connection


class MySQLConnection(BaseDatabaseConnection):
    """Handles MySQL database connections."""
    
    def __init__(self, connection_name: str = "mysql_default", host: Optional[str] = None,
                 port: Optional[int] = None, database: Optional[str] = None,
                 user: Optional[str] = None, password: Optional[str] = None, **kwargs):
        self.host, self.port = host or MYSQL_HOST, port or MYSQL_PORT
        self.database, self.user, self.password = database or MYSQL_DATABASE, user or MYSQL_USER, password or MYSQL_PASSWORD
        super().__init__(connection_name, host=self.host, port=self.port, database=self.database,
                        user=self.user, password=self.password, **kwargs)
    
    def connect(self) -> bool:
        try:
            self.connection = mysql.connector.connect(host=self.host, port=self.port, database=self.database,
                                                     user=self.user, password=self.password)
            print(f"✓ Successfully connected to MySQL database '{self.connection_name}': {self.host}:{self.port}/{self.database}")
            return True
        except Exception as e:
            print(f"✗ MySQL database error ({self.connection_name}): {str(e)}")
            return False
    
    def disconnect(self) -> None:
        safe_disconnect(self.connection, lambda: self.connection.close() if self.connection.is_connected() else None,
                       self.connection_name, "MySQL")
        self.connection = None
    
    def is_connected(self) -> bool:
        return check_connection(self.connection, lambda: self.connection.is_connected())
    
    def get_connection(self) -> Optional[mysql.connector.MySQLConnection]:
        return self.connection if self.is_connected() else None
    
    def execute_query(self, query: str) -> Optional[list]:
        return exec_query(self.connection, lambda: self.connection.cursor(), self.is_connected,
                         self.connection_name, "MySQL", query)
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> bool:
        return exec_update(self.connection, lambda: self.connection.cursor(), lambda: self.connection.commit(),
                          lambda: self.connection.rollback(), self.is_connected, self.connection_name,
                          "MySQL", query, params)
