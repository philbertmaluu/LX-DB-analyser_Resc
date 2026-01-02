"""Oracle database connection implementation."""

import oracledb
from typing import Optional
from .base import BaseDatabaseConnection
from config.settings import ORACLE_HOST, ORACLE_PORT, ORACLE_SID, ORACLE_USER, ORACLE_PASSWORD
from src.utils.db_helpers import exec_query, exec_update, safe_disconnect, check_connection

# Initialize thick mode to support all password verifier types
try:
    oracledb.init_oracle_client()
except Exception:
    pass  # Thick mode already initialized or client not available


class OracleConnection(BaseDatabaseConnection):
    """Handles Oracle database connections."""
    
    def __init__(self, connection_name: str = "oracle_default", host: Optional[str] = None,
                 port: Optional[int] = None, sid: Optional[str] = None,
                 user: Optional[str] = None, password: Optional[str] = None, **kwargs):
        self.host, self.port, self.sid = host or ORACLE_HOST, port or ORACLE_PORT, sid or ORACLE_SID
        self.user, self.password = user or ORACLE_USER, password or ORACLE_PASSWORD
        super().__init__(connection_name, host=self.host, port=self.port, sid=self.sid,
                        user=self.user, password=self.password, **kwargs)
    
    def connect(self) -> bool:
        try:
            self.connection = oracledb.connect(user=self.user, password=self.password,
                                             dsn=oracledb.makedsn(self.host, self.port, sid=self.sid))
            print(f"✓ Successfully connected to Oracle database '{self.connection_name}': {self.host}:{self.port}/{self.sid}")
            return True
        except Exception as e:
            print(f"✗ Oracle database error ({self.connection_name}): {str(e)}")
            return False
    
    def disconnect(self) -> None:
        safe_disconnect(self.connection, lambda: self.connection.close(), self.connection_name, "Oracle")
        self.connection = None
    
    def is_connected(self) -> bool:
        try:
            if self.connection:
                # Try a simple query to check connection
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                cursor.close()
                return True
            return False
        except Exception:
            return False
    
    def get_connection(self) -> Optional[oracledb.Connection]:
        return self.connection if self.is_connected() else None
    
    def execute_query(self, query: str) -> Optional[list]:
        return exec_query(self.connection, lambda: self.connection.cursor(), self.is_connected,
                         self.connection_name, "Oracle", query)
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> bool:
        return exec_update(self.connection, lambda: self.connection.cursor(), lambda: self.connection.commit(),
                          lambda: self.connection.rollback(), self.is_connected, self.connection_name,
                          "Oracle", query, params)
