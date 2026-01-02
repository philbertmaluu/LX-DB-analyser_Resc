"""Database helper functions for common operations."""

from typing import Optional, List, Any, Callable


def safe_execute(func: Callable, error_msg: str) -> tuple:
    """Safely execute a function and return (success, result)."""
    try:
        return True, func()
    except Exception as e:
        print(error_msg.format(str(e)))
        return False, None


def exec_query(conn, cursor_func, is_connected_func, conn_name: str, db_type: str, query: str) -> Optional[List]:
    """Execute a SELECT query."""
    if not is_connected_func():
        print(f"✗ Not connected to {db_type} database '{conn_name}'. Please connect first.")
        return None
    try:
        cursor = cursor_func()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
    except Exception as e:
        print(f"✗ {db_type} query error ({conn_name}): {str(e)}")
        return None


def exec_update(conn, cursor_func, commit_func, rollback_func, is_connected_func, conn_name: str, db_type: str, query: str, params: Optional[tuple]) -> bool:
    """Execute an INSERT/UPDATE/DELETE query."""
    if not is_connected_func():
        print(f"✗ Not connected to {db_type} database '{conn_name}'. Please connect first.")
        return False
    try:
        cursor = cursor_func()
        cursor.execute(query, params) if params else cursor.execute(query)
        commit_func()
        cursor.close()
        return True
    except Exception as e:
        print(f"✗ {db_type} update error ({conn_name}): {str(e)}")
        rollback_func()
        return False


def safe_disconnect(conn, close_func, conn_name: str, db_type: str):
    """Safely disconnect from database."""
    if conn:
        try:
            close_func()
            print(f"✓ {db_type} connection '{conn_name}' closed")
        except Exception as e:
            print(f"✗ Error closing {db_type} connection ({conn_name}): {str(e)}")


def check_connection(conn, check_func: Callable) -> bool:
    """Check if connection is active."""
    try:
        return conn and check_func()
    except:
        return False
