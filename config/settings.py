"""Configuration settings loaded from environment variables."""

import os
from dotenv import load_dotenv
from typing import Optional

# Load environment variables from .env file
load_dotenv()


def get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Get environment variable value.
    
    Args:
        key: Environment variable name
        default: Default value if not found
        
    Returns:
        Environment variable value or default
    """
    return os.getenv(key, default)


# Oracle Database Configuration
ORACLE_HOST = get_env("ORACLE_HOST", "localhost")
ORACLE_PORT = int(get_env("ORACLE_PORT", "1521"))
ORACLE_SID = get_env("ORACLE_SID", "")
ORACLE_USER = get_env("ORACLE_USER", "")
ORACLE_PASSWORD = get_env("ORACLE_PASSWORD", "")

# MySQL Database Configuration
MYSQL_HOST = get_env("MYSQL_HOST", "localhost")
MYSQL_PORT = int(get_env("MYSQL_PORT", "3306"))
MYSQL_DATABASE = get_env("MYSQL_DATABASE", "")
MYSQL_USER = get_env("MYSQL_USER", "root")
MYSQL_PASSWORD = get_env("MYSQL_PASSWORD", "")

# PostgreSQL Database Configuration
POSTGRES_HOST = get_env("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(get_env("POSTGRES_PORT", "5432"))
POSTGRES_DATABASE = get_env("POSTGRES_DATABASE", "postgres")
POSTGRES_USER = get_env("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = get_env("POSTGRES_PASSWORD", "")

# SQLite Database Configuration
SQLITE_DATABASE = get_env("SQLITE_DATABASE", ":memory:")

