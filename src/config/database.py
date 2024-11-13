import os
from typing import Dict

import mysql.connector
from mysql.connector import pooling

from src.exceptions import DatabaseConnectionError

# Database configuration from Lambda environment variables
DB_CONFIG: Dict = {
    "host": os.environ["DB_HOST"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "database": os.environ["DB_NAME"],
    "pool_name": "mypool",
    "pool_size": 5,  # Smaller pool size for Lambda environment
    "connect_timeout": 10,
    "use_pure": True,  # Use pure Python implementation
}


def get_connection_pool() -> pooling.MySQLConnectionPool:
    """
    Get the database connection pool

    Returns:
        MySQLConnectionPool instance

    Raises:
        Exception: When unable to create connection pool
    """
    try:
        return mysql.connector.pooling.MySQLConnectionPool(**DB_CONFIG)
    except Exception as e:
        raise DatabaseConnectionError(
            f"Failed to create database connection pool: {str(e)}"
        ) from e
