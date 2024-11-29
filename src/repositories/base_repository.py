import logging
from typing import Any, Dict, List, Optional

import mysql.connector
from mysql.connector import Error

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class BaseRepository:
    def __init__(self, connection_pool: mysql.connector.pooling.MySQLConnectionPool):
        """
        Initialize the base repository

        Args:
            connection_pool: MySQL connection pool
        """
        self.pool = connection_pool

    def _execute_query(
        self, query: str, params: Optional[tuple] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a SELECT query

        Args:
            query: SQL query statement
            params: Query parameters

        Returns:
            List of query results or None if error occurs

        Raises:
            Error: When database operation fails
        """
        connection = None
        try:
            connection = self.pool.get_connection()
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute(query, params or ())
                return cursor.fetchall()
        except Error as e:
            logger.error("Database error during query execution: %s", str(e))
            raise
        except Exception as e:
            logger.error("Unexpected error during query execution: %s", str(e))
            raise
        finally:
            if connection:
                connection.close()

    def _execute_write(
        self, query: str, params: Optional[tuple] = None
    ) -> Optional[int]:
        """
        Execute a write operation (INSERT, UPDATE, DELETE)

        Args:
            query: SQL statement
            params: Query parameters

        Returns:
            Last inserted ID or number of affected rows

        Raises:
            Error: When database operation fails
        """
        connection = None
        try:
            connection = self.pool.get_connection()
            with connection.cursor() as cursor:
                cursor.execute(query, params or ())
                connection.commit()
                return cursor.lastrowid or cursor.rowcount
        except Error as e:
            logger.error("Database error during write operation: %s", str(e))
            if connection:
                connection.rollback()
            raise
        except Exception as e:
            logger.error("Unexpected error during write operation: %s", str(e))
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                connection.close()

    def _execute_many(self, query: str, params: List[tuple]) -> Optional[int]:
        """
        Execute a bulk write operation

        Args:
            query: SQL statement
            params: List of parameter tuples

        Returns:
            Number of affected rows

        Raises:
            Error: When database operation fails
        """
        connection = None
        try:
            connection = self.pool.get_connection()
            with connection.cursor() as cursor:
                cursor.executemany(query, params)
                connection.commit()
                return cursor.rowcount
        except Error as e:
            logger.error("Database error during bulk write operation: %s", str(e))
            if connection:
                connection.rollback()
            raise
        except Exception as e:
            logger.error("Unexpected error during bulk write operation: %s", str(e))
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
