# Database exceptions
class DatabaseError(Exception):
    """Base exception for database related errors"""

    pass


class DatabaseConnectionError(DatabaseError):
    """Exception raised for database connection errors"""

    pass


class DatabaseQueryError(DatabaseError):
    """Exception raised for database query errors"""

    pass
