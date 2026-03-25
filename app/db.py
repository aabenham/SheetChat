import sqlite3


def get_connection(db_path: str) -> sqlite3.Connection:
    """
    Create and return a SQLite database connection.
    """
    return sqlite3.connect(db_path)