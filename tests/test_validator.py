from app.db import get_connection
from app.schema_manager import SchemaManager
from app.validator import SQLValidator


def setup_users_table():
    conn = get_connection(":memory:")
    schema_manager = SchemaManager(conn)

    schema = [
        {"name": "name", "normalized_name": "name", "type": "TEXT"},
        {"name": "age", "normalized_name": "age", "type": "INTEGER"},
    ]

    schema_manager.create_table("users", schema)
    validator = SQLValidator(schema_manager)

    return conn, validator


def test_allows_simple_select():
    conn, validator = setup_users_table()

    result = validator.validate("SELECT name FROM users")

    assert result["valid"] is True
    assert result["error"] is None

    conn.close()


def test_rejects_non_select_query():
    conn, validator = setup_users_table()

    result = validator.validate("DELETE FROM users")

    assert result["valid"] is False
    assert "Only SELECT queries are allowed" in result["error"]

    conn.close()


def test_rejects_unknown_table():
    conn, validator = setup_users_table()

    result = validator.validate("SELECT name FROM customers")

    assert result["valid"] is False
    assert "Unknown table" in result["error"]

    conn.close()


def test_rejects_unknown_column():
    conn, validator = setup_users_table()

    result = validator.validate("SELECT salary FROM users")

    assert result["valid"] is False
    assert "Unknown column" in result["error"]

    conn.close()


def test_allows_select_all():
    conn, validator = setup_users_table()

    result = validator.validate("SELECT * FROM users")

    assert result["valid"] is True
    assert result["error"] is None

    conn.close()


def test_rejects_empty_query():
    conn, validator = setup_users_table()

    result = validator.validate("")

    assert result["valid"] is False
    assert "Query cannot be empty" in result["error"]

    conn.close()