import pandas as pd

from app.db import get_connection
from app.schema_manager import SchemaManager
from app.query_service import QueryService


def test_list_tables():
    conn = get_connection(":memory:")
    schema_manager = SchemaManager(conn)

    schema = [
        {"name": "id", "normalized_name": "id", "type": "INTEGER"},
    ]

    schema_manager.create_table("users", schema)

    service = QueryService(conn, schema_manager)

    tables = service.list_tables()

    assert "users" in tables

    conn.close()


def test_get_schema():
    conn = get_connection(":memory:")
    schema_manager = SchemaManager(conn)

    schema = [
        {"name": "id", "normalized_name": "id", "type": "INTEGER"},
    ]

    schema_manager.create_table("users", schema)

    service = QueryService(conn, schema_manager)

    result = service.get_schema("users")

    assert result == schema

    conn.close()


def test_execute_sql():
    conn = get_connection(":memory:")
    schema_manager = SchemaManager(conn)

    schema = [
        {"name": "id", "normalized_name": "id", "type": "INTEGER"},
    ]

    schema_manager.create_table("users", schema)

    conn.execute("INSERT INTO users (id) VALUES (1)")
    conn.commit()

    service = QueryService(conn, schema_manager)

    result = service.execute_sql("SELECT * FROM users")

    assert result["columns"] == ["id"]
    assert result["rows"] == [(1,)]

    conn.close()