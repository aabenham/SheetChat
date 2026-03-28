import pandas as pd

from app.db import get_connection
from app.schema_manager import SchemaManager


def test_table_exists_false_initially():
    conn = get_connection(":memory:")
    manager = SchemaManager(conn)

    assert manager.table_exists("users") is False
    conn.close()


def test_create_table_and_detect_schema():
    conn = get_connection(":memory:")
    manager = SchemaManager(conn)

    schema = [
        {"name": "name", "normalized_name": "name", "type": "TEXT"},
    ]

    manager.create_table("users", schema)

    assert manager.table_exists("users") is True

    actual_schema = manager.get_table_schema("users")
    column_names = [col["normalized_name"] for col in actual_schema]

    assert "id" in column_names
    assert "name" in column_names

    id_column = next(col for col in actual_schema if col["normalized_name"] == "id")
    name_column = next(col for col in actual_schema if col["normalized_name"] == "name")

    assert id_column["type"] == "INTEGER"
    assert name_column["type"] == "TEXT"

    conn.close()


def test_infer_schema_from_dataframe():
    conn = get_connection(":memory:")
    manager = SchemaManager(conn)

    df = pd.DataFrame(
        {
            "id": [1, 2],
            "price": [9.5, 10.2],
            "name": ["alice", "bob"],
        }
    )

    schema = manager.infer_schema_from_dataframe(df)

    assert schema == [
        {"name": "id", "normalized_name": "id", "type": "INTEGER"},
        {"name": "price", "normalized_name": "price", "type": "REAL"},
        {"name": "name", "normalized_name": "name", "type": "TEXT"},
    ]

    conn.close()


def test_normalize_column_name():
    conn = get_connection(":memory:")
    manager = SchemaManager(conn)

    assert manager.normalize_column_name("First Name") == "first_name"
    assert manager.normalize_column_name(" User-ID ") == "userid"
    assert manager.normalize_column_name("Account Number") == "account_number"

    conn.close()


def test_schemas_match_when_equal():
    conn = get_connection(":memory:")
    manager = SchemaManager(conn)

    schema_a = [
        {"name": "First Name", "normalized_name": "first_name", "type": "TEXT"},
        {"name": "Age", "normalized_name": "age", "type": "INTEGER"},
    ]

    schema_b = [
        {"name": "first_name", "normalized_name": "first_name", "type": "TEXT"},
        {"name": "age", "normalized_name": "age", "type": "INTEGER"},
    ]

    assert manager.schemas_match(schema_a, schema_b) is True
    conn.close()


def test_schemas_do_not_match_when_names_differ():
    conn = get_connection(":memory:")
    manager = SchemaManager(conn)

    schema_a = [
        {"name": "First Name", "normalized_name": "first_name", "type": "TEXT"},
    ]

    schema_b = [
        {"name": "Nombre", "normalized_name": "nombre", "type": "TEXT"},
    ]

    assert manager.schemas_match(schema_a, schema_b) is False
    conn.close()


def test_schemas_do_not_match_when_types_differ():
    conn = get_connection(":memory:")
    manager = SchemaManager(conn)

    schema_a = [
        {"name": "age", "normalized_name": "age", "type": "INTEGER"},
    ]

    schema_b = [
        {"name": "age", "normalized_name": "age", "type": "TEXT"},
    ]

    assert manager.schemas_match(schema_a, schema_b) is False
    conn.close()


def test_decide_create_or_append_returns_create_for_new_table():
    conn = get_connection(":memory:")
    manager = SchemaManager(conn)

    inferred_schema = [
        {"name": "name", "normalized_name": "name", "type": "TEXT"},
    ]

    assert manager.decide_create_or_append("users", inferred_schema) == "create"
    conn.close()


def test_decide_create_or_append_returns_append_for_matching_schema():
    conn = get_connection(":memory:")
    manager = SchemaManager(conn)

    schema = [
        {"name": "name", "normalized_name": "name", "type": "TEXT"},
    ]

    manager.create_table("users", schema)

    inferred_schema = [
        {"name": "name", "normalized_name": "name", "type": "TEXT"},
    ]

    assert manager.decide_create_or_append("users", inferred_schema) == "append"
    conn.close()


def test_decide_create_or_append_returns_conflict_for_mismatch():
    conn = get_connection(":memory:")
    manager = SchemaManager(conn)

    existing_schema = [
        {"name": "name", "normalized_name": "name", "type": "TEXT"},
    ]

    manager.create_table("users", existing_schema)

    inferred_schema = [
        {"name": "age", "normalized_name": "age", "type": "INTEGER"},
    ]

    assert manager.decide_create_or_append("users", inferred_schema) == "conflict"
    conn.close()