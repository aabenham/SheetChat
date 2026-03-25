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
        {"name": "id", "type": "INTEGER"},
        {"name": "name", "type": "TEXT"},
    ]

    manager.create_table("users", schema)

    assert manager.table_exists("users") is True
    actual_schema = manager.get_table_schema("users")

    assert actual_schema == schema
    conn.close()


def test_infer_schema_from_dataframe():
    conn = get_connection(":memory:")
    manager = SchemaManager(conn)

    df = pd.DataFrame({
        "id": [1, 2],
        "price": [9.5, 10.2],
        "name": ["alice", "bob"]
    })

    schema = manager.infer_schema_from_dataframe(df)

    assert schema == [
        {"name": "id", "type": "INTEGER"},
        {"name": "price", "type": "REAL"},
        {"name": "name", "type": "TEXT"},
    ]
    conn.close()