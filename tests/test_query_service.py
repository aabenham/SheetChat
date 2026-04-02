from app.db import get_connection
from app.llm_adapter import LLMAdapter
from app.query_service import QueryService
from app.schema_manager import SchemaManager
from app.validator import SQLValidator


def build_service(with_llm=False):
    conn = get_connection(":memory:")
    schema_manager = SchemaManager(conn)

    schema = [
        {"name": "name", "normalized_name": "name", "type": "TEXT"},
        {"name": "age", "normalized_name": "age", "type": "INTEGER"},
    ]

    schema_manager.create_table("users", schema)

    conn.execute("INSERT INTO users (name, age) VALUES ('alice', 22)")
    conn.commit()

    validator = SQLValidator(schema_manager)
    llm_adapter = LLMAdapter() if with_llm else None
    service = QueryService(conn, schema_manager, validator, llm_adapter)

    return conn, service


def test_list_tables():
    conn, service = build_service()

    tables = service.list_tables()

    assert "users" in tables
    conn.close()


def test_get_schema():
    conn, service = build_service()

    schema = service.get_schema("users")
    column_names = [col["normalized_name"] for col in schema]

    assert "id" in column_names
    assert "name" in column_names
    assert "age" in column_names

    conn.close()


def test_execute_valid_sql():
    conn, service = build_service()

    result = service.execute_sql("SELECT name, age FROM users")

    assert result["success"] is True
    assert result["error"] is None
    assert result["columns"] == ["name", "age"]
    assert result["rows"] == [("alice", 22)]
    assert result["generated_sql"] is None

    conn.close()


def test_reject_invalid_sql():
    conn, service = build_service()

    result = service.execute_sql("DELETE FROM users")

    assert result["success"] is False
    assert "Only SELECT queries are allowed" in result["error"]
    assert result["columns"] == []
    assert result["rows"] == []
    assert result["generated_sql"] is None

    conn.close()


def test_reject_unknown_column_query():
    conn, service = build_service()

    result = service.execute_sql("SELECT salary FROM users")

    assert result["success"] is False
    assert "Unknown column" in result["error"]
    assert result["columns"] == []
    assert result["rows"] == []
    assert result["generated_sql"] is None

    conn.close()


def test_handle_user_input_accepts_sql_directly():
    conn, service = build_service(with_llm=True)

    result = service.handle_user_input("SELECT name FROM users")

    assert result["success"] is True
    assert result["columns"] == ["name"]
    assert result["rows"] == [("alice",)]
    assert result["generated_sql"] is None

    conn.close()


def test_handle_user_input_uses_llm_for_natural_language():
    conn, service = build_service(with_llm=True)

    result = service.handle_user_input("show all users")

    assert result["success"] is True
    assert result["generated_sql"] == "SELECT * FROM users"
    assert result["columns"] == ["id", "name", "age"]
    assert result["rows"] == [(1, "alice", 22)]

    conn.close()


def test_handle_user_input_fails_without_llm():
    conn, service = build_service(with_llm=False)

    result = service.handle_user_input("show all users")

    assert result["success"] is False
    assert result["columns"] == []
    assert result["rows"] == []
    assert result["generated_sql"] is None
    assert "LLM adapter" in result["error"]

    conn.close()


def test_handle_user_input_rejects_sql_without_using_llm():
    conn, service = build_service(with_llm=True)

    result = service.handle_user_input("DELETE FROM users")

    assert result["success"] is False
    assert "Only SELECT queries are allowed" in result["error"]
    assert result["columns"] == []
    assert result["rows"] == []
    assert result["generated_sql"] is None

    conn.close()