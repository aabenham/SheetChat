from app.db import get_connection
from app.schema_manager import SchemaManager
from app.query_service import QueryService
from app.validator import SQLValidator


def test_llm_bad_query_is_rejected():
    conn = get_connection(":memory:")
    schema_manager = SchemaManager(conn)

    schema = [
        {"name": "name", "normalized_name": "name", "type": "TEXT"},
        {"name": "age", "normalized_name": "age", "type": "INTEGER"},
    ]

    schema_manager.create_table("users", schema)

    validator = SQLValidator(schema_manager)
    service = QueryService(conn, schema_manager, validator)

    # Simulated bad LLM output
    llm_sql = "SELECT salary FROM users"

    result = service.execute_sql(llm_sql)

    assert result["success"] is False
    assert "Unknown column" in result["error"]

    conn.close()