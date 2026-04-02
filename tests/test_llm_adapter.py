import pytest

from app.llm_adapter import LLMAdapter


def test_llm_adapter_exists():
    adapter = LLMAdapter()
    assert adapter is not None


def test_generate_sql_show_all():
    adapter = LLMAdapter()

    sql = adapter.generate_sql("show all users", "users, orders")

    assert sql == "SELECT * FROM users"


def test_generate_sql_count():
    adapter = LLMAdapter()

    sql = adapter.generate_sql("how many users are there", "users, orders")

    assert sql == "SELECT COUNT(*) FROM users"


def test_generate_sql_uses_first_table_as_fallback():
    adapter = LLMAdapter()

    sql = adapter.generate_sql("show all records", "users, orders")

    assert sql == "SELECT * FROM users"


def test_generate_sql_rejects_empty_question():
    adapter = LLMAdapter()

    with pytest.raises(ValueError):
        adapter.generate_sql("", "users, orders")


def test_generate_sql_rejects_empty_schema():
    adapter = LLMAdapter()

    with pytest.raises(ValueError):
        adapter.generate_sql("show all users", "")


def test_generate_sql_rejects_unknown_prompt():
    adapter = LLMAdapter()

    with pytest.raises(ValueError):
        adapter.generate_sql("find the best customers by lifetime value", "users, orders")