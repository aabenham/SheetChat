import pytest

from app.llm_adapter import LLMAdapter


def test_llm_adapter_exists():
    adapter = LLMAdapter()
    assert adapter is not None


def test_llm_adapter_generate_sql_not_implemented():
    adapter = LLMAdapter()

    with pytest.raises(NotImplementedError):
        adapter.generate_sql("show all users", "users(id, name, age)")