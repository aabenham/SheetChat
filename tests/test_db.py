from app.db import get_connection


def test_get_connection():
    conn = get_connection(":memory:")
    assert conn is not None
    conn.close()