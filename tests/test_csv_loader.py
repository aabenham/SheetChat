import pandas as pd

from app.csv_loader import CSVLoader
from app.db import get_connection


def test_csv_loader_creates_table(tmp_path):
    conn = get_connection(":memory:")
    loader = CSVLoader(conn)

    df = pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["alice", "bob"],
        }
    )

    csv_file = tmp_path / "test.csv"
    df.to_csv(csv_file, index=False)

    result = loader.load_csv(str(csv_file), "users")

    assert result == "created"

    rows = conn.execute("SELECT id, name FROM users ORDER BY id").fetchall()
    assert rows == [(1, "alice"), (2, "bob")]

    conn.close()


def test_csv_loader_appends(tmp_path):
    conn = get_connection(":memory:")
    loader = CSVLoader(conn)

    df = pd.DataFrame(
        {
            "id": [1],
            "name": ["alice"],
        }
    )

    csv_file = tmp_path / "test.csv"
    df.to_csv(csv_file, index=False)

    loader.load_csv(str(csv_file), "users")

    df2 = pd.DataFrame(
        {
            "id": [2],
            "name": ["bob"],
        }
    )

    csv_file2 = tmp_path / "test2.csv"
    df2.to_csv(csv_file2, index=False)

    result = loader.load_csv(str(csv_file2), "users")

    assert result == "appended"

    rows = conn.execute("SELECT id, name FROM users ORDER BY id").fetchall()
    assert rows == [(1, "alice"), (2, "bob")]

    conn.close()


def test_csv_loader_conflict(tmp_path):
    conn = get_connection(":memory:")
    loader = CSVLoader(conn)

    df = pd.DataFrame(
        {
            "id": [1],
            "name": ["alice"],
        }
    )

    csv_file = tmp_path / "test.csv"
    df.to_csv(csv_file, index=False)

    loader.load_csv(str(csv_file), "users")

    df2 = pd.DataFrame(
        {
            "id": [2],
            "age": [25],
        }
    )

    csv_file2 = tmp_path / "test2.csv"
    df2.to_csv(csv_file2, index=False)

    result = loader.load_csv(str(csv_file2), "users")

    assert result == "conflict"

    rows = conn.execute("SELECT id, name FROM users ORDER BY id").fetchall()
    assert rows == [(1, "alice")]

    conn.close()