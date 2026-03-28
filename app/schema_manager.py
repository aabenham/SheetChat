import re
from typing import Any


class SchemaManager:
    def __init__(self, conn):
        self.conn = conn

    def normalize_column_name(self, column_name: str) -> str:
        """
        Normalize a column name so comparisons are more reliable.
        Example: 'First Name' -> 'first_name'
        """
        cleaned = column_name.strip().lower()
        cleaned = re.sub(r"\s+", "_", cleaned)
        cleaned = re.sub(r"[^a-z0-9_]", "", cleaned)
        return cleaned

    def infer_sqlite_type(self, series) -> str:
        """
        Infer a SQLite type from a pandas Series.
        """
        dtype = str(series.dtype).lower()

        if "int" in dtype:
            return "INTEGER"
        if "float" in dtype:
            return "REAL"
        return "TEXT"

    def table_exists(self, table_name: str) -> bool:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return cursor.fetchone() is not None

    def get_existing_tables(self) -> list[str]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        rows = cursor.fetchall()
        return [row[0] for row in rows]

    def get_table_schema(self, table_name: str) -> list[dict[str, str]]:
        """
        Return the schema of an existing SQLite table.
        """
        cursor = self.conn.cursor()
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        rows = cursor.fetchall()

        schema = []
        for row in rows:
            schema.append(
                {
                    "name": row[1],
                    "normalized_name": self.normalize_column_name(row[1]),
                    "type": row[2].upper(),
                }
            )

        return schema

    def infer_schema_from_dataframe(self, df) -> list[dict[str, str]]:
        """
        Infer a schema from a pandas DataFrame.
        """
        schema = []

        for column in df.columns:
            schema.append(
                {
                    "name": column,
                    "normalized_name": self.normalize_column_name(column),
                    "type": self.infer_sqlite_type(df[column]),
                }
            )

        return schema

    def schemas_match(
        self,
        inferred_schema: list[dict[str, str]],
        existing_schema: list[dict[str, str]],
    ) -> bool:
        """
        A match means:
        - same number of user-defined columns
        - normalized column names match in order
        - SQLite types match exactly

        Auto-generated primary key 'id' in existing tables is ignored
        if it is not present in the inferred schema.
        """
        filtered_existing = existing_schema

        inferred_has_id = any(col["normalized_name"] == "id" for col in inferred_schema)

        if not inferred_has_id:
            filtered_existing = [
                col for col in existing_schema
                if col["normalized_name"] != "id"
            ]

        if len(inferred_schema) != len(filtered_existing):
            return False

        for inferred_col, existing_col in zip(inferred_schema, filtered_existing):
            if inferred_col["normalized_name"] != existing_col["normalized_name"]:
                return False

            if inferred_col["type"].upper() != existing_col["type"].upper():
                return False

        return True

    def create_table(self, table_name, schema):
        """
        Create table with given schema.
        Automatically adds primary key if not present.
        """

        columns = []

        has_id = any(col["normalized_name"] == "id" for col in schema)

        if not has_id:
            columns.append("id INTEGER PRIMARY KEY AUTOINCREMENT")

        for col in schema:
            name = col["normalized_name"]
            col_type = col["type"]

            if name == "id" and not has_id:
                continue

            columns.append(f"{name} {col_type}")

        column_sql = ", ".join(columns)

        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {column_sql}
        )
        """

        self.conn.execute(query)
        self.conn.commit()

    def decide_create_or_append(self, table_name: str, inferred_schema: list[dict[str, str]]) -> str:
        """
        Return:
        - 'create' if table does not exist
        - 'append' if schema matches
        - 'conflict' if table exists but schema does not match
        """
        if not self.table_exists(table_name):
            return "create"

        existing_schema = self.get_table_schema(table_name)

        if self.schemas_match(inferred_schema, existing_schema):
            return "append"

        return "conflict"