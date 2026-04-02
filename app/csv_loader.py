import pandas as pd

from app.schema_manager import SchemaManager


class CSVLoader:
    def __init__(self, conn):
        self.conn = conn
        self.schema_manager = SchemaManager(conn)

    def insert_rows(self, table_name: str, df: pd.DataFrame) -> None:
        """
        Manually insert rows into SQLite without using df.to_sql().
        """
        columns = [self.schema_manager.normalize_column_name(col) for col in df.columns]

        placeholders = ", ".join(["?"] * len(columns))
        column_sql = ", ".join(columns)

        insert_sql = f"""
        INSERT INTO {table_name} ({column_sql})
        VALUES ({placeholders})
        """

        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

        self.conn.executemany(insert_sql, rows)
        self.conn.commit()

    def load_csv(self, csv_path: str, table_name: str) -> str:
        """
        Load CSV into SQLite.

        Returns:
        - 'created'
        - 'appended'
        - 'conflict'
        """

        df = pd.read_csv(csv_path)
        inferred_schema = self.schema_manager.infer_schema_from_dataframe(df)

        decision = self.schema_manager.decide_create_or_append(
            table_name,
            inferred_schema,
        )

        if decision == "create":
            self.schema_manager.create_table(table_name, inferred_schema)
            self.insert_rows(table_name, df)
            return "created"

        if decision == "append":
            self.insert_rows(table_name, df)
            return "appended"

        return "conflict"