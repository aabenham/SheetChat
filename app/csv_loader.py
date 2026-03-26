import pandas as pd

from app.schema_manager import SchemaManager


class CSVLoader:
    def __init__(self, conn):
        self.conn = conn
        self.schema_manager = SchemaManager(conn)

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
            df.to_sql(table_name, self.conn, if_exists="append", index=False)
            return "created"

        if decision == "append":
            df.to_sql(table_name, self.conn, if_exists="append", index=False)
            return "appended"

        return "conflict"