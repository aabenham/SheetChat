class SchemaManager:
    def __init__(self, conn):
        self.conn = conn

    def table_exists(self, table_name: str) -> bool:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return cursor.fetchone() is not None

    def get_table_schema(self, table_name: str) -> list[dict]:
        cursor = self.conn.cursor()
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        rows = cursor.fetchall()

        schema = []
        for row in rows:
            schema.append({
                "name": row[1],
                "type": row[2]
            })
        return schema

    def infer_schema_from_dataframe(self, df) -> list[dict]:
        schema = []

        for column in df.columns:
            dtype = str(df[column].dtype)

            if "int" in dtype:
                sql_type = "INTEGER"
            elif "float" in dtype:
                sql_type = "REAL"
            else:
                sql_type = "TEXT"

            schema.append({
                "name": column.strip(),
                "type": sql_type
            })

        return schema

    def create_table(self, table_name: str, schema: list[dict]) -> None:
        columns_sql = []

        for column in schema:
            col_name = column["name"]
            col_type = column["type"]
            columns_sql.append(f'"{col_name}" {col_type}')

        create_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns_sql)})'

        cursor = self.conn.cursor()
        cursor.execute(create_sql)
        self.conn.commit()