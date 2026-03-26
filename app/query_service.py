class QueryService:
    def __init__(self, conn, schema_manager):
        self.conn = conn
        self.schema_manager = schema_manager

    def execute_sql(self, sql: str):
        """
        Execute SQL safely and return results.
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        return {
            "columns": columns,
            "rows": rows,
        }

    def list_tables(self):
        """
        Return all tables in database.
        """
        return self.schema_manager.get_existing_tables()

    def get_schema(self, table_name: str):
        """
        Return schema for a table.
        """
        return self.schema_manager.get_table_schema(table_name)