class QueryService:
    def __init__(self, conn, schema_manager, validator):
        self.conn = conn
        self.schema_manager = schema_manager
        self.validator = validator

    def execute_sql(self, sql: str):
        """
        Validate SQL before execution.
        Return either query results or an error.
        """
        validation_result = self.validator.validate(sql)

        if not validation_result["valid"]:
            return {
                "success": False,
                "error": validation_result["error"],
                "columns": [],
                "rows": [],
            }

        cursor = self.conn.cursor()
        cursor.execute(sql)

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        return {
            "success": True,
            "error": None,
            "columns": columns,
            "rows": rows,
        }

    def list_tables(self):
        """
        Return all tables in the database.
        """
        return self.schema_manager.get_existing_tables()

    def get_schema(self, table_name: str):
        """
        Return schema for a given table.
        """
        return self.schema_manager.get_table_schema(table_name)