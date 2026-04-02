class QueryService:
    def __init__(self, conn, schema_manager, validator, llm_adapter=None):
        self.conn = conn
        self.schema_manager = schema_manager
        self.validator = validator
        self.llm_adapter = llm_adapter

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
                "generated_sql": None,
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
            "generated_sql": None,
        }

    def looks_like_sql(self, user_input: str) -> bool:
        """
        Detect whether the input appears to be SQL.
        """
        if not user_input or not user_input.strip():
            return False

        first_word = user_input.strip().split()[0].lower()
        sql_keywords = {
            "select",
            "delete",
            "insert",
            "update",
            "drop",
            "alter",
            "create",
        }
        return first_word in sql_keywords

    def handle_user_input(self, user_input: str):
        """
        Handle either direct SQL or natural language input.

        If the input looks like SQL, validate and execute it directly.
        Otherwise, try to generate SQL through the LLM adapter.
        """
        if self.looks_like_sql(user_input):
            return self.execute_sql(user_input)

        if self.llm_adapter is None:
            return {
                "success": False,
                "error": "Natural language queries require an LLM adapter",
                "columns": [],
                "rows": [],
                "generated_sql": None,
            }

        try:
            schema_info = ", ".join(self.schema_manager.get_existing_tables())
            generated_sql = self.llm_adapter.generate_sql(user_input, schema_info)
        except Exception as e:
            return {
                "success": False,
                "error": f"LLM generation failed: {e}",
                "columns": [],
                "rows": [],
                "generated_sql": None,
            }

        generated_result = self.execute_sql(generated_sql)
        generated_result["generated_sql"] = generated_sql
        return generated_result

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