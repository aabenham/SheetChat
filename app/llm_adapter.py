class LLMAdapter:
    def generate_sql(self, user_question: str, schema_info: str) -> str:
        """
        Generate SQL from a natural language question using simple rule-based logic.

        schema_info should be a comma-separated list of table names, for example:
        "users, orders"
        """
        if not user_question or not user_question.strip():
            raise ValueError("User question cannot be empty")

        if not schema_info or not schema_info.strip():
            raise ValueError("Schema info cannot be empty")

        question = user_question.strip().lower()
        table_name = self._extract_table_name(question, schema_info)

        if "show all" in question or "list all" in question:
            return f"SELECT * FROM {table_name}"

        if "how many" in question or "count" in question:
            return f"SELECT COUNT(*) FROM {table_name}"

        raise ValueError("Unable to generate SQL from question")

    def _extract_table_name(self, question: str, schema_info: str) -> str:
        tables = [table.strip() for table in schema_info.split(",") if table.strip()]

        for table in tables:
            if table.lower() in question:
                return table

        return tables[0]