import re


class SQLValidator:
    def __init__(self, schema_manager):
        self.schema_manager = schema_manager

    def validate(self, sql: str) -> dict:
        if not sql or not sql.strip():
            return {"valid": False, "error": "Query cannot be empty"}

        sql = sql.strip().rstrip(";")

        if not sql.lower().startswith("select"):
            return {"valid": False, "error": "Only SELECT queries are allowed"}

        table_match = re.search(r"\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, re.IGNORECASE)
        if not table_match:
            return {"valid": False, "error": "Could not determine table name"}

        table_name = table_match.group(1)

        existing_tables = self.schema_manager.get_existing_tables()
        if table_name not in existing_tables:
            return {"valid": False, "error": f"Unknown table: {table_name}"}

        schema = self.schema_manager.get_table_schema(table_name)
        valid_columns = {col["normalized_name"] for col in schema}

        select_match = re.search(r"select\s+(.*?)\s+from\b", sql, re.IGNORECASE)
        if not select_match:
            return {"valid": False, "error": "Could not determine selected columns"}

        selected_columns_raw = select_match.group(1).strip()

        if selected_columns_raw == "*":
            return {"valid": True, "error": None}

        selected_columns = [col.strip() for col in selected_columns_raw.split(",")]

        for col in selected_columns:
            if not self._is_valid_selected_expression(col, valid_columns):
                return {"valid": False, "error": f"Unknown column: {col}"}

        return {"valid": True, "error": None}

    def _is_valid_selected_expression(self, expression: str, valid_columns: set[str]) -> bool:
        """
        Allow:
        - plain columns: name
        - aliases: name AS person_name
        - aggregates: AVG(age), COUNT(*), MAX(age)
        - aggregates with aliases: AVG(age) AS average_age
        """
        expression = expression.strip()

        alias_match = re.match(r"(.+?)\s+as\s+[a-zA-Z_][a-zA-Z0-9_]*$", expression, re.IGNORECASE)
        if alias_match:
            expression = alias_match.group(1).strip()

        if expression == "*":
            return True

        if expression.lower() in valid_columns:
            return True

        agg_match = re.match(
            r"^(count|avg|min|max|sum)\((\*|[a-zA-Z_][a-zA-Z0-9_]*)\)$",
            expression,
            re.IGNORECASE,
        )
        if agg_match:
            func_name = agg_match.group(1).lower()
            arg = agg_match.group(2).lower()

            if func_name == "count" and arg == "*":
                return True

            return arg in valid_columns

        return False