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
            normalized = col.lower()
            if normalized not in valid_columns:
                return {"valid": False, "error": f"Unknown column: {col}"}

        return {"valid": True, "error": None}