import os


class LLMAdapter:
    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY")

    def generate_sql(self, user_question: str, schema_info: str) -> str:
        """
        Generate SQL from a natural language question.

        If OPENAI_API_KEY is available, use the OpenAI API.
        Otherwise, fall back to local rule-based logic.
        """
        if not user_question or not user_question.strip():
            raise ValueError("User question cannot be empty")

        if not schema_info or not schema_info.strip():
            raise ValueError("Schema info cannot be empty")

        if self.api_key:
            return self._generate_sql_with_openai(user_question, schema_info)

        return self._generate_sql_rule_based(user_question, schema_info)

    def _generate_sql_rule_based(self, user_question: str, schema_info: str) -> str:
        question = user_question.strip().lower()
        table_name = self._extract_table_name(question, schema_info)

        if "show all" in question or "list all" in question:
            return f"SELECT * FROM {table_name}"

        if "how many" in question or "count" in question:
            return f"SELECT COUNT(*) FROM {table_name}"

        raise ValueError("Unable to generate SQL from question")

    def _generate_sql_with_openai(self, user_question: str, schema_info: str) -> str:
        try:
            from openai import OpenAI
        except ImportError as e:
            raise RuntimeError(
                "OpenAI package not installed. Run: pip install openai"
            ) from e

        client = OpenAI(api_key=self.api_key)

        prompt = f"""
You are an assistant that converts natural language to SQLite SQL.

Database tables:
{schema_info}

User request:
{user_question}

Rules:
- Return only one SQL query
- Use SQLite syntax
- Only generate SELECT queries
- Do not include explanation
"""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Convert natural language into safe SQLite SELECT queries."},
                {"role": "user", "content": prompt},
            ],
        )

        sql = response.choices[0].message.content.strip()

        if sql.startswith("```"):
            sql = sql.strip("`")
            sql = sql.replace("sql", "", 1).strip()

        return sql

    def _extract_table_name(self, question: str, schema_info: str) -> str:
        tables = [table.strip() for table in schema_info.split(",") if table.strip()]

        for table in tables:
            if table.lower() in question:
                return table

        return tables[0]