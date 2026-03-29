from app.db import get_connection
from app.query_service import QueryService
from app.schema_manager import SchemaManager
from app.validator import SQLValidator


def print_welcome():
    print("Welcome to SheetChat")
    print("Type a SQL SELECT query, 'tables', 'schema <table>', or 'exit'")


def main():
    conn = get_connection("sheetchat.db")
    schema_manager = SchemaManager(conn)
    validator = SQLValidator(schema_manager)
    query_service = QueryService(conn, schema_manager, validator)

    print_welcome()

    while True:
        user_input = input("SheetChat> ").strip()

        if user_input.lower() == "exit":
            print("Goodbye.")
            break

        if user_input.lower() == "tables":
            tables = query_service.list_tables()
            if tables:
                print("Tables:")
                for table in tables:
                    print(f"- {table}")
            else:
                print("No tables found.")
            continue

        if user_input.lower().startswith("schema "):
            table_name = user_input[7:].strip()
            schema = query_service.get_schema(table_name)

            if not schema:
                print(f"No schema found for table '{table_name}'.")
            else:
                print(f"Schema for {table_name}:")
                for column in schema:
                    print(f"- {column['normalized_name']} ({column['type']})")
            continue

        result = query_service.execute_sql(user_input)

        if not result["success"]:
            print(f"Error: {result['error']}")
            continue

        if not result["rows"]:
            print("Query succeeded but returned no rows.")
            continue

        print("Columns:", ", ".join(result["columns"]))
        for row in result["rows"]:
            print(row)

    conn.close()


if __name__ == "__main__":
    main()