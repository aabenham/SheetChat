from app.csv_loader import CSVLoader
from app.db import get_connection
from app.llm_adapter import LLMAdapter
from app.query_service import QueryService
from app.schema_manager import SchemaManager
from app.validator import SQLValidator


def print_welcome():
    print("Welcome to SheetChat")
    print("Commands:")
    print("- load <csv_path> <table_name>")
    print("- tables")
    print("- schema <table>")
    print("- any SQL SELECT query")
    print("- natural language queries like: show all users")
    print("- exit")


def log_error(message: str):
    with open("error_log.txt", "a") as f:
        f.write(message + "\n")


def handle_load_command(loader: CSVLoader, user_input: str):
    parts = user_input.split(maxsplit=2)

    if len(parts) != 3:
        print("Usage: load <csv_path> <table_name>")
        return

    _, csv_path, table_name = parts

    try:
        result = loader.load_csv(csv_path, table_name)

        if result == "created":
            print(f"Created table '{table_name}' and loaded CSV data.")
            return

        if result == "appended":
            print(f"Appended CSV data to '{table_name}'.")
            return

        if result == "conflict":
            print(f"Schema conflict detected for table '{table_name}'.")
            print("Choose one: overwrite, rename, skip")
            choice = input("Choice> ").strip().lower()

            if choice == "overwrite":
                loader.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                loader.conn.commit()
                retry_result = loader.load_csv(csv_path, table_name)
                print(f"Overwrite complete: {retry_result}")
                return

            if choice == "rename":
                new_table_name = input("New table name> ").strip()
                retry_result = loader.load_csv(csv_path, new_table_name)
                print(f"Loaded CSV into '{new_table_name}': {retry_result}")
                return

            if choice == "skip":
                print("Skipped loading CSV.")
                return

            print("Invalid choice. Skipped loading CSV.")
            return

    except Exception as e:
        message = f"CSV load error: {e}"
        log_error(message)
        print(f"Error: {message}")


def main():
    conn = get_connection("sheetchat.db")
    schema_manager = SchemaManager(conn)
    validator = SQLValidator(schema_manager)
    llm_adapter = LLMAdapter()
    query_service = QueryService(conn, schema_manager, validator, llm_adapter)
    loader = CSVLoader(conn)

    print_welcome()

    while True:
        user_input = input("SheetChat> ").strip()

        if user_input.lower() == "exit":
            print("Goodbye.")
            break

        if user_input.lower().startswith("load "):
            handle_load_command(loader, user_input)
            continue

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

        result = query_service.handle_user_input(user_input)

        if not result["success"]:
            message = f"Query error: {result['error']}"
            log_error(message)
            print(f"Error: {result['error']}")
            continue

        if result["generated_sql"]:
            print(f"Generated SQL: {result['generated_sql']}")

        if not result["rows"]:
            print("Query succeeded but returned no rows.")
            continue

        print("Columns:", ", ".join(result["columns"]))
        for row in result["rows"]:
            print(row)

    conn.close()


if __name__ == "__main__":
    main()