# SheetChat

SheetChat is a command-line tool that allows users to query CSV files
using natural language. The system converts natural language into SQL
using an LLM, validates the SQL query, and executes it safely against an
SQLite database.

------------------------------------------------------------------------

# System Overview

SheetChat allows users to:

-   Load CSV files into a database
-   Query data using SQL
-   Query data using natural language
-   Validate SQL before execution
-   Execute safe queries

Example:

    load data/users.csv users
    show users older than 25
    what is the average age
    count users

------------------------------------------------------------------------

# Architecture

Components:

-   CLI
-   Query Service
-   LLM Adapter
-   SQL Validator
-   Schema Manager
-   CSV Loader
-   SQLite Database

System Flow:

User Input\
↓\
CLI\
↓\
Query Service\
↓\
LLM Adapter\
↓\
SQL Validator\
↓\
SQLite\
↓\
Results

------------------------------------------------------------------------

# How to Run

Clone repository:

    git clone https://github.com/aabenham/SheetChat.git
    cd SheetChat

Create virtual environment:

    python -m venv venv
    source venv/bin/activate

Install dependencies:

    pip install pandas pytest

Run CLI:

    python -m app.cli

------------------------------------------------------------------------

# Example Usage

    load data/products_test.csv products
    show all products
    what is the average price
    count products

------------------------------------------------------------------------

# Running Tests

    pytest

GitHub Actions runs tests on every push.

------------------------------------------------------------------------

# Testing Strategy

Unit tests for:

-   CLI
-   CSV Loader
-   Query Service
-   LLM Adapter
-   Schema Manager
-   SQL Validator

------------------------------------------------------------------------

# LLM Integration

The LLM is used only to generate SQL.

System workflow:

1.  Generate SQL
2.  Validate SQL
3.  Execute SQL

------------------------------------------------------------------------

# Limitations

-   No JOIN support
-   Limited SQL parsing

------------------------------------------------------------------------

# Design Justification

The system was designed using modular components:

-   CLI
-   Query Service
-   LLM Adapter
-   SQL Validator
-   Schema Manager
-   CSV Loader

This separation improves:

-   Testability
-   Maintainability
-   Debugging
-   Extensibility

Each module has a single responsibility and communicates through well-defined interfaces.

The LLM is used only for SQL generation, not execution.

This design ensures:

-   Safer execution
-   Better validation
-   Reduced reliance on LLM correctness

All LLM output is validated before execution.

The validator ensures:

-   Only SELECT queries are allowed
-   Tables exist
-   Columns exist
-   Aggregates are supported

This protects the system from incorrect or unsafe queries.

SQLite was chosen because:

-   Lightweight
-   No external dependencies
-   Supports in-memory databases
-   Easy schema inspection

This simplifies development and testing.

Each module is tested independently using unit tests.

This allows:

-   Isolated testing
-   Mocking dependencies
-   Easier debugging

GitHub Actions automatically runs tests on every push.

------------------------------------------------------------------------

# Author

Anas Benhamida\
Boston University\
EC530 Software Engineering
