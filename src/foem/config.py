import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()

def get_db_connection() -> Engine:
    """
    Get database connection based on DB_TYPE environment variable.

    Supported databases:
    - PostgreSQL (default): DB_TYPE=postgresql or not set
    - Databricks: DB_TYPE=databricks

    Returns:
        SQLAlchemy Engine object
    """
    db_type = os.getenv("DB_TYPE", "postgresql").lower()

    if db_type == "databricks":
        return _get_databricks_engine()
    else:
        return _get_postgresql_engine()


def _get_postgresql_engine() -> Engine:
    """Get PostgreSQL engine using SQLAlchemy."""
    db_url = os.getenv("DB_CONNECTION_STRING")
    return create_engine(db_url)


def _get_databricks_engine() -> Engine:
    """
    Get Databricks engine using SQLAlchemy with databricks-sql-connector.

    Environment variables required:
    - DATABRICKS_SERVER_HOSTNAME: Databricks workspace hostname
    - DATABRICKS_HTTP_PATH: SQL warehouse HTTP path
    - DATABRICKS_ACCESS_TOKEN: Personal access token

    Optional:
    - DATABRICKS_CATALOG: Catalog name (default: main)
    - DATABRICKS_SCHEMA: Schema name (default: default)
    """

    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "default")

    if not all([server_hostname, http_path, access_token]):
        raise ValueError(
            "Databricks connection requires: DATABRICKS_SERVER_HOSTNAME, "
            "DATABRICKS_HTTP_PATH, and DATABRICKS_ACCESS_TOKEN environment variables"
        )

    # Construct Databricks SQLAlchemy connection string
    connection_string = (
        f"databricks://token:{access_token}@{server_hostname}?"
        f"http_path={http_path}&catalog={catalog}&schema={schema}"
    )

    return create_engine(connection_string)