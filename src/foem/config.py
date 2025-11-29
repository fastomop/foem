import os
from dotenv import load_dotenv
from urllib.parse import urlparse
import psycopg2
from databricks import sql

load_dotenv()

def get_db_connection():
    """
    Get database connection based on DB_TYPE environment variable.

    Supported databases:
    - PostgreSQL (default): DB_TYPE=postgresql or not set
    - Databricks: DB_TYPE=databricks

    Returns:
        Database connection object
    """
    db_type = os.getenv("DB_TYPE", "postgresql").lower()

    if db_type == "databricks":
        return _get_databricks_connection()
    else:
        return _get_postgresql_connection()


def _get_postgresql_connection():
    """Get PostgreSQL connection using psycopg2."""

    db_url = os.getenv("DB_CONNECTION_STRING")
    parsed = urlparse(db_url)

    # Get statement timeout from env (default: 5 minutes)
    statement_timeout = int(os.getenv("DB_STATEMENT_TIMEOUT", "300000"))  # milliseconds

    conn = psycopg2.connect(
        dbname=parsed.path[1:],
        user=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port,
        connect_timeout=10,  # 10 second connection timeout
        gssencmode='disable',  # Disable GSSAPI/Kerberos authentication
    )

    # Set search path to omop schema and statement timeout
    with conn.cursor() as cur:
        cur.execute("SET search_path TO omop, public;")
        cur.execute(f"SET statement_timeout = {statement_timeout};")  # Prevent queries from hanging indefinitely
    conn.commit()

    return conn


def _get_databricks_connection():
    """
    Get Databricks connection using databricks-sql-connector.

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

    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
        catalog=catalog,
        schema=schema
    )