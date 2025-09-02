import os
import psycopg2
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()

def get_db_connection():
    db_url = os.getenv("DB_CONNECTION_STRING")
    parsed = urlparse(db_url)

    return psycopg2.connect(
        dbname=parsed.path[1:],
        user=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port,
    )