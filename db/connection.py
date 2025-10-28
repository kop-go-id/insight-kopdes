# db/connection.py
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine: Engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def execute_read_query(sql: str, params=None, max_rows: int = 5000):
    """
    Execute a read-only SQL query safely.
    Returns rows as a list of dicts + column metadata.
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        columns = result.keys()
        rows = [dict(zip(columns, r)) for r in result.fetchall()[:max_rows]]
    return {"columns": columns, "rows": rows}

def fetch_sample_rows(table_name: str, limit: int = 2):
    """Retrieve a few sample rows from a given table."""
    query = text(f"SELECT * FROM {table_name} LIMIT :limit")
    with engine.connect() as conn:
        result = conn.execute(query, {"limit": limit})
        cols = result.keys()
        return [dict(zip(cols, row)) for row in result.fetchall()]