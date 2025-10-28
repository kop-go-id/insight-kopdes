#!/usr/bin/env bash
# extract_schema.sh
# Extract all tables and columns from the PostgreSQL public schema and produce a JSON file.
# Output format:
# {
#   "tables": {
#     "table_name": {
#       "description": "",
#       "columns": [
#         {"name": "col1", "type": "text", "description": ""},
#         ...
#       ]
#     },
#     ...
#   }
# }
#
# Usage:
#   ./extract_schema.sh                # writes schema.json in current dir
#   ./extract_schema.sh out.json       # write to out.json
#
# Requirements:
#  - python3
#  - sqlalchemy and a DB driver for PostgreSQL (e.g. psycopg2-binary)
#    Install with: pip install sqlalchemy psycopg2-binary
#  - A DATABASE_URL environment variable (e.g. postgres://user:pass@host:5432/dbname)
#    or a .env file containing DATABASE_URL="postgres://..."
#
# The script will try to read DATABASE_URL from the environment first, then from a .env file.

set -euo pipefail

OUT_FILE="${1:-schema.json}"
ENV_FILE=".env"

# Load DATABASE_URL from environment if present
if [ -z "${DATABASE_URL:-}" ] ; then
  if [ -f "$ENV_FILE" ]; then
    # Try to parse DATABASE_URL from .env (handles quoted values)
    # This extracts the value after the first '=' on the DATABASE_URL line and strips surrounding quotes.
    _val="$(grep -E '^DATABASE_URL=' "$ENV_FILE" | head -n 1 | cut -d '=' -f 2- || true)"
    if [ -n "$_val" ]; then
      # Remove surrounding single/double quotes if present
      _val="${_val%\"}"
      _val="${_val#\"}"
      _val="${_val%\'}"
      _val="${_val#\'}"
      export DATABASE_URL="$_val"
    fi
  fi
fi

if [ -z "${DATABASE_URL:-}" ]; then
  echo "ERROR: DATABASE_URL is not set in environment and not found in $ENV_FILE" >&2
  echo "Please set DATABASE_URL or put DATABASE_URL in $ENV_FILE" >&2
  exit 2
fi

# Run an embedded Python snippet that uses SQLAlchemy inspector to build JSON
python3 - <<'PYTHON' > "$OUT_FILE"
import os
import json
import sys

db_url = os.environ.get("DATABASE_URL")
if not db_url:
    print("DATABASE_URL not set", file=sys.stderr)
    sys.exit(2)

try:
    # Lazy import; if missing, provide helpful error
    from sqlalchemy import create_engine, inspect
except Exception as e:
    print("Python dependency missing: sqlalchemy (and a DB driver like psycopg2-binary).", file=sys.stderr)
    print("Install with: pip install sqlalchemy psycopg2-binary", file=sys.stderr)
    raise

try:
    engine = create_engine(db_url, pool_pre_ping=True, future=True)
    inspector = inspect(engine)
    # get tables in public schema (adjust if you need other schemas)
    tables = inspector.get_table_names(schema="public")
    schema = {"tables": {}}
    for t in tables:
        try:
            cols = inspector.get_columns(t, schema="public")
        except Exception:
            # fallback: try without schema
            cols = inspector.get_columns(t)
        col_list = []
        for c in cols:
            # The inspector's column dict usually contains 'name' and 'type'
            cname = c.get("name")
            ctype = c.get("type")
            # convert SQLAlchemy type object to string if necessary
            try:
                ctype_str = str(ctype)
            except Exception:
                ctype_str = "unknown"
            col_list.append({"name": cname, "type": ctype_str, "description": ""})
        schema["tables"][t] = {"description": "", "columns": col_list}
    # Write pretty JSON
    json.dump(schema, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
except Exception as e:
    print("Error while connecting/querying the database:", file=sys.stderr)
    raise
PYTHON

echo "Wrote schema to $OUT_FILE"