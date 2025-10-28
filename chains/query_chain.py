import os
import re
import json
import datetime
import decimal
from typing import Dict, Any, List
from langchain_openai import ChatOpenAI
from langchain.schema import SystemMessage, HumanMessage
from sqlalchemy import text
from dotenv import load_dotenv
from openai import OpenAI

from db.connection import fetch_sample_rows, execute_read_query, engine
from chains.summarizer import summarize_for_minister

load_dotenv()

# Configuration
MAX_ROWS = int(os.getenv("MAX_ROWS", "5000"))
SAMPLE_ROWS = int(os.getenv("SAMPLE_ROWS", "2"))
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o")
VECTOR_STORE_ID = os.getenv("VECTOR_STORE_ID", "vs_6900326ca24c81919d91d70dbbcaee09")
SCHEMA_ASSISTANT_ID = os.getenv("SCHEMA_ASSISTANT_ID", "")

# Initialize clients
llm = ChatOpenAI(model=LLM_MODEL, temperature=0)
openai_client = OpenAI()


# Utility: Safe JSON serialization
def safe_serialize(obj):
    """Safely convert non-JSON types (datetime, Decimal, etc.) into strings."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return str(obj)


# Vector Store Functions
def search_relevant_tables(question: str, max_tables: int = 5) -> List[str]:
    """
    Search vector store untuk menemukan tabel yang relevan dengan pertanyaan.
    Returns list of table names yang paling relevan.
    """
    try:
        print(f"[DEBUG] Searching vector store for: {question}")
        
        # Create a thread and add the question as a message
        thread = openai_client.beta.threads.create()
        
        # Add the question as a message to the thread
        openai_client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=question
        )
        
        # Use existing assistant from environment variable
        if not SCHEMA_ASSISTANT_ID:
            raise ValueError("SCHEMA_ASSISTANT_ID environment variable is required")
        
        run = openai_client.beta.threads.runs.create(
            thread_id=thread.id,
            assistant_id=SCHEMA_ASSISTANT_ID
        )
        
        # Wait for completion (simplified - should implement proper polling)
        import time
        while run.status in ['queued', 'in_progress']:
            time.sleep(1)
            run = openai_client.beta.threads.runs.retrieve(
                thread_id=thread.id,
                run_id=run.id
            )
        
        if run.status == 'completed':
            # Get messages from the thread
            messages = openai_client.beta.threads.messages.list(thread_id=thread.id)
            
            # Extract table names from the response
            response_content = messages.data[0].content[0].text.value
            relevant_tables = extract_table_names(response_content)
            
            print(f"[DEBUG] Vector store found tables: {relevant_tables}")
            return relevant_tables[:max_tables]
        else:
            print(f"[DEBUG] Vector store search failed: {run.status}")
            return get_fallback_tables(question)
            
    except Exception as e:
        print(f"[DEBUG] Vector store error: {e}")
        return get_fallback_tables(question)


def extract_table_names(response_content: str) -> List[str]:
    """Extract table names from assistant response"""
    # Look for common table names in the response
    common_tables = [
        "cooperatives", "provinces", "districts", "subdistricts", "villages", "users", 
        "cooperative_types", "klus", "npaks", "institutions", "news", "village_potentials"
    ]
    
    found_tables = []
    response_lower = response_content.lower()
    
    for table in common_tables:
        if table in response_lower:
            found_tables.append(table)
    
    # Auto-include geographical chain for village queries
    if any(geo_table in found_tables for geo_table in ["villages", "village_potentials"]):
        geographical_tables = ["provinces", "districts", "subdistricts", "villages", "village_potentials"]
        for geo_table in geographical_tables:
            if geo_table not in found_tables:
                found_tables.append(geo_table)
    
    return found_tables


def get_fallback_tables(question: str) -> List[str]:
    """Fallback table selection based on keywords when vector store fails"""
    question_lower = question.lower()
    
    # Rule-based fallback
    if any(word in question_lower for word in ["koperasi", "cooperative", "berapa koperasi", "jumlah koperasi"]):
        return ["cooperatives", "provinces"]
    elif any(word in question_lower for word in ["provinsi", "daerah", "wilayah"]):
        return ["provinces", "districts", "cooperatives"]
    elif any(word in question_lower for word in ["user", "pengguna", "anggota"]):
        return ["users", "user_roles"]
    elif any(word in question_lower for word in ["berita", "news"]):
        return ["news"]
    else:
        # Default core tables
        return ["cooperatives", "provinces", "users"]


# Schema Loader (kdmp-tables.json)
def build_schema_summary(relevant_tables: List[str] = None):
    """
    Load schema context strictly from kdmp-tables.json.
    If relevant_tables is provided, load only those tables.
    Otherwise, load all tables (fallback behavior).
    """
    tables_path = os.path.join(os.path.dirname(__file__), "..", "kdmp-tables.json")

    if os.path.exists(tables_path):
        try:
            with open(tables_path, "r", encoding="utf-8") as f:
                schema_data = json.load(f)

            tables = schema_data.get("tables", {})
            summary = []

            # Filter tables if relevant_tables is provided
            tables_to_process = relevant_tables if relevant_tables else tables.keys()
            
            print(f"[DEBUG] Building schema for {len(tables_to_process)} tables: {list(tables_to_process)}")

            for table_name in tables_to_process:
                if table_name not in tables:
                    print(f"[WARNING] Table {table_name} not found in kdmp-tables.json")
                    continue
                    
                meta = tables[table_name]
                desc = meta.get("description", "")
                cols = meta.get("columns", [])
                col_desc = [
                    f"{c['name']} ({c['type']}): {c.get('description', '')}" for c in cols
                ]
                
                # Only fetch samples for relevant tables to reduce DB load
                try:
                    samples = fetch_sample_rows(table_name, SAMPLE_ROWS)
                    safe_samples = [
                        {k: safe_serialize(v) for k, v in r.items()} for r in samples[:SAMPLE_ROWS]
                    ]
                except Exception as e:
                    print(f"[WARNING] Failed to fetch samples for {table_name}: {e}")
                    safe_samples = []

                summary.append({
                    "table": table_name,
                    "description": desc,
                    "columns": col_desc,
                    "sample_rows": safe_samples,
                })

            print(f"[DEBUG] Successfully loaded schema for {len(summary)} tables")
            return summary
        except Exception as e:
            print(f"Failed to load kdmp-tables.json, fallback to DB schema: {e}")

    print("No kdmp-tables.json found — using live DB schema instead")
    return _build_schema_from_db(relevant_tables)


def _build_schema_from_db(relevant_tables: List[str] = None):
    """Fallback: auto-discover schema directly from PostgreSQL."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        )
        all_tables = [r[0] for r in result.fetchall()]
    
    # Filter tables if relevant_tables is provided
    tables_to_process = relevant_tables if relevant_tables else all_tables
    tables_to_process = [t for t in tables_to_process if t in all_tables]  # Ensure table exists

    parts = []
    with engine.connect() as conn:
        for t in tables_to_process:
            try:
                col_query = text("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=:table
                """)
                cols = conn.execute(col_query, {"table": t}).fetchall()
                col_desc = [f"{c[0]} ({c[1]})" for c in cols]
                samples = fetch_sample_rows(t, SAMPLE_ROWS)
                safe_samples = [
                    {k: safe_serialize(v) for k, v in r.items()} for r in samples[:SAMPLE_ROWS]
                ]
                parts.append({
                    "table": t,
                    "columns": col_desc,
                    "sample_rows": safe_samples,
                })
            except Exception as e:
                print(f"Failed to describe {t}: {e}")
    return parts


def build_llm_friendly_schema(schema_summary: list) -> str:
    """
    Build a cleaner, more LLM-friendly representation of the schema.
    This format is easier for LLM to understand and follow.
    """
    schema_parts = []
    
    for table_info in schema_summary:
        table_name = table_info["table"]
        description = table_info.get("description", "")
        columns = table_info.get("columns", [])
        
        # Build table section
        table_section = f"TABEL: {table_name}"
        if description:
            table_section += f"\nDESKRIPSI: {description}"
        
        table_section += "\nKOLOM:"
        for col in columns:
            # Extract column name (before first space/parenthesis)
            col_name = col.split(" ")[0].split("(")[0]
            
            # Check if column name is camelCase (has uppercase letters)
            if any(c.isupper() for c in col_name):
                # Show both quoted and unquoted formats
                table_section += f"\n  - {col_name} (gunakan: \"{col_name}\")"
            else:
                table_section += f"\n  - {col_name}"
        
        # Add sample data if available
        samples = table_info.get("sample_rows", [])
        if samples:
            table_section += f"\nCONTOH DATA: {samples[0] if samples else '{}'}"
        
        table_section += "\n" + "-" * 50
        schema_parts.append(table_section)
    
    return "\n\n".join(schema_parts)


def get_example_queries() -> str:
    """
    Provide good examples of queries based on common tables.
    This helps LLM learn the correct patterns.
    """
    examples = [
        "Contoh query yang BENAR:",
        "1. SELECT name FROM cooperatives LIMIT 10",
        "2. SELECT COUNT(*) FROM cooperatives WHERE created_at > '2023-01-01'",
        "3. SELECT \"provinceId\", COUNT(*) as total FROM cooperatives GROUP BY \"provinceId\"",
        "4. SELECT c.name, p.name as province FROM cooperatives c JOIN provinces p ON c.\"provinceId\" = p.province_id",
        "5. SELECT name FROM users WHERE email LIKE '%@gmail.com'",
        "6. SELECT COUNT(*) FROM cooperatives c1 WHERE c1.\"villageId\" IN (SELECT c2.\"villageId\" FROM cooperatives c2 GROUP BY c2.\"villageId\" HAVING COUNT(*) > 1)",
        "",
        "PENTING - Aturan untuk KOLOM CAMELCASE:",
        "- Kolom dengan camelCase (seperti villageId, provinceId) WAJIB pakai double quotes",
        "- BENAR: c.\"villageId\", c.\"provinceId\", c.\"districtId\"",
        "- SALAH: c.villageId, c.provinceId, c.districtId",
        "",
        "PENTING - Aturan untuk SUBQUERY dan ALIAS:",
        "- Jika menggunakan alias pada tabel utama, WAJIB gunakan alias di subquery juga",
        "- BENAR: SELECT c1.name FROM cooperatives c1 WHERE c1.id IN (SELECT c2.id FROM cooperatives c2 WHERE c2.status = 'active')",
        "- SALAH: SELECT c1.name FROM cooperatives c1 WHERE c1.id IN (SELECT id FROM cooperatives WHERE status = 'active')",
        "",
        "Contoh query yang SALAH:",
        "SALAH: SELECT cooperative_name FROM koperasi (nama tabel/kolom tidak ada)",
        "SALAH: SELECT * FROM coop (singkatan tidak boleh)",
        "SALAH: SELECT name FROM cooperative (bentuk singular, harus 'cooperatives')",
        "SALAH: SELECT c1.name FROM cooperatives c1 WHERE c1.id IN (SELECT id FROM cooperatives) (subquery harus pakai alias)",
        "SALAH: SELECT c.villageId FROM cooperatives c (camelCase harus pakai quotes: c.\"villageId\")",
    ]
    return "\n".join(examples)


# SQL Generation (schema-aware)
def ask_llm_for_sql(question: str) -> Dict[str, Any]:
    """
    Ask LLM to generate a SQL query using vector store to find relevant tables.
    Must return valid JSON: { "sql": "<query>" }.
    """
    # Step 1: Search vector store for relevant tables
    relevant_tables = search_relevant_tables(question, max_tables=5)
    
    # Step 2: Build schema only for relevant tables
    schema_summary = build_schema_summary(relevant_tables)

    # Build cleaner schema format for LLM
    schema_text = build_llm_friendly_schema(schema_summary)
    
    # Build explicit list of valid identifiers
    valid_tables = []
    valid_columns = []
    for table in schema_summary:
        valid_tables.append(table["table"])
        for col in table["columns"]:
            col_name = col.split(" ")[0]
            valid_columns.append(f"{table['table']}.{col_name}")
    
    system_msg = SystemMessage(
        content=(
            "Anda adalah asisten generator SQL PostgreSQL untuk dashboard data pemerintah. "
            "SANGAT PENTING: Anda HANYA boleh menggunakan tabel dan kolom yang PERSIS seperti "
            "yang ada dalam schema di bawah ini. Schema ini sudah difilter berdasarkan pertanyaan Anda. "
            "Jika pertanyaan tidak bisa dijawab dengan schema ini, kembalikan JSON "
            '{\"sql\": \"SELECT \'Data tidak tersedia\'::text as message\"}. '
            "JANGAN PERNAH mengarang, mengira-ngira, atau menebak nama kolom/tabel."
        )
    )

    human_msg = HumanMessage(
        content=f"""
PERTANYAAN USER:
{question}

SCHEMA YANG RELEVAN (dipilih otomatis berdasarkan pertanyaan Anda):
{schema_text}

TABEL YANG TERSEDIA UNTUK PERTANYAAN INI:
{', '.join(valid_tables)}

KOLOM YANG BISA DIGUNAKAN:
{', '.join(valid_columns)}

{get_example_queries()}

ATURAN WAJIB:
1. HANYA gunakan tabel dan kolom yang ADA di schema di atas
2. Schema ini sudah difilter khusus untuk pertanyaan Anda
3. Untuk menghitung koperasi: SELECT COUNT(*) FROM cooperatives
4. Format output: JSON saja {{ "sql": "SELECT ..." }}
5. Jangan tambahkan komentar atau penjelasan di luar JSON
6. WAJIB: Jika pakai alias di main query, SEMUA reference ke tabel HARUS pakai alias yang sama
7. WAJIB: Di subquery, SELALU gunakan alias baru yang berbeda (c1, c2, c3, dst)
8. WAJIB: Kolom camelCase (villageId, provinceId, districtId) HARUS pakai double quotes
9. BENAR: FROM cooperatives c1 WHERE c1.\"villageId\" IN (SELECT c2.\"villageId\" FROM cooperatives c2)
10. SALAH: FROM cooperatives c1 WHERE c1.villageId IN (SELECT villageId FROM cooperatives)

CONTOH UNTUK PERTANYAAN KOPERASI:
- "ada berapa koperasi" → {{ "sql": "SELECT COUNT(*) FROM cooperatives" }}
- "daftar koperasi" → {{ "sql": "SELECT name FROM cooperatives LIMIT 10" }}
- "koperasi per desa" → {{ "sql": "SELECT \\\"villageId\\\", COUNT(*) FROM cooperatives GROUP BY \\\"villageId\\\"" }}
"""
    )

    response = llm.invoke([system_msg, human_msg])
    text = response.content.strip()

    try:
        obj = json.loads(text)
        if "sql" in obj:
            return {"sql": obj["sql"]}
    except json.JSONDecodeError:
        json_sub = re.search(r"\{.*\}", text, re.DOTALL)
        if json_sub:
            try:
                obj = json.loads(json_sub.group(0))
                if "sql" in obj:
                    return {"sql": obj["sql"]}
            except json.JSONDecodeError:
                pass

    raise ValueError("Invalid JSON response from LLM:\n" + text)


# Validation: Basic + Schema enforcement
_SELECT_ONLY_RE = re.compile(r"^\s*SELECT\s", re.IGNORECASE)
_FORBIDDEN_RE = re.compile(
    r";|--|\bINSERT\b|\bUPDATE\b|\bDELETE\b|\bDROP\b|\bALTER\b|\bTRUNCATE\b|\bCREATE\b",
    re.IGNORECASE,
)


def validate_sql(sql: str) -> bool:
    """Ensure SQL is safe and read-only."""
    if not _SELECT_ONLY_RE.search(sql):
        return False
    if _FORBIDDEN_RE.search(sql):
        return False
    return True


def enforce_schema_strictly(sql: str, schema_summary: list) -> tuple[bool, str | None]:
    """
    Ensure that all table and column names used in the SQL exist in kdmp-tables.json.
    Now supports table aliases and more sophisticated SQL parsing.
    Returns (is_valid, offending_identifier)
    """
    # Build comprehensive list of valid identifiers
    valid_tables = set()
    valid_columns = set()
    table_columns = {}  # table_name -> set of columns
    
    for table_info in schema_summary:
        table_name = table_info["table"]
        valid_tables.add(table_name.lower())
        table_columns[table_name.lower()] = set()
        
        for col in table_info["columns"]:
            col_name = col.split(" ")[0].split("(")[0]  # Extract clean column name
            valid_columns.add(col_name.lower())
            table_columns[table_name.lower()].add(col_name.lower())

    # Reserved SQL keywords that should be ignored
    sql_keywords = {
        'select', 'from', 'where', 'join', 'on', 'as', 'and', 'or', 'not', 'in', 'is', 'null',
        'count', 'sum', 'avg', 'max', 'min', 'distinct', 'group', 'by', 'order', 'having',
        'left', 'right', 'inner', 'outer', 'union', 'all', 'exists', 'like', 'between',
        'case', 'when', 'then', 'else', 'end', 'limit', 'offset', 'desc', 'asc',
        'true', 'false', 'cast', 'extract', 'year', 'month', 'day', 'date', 'timestamp',
        'varchar', 'text', 'integer', 'bigint', 'boolean', 'numeric', 'decimal', 'with'
    }
    
    # Extract table aliases from SQL
    table_aliases = extract_table_aliases(sql, valid_tables)
    
    # Find quoted identifiers (preserve case)
    quoted_identifiers = re.findall(r'"([^"]*)"', sql)
    
    # Remove quoted strings (literals) but preserve quoted identifiers
    sql_for_checking = sql
    sql_for_checking = re.sub(r"'[^']*'", '', sql_for_checking)  # Remove string literals
    
    # Find potential unquoted identifiers
    # First remove quoted identifiers to avoid double-checking
    sql_without_quoted = re.sub(r'"[^"]*"', '', sql_for_checking)
    potential_identifiers = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', sql_without_quoted)
    
    # Check quoted identifiers (case-sensitive)
    for identifier in quoted_identifiers:
        # For quoted identifiers, check exact case match
        is_valid_column = False
        for table_info in schema_summary:
            for col in table_info["columns"]:
                col_name = col.split(" ")[0].split("(")[0]  # Extract clean column name
                if col_name == identifier:  # Exact case match
                    is_valid_column = True
                    break
            if is_valid_column:
                break
        
        if not is_valid_column:
            return False, f'quoted_column:"{identifier}"'
    
    # Check unquoted identifiers (case-insensitive)
    for identifier in potential_identifiers:
        identifier_lower = identifier.lower()
        
        # Skip SQL keywords
        if identifier_lower in sql_keywords:
            continue
            
        # Skip common SQL functions
        if identifier_lower in {'now', 'current_date', 'current_timestamp', 'coalesce', 'concat', 'upper', 'lower'}:
            continue
        
        # Skip table aliases
        if identifier_lower in table_aliases:
            continue
            
        # Check if it's a valid table or column
        is_valid_table = identifier_lower in valid_tables
        is_valid_column = identifier_lower in valid_columns
        
        if not (is_valid_table or is_valid_column):
            return False, identifier
    
    # Additional check: verify table names in FROM and JOIN clauses
    from_tables = re.findall(r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
    join_tables = re.findall(r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
    
    for table in from_tables + join_tables:
        if table.lower() not in valid_tables:
            return False, f"table:{table}"
    
    return True, None


def extract_table_aliases(sql: str, valid_tables: set) -> set:
    """
    Extract table aliases from SQL query.
    Examples:
    - FROM cooperatives c -> alias 'c'
    - JOIN provinces p -> alias 'p'
    """
    aliases = set()
    
    # Pattern for table aliases in FROM clause: FROM table_name alias
    from_pattern = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    from_matches = re.findall(from_pattern, sql, re.IGNORECASE)
    
    for table_name, alias in from_matches:
        if table_name.lower() in valid_tables:
            aliases.add(alias.lower())
    
    # Pattern for table aliases in JOIN clause: JOIN table_name alias
    join_pattern = r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    join_matches = re.findall(join_pattern, sql, re.IGNORECASE)
    
    for table_name, alias in join_matches:
        if table_name.lower() in valid_tables:
            aliases.add(alias.lower())
    
    # Pattern for table aliases with AS keyword: FROM table_name AS alias
    as_pattern = r'\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    as_matches = re.findall(as_pattern, sql, re.IGNORECASE)
    
    for table_name, alias in as_matches:
        if table_name.lower() in valid_tables:
            aliases.add(alias.lower())
    
    return aliases


def ask_llm_for_sql_with_retry(question: str, max_retries: int = 2) -> Dict[str, Any]:
    """
    Ask LLM for SQL with retry mechanism and specific error feedback.
    """
    schema_summary = build_schema_summary()
    last_error = None
    
    for attempt in range(max_retries + 1):
        try:
            if attempt == 0:
                # First attempt - normal prompt
                result = ask_llm_for_sql(question)
            else:
                # Retry with error feedback
                result = ask_llm_for_sql_with_feedback(question, last_error, schema_summary)
            
            sql = result["sql"].strip()
            if sql.endswith(";"):
                sql = sql[:-1]
            
            # Validate the SQL
            if not validate_sql(sql):
                last_error = "SQL harus dimulai dengan SELECT dan tidak boleh mengandung operasi modifikasi data"
                continue
                
            ok, invalid_name = enforce_schema_strictly(sql, schema_summary)
            if not ok:
                last_error = f"Nama tabel/kolom '{invalid_name}' tidak ditemukan dalam schema. Gunakan hanya nama yang ada di kdmp-tables.json"
                continue
                
            # If we get here, SQL is valid
            return {"sql": sql}
            
        except Exception as e:
            last_error = f"Error dalam generate SQL: {str(e)}"
            
    # If all retries failed
    return {"sql": "SELECT 'Gagal generate SQL yang valid'::text as error"}


def ask_llm_for_sql_with_feedback(question: str, error_feedback: str, schema_summary: list) -> Dict[str, Any]:
    """
    Ask LLM for SQL with specific error feedback from previous attempt.
    """
    schema_text = build_llm_friendly_schema(schema_summary)
    
    # Build valid identifiers list
    valid_tables = []
    valid_columns = []
    for table in schema_summary:
        valid_tables.append(table["table"])
        for col in table["columns"]:
            col_name = col.split(" ")[0].split("(")[0]
            valid_columns.append(f"{table['table']}.{col_name}")
    
    system_msg = SystemMessage(
        content=(
            "Anda adalah asisten generator SQL PostgreSQL yang HARUS memperbaiki kesalahan. "
            "Percobaan sebelumnya GAGAL. Anda HARUS belajar dari error dan menggunakan "
            "HANYA nama tabel dan kolom yang PERSIS ADA dalam schema."
        )
    )

    human_msg = HumanMessage(
        content=f"""
PERTANYAAN USER:
{question}

ERROR DARI PERCOBAAN SEBELUMNYA:
{error_feedback}

SCHEMA LENGKAP:
{schema_text}

TABEL YANG TERSEDIA:
{', '.join(valid_tables)}

{get_example_queries()}

PERBAIKI ERROR DI ATAS!
- Jika nama tabel/kolom salah, ganti dengan yang ADA di schema
- Jika tidak yakin, pilih tabel/kolom yang paling relevan
- Pelajari contoh query yang benar di atas
- Format: {{ "sql": "SELECT ..." }}
"""
    )

    response = llm.invoke([system_msg, human_msg])
    text = response.content.strip()

    try:
        obj = json.loads(text)
        if "sql" in obj:
            return {"sql": obj["sql"]}
    except json.JSONDecodeError:
        json_sub = re.search(r"\{.*\}", text, re.DOTALL)
        if json_sub:
            try:
                obj = json.loads(json_sub.group(0))
                if "sql" in obj:
                    return {"sql": obj["sql"]}
            except json.JSONDecodeError:
                pass

    raise ValueError("Invalid JSON response from LLM on retry:\n" + text)


# Main Query Pipeline
def run_query_pipeline(question: str, user_id: int | None = None):
    """
    1. Search vector store for relevant tables
    2. Generate SQL via LLM using only relevant schema
    3. Validate SQL safety and schema compliance
    4. Execute query on PostgreSQL
    5. Summarize using actual data
    """
    print(f"[DEBUG] Processing question: {question}")
    
    # Step 1: Get relevant tables from vector store
    relevant_tables = search_relevant_tables(question, max_tables=5)
    
    # Step 2: Generate SQL using filtered schema
    llm_resp = ask_llm_for_sql(question)
    raw_sql = llm_resp["sql"].strip()

    if raw_sql.endswith(";"):
        raw_sql = raw_sql[:-1]

    # Check if this is an error response
    if "Gagal generate SQL yang valid" in raw_sql or "Data tidak tersedia" in raw_sql:
        return {
            "error": "Tidak dapat membuat query yang valid untuk pertanyaan ini", 
            "generated_sql": raw_sql,
            "suggestion": "Coba perbaiki pertanyaan atau pastikan data yang dicari tersedia dalam sistem"
        }

    # Step 3: Validation using the same relevant tables
    schema_summary = build_schema_summary(relevant_tables)
    if not validate_sql(raw_sql):
        return {"error": "Generated SQL failed final validation.", "generated_sql": raw_sql}

    ok, invalid_name = enforce_schema_strictly(raw_sql, schema_summary)
    if not ok:
        return {
            "error": f"Query uses invalid column or table: {invalid_name}",
            "generated_sql": raw_sql,
        }

    # Add debug logging
    print(f"[DEBUG] Generated SQL: {raw_sql}")
    print(f"[DEBUG] Question: {question}")

    try:
        result = execute_read_query(raw_sql, params=None, max_rows=MAX_ROWS)
        print(f"[DEBUG] Query executed successfully, got {len(result['rows'])} rows")
    except Exception as e:
        print(f"[ERROR] Query execution failed: {str(e)}")
        return {
            "error": "Query execution failed",
            "details": str(e),
            "generated_sql": raw_sql,
        }

    rows = result["rows"]
    for row in rows:
        for k, v in row.items():
            row[k] = safe_serialize(v)

    columns = list(result["columns"])
    payload = {
        "type": "table",
        "columns": columns,
        "rows": [list(r.values()) for r in rows],
    }

    summary = summarize_for_minister(question, rows[:5] if rows else [])
    return {
        "text": summary,
        "payload": payload,
        "meta": {"row_count": len(rows), "generated_sql": raw_sql},
    }