# Vector Store Integration untuk Insight KOPDES

## Overview

Implementasi vector store OpenAI untuk meningkatkan akurasi LLM dalam generate SQL dengan cara:
- Mencari tabel yang relevan berdasarkan pertanyaan
- Mengurangi prompt size dari 100+ tabel menjadi 3-5 tabel
- Meningkatkan fokus dan akurasi LLM

## Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Environment Variables
Tambahkan ke `.env`:
```bash
VECTOR_STORE_ID="vs_6900326ca24c81919d91d70dbbcaee09"
OPENAI_API_KEY="your-openai-api-key"
```

### 3. Populate Vector Store (One-time)
```bash
python populate_vector_store.py upload
```

## How It Works

### Before (Original Flow):
```
Question → Load ALL kdmp-tables.json → Send 100+ tables to LLM → Generate SQL
```
**Problems:**
- Prompt too long (50k+ tokens)
- LLM overwhelmed with information
- Low accuracy for simple questions

### After (Vector Store Flow):
```
Question → Search Vector Store → Get 3-5 relevant tables → Send to LLM → Generate SQL
```
**Benefits:**
- Smaller prompt (5k tokens)
- Higher accuracy
- Faster response
- Lower cost

## Example

### Question: "ada berapa koperasi sekarang"

**Vector Store Search Result:**
```python
relevant_tables = ["cooperatives", "provinces"]
```

**Schema Sent to LLM:**
```
TABEL: cooperatives
DESKRIPSI: Entitas koperasi utama yang terdaftar dalam sistem
KOLOM:
- cooperative_id
- name  
- created_at
- provinceId

TABEL: provinces
DESKRIPSI: Data master provinsi Indonesia
KOLOM:
- province_id
- name
- code
```

**Expected LLM Response:**
```json
{"sql": "SELECT COUNT(*) FROM cooperatives"}
```

## Files Modified

### Core Changes:
- `chains/query_chain.py` - Main integration
- `requirements.txt` - Added openai dependency
- `.env.example` - Added VECTOR_STORE_ID

### Helper Scripts:
- `populate_vector_store.py` - Upload schema to vector store

### Key Functions Added:
- `search_relevant_tables()` - Search vector store
- `build_schema_summary(relevant_tables)` - Build filtered schema
- `get_fallback_tables()` - Fallback when vector store fails

## Testing

### Test Vector Store Search:
```python
from chains.query_chain import search_relevant_tables

tables = search_relevant_tables("ada berapa koperasi")
print(tables)  # Expected: ["cooperatives", "provinces"]
```

### Test Full Pipeline:
```python
from chains.query_chain import run_query_pipeline

result = run_query_pipeline("ada berapa koperasi sekarang")
print(result)
```

## Vector Store Content Structure

Each table becomes a document:
```
Table: cooperatives
Description: Entitas koperasi utama yang terdaftar dalam sistem

Columns:
- cooperative_id (BIGINT): Kunci utama - pengenal unik untuk setiap koperasi
- name (VARCHAR): Nama resmi koperasi
- created_at (TIMESTAMP): Waktu pembuatan record
...

Keywords: koperasi, cooperative, coop
Use cases: menghitung jumlah koperasi, daftar koperasi, statistik koperasi
```

## Expected Performance

### Metrics:
- **Prompt Size**: 50k → 5k tokens (90% reduction)
- **Accuracy**: 10% → 90%+ for common questions
- **Response Time**: Faster (smaller prompt)
- **Cost**: Lower (fewer tokens)

### Success Cases:
- "ada berapa koperasi" → `SELECT COUNT(*) FROM cooperatives`
- "daftar provinsi" → `SELECT name FROM provinces`
- "koperasi per provinsi" → `SELECT p.name, COUNT(*) FROM cooperatives c JOIN provinces p...`

## Fallback Strategy

If vector store fails:
1. Use rule-based table selection
2. Core tables: cooperatives, provinces, users
3. Keyword matching for specific domains

## Maintenance

### Update Vector Store:
When kdmp-tables.json changes, re-run:
```bash
python populate_vector_store.py upload
```

### Monitor Performance:
Check logs for:
- Vector store search results
- Fallback usage
- LLM accuracy

## Next Steps

1. **Deploy & Test** - Test with real questions
2. **Fine-tune** - Adjust search parameters
3. **Monitor** - Track accuracy improvements
4. **Expand** - Add more sophisticated search logic