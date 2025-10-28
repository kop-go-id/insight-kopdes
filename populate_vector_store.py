#!/usr/bin/env python3
"""
Script untuk populate vector store dengan data dari tables.json
Jalankan script ini sekali untuk upload semua table descriptions ke vector store
"""
import os
import json
import sys
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

def create_table_documents():
    """
    Buat dokumen untuk setiap tabel dari tables.json
    """
    # Load tables.json
    tables_path = "tables.json"
    if not os.path.exists(tables_path):
        print("Error: tables.json tidak ditemukan")
        return []
    
    with open(tables_path, "r", encoding="utf-8") as f:
        schema_data = json.load(f)
    
    tables = schema_data.get("tables", {})
    documents = []
    
    for table_name, table_info in tables.items():
        description = table_info.get("description", "")
        columns = table_info.get("columns", [])
        
        # Build document content
        content = f"""Table: {table_name}
Description: {description}

Columns:"""
        
        for col in columns[:10]:  # Limit columns to prevent too long content
            col_name = col.get("name", "")
            col_type = col.get("type", "")
            col_desc = col.get("description", "")
            content += f"\n- {col_name} ({col_type}): {col_desc}"
        
        # Add use cases and keywords
        keywords = generate_keywords(table_name, description)
        content += f"\n\nKeywords: {', '.join(keywords)}"
        content += f"\nUse cases: {generate_use_cases(table_name, description)}"
        
        documents.append({
            "content": content,
            "metadata": {
                "table_name": table_name,
                "category": categorize_table(table_name),
                "column_count": len(columns)
            }
        })
    
    return documents

def generate_keywords(table_name, description):
    """Generate keywords untuk search"""
    keywords = [table_name]
    
    # Add Indonesian translations
    if "cooperative" in table_name.lower():
        keywords.extend(["koperasi", "cooperative", "coop"])
    if "province" in table_name.lower():
        keywords.extend(["provinsi", "province", "daerah", "wilayah"])
    if "user" in table_name.lower():
        keywords.extend(["user", "pengguna", "anggota"])
    if "district" in table_name.lower():
        keywords.extend(["kabupaten", "kota", "district"])
    if "village" in table_name.lower():
        keywords.extend(["desa", "kelurahan", "village"])
    
    # Add from description
    desc_words = description.lower().split()
    for word in ["koperasi", "provinsi", "daerah", "user", "pengguna"]:
        if word in desc_words:
            keywords.append(word)
    
    return list(set(keywords))

def generate_use_cases(table_name, description):
    """Generate use cases berdasarkan nama tabel"""
    if "cooperative" in table_name.lower():
        return "menghitung jumlah koperasi, daftar koperasi, statistik koperasi, analisis koperasi per wilayah"
    elif "province" in table_name.lower():
        return "analisis per provinsi, join dengan data koperasi, statistik wilayah"
    elif "user" in table_name.lower():
        return "manajemen pengguna, statistik pengguna, analisis user"
    else:
        return "query data, analisis statistik, join dengan tabel lain"

def categorize_table(table_name):
    """Kategorikan tabel"""
    if table_name in ["cooperatives", "provinces", "districts", "users"]:
        return "core"
    elif "cooperative" in table_name:
        return "cooperative"
    elif any(word in table_name for word in ["province", "district", "village"]):
        return "geography"
    else:
        return "other"

def upload_to_vector_store():
    """Upload documents ke OpenAI vector store"""
    client = OpenAI()
    vector_store_id = os.getenv("VECTOR_STORE_ID", "vs_6900326ca24c81919d91d70dbbcaee09")
    
    print("Creating table documents...")
    documents = create_table_documents()
    print(f"Created {len(documents)} documents")
    
    # Save documents as temp files and upload
    uploaded_files = []
    temp_dir = "temp_vector_docs"
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        for i, doc in enumerate(documents):
            # Save to temp file
            filename = f"{temp_dir}/table_{doc['metadata']['table_name']}.txt"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(doc["content"])
            
            # Upload to OpenAI
            print(f"Uploading {doc['metadata']['table_name']}...")
            
            with open(filename, "rb") as f:
                file_obj = client.files.create(
                    file=f,
                    purpose="assistants"
                )
            
            # Add to vector store
            client.beta.vector_stores.files.create(
                vector_store_id=vector_store_id,
                file_id=file_obj.id
            )
            
            uploaded_files.append({
                "table": doc['metadata']['table_name'],
                "file_id": file_obj.id
            })
        
        print(f"\nSuccessfully uploaded {len(uploaded_files)} files to vector store!")
        print(f"Vector Store ID: {vector_store_id}")
        
        # Clean up temp files
        import shutil
        shutil.rmtree(temp_dir)
        
        return uploaded_files
        
    except Exception as e:
        print(f"Error uploading to vector store: {e}")
        # Clean up temp files
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        return []

def test_vector_store():
    """Test search vector store"""
    client = OpenAI()
    vector_store_id = os.getenv("VECTOR_STORE_ID", "vs_6900326ca24c81919d91d70dbbcaee09")
    
    try:
        # List files in vector store
        files = client.beta.vector_stores.files.list(vector_store_id)
        print(f"\nVector store contains {len(files.data)} files")
        
        # Test search with assistant
        test_questions = [
            "ada berapa koperasi sekarang",
            "daftar provinsi",
            "berapa user aktif"
        ]
        
        for question in test_questions:
            print(f"\nTesting: {question}")
            # This would require implementing the full search logic
            # For now, just show that we can access the vector store
            
    except Exception as e:
        print(f"Error testing vector store: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "upload":
        print("Uploading tables to vector store...")
        upload_to_vector_store()
    elif len(sys.argv) > 1 and sys.argv[1] == "test":
        print("Testing vector store...")
        test_vector_store()
    else:
        print("Usage:")
        print("  python populate_vector_store.py upload  # Upload tables to vector store")
        print("  python populate_vector_store.py test    # Test vector store")