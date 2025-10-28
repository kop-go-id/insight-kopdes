#!/usr/bin/env python3

import json
import requests

def test_query():
    url = "http://localhost:8000/chat"
    question = "berapa jumlah KDKMP saat ini yang 1 desa terdapat 2 atau lebih koperasi"
    
    payload = {
        "question": question
    }
    
    try:
        response = requests.post(url, json=payload)
        result = response.json()
        
        print(f"Question: {question}")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(result, indent=2, ensure_ascii=False)}")
        
        if "generated_sql" in result.get("meta", {}):
            print(f"\nGenerated SQL: {result['meta']['generated_sql']}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_query()