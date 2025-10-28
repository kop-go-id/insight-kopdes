#!/usr/bin/env python3

# Test our updated validation logic
import re
import sys
import os

# Add the project directory to Python path
sys.path.insert(0, '/Users/zain/Work/KEMENKOP/insight-kopdes')

def test_validation():
    """Test the updated validation logic"""
    
    # Mock schema for testing
    mock_schema = [
        {
            "table": "cooperatives",
            "columns": [
                "cooperative_id BIGINT",
                "name VARCHAR(256)", 
                "villageId BIGINT",
                "provinceId BIGINT"
            ]
        }
    ]
    
    # Test SQL with quoted camelCase columns
    test_sql = 'SELECT COUNT(*) FROM cooperatives c1 WHERE c1."villageId" IN (SELECT c2."villageId" FROM cooperatives c2 GROUP BY c2."villageId" HAVING COUNT(*) >= 2)'
    
    print(f"Testing SQL: {test_sql}")
    
    # Import our validation function
    try:
        from chains.query_chain import enforce_schema_strictly
        
        is_valid, error = enforce_schema_strictly(test_sql, mock_schema)
        print(f"Validation result: Valid={is_valid}, Error={error}")
        
        if is_valid:
            print("✅ SUCCESS: Quoted camelCase columns are now properly validated!")
        else:
            print(f"❌ FAILED: {error}")
            
    except Exception as e:
        print(f"Error importing or running validation: {e}")

if __name__ == "__main__":
    test_validation()