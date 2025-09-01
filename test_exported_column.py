#!/usr/bin/env python3
"""
Test if exported column exists
"""

import psycopg2

def test_exported_column():
    """Test if exported column exists."""
    print("TESTING EXPORTED COLUMN")
    print("=" * 30)
    
    db_params = {
        'host': 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com',
        'port': 5432,
        'dbname': 'fullbay_data',
        'user': 'postgres',
        'password': '5255Tillman'
    }
    
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Try to select the exported column
        try:
            cursor.execute("SELECT exported FROM fullbay_line_items LIMIT 1")
            print("❌ EXPORTED COLUMN STILL EXISTS - Query succeeded")
        except Exception as e:
            if "column" in str(e).lower() and "exported" in str(e).lower():
                print("✅ EXPORTED COLUMN REMOVED - Query failed as expected")
                print(f"   Error: {e}")
            else:
                print(f"❓ Unexpected error: {e}")
        
        # Try to select a renamed column
        try:
            cursor.execute("SELECT service_order FROM fullbay_line_items LIMIT 1")
            print("✅ service_order column exists (renamed)")
        except Exception as e:
            print(f"❌ service_order column not found: {e}")
        
        # Try to select total column
        try:
            cursor.execute("SELECT total FROM fullbay_line_items LIMIT 1")
            print("✅ total column exists (renamed)")
        except Exception as e:
            print(f"❌ total column not found: {e}")
        
        conn.close()
        
    except Exception as e:
        print(f"Connection error: {e}")

if __name__ == "__main__":
    test_exported_column()
