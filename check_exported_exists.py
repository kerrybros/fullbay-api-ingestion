#!/usr/bin/env python3
"""
Check if the exported column still exists
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def check_exported_exists():
    """Check if the exported column still exists."""
    print("CHECKING IF EXPORTED COLUMN EXISTS")
    print("=" * 40)
    
    db_params = {
        'host': 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com',
        'port': 5432,
        'dbname': 'fullbay_data',
        'user': 'postgres',
        'password': '5255Tillman'
    }
    
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check if exported column exists
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND column_name = 'exported'
        """)
        
        result = cursor.fetchone()
        
        if result:
            print("❌ EXPORTED COLUMN STILL EXISTS")
            print(f"   Column found: {result['column_name']}")
        else:
            print("✅ EXPORTED COLUMN SUCCESSFULLY REMOVED")
        
        # Also check a few other columns that should be removed
        columns_to_check = [
            'so_total_parts_cost',
            'so_total_parts_price',
            'quickbooks_item',
            'exported'
        ]
        
        print(f"\n🔍 CHECKING OTHER REMOVED COLUMNS:")
        for col in columns_to_check:
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'fullbay_line_items' 
                AND column_name = '{col}'
            """)
            result = cursor.fetchone()
            if result:
                print(f"   ❌ {col} - STILL EXISTS")
            else:
                print(f"   ✅ {col} - REMOVED")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    check_exported_exists()
