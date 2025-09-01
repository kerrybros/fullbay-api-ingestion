#!/usr/bin/env python3
"""
Simple schema check
"""

import psycopg2

def check_schema():
    """Check the database schema."""
    print("CHECKING DATABASE SCHEMA")
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
        
        # Get all columns
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            ORDER BY ordinal_position
        """)
        
        columns = [row[0] for row in cursor.fetchall()]
        
        print(f"Total columns: {len(columns)}")
        
        # Check for exported column
        if 'exported' in columns:
            print("❌ EXPORTED COLUMN STILL EXISTS")
        else:
            print("✅ EXPORTED COLUMN REMOVED")
        
        # Check for other removed columns
        removed_cols = ['so_total_parts_cost', 'quickbooks_item', 'exported']
        for col in removed_cols:
            if col in columns:
                print(f"❌ {col} - STILL EXISTS")
            else:
                print(f"✅ {col} - REMOVED")
        
        # Check for renamed columns
        if 'service_order' in columns:
            print("✅ service_order - RENAMED")
        if 'total' in columns:
            print("✅ total - RENAMED")
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_schema()
