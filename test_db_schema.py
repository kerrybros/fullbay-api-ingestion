#!/usr/bin/env python3
"""
Test to check the actual database schema.
"""

import os
import psycopg2
import psycopg2.extras

# Set up environment variables for testing
os.environ.setdefault('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com')
os.environ.setdefault('DB_PORT', '5432')
os.environ.setdefault('DB_NAME', 'fullbay_data')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', '5255Tillman')

def check_schema():
    """Check the actual database schema."""
    print("🔍 Checking database schema...")
    
    try:
        conn = psycopg2.connect(
            host=os.environ.get('DB_HOST'),
            port=int(os.environ.get('DB_PORT')),
            dbname=os.environ.get('DB_NAME'),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        
        with conn.cursor() as cursor:
            # Check fullbay_raw_data table structure
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'fullbay_raw_data'
                ORDER BY ordinal_position
            """)
            
            raw_data_columns = cursor.fetchall()
            print("\n📋 fullbay_raw_data table columns:")
            for col in raw_data_columns:
                print(f"  - {col['column_name']}: {col['data_type']} ({'NULL' if col['is_nullable'] == 'YES' else 'NOT NULL'})")
            
            # Check fullbay_line_items table structure
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'fullbay_line_items'
                ORDER BY ordinal_position
            """)
            
            line_items_columns = cursor.fetchall()
            print("\n📋 fullbay_line_items table columns:")
            for col in line_items_columns:
                print(f"  - {col['column_name']}: {col['data_type']} ({'NULL' if col['is_nullable'] == 'YES' else 'NOT NULL'})")
            
            # Check if there's any data
            cursor.execute("SELECT COUNT(*) as count FROM fullbay_raw_data")
            raw_count = cursor.fetchone()['count']
            print(f"\n📊 Data counts:")
            print(f"  - fullbay_raw_data: {raw_count} records")
            
            cursor.execute("SELECT COUNT(*) as count FROM fullbay_line_items")
            line_count = cursor.fetchone()['count']
            print(f"  - fullbay_line_items: {line_count} records")
            
            # Check sample data structure
            if raw_count > 0:
                cursor.execute("SELECT * FROM fullbay_raw_data LIMIT 1")
                sample = cursor.fetchone()
                print(f"\n📋 Sample raw_data record keys:")
                if sample and 'raw_json_data' in sample:
                    import json
                    try:
                        data = json.loads(sample['raw_json_data'])
                        print(f"  - JSON keys: {list(data.keys())}")
                    except:
                        print(f"  - Could not parse JSON data")
                else:
                    print(f"  - Available columns: {list(sample.keys()) if sample else 'None'}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Schema check failed: {e}")
        return False

if __name__ == "__main__":
    success = check_schema()
    if success:
        print("\n🎉 Schema check completed!")
    else:
        print("\n❌ Schema check failed!")
