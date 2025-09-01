#!/usr/bin/env python3
"""
Script to check the actual database schema for the fullbay_line_items table.
"""

import sys
import os

# Set default environment variables for local testing
os.environ.setdefault('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', '5255Tillman')
os.environ.setdefault('DB_NAME', 'fullbay_data')
os.environ.setdefault('DB_PORT', '5432')

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import psycopg2
from psycopg2.extras import RealDictCursor

def main():
    print("🔍 Checking Database Schema")
    print("=" * 40)
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            host=os.environ.get('DB_HOST'),
            port=os.environ.get('DB_PORT'),
            database=os.environ.get('DB_NAME'),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD')
        )
        
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Check if the table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'fullbay_line_items'
                );
            """)
            
            table_exists = cursor.fetchone()['exists']
            print(f"📋 Table fullbay_line_items exists: {table_exists}")
            
            if table_exists:
                # Get table schema
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'fullbay_line_items'
                    ORDER BY ordinal_position;
                """)
                
                columns = cursor.fetchall()
                print(f"\n📊 Table schema ({len(columns)} columns):")
                for col in columns:
                    nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                    default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
                    print(f"  - {col['column_name']}: {col['data_type']} {nullable}{default}")
            
            # Check raw data table too
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'fullbay_raw_data'
                );
            """)
            
            raw_table_exists = cursor.fetchone()['exists']
            print(f"\n📋 Table fullbay_raw_data exists: {raw_table_exists}")
            
            if raw_table_exists:
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'fullbay_raw_data'
                    ORDER BY ordinal_position;
                """)
                
                raw_columns = cursor.fetchall()
                print(f"\n📊 Raw data table schema ({len(raw_columns)} columns):")
                for col in raw_columns:
                    nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                    default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
                    print(f"  - {col['column_name']}: {col['data_type']} {nullable}{default}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
