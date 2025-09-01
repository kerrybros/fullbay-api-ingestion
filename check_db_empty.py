#!/usr/bin/env python3
"""
Check if database tables are empty
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def check_database_empty():
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host='fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com',
            port=5432,
            dbname='fullbay_data',
            user='postgres',
            password='5255Tillman'
        )
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get all tables in the public schema
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        
        print("🔍 Checking database tables for data...")
        print("=" * 60)
        
        total_rows = 0
        empty_tables = []
        non_empty_tables = []
        
        for table in tables:
            table_name = table['table_name']
            
            # Count rows in each table
            cursor.execute(f"SELECT COUNT(*) as row_count FROM {table_name}")
            result = cursor.fetchone()
            row_count = result['row_count']
            
            if row_count == 0:
                empty_tables.append(table_name)
                print(f"📭 {table_name}: {row_count} rows (EMPTY)")
            else:
                non_empty_tables.append(table_name)
                print(f"📊 {table_name}: {row_count} rows")
                total_rows += row_count
        
        print("=" * 60)
        print(f"📈 Summary:")
        print(f"   Total tables: {len(tables)}")
        print(f"   Empty tables: {len(empty_tables)}")
        print(f"   Tables with data: {len(non_empty_tables)}")
        print(f"   Total rows across all tables: {total_rows}")
        
        if total_rows == 0:
            print("\n✅ Database is CLEAN and EMPTY!")
        else:
            print(f"\n⚠️  Database contains data in {len(non_empty_tables)} tables:")
            for table in non_empty_tables:
                cursor.execute(f"SELECT COUNT(*) as row_count FROM {table}")
                result = cursor.fetchone()
                print(f"   - {table}: {result['row_count']} rows")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error checking database: {e}")

if __name__ == "__main__":
    check_database_empty()
