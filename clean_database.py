#!/usr/bin/env python3
"""
Clean the database by removing all data
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def clean_database():
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
        
        print("🧹 Cleaning database...")
        print("=" * 60)
        
        # Get all tables in the public schema
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        
        total_deleted = 0
        
        for table in tables:
            table_name = table['table_name']
            
            # Count rows before deletion
            cursor.execute(f"SELECT COUNT(*) as row_count FROM {table_name}")
            result = cursor.fetchone()
            row_count = result['row_count']
            
            if row_count > 0:
                # Delete all rows from the table
                cursor.execute(f"DELETE FROM {table_name}")
                deleted_rows = cursor.rowcount
                total_deleted += deleted_rows
                print(f"🗑️  {table_name}: Deleted {deleted_rows} rows")
            else:
                print(f"✅ {table_name}: Already empty")
        
        # Commit the changes
        conn.commit()
        
        print("=" * 60)
        print(f"✅ Database cleaning completed!")
        print(f"   Total rows deleted: {total_deleted}")
        
        # Verify the database is now empty
        print("\n🔍 Verifying database is now empty...")
        cursor.execute("""
            SELECT table_name, 
                   (SELECT COUNT(*) FROM information_schema.tables t2 WHERE t2.table_name = t1.table_name) as row_count
            FROM information_schema.tables t1
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        
        # Actually count rows in each table
        total_rows = 0
        for table in tables:
            table_name = table['table_name']
            cursor.execute(f"SELECT COUNT(*) as row_count FROM {table_name}")
            result = cursor.fetchone()
            row_count = result['row_count']
            if row_count > 0:
                print(f"⚠️  {table_name}: Still has {row_count} rows")
                total_rows += row_count
            else:
                print(f"✅ {table_name}: Empty")
        
        if total_rows == 0:
            print("\n🎉 Database is now CLEAN and EMPTY!")
        else:
            print(f"\n⚠️  Database still contains {total_rows} total rows")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error cleaning database: {e}")

if __name__ == "__main__":
    clean_database()
