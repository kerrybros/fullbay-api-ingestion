#!/usr/bin/env python3
"""
Simple database verification script
"""

import psycopg2

def verify_database():
    try:
        # Connect to the new database
        conn = psycopg2.connect(
            host='fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com',
            port=5432,
            dbname='fullbay_data',
            user='postgres',
            password='5255Tillman'
        )
        
        cursor = conn.cursor()
        
        # Check current database
        cursor.execute("SELECT current_database();")
        db_name = cursor.fetchone()[0]
        print(f"✅ Connected to database: {db_name}")
        
        # Check table count
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
        table_count = cursor.fetchone()[0]
        print(f"✅ Tables in database: {table_count}")
        
        # Check function count
        cursor.execute("SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema = 'public';")
        function_count = cursor.fetchone()[0]
        print(f"✅ Functions in database: {function_count}")
        
        conn.close()
        print("✅ Database verification completed successfully!")
        
    except Exception as e:
        print(f"❌ Database verification failed: {e}")

if __name__ == "__main__":
    verify_database()
