#!/usr/bin/env python3
"""
Check what's in the daily_summary table
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def check_daily_summary():
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
        
        # Check the daily_summary table structure
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'daily_summary' 
            AND table_schema = 'public'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        
        print("📋 Daily Summary Table Structure:")
        print("=" * 60)
        for col in columns:
            print(f"   {col['column_name']}: {col['data_type']} ({'NULL' if col['is_nullable'] == 'YES' else 'NOT NULL'})")
        
        print("\n📊 Daily Summary Table Data:")
        print("=" * 60)
        
        # Get all data from daily_summary
        cursor.execute("SELECT * FROM daily_summary ORDER BY summary_date DESC LIMIT 10;")
        rows = cursor.fetchall()
        
        if rows:
            for i, row in enumerate(rows, 1):
                print(f"\nRow {i}:")
                for key, value in row.items():
                    print(f"   {key}: {value}")
        else:
            print("No data found in daily_summary table")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error checking daily_summary: {e}")

if __name__ == "__main__":
    check_daily_summary()
