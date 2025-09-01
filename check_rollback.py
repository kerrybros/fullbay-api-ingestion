#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import RealDictCursor

try:
    conn = psycopg2.connect(
        host='fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com',
        port=5432,
        dbname='fullbay_data',
        user='postgres',
        password='5255Tillman'
    )
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Check column count
    cursor.execute("""
        SELECT COUNT(*) as column_count 
        FROM information_schema.columns 
        WHERE table_name = 'fullbay_line_items' 
        AND table_schema = 'public'
    """)
    
    column_count = cursor.fetchone()['column_count']
    print(f"Current column count: {column_count}")
    
    # Check row count
    cursor.execute("SELECT COUNT(*) as row_count FROM fullbay_line_items")
    row_count = cursor.fetchone()['row_count']
    print(f"Current row count: {row_count}")
    
    # Show first few columns
    cursor.execute("""
        SELECT column_name, ordinal_position
        FROM information_schema.columns 
        WHERE table_name = 'fullbay_line_items' 
        AND table_schema = 'public'
        ORDER BY ordinal_position
        LIMIT 10
    """)
    
    columns = cursor.fetchall()
    print("\nFirst 10 columns:")
    for col in columns:
        print(f"  {col['ordinal_position']}. {col['column_name']}")
    
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
