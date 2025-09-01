#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(
    host='fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com',
    port=5432,
    dbname='fullbay_data',
    user='postgres',
    password='5255Tillman'
)

cursor = conn.cursor(cursor_factory=RealDictCursor)
cursor.execute("""
    SELECT column_name, data_type, is_nullable, ordinal_position 
    FROM information_schema.columns 
    WHERE table_name = 'fullbay_line_items' 
    AND table_schema = 'public' 
    ORDER BY ordinal_position
""")

columns = cursor.fetchall()

print("EXACT DATABASE SCHEMA - fullbay_line_items TABLE")
print("=" * 80)
print(f"{'Position':<8} {'Column Name':<35} {'Data Type':<20} {'Nullable'}")
print("-" * 80)

for col in columns:
    print(f"{col['ordinal_position']:<8} {col['column_name']:<35} {col['data_type']:<20} {col['is_nullable']}")

print(f"\nTotal columns: {len(columns)}")
conn.close()
